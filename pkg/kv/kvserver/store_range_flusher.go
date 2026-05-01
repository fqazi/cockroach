// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"bytes"
	"container/heap"
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/pebble"
)

// rangeContainsTimeseriesData returns true if the range's start key is prefixed
// by the timeseries prefix. Such ranges contain MERGE keys that are
// incompatible with the ScanInternal used by range flush.
func rangeContainsTimeseriesData(startKey roachpb.RKey) bool {
	return bytes.HasPrefix(startKey.AsRawKey(), keys.TimeseriesPrefix)
}

// rangeFlushBytesThreshold is the minimum ApproxStoreLocalBytes for a range to
// be eligible for flushing.
var rangeFlushBytesThreshold = settings.RegisterByteSizeSetting(
	settings.SystemOnly,
	"kv.range_flush.bytes_threshold",
	"minimum approximate store-local bytes for a range to be eligible for range flush",
	2<<20, // 2 MiB
)

// rangeFlushMaxConcurrency is the maximum number of concurrent range flushes
// per store.
var rangeFlushMaxConcurrency = settings.RegisterIntSetting(
	settings.SystemOnly,
	"kv.range_flush.max_concurrency",
	"maximum number of concurrent range flushes per store",
	4,
	settings.PositiveInt,
)

// rangeFlushScanInterval is the interval between periodic scans for eligible
// ranges.
var rangeFlushScanInterval = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"kv.range_flush.scan_interval",
	"interval between periodic scans for ranges eligible for range flush",
	time.Minute,
	settings.PositiveDuration,
)

// rangeFlushExecutor abstracts flush execution for testability.
type rangeFlushExecutor interface {
	executeRangeFlush(ctx context.Context, rangeID roachpb.RangeID) (flushedBytes int64, err error)
}

// rangeFlushItem is an entry in the flush priority queue.
type rangeFlushItem struct {
	rangeID     roachpb.RangeID
	approxBytes int64
	// flushFailedCount is the number of consecutive flush failures. Items with
	// lower failedCount are flushed first; among equal failedCount, larger
	// approxBytes are flushed first.
	flushFailedCount int
	retryAfter       time.Time // earliest time this item can be retried
	index            int       // heap index, maintained by container/heap
}

// rangeFlushQueue implements container/heap as a max-heap ordered by
// (flushFailedCount ASC, retryAfter ASC, approxBytes DESC). The retryAfter
// tiebreaker ensures the heap top is always the next item to become eligible,
// so callers only need to check index 0 rather than scanning the queue.
type rangeFlushQueue []*rangeFlushItem

var _ heap.Interface = (*rangeFlushQueue)(nil)

func (q rangeFlushQueue) Len() int { return len(q) }

func (q rangeFlushQueue) Less(i, j int) bool {
	if q[i].flushFailedCount != q[j].flushFailedCount {
		return q[i].flushFailedCount < q[j].flushFailedCount
	}
	if q[i].retryAfter != q[j].retryAfter {
		// Zero retryAfter (never failed / eligible now) sorts before non-zero.
		if q[i].retryAfter.IsZero() {
			return true
		}
		if q[j].retryAfter.IsZero() {
			return false
		}
		return q[i].retryAfter.Before(q[j].retryAfter)
	}
	return q[i].approxBytes > q[j].approxBytes
}

func (q rangeFlushQueue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].index = i
	q[j].index = j
}

func (q *rangeFlushQueue) Push(x interface{}) {
	item := x.(*rangeFlushItem)
	item.index = len(*q)
	*q = append(*q, item)
}

func (q *rangeFlushQueue) Pop() interface{} {
	old := *q
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	item.index = -1
	*q = old[:n-1]
	return item
}

// rangeFlushBackoff returns the backoff duration for a given failure count.
// Produces: 1s, 2s, 4s, 8s, 16s, 32s, 60s, 60s, ...
//
// TODO(basalt): consider exporting a backoff computation helper from
// pkg/util/retry (its retryIn method is unexported) and using it here.
func rangeFlushBackoff(failedCount int) time.Duration {
	const (
		initialBackoff = time.Second
		maxBackoff     = 60 * time.Second
	)
	if failedCount <= 0 {
		return 0
	}
	shift := failedCount - 1
	if shift > 5 { // 2^6 = 64s > 60s, so cap
		return maxBackoff
	}
	return initialBackoff << uint(shift)
}

// rangeFlushScheduler is a trivial CompactionScheduler paired 1:1 with a
// rangeFlusher. It uses GetAllowedWithoutPermission as its sole concurrency
// gate, and proactively schedules work via tryGrant when flushes complete
// or the allowed count increases.
type rangeFlushScheduler struct {
	// db is set in Register; not protected by mu since it is set strictly
	// before any calls to other methods.
	db pebble.DBForCompaction
	mu struct {
		syncutil.Mutex
		running      int
		unregistered bool
		// granting serializes tryGrant calls from concurrent Done() callers.
		granting bool
	}
}

var _ pebble.CompactionScheduler = (*rangeFlushScheduler)(nil)

func newRangeFlushScheduler() *rangeFlushScheduler {
	return &rangeFlushScheduler{}
}

func (s *rangeFlushScheduler) Register(_ int, db pebble.DBForCompaction) {
	s.db = db
}

func (s *rangeFlushScheduler) Unregister() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.unregistered = true
}

func (s *rangeFlushScheduler) TrySchedule() (bool, pebble.CompactionGrantHandle) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.unregistered || s.mu.running >= s.db.GetAllowedWithoutPermission() {
		return false, nil
	}
	s.mu.running++
	return true, &rangeFlushGrantHandle{scheduler: s}
}

func (s *rangeFlushScheduler) UpdateGetAllowedWithoutPermission() {
	s.tryGrant()
}

// tryGrant attempts to schedule additional flushes when capacity is available.
// It releases s.mu before calling into DBForCompaction (GetWaitingCompaction,
// Schedule) to respect lock ordering (DBForCompaction mutex before scheduler
// mutex). The granting flag serializes concurrent tryGrant calls.
func (s *rangeFlushScheduler) tryGrant() {
	s.mu.Lock()
	if s.mu.unregistered || s.mu.granting {
		s.mu.Unlock()
		return
	}
	s.mu.granting = true
	for {
		if s.mu.running >= s.db.GetAllowedWithoutPermission() {
			break
		}
		s.mu.Unlock()
		waiting, _ := s.db.GetWaitingCompaction()
		if !waiting {
			s.mu.Lock()
			break
		}
		handle := &rangeFlushGrantHandle{scheduler: s}
		accepted := s.db.Schedule(handle)
		s.mu.Lock()
		if !accepted {
			break
		}
		s.mu.running++
	}
	s.mu.granting = false
	s.mu.Unlock()
}

// rangeFlushGrantHandle implements pebble.CompactionGrantHandle for range
// flushes. All methods are no-ops except Done, which decrements the
// scheduler's running count and attempts to schedule more work.
type rangeFlushGrantHandle struct {
	scheduler *rangeFlushScheduler
}

var _ pebble.CompactionGrantHandle = (*rangeFlushGrantHandle)(nil)

func (h *rangeFlushGrantHandle) Started()                                          {}
func (h *rangeFlushGrantHandle) MeasureCPU(pebble.CompactionGoroutineKind)         {}
func (h *rangeFlushGrantHandle) CumulativeStats(pebble.CompactionGrantHandleStats) {}

func (h *rangeFlushGrantHandle) Done() {
	s := h.scheduler
	s.mu.Lock()
	s.mu.running--
	s.mu.Unlock()
	s.tryGrant()
}

// RangeFlusherMetrics is a snapshot of rangeFlusher stats for metrics.
type RangeFlusherMetrics struct {
	BytesQueued  int64
	BytesFlushed int64
}

// rangeFlusher holds state for range flush scheduling. It maintains a
// priority queue of ranges eligible for flushing, ordered by
// (flushFailedCount ASC, approxBytes DESC).
type rangeFlusher struct {
	// scheduler is the CompactionScheduler used to gate flush concurrency.
	// Set once at construction.
	scheduler pebble.CompactionScheduler
	mu        struct {
		syncutil.Mutex
		// queue is a max-heap of ranges eligible for flushing.
		queue rangeFlushQueue
		// queued maps range IDs to their items in queue for O(1) lookup.
		queued map[roachpb.RangeID]*rangeFlushItem
		// flushing is the set of range IDs with in-progress flushes.
		flushing map[roachpb.RangeID]struct{}
		// bytesQueued is the sum of approxBytes for all items in queue.
		bytesQueued int64
		// bytesFlushed is the cumulative bytes flushed successfully.
		bytesFlushed int64
		// sumApproxStoreLocalBytes is the sum of ApproxStoreLocalBytes across
		// all replicas that are initialized, not timeseries, and leaseholder.
		// Updated by scanAndEnqueue.
		sumApproxStoreLocalBytes int64
		// flushesCompleted and flushesFailed track counts since the last
		// periodic stats report.
		flushesCompleted int64
		flushesFailed    int64
	}
	// notifyCh is a buffered(1) channel that wakes the coordinator.
	notifyCh chan struct{}
}

func newRangeFlusher(scheduler pebble.CompactionScheduler) *rangeFlusher {
	rf := &rangeFlusher{
		scheduler: scheduler,
		notifyCh:  make(chan struct{}, 1),
	}
	rf.mu.queued = make(map[roachpb.RangeID]*rangeFlushItem)
	rf.mu.flushing = make(map[roachpb.RangeID]struct{})
	return rf
}

// enqueueLocked adds or updates a range in the queue. Must be called with
// mu held. Does not enqueue ranges that are currently flushing.
func (rf *rangeFlusher) enqueueLocked(rangeID roachpb.RangeID, approxBytes int64) {
	if _, ok := rf.mu.flushing[rangeID]; ok {
		return
	}
	if item, ok := rf.mu.queued[rangeID]; ok {
		rf.mu.bytesQueued += approxBytes - item.approxBytes
		item.approxBytes = approxBytes
		heap.Fix(&rf.mu.queue, item.index)
		return
	}
	item := &rangeFlushItem{
		rangeID:     rangeID,
		approxBytes: approxBytes,
	}
	heap.Push(&rf.mu.queue, item)
	rf.mu.queued[rangeID] = item
	rf.mu.bytesQueued += approxBytes
}

// dequeueLocked removes and returns the highest-priority eligible item.
// Returns nil if the queue is empty or all items are backed off. Must be
// called with mu held.
func (rf *rangeFlusher) dequeueLocked() *rangeFlushItem {
	if rf.mu.queue.Len() == 0 {
		return nil
	}
	top := rf.mu.queue[0]
	if !top.retryAfter.IsZero() && timeutil.Now().Before(top.retryAfter) {
		return nil
	}
	item := heap.Pop(&rf.mu.queue).(*rangeFlushItem)
	delete(rf.mu.queued, item.rangeID)
	rf.mu.bytesQueued -= item.approxBytes
	return item
}

// removeLocked removes a range from the queue if present. Must be called with
// mu held.
func (rf *rangeFlusher) removeLocked(rangeID roachpb.RangeID) {
	item, ok := rf.mu.queued[rangeID]
	if !ok {
		return
	}
	heap.Remove(&rf.mu.queue, item.index)
	delete(rf.mu.queued, rangeID)
	rf.mu.bytesQueued -= item.approxBytes
}

// enqueueAndNotify adds or updates a range in the queue and wakes the
// coordinator.
func (rf *rangeFlusher) enqueueAndNotify(rangeID roachpb.RangeID, approxBytes int64) {
	func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.enqueueLocked(rangeID, approxBytes)
	}()
	rf.maybeNotify()
}

// maybeNotify sends a non-blocking signal to the coordinator.
func (rf *rangeFlusher) maybeNotify() {
	select {
	case rf.notifyCh <- struct{}{}:
	default:
	}
}

// getWaitingCompactionLocked returns whether there is an eligible (non-backed-
// off) item in the queue and the associated WaitingCompaction. Must be called
// with mu held.
func (rf *rangeFlusher) getWaitingCompactionLocked() (bool, pebble.WaitingCompaction) {
	if rf.mu.queue.Len() == 0 {
		return false, pebble.WaitingCompaction{}
	}
	top := rf.mu.queue[0]
	if !top.retryAfter.IsZero() && timeutil.Now().Before(top.retryAfter) {
		return false, pebble.WaitingCompaction{}
	}
	score := 0.75
	if rf.mu.sumApproxStoreLocalBytes > 0 {
		score += 2 * (float64(rf.mu.bytesQueued) / float64(rf.mu.sumApproxStoreLocalBytes))
	}
	// TODO(basalt): export the priority constant from Pebble instead of
	// hardcoding 60.
	return true, pebble.WaitingCompaction{
		Optional: false,
		Priority: 60,
		Score:    score,
	}
}

// nextRetryTimeLocked returns the retryAfter of the heap top, for the
// coordinator to set a wakeup timer. Returns the zero value if the queue is
// empty or the top item is already eligible. Must be called with mu held.
func (rf *rangeFlusher) nextRetryTimeLocked() time.Time {
	if rf.mu.queue.Len() == 0 {
		return time.Time{}
	}
	return rf.mu.queue[0].retryAfter
}

// Metrics returns a snapshot of rangeFlusher stats.
func (rf *rangeFlusher) Metrics() RangeFlusherMetrics {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return RangeFlusherMetrics{
		BytesQueued:  rf.mu.bytesQueued,
		BytesFlushed: rf.mu.bytesFlushed,
	}
}

// StoreRangeFlusher coordinates range flushes across all replicas on a store.
type StoreRangeFlusher Store

func (s *StoreRangeFlusher) store() *Store {
	return (*Store)(s)
}

var _ pebble.DBForCompaction = (*StoreRangeFlusher)(nil)

// GetAllowedWithoutPermission implements pebble.DBForCompaction. Returns the
// cluster setting value. No mutex needed — reads a cluster setting atomically.
func (s *StoreRangeFlusher) GetAllowedWithoutPermission() int {
	return int(rangeFlushMaxConcurrency.Get(&s.store().ClusterSettings().SV))
}

// GetWaitingCompaction implements pebble.DBForCompaction.
func (s *StoreRangeFlusher) GetWaitingCompaction() (bool, pebble.WaitingCompaction) {
	rf := s.store().rangeFlusher
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.getWaitingCompactionLocked()
}

// Schedule implements pebble.DBForCompaction.
func (s *StoreRangeFlusher) Schedule(handle pebble.CompactionGrantHandle) bool {
	rf := s.store().rangeFlusher
	rf.mu.Lock()
	item := rf.dequeueLocked()
	if item == nil {
		rf.mu.Unlock()
		return false
	}
	rf.mu.flushing[item.rangeID] = struct{}{}
	rf.mu.Unlock()
	return s.launchFlush(context.Background(), s.store().stopper, item, handle)
}

// Start begins the coordinator loop that scans for eligible ranges and
// dispatches flushes.
func (s *StoreRangeFlusher) Start(ctx context.Context, stopper *stop.Stopper) {
	_ = stopper.RunAsyncTask(ctx, "store-range-flusher", func(ctx context.Context) {
		rf := s.store().rangeFlusher
		defer rf.scheduler.Unregister()
		st := s.store().ClusterSettings()
		var timer timeutil.Timer
		defer timer.Stop()
		var retryTimer timeutil.Timer
		defer retryTimer.Stop()
		resetTimer := func() {
			interval := rangeFlushScanInterval.Get(&st.SV)
			timer.Reset(jitteredInterval(interval))
		}
		resetTimer()
		for {
			select {
			case <-stopper.ShouldQuiesce():
				return
			case <-timer.C:
				timer.Read = true
				resetTimer()
				s.scanAndEnqueue(ctx, st)
			case <-rf.notifyCh:
			case <-retryTimer.C:
				retryTimer.Read = true
			}
			s.dispatchFlushes(ctx, stopper)
			// Set retry timer for the next backed-off item.
			rf.mu.Lock()
			if next := rf.nextRetryTimeLocked(); !next.IsZero() {
				if d := time.Until(next); d > 0 {
					retryTimer.Reset(d)
				}
			}
			rf.mu.Unlock()
		}
	})
}

// scanAndEnqueue iterates all replicas and incrementally updates the queue.
// Ranges that are above the threshold and where this store is the leaseholder
// are added/updated; ranges below threshold or no longer leaseholder are
// removed. Ranges currently flushing are skipped. flushFailedCount is
// preserved across scans for ranges that remain in the queue.
//
// Uses TryRLock to avoid blocking on contended replica locks — replicas that
// can't be locked are simply skipped and will be picked up on the next scan
// or via notification.
func (s *StoreRangeFlusher) scanAndEnqueue(ctx context.Context, st *cluster.Settings) {
	// TODO(basalt): gate behind basalt enablement check.
	rf := s.store().rangeFlusher
	threshold := rangeFlushBytesThreshold.Get(&st.SV)
	seen := make(map[roachpb.RangeID]struct{})
	now := s.store().Clock().NowAsClockTimestamp()
	var sumApproxStoreLocalBytes int64
	s.store().mu.replicasByRangeID.Range(func(_ roachpb.RangeID, repl *Replica) bool {
		rangeID := repl.RangeID
		seen[rangeID] = struct{}{}
		// Reset the notification flag so the replica can re-notify after
		// accumulating more bytes.
		repl.rangeFlushNotified.Store(false)
		// Use TryRLock to avoid blocking on contended replica locks.
		if !repl.mu.TryRLock() {
			return true // skip, will catch on next scan
		}
		approxBytes := repl.shMu.state.ApproxStoreLocalBytes
		desc := repl.shMu.state.Desc
		initialized := repl.IsInitialized()
		ownsLease := repl.ownsValidLeaseRLocked(ctx, now)
		repl.mu.RUnlock()
		if !initialized {
			return true // continue
		}
		isTimeseries := rangeContainsTimeseriesData(desc.StartKey)
		if isTimeseries || !ownsLease {
			func() {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				rf.removeLocked(rangeID)
			}()
			return true // continue
		}
		// Accumulate across all eligible replicas (initialized, not timeseries,
		// leaseholder), regardless of byte threshold.
		sumApproxStoreLocalBytes += approxBytes
		if approxBytes < threshold {
			func() {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				rf.removeLocked(rangeID)
			}()
			return true // continue
		}
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.enqueueLocked(rangeID, approxBytes)
		}()
		return true // continue
	})
	// Remove ranges no longer present on this store and log a summary.
	func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		for rangeID := range rf.mu.queued {
			if _, ok := seen[rangeID]; !ok {
				rf.removeLocked(rangeID)
			}
		}
		rf.mu.sumApproxStoreLocalBytes = sumApproxStoreLocalBytes
		completed := rf.mu.flushesCompleted
		failed := rf.mu.flushesFailed
		rf.mu.flushesCompleted = 0
		rf.mu.flushesFailed = 0
		log.KvDistribution.Infof(ctx,
			"range flusher: queued=%d (%s) flushing=%d "+
				"completed=%d failed=%d since last scan, "+
				"cumulative-flushed=%s approx-store-local=%s",
			len(rf.mu.queued), humanizeutil.IBytes(rf.mu.bytesQueued),
			len(rf.mu.flushing),
			completed, failed,
			humanizeutil.IBytes(rf.mu.bytesFlushed),
			humanizeutil.IBytes(sumApproxStoreLocalBytes),
		)
	}()
}

// dispatchFlushes pops items from the queue and launches flush goroutines,
// gated by the scheduler.
func (s *StoreRangeFlusher) dispatchFlushes(ctx context.Context, stopper *stop.Stopper) {
	rf := s.store().rangeFlusher
	for {
		granted, handle := rf.scheduler.TrySchedule()
		if !granted {
			return
		}
		item := func() *rangeFlushItem {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			item := rf.dequeueLocked()
			if item != nil {
				rf.mu.flushing[item.rangeID] = struct{}{}
			}
			return item
		}()
		if item == nil {
			handle.Done()
			return
		}
		if !s.launchFlush(ctx, stopper, item, handle) {
			handle.Done()
			return
		}
	}
}

// launchFlush starts an async task to flush the given item. Returns true if
// the async task was started. On successful start, the task handles
// success/failure bookkeeping, calls handle.Done() (after releasing rf.mu to
// respect lock ordering), and wakes the coordinator. On failure (stopper
// quiescing), re-enqueues the item and returns false without calling
// handle.Done() — the caller is responsible for releasing the grant.
func (s *StoreRangeFlusher) launchFlush(
	ctx context.Context,
	stopper *stop.Stopper,
	item *rangeFlushItem,
	handle pebble.CompactionGrantHandle,
) bool {
	rf := s.store().rangeFlusher
	rangeID := item.rangeID
	failedCount := item.flushFailedCount
	if err := stopper.RunAsyncTask(ctx, "range-flush", func(ctx context.Context) {
		executor := s.getExecutor()
		flushedBytes, err := executor.executeRangeFlush(ctx, rangeID)
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			delete(rf.mu.flushing, rangeID)
			if err != nil {
				log.KvDistribution.Warningf(ctx, "range flush failed for r%d: %v", rangeID, err)
				rf.mu.flushesFailed++
				// Re-enqueue with incremented failedCount and exponential backoff.
				newFailedCount := failedCount + 1
				reItem := &rangeFlushItem{
					rangeID:          rangeID,
					approxBytes:      item.approxBytes,
					flushFailedCount: newFailedCount,
					retryAfter:       timeutil.Now().Add(rangeFlushBackoff(newFailedCount)),
				}
				heap.Push(&rf.mu.queue, reItem)
				rf.mu.queued[rangeID] = reItem
				rf.mu.bytesQueued += reItem.approxBytes
			} else {
				rf.mu.bytesFlushed += flushedBytes
				rf.mu.flushesCompleted++
				log.KvDistribution.Infof(ctx, "range flush completed for r%d (approx %s queued)",
					rangeID, humanizeutil.IBytes(item.approxBytes))
				// Reset notification so the replica can re-notify.
				if repl := s.store().GetReplicaIfExists(rangeID); repl != nil {
					repl.rangeFlushNotified.Store(false)
				}
			}
		}()
		// handle.Done() acquires scheduler.mu, so call after releasing rf.mu.
		handle.Done()
		rf.maybeNotify()
	}); err != nil {
		// Stopper is quiescing; put range back.
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			delete(rf.mu.flushing, rangeID)
			rf.enqueueLocked(rangeID, item.approxBytes)
		}()
		return false
	}
	return true
}

// getExecutor returns the rangeFlushExecutor, checking for a testing knob.
func (s *StoreRangeFlusher) getExecutor() rangeFlushExecutor {
	if knobs := s.store().TestingKnobs(); knobs != nil && knobs.RangeFlushExecutor != nil {
		return knobs.RangeFlushExecutor
	}
	return s
}

// executeRangeFlush implements rangeFlushExecutor for production use.
func (s *StoreRangeFlusher) executeRangeFlush(
	ctx context.Context, rangeID roachpb.RangeID,
) (int64, error) {
	repl := s.store().GetReplicaIfExists(rangeID)
	if repl == nil {
		return 0, nil
	}
	if rangeContainsTimeseriesData(repl.Desc().StartKey) {
		return 0, nil
	}
	committer := (*ReplicaManifestCommitter)(repl)
	err := committer.RangeFlush()
	if err != nil {
		return 0, err
	}
	// After a successful flush, ApproxStoreLocalBytes is decremented by the
	// flush commit path, so we return what was queued as a rough measure.
	return 0, nil
}
