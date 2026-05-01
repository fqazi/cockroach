// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"container/heap"
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/pebble"
)

// EngineType identifies the kind of engine registered with the scheduler.
type EngineType uint8

const (
	EngineTypeStoreLocal EngineType = iota
	EngineTypeRangeShared
	EngineTypeRangeFlusher
)

// String returns a human-readable name for the engine type.
func (t EngineType) String() string {
	switch t {
	case EngineTypeStoreLocal:
		return "store-local"
	case EngineTypeRangeShared:
		return "range-shared"
	case EngineTypeRangeFlusher:
		return "range-flusher"
	default:
		return fmt.Sprintf("unknown(%d)", t)
	}
}

// numEngineTypes is the number of EngineType values, used for per-type stats
// arrays.
const numEngineTypes = 3

// CompactionSchedulerPlus extends pebble.CompactionScheduler with an
// additional method for RS engines to invalidate cached WaitingCompaction
// state after FlushSSTables.
type CompactionSchedulerPlus interface {
	pebble.CompactionScheduler
	// InvalidateRememberedWaitingState clears the cached WaitingCompaction
	// for this engine. Called after FlushSSTables to ensure the scheduler
	// re-probes with fresh data on the next tryGrant cycle.
	InvalidateRememberedWaitingState()
}

// defaultStatsInterval is the default interval for periodic stats logging.
const defaultStatsInterval = 30 * time.Second

// SchedulerOptions configures a MultiEngineCompactionScheduler.
type SchedulerOptions struct {
	// LogCtx is the context used for logging. Must be set for production use.
	LogCtx context.Context
	// GetMaxConcurrency returns the global maximum concurrent compactions.
	GetMaxConcurrency func() int
	// RSEngineDeprioritizationRatio returns the factor by which RS engine
	// WaitingCompaction scores are divided when comparing against other engine
	// types. A value of 1.0 means no deprioritization. Applied at comparison
	// time so relative RS ordering is preserved.
	RSEngineDeprioritizationRatio func() float64
	// StatsInterval is the interval for periodic stats logging. If zero,
	// defaults to defaultStatsInterval.
	StatsInterval time.Duration
	// testingDisableBackgroundGranter, if true, disables the background granter
	// goroutine. The caller must use tryGrantForTesting to trigger grants.
	testingDisableBackgroundGranter bool
}

// MultiEngineCompactionScheduler coordinates compaction concurrency across
// three engine types: store-local, range-flusher, and range-shared (RS).
// It enforces a global concurrency budget and uses WaitingCompaction
// priorities to decide which engine gets the next grant.
//
// Each engine is represented by an engineState that implements
// pebble.CompactionScheduler. Engines register via OpeningEngine and
// unregister via the returned CompactionSchedulerPlus.Unregister.
//
// RS engines may have many instances (one per range). To avoid O(N) probing,
// the scheduler caches WaitingCompaction data in a min-heap (rsHeap) and
// maintains an unprobed list of RS engines that need fresh GetWaitingCompaction
// calls. The unprobed list is drained at the start of each tryGrant cycle.
type MultiEngineCompactionScheduler struct {
	opts        SchedulerOptions
	nextID      atomic.Uint64
	pokeGranter chan struct{}
	stopGranter chan struct{}
	mu          struct {
		sync.Mutex
		engines      map[uint64]*engineState
		storeLocal   *engineState
		rangeFlusher *engineState
		rsHeap       rsEngineHeap
		// unprobed is the list of RS engines with waiting=true that need a
		// fresh GetWaitingCompaction probe.
		//
		// INVARIANT: rsHeap and unprobed are disjoint (no engine in both).
		// INVARIANT: for RS engines, waiting => hasRemembered || inUnprobed.
		unprobed      []*engineState
		globalRunning int
		// runningByType tracks in-flight compactions per engine type.
		runningByType [numEngineTypes]int
		// isGranting serializes tryGrant calls from Done and the background
		// granter goroutine. While true, new TrySchedule calls that fail defer
		// to the granter instead of trying inline.
		isGranting     bool
		isGrantingCond *sync.Cond
		closed         bool
		// stats tracks grant/deny/done counts per engine type since the last
		// periodic stats report.
		stats schedulerStats
		// cumulativeDuration tracks cumulative compaction duration per engine
		// type (in nanoseconds) since the scheduler was created.
		cumulativeDuration [numEngineTypes]int64
	}
}

// schedulerStats holds per-type counters reset after each periodic report.
type schedulerStats struct {
	grants  [numEngineTypes]int64
	denials [numEngineTypes]int64
	done    [numEngineTypes]int64
}

// CompactionSchedulerMetrics is a snapshot of scheduler state for external
// consumption (e.g., TSDB metrics).
type CompactionSchedulerMetrics struct {
	// RunningByType is the number of in-flight compactions per engine type.
	RunningByType [numEngineTypes]int
	// CumulativeDurationByType is the cumulative compaction duration per
	// engine type since the scheduler was created. The rate of this value
	// gives the effective compaction concurrency for each engine type.
	CumulativeDurationByType [numEngineTypes]time.Duration
}

// Metrics returns a snapshot of the scheduler's current metrics.
func (s *MultiEngineCompactionScheduler) Metrics() CompactionSchedulerMetrics {
	s.mu.Lock()
	defer s.mu.Unlock()
	var m CompactionSchedulerMetrics
	m.RunningByType = s.mu.runningByType
	for i := range m.CumulativeDurationByType {
		m.CumulativeDurationByType[i] = time.Duration(s.mu.cumulativeDuration[i])
	}
	return m
}

// NewMultiEngineCompactionScheduler creates a scheduler. The caller must
// call Close when done.
func NewMultiEngineCompactionScheduler(opts SchedulerOptions) *MultiEngineCompactionScheduler {
	if opts.StatsInterval == 0 {
		opts.StatsInterval = defaultStatsInterval
	}
	s := &MultiEngineCompactionScheduler{
		opts:        opts,
		pokeGranter: make(chan struct{}, 1),
		stopGranter: make(chan struct{}),
	}
	s.mu.isGrantingCond = sync.NewCond(&s.mu.Mutex)
	s.mu.engines = make(map[uint64]*engineState)
	if !opts.testingDisableBackgroundGranter {
		go s.backgroundGranter()
	}
	return s
}

// OpeningEngine creates a CompactionSchedulerPlus for the given engine type.
// The caller wraps the result as func() pebble.CompactionScheduler for
// pebble.Options.Experimental.CompactionScheduler.
//
// For EngineTypeStoreLocal and EngineTypeRangeFlusher, at most one engine of
// each type may be registered. For EngineTypeRangeShared, many engines may be
// registered (one per range).
func (s *MultiEngineCompactionScheduler) OpeningEngine(typ EngineType) CompactionSchedulerPlus {
	es := &engineState{
		scheduler: s,
		typ:       typ,
		heapIndex: -1,
	}
	if typ == EngineTypeRangeShared {
		es.id = s.nextID.Add(1)
	}
	return es
}

// Close stops the background granter goroutine and waits for any in-progress
// granting to finish.
func (s *MultiEngineCompactionScheduler) Close() {
	if !s.opts.testingDisableBackgroundGranter {
		s.stopGranter <- struct{}{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.closed = true
	for s.mu.isGranting {
		s.mu.isGrantingCond.Wait()
	}
}

func (s *MultiEngineCompactionScheduler) poke() {
	select {
	case s.pokeGranter <- struct{}{}:
	default:
	}
}

// tryGrantForTesting triggers the granter from tests. Only effective when
// testingDisableBackgroundGranter is true.
func (s *MultiEngineCompactionScheduler) tryGrantForTesting() {
	s.tryGrant()
}

func (s *MultiEngineCompactionScheduler) backgroundGranter() {
	ticker := time.NewTicker(s.opts.StatsInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.pokeGranter:
			s.tryGrant()
		case <-ticker.C:
			s.logStats()
		case <-s.stopGranter:
			return
		}
	}
}

// logStats logs a periodic summary of compaction scheduler activity and resets
// the counters. Only logs if there was any activity since the last report.
func (s *MultiEngineCompactionScheduler) logStats() {
	s.mu.Lock()
	stats := s.mu.stats
	s.mu.stats = schedulerStats{}
	globalRunning := s.mu.globalRunning
	runningByType := s.mu.runningByType
	rsHeapLen := s.mu.rsHeap.Len()
	unprobedLen := len(s.mu.unprobed)
	rsEngineCount := len(s.mu.engines)
	s.mu.Unlock()

	totalGrants := stats.grants[EngineTypeStoreLocal] + stats.grants[EngineTypeRangeFlusher] + stats.grants[EngineTypeRangeShared]
	totalDenials := stats.denials[EngineTypeStoreLocal] + stats.denials[EngineTypeRangeFlusher] + stats.denials[EngineTypeRangeShared]
	totalDone := stats.done[EngineTypeStoreLocal] + stats.done[EngineTypeRangeFlusher] + stats.done[EngineTypeRangeShared]
	if totalGrants == 0 && totalDenials == 0 && totalDone == 0 {
		return
	}

	ctx := s.opts.LogCtx
	log.Storage.Infof(ctx,
		"compaction scheduler stats: "+
			"grants(store-local=%d range-flusher=%d range-shared=%d) "+
			"denials(store-local=%d range-flusher=%d range-shared=%d) "+
			"done(store-local=%d range-flusher=%d range-shared=%d) "+
			"running=%d/%d(store-local=%d range-flusher=%d range-shared=%d) "+
			"rs-engines=%d rs-heap=%d rs-unprobed=%d",
		stats.grants[EngineTypeStoreLocal], stats.grants[EngineTypeRangeFlusher], stats.grants[EngineTypeRangeShared],
		stats.denials[EngineTypeStoreLocal], stats.denials[EngineTypeRangeFlusher], stats.denials[EngineTypeRangeShared],
		stats.done[EngineTypeStoreLocal], stats.done[EngineTypeRangeFlusher], stats.done[EngineTypeRangeShared],
		globalRunning, s.opts.GetMaxConcurrency(),
		runningByType[EngineTypeStoreLocal], runningByType[EngineTypeRangeFlusher], runningByType[EngineTypeRangeShared],
		rsEngineCount, rsHeapLen, unprobedLen,
	)
}

// formatCandidate returns a human-readable string for a candidate.
func formatCandidate(c candidate, depriRatio float64) string {
	score := c.wc.Score
	suffix := ""
	if c.es.typ == EngineTypeRangeShared && depriRatio > 0 && depriRatio != 1.0 {
		suffix = fmt.Sprintf(" depri=%.2f->%.2f", score, score/depriRatio)
	}
	return fmt.Sprintf("%s(optional=%t pri=%d score=%.2f%s)",
		c.es.typ, c.wc.Optional, c.wc.Priority, score, suffix)
}

// tryGrant runs the grant algorithm.
//
// The algorithm:
//  1. Drains the unprobed list into the RS heap by calling GetWaitingCompaction
//     (with mu released) for each unprobed RS engine.
//  2. Collects up to 3 candidates (store-local, range-flusher, RS heap top).
//  3. Sorts candidates by WaitingCompaction priority, applying the
//     deprioritization ratio to RS engine scores.
//  4. Tries candidates in priority order via db.Schedule. Accepted grants
//     increment running counts and continue the outer loop.
func (s *MultiEngineCompactionScheduler) tryGrant() {
	s.mu.Lock()
	if s.mu.closed || s.mu.isGranting {
		s.mu.Unlock()
		return
	}
	s.mu.isGranting = true
	defer func() {
		s.mu.isGranting = false
		s.mu.isGrantingCond.Broadcast()
		s.mu.Unlock()
	}()
	globalLimit := s.opts.GetMaxConcurrency()
	for s.mu.globalRunning < globalLimit {
		// 1. Drain unprobed RS engines into the heap.
		s.drainUnprobedLocked()
		// 2-4. Collect candidates, sort, and try.
		if !s.tryGrantOneLocked(globalLimit) {
			break
		}
	}
}

// drainUnprobedLocked probes each RS engine in the unprobed list by calling
// GetWaitingCompaction (releasing mu). Engines with waiting compactions are
// added to the heap; engines with no work have their waiting flag cleared
// (if no concurrent TrySchedule arrived).
func (s *MultiEngineCompactionScheduler) drainUnprobedLocked() {
	unprobed := s.mu.unprobed
	s.mu.unprobed = s.mu.unprobed[:0]
	for _, es := range unprobed {
		es.inUnprobed = false
		if !es.registered {
			continue
		}
		snapshot := es.tryScheduleCount
		s.mu.Unlock()
		waiting, wc := es.db.GetWaitingCompaction()
		s.mu.Lock()
		if !es.registered {
			continue
		}
		if !waiting {
			es.trySetNotWaitingLocked(snapshot)
			continue
		}
		es.hasRemembered = true
		es.remembered = wc
		heap.Push(&s.mu.rsHeap, es)
	}
}

// candidate is a potential grant target collected during tryGrant.
type candidate struct {
	es *engineState
	wc pebble.WaitingCompaction
}

// tryGrantOneLocked collects candidates, picks the best, and attempts to
// grant. Returns true if a grant was made and the outer loop should continue.
func (s *MultiEngineCompactionScheduler) tryGrantOneLocked(globalLimit int) bool {
	var candidates []candidate
	// Collect store-local candidate.
	if es := s.mu.storeLocal; es != nil && es.waiting {
		perEngine := es.db.GetAllowedWithoutPermission()
		if es.running < perEngine {
			snapshot := es.tryScheduleCount
			s.mu.Unlock()
			waiting, wc := es.db.GetWaitingCompaction()
			s.mu.Lock()
			if es.registered && waiting {
				candidates = append(candidates, candidate{es: es, wc: wc})
			} else if es.registered {
				es.trySetNotWaitingLocked(snapshot)
			}
		}
	}
	// Collect range-flusher candidate.
	if es := s.mu.rangeFlusher; es != nil && es.waiting {
		perEngine := es.db.GetAllowedWithoutPermission()
		if es.running < perEngine {
			snapshot := es.tryScheduleCount
			s.mu.Unlock()
			waiting, wc := es.db.GetWaitingCompaction()
			s.mu.Lock()
			if es.registered && waiting {
				candidates = append(candidates, candidate{es: es, wc: wc})
			} else if es.registered {
				es.trySetNotWaitingLocked(snapshot)
			}
		}
	}
	// Collect RS heap top candidate.
	if s.mu.rsHeap.Len() > 0 {
		es := s.mu.rsHeap[0]
		perEngine := es.db.GetAllowedWithoutPermission()
		if es.running < perEngine && es.hasRemembered {
			candidates = append(candidates, candidate{es: es, wc: es.remembered})
		}
	}
	if len(candidates) == 0 {
		return false
	}
	// Sort candidates best-first.
	depriRatio := s.opts.RSEngineDeprioritizationRatio()
	sortCandidates(candidates, depriRatio)
	// Try candidates in priority order.
	for _, c := range candidates {
		es := c.es
		if s.mu.globalRunning >= globalLimit {
			return false
		}
		es.tryInvalidateRememberedStateLocked()
		snapshot := es.tryScheduleCount
		s.mu.Unlock()
		accepted := es.db.Schedule(&compactionHandle{es: es})
		s.mu.Lock()
		if accepted {
			es.running++
			s.mu.globalRunning++
			s.mu.runningByType[es.typ]++
			s.mu.stats.grants[es.typ]++
			if ctx := s.opts.LogCtx; ctx != nil {
				if log.V(2) {
					var buf strings.Builder
					for i, cc := range candidates {
						if i > 0 {
							buf.WriteString(", ")
						}
						buf.WriteString(formatCandidate(cc, depriRatio))
					}
					log.Storage.VEventf(ctx, 2,
						"compaction grant: winner=%s running=%d/%d candidates=[%s]",
						es.typ, s.mu.globalRunning, globalLimit, buf.String())
				}
			}
			return true
		}
		// Rejected: try to clear waiting if no new TrySchedule arrived.
		es.trySetNotWaitingLocked(snapshot)
	}
	return false
}

// sortCandidates sorts candidates best-first using compareCompactions.
// RS engine scores are divided by depriRatio at comparison time.
func sortCandidates(candidates []candidate, depriRatio float64) {
	if len(candidates) <= 1 {
		return
	}
	// Simple insertion sort for at most 3 elements.
	for i := 1; i < len(candidates); i++ {
		for j := i; j > 0; j-- {
			if compareCandidates(candidates[j], candidates[j-1], depriRatio) < 0 {
				candidates[j], candidates[j-1] = candidates[j-1], candidates[j]
			} else {
				break
			}
		}
	}
}

// compareCandidates returns <0 if a should run before b (higher priority).
// Lower Optional wins; then higher Priority wins; then higher Score wins.
// RS engine scores are divided by depriRatio.
func compareCandidates(a, b candidate, depriRatio float64) int {
	return compareCompactions(a.wc, a.es.typ, b.wc, b.es.typ, depriRatio)
}

// compareCompactions compares two WaitingCompaction values. Returns <0 if a
// is higher priority. Optional=false beats Optional=true; higher Priority
// wins; higher Score wins. RS engine scores are divided by depriRatio.
func compareCompactions(
	a pebble.WaitingCompaction,
	aType EngineType,
	b pebble.WaitingCompaction,
	bType EngineType,
	depriRatio float64,
) int {
	// Optional=false is more important.
	if !a.Optional && b.Optional {
		return -1
	}
	if a.Optional && !b.Optional {
		return 1
	}
	// Higher priority wins.
	if a.Priority > b.Priority {
		return -1
	}
	if a.Priority < b.Priority {
		return 1
	}
	// Higher score wins, applying deprioritization to RS engines.
	aScore := a.Score
	bScore := b.Score
	if aType == EngineTypeRangeShared && depriRatio > 0 {
		aScore /= depriRatio
	}
	if bType == EngineTypeRangeShared && depriRatio > 0 {
		bScore /= depriRatio
	}
	if aScore > bScore {
		return -1
	}
	if aScore < bScore {
		return 1
	}
	return 0
}

func (s *MultiEngineCompactionScheduler) removeFromHeapLocked(es *engineState) {
	if es.heapIndex >= 0 {
		heap.Remove(&s.mu.rsHeap, es.heapIndex)
	}
	es.hasRemembered = false
}

// removeFromUnprobedLocked removes es from the unprobed list using
// swap-with-last for O(1) removal (ordering does not matter).
func (s *MultiEngineCompactionScheduler) removeFromUnprobedLocked(es *engineState) {
	for i, e := range s.mu.unprobed {
		if e == es {
			last := len(s.mu.unprobed) - 1
			s.mu.unprobed[i] = s.mu.unprobed[last]
			s.mu.unprobed[last] = nil // avoid memory leak
			s.mu.unprobed = s.mu.unprobed[:last]
			break
		}
	}
	es.inUnprobed = false
}

// engineState represents a single engine registered with the scheduler. It
// implements pebble.CompactionScheduler (for TrySchedule/Register/Unregister)
// and CompactionSchedulerPlus (for InvalidateRememberedWaitingState).
// Per-compaction grant handling is done by compactionHandle.
//
// Fields are protected by scheduler.mu unless noted otherwise.
type engineState struct {
	scheduler *MultiEngineCompactionScheduler
	typ       EngineType
	id        uint64 // unique ID for RS engines
	// db is set in Register. Not protected by scheduler.mu since it is set
	// strictly before any calls to other methods.
	db pebble.DBForCompaction
	// registered is true after Register and false after Unregister.
	registered bool
	// running is the number of in-flight compactions for this engine.
	running int
	// waiting indicates the engine has pending compaction work. Set to true
	// by TrySchedule when permission is denied; cleared by tryGrant when
	// GetWaitingCompaction returns false (guarded by tryScheduleCount).
	waiting bool
	// tryScheduleCount is incremented on each TrySchedule. Used to prevent
	// stale clearing of waiting: if a new TrySchedule arrives while mu is
	// released for GetWaitingCompaction, the snapshot won't match and waiting
	// won't be cleared.
	tryScheduleCount uint64
	// RS-engine-only fields:
	//
	// hasRemembered indicates whether remembered contains a valid cached
	// WaitingCompaction from GetWaitingCompaction.
	//
	// INVARIANT: hasRemembered => waiting.
	// INVARIANT: hasRemembered => heapIndex >= 0 (in rsHeap).
	// INVARIANT: hasRemembered => !inUnprobed.
	hasRemembered bool
	remembered    pebble.WaitingCompaction
	// heapIndex is the index in rsHeap, or -1 if not in the heap.
	heapIndex int
	// inUnprobed is true if this engine is in the unprobed list.
	//
	// INVARIANT: inUnprobed => waiting.
	// INVARIANT: inUnprobed => !hasRemembered.
	// INVARIANT (RS engines): waiting => hasRemembered || inUnprobed.
	inUnprobed bool
}

var _ CompactionSchedulerPlus = (*engineState)(nil)

// compactionHandle wraps an engineState for a single compaction grant,
// recording the start time so Done() can accumulate per-type duration.
type compactionHandle struct {
	es        *engineState
	startTime time.Time
}

var _ pebble.CompactionGrantHandle = (*compactionHandle)(nil)

func (h *compactionHandle) Started() {
	h.startTime = time.Now()
}

func (h *compactionHandle) MeasureCPU(k pebble.CompactionGoroutineKind) {}

func (h *compactionHandle) CumulativeStats(pebble.CompactionGrantHandleStats) {}

func (h *compactionHandle) Done() {
	es := h.es
	s := es.scheduler
	s.mu.Lock()
	if !h.startTime.IsZero() {
		s.mu.cumulativeDuration[es.typ] += int64(time.Since(h.startTime))
	}
	es.running--
	s.mu.globalRunning--
	s.mu.runningByType[es.typ]--
	s.mu.stats.done[es.typ]++
	es.tryInvalidateRememberedStateLocked()
	if ctx := s.opts.LogCtx; ctx != nil {
		log.Storage.VEventf(ctx, 2,
			"compaction done: %s running=%d/%d",
			es.typ, s.mu.globalRunning, s.opts.GetMaxConcurrency())
	}
	s.mu.Unlock()
	s.poke()
}

// tryInvalidateRememberedStateLocked clears the cached WaitingCompaction
// for this engine. If no remembered state exists, this is a noop. Otherwise
// the engine is removed from the heap, and if still waiting, added to the
// unprobed list so it can be re-probed.
func (es *engineState) tryInvalidateRememberedStateLocked() {
	if !es.hasRemembered {
		return
	}
	es.scheduler.removeFromHeapLocked(es)
	if es.waiting && !es.inUnprobed {
		es.scheduler.mu.unprobed = append(es.scheduler.mu.unprobed, es)
		es.inUnprobed = true
	}
}

// trySetNotWaitingLocked attempts to clear waiting. Noop if not waiting or
// if tryScheduleCount doesn't match the snapshot (a new TrySchedule arrived
// while mu was released). When clearing, removes from heap or unprobed as
// needed.
func (es *engineState) trySetNotWaitingLocked(snapshot uint64) {
	if !es.waiting || es.tryScheduleCount != snapshot {
		return
	}
	es.waiting = false
	if es.typ == EngineTypeRangeShared {
		if es.hasRemembered {
			es.scheduler.removeFromHeapLocked(es)
		}
		if es.inUnprobed {
			es.scheduler.removeFromUnprobedLocked(es)
		}
	}
}

// trySetWaitingLocked marks the engine as waiting. Always increments
// tryScheduleCount (even if already waiting) to prevent stale clearing.
// For RS engines, invalidates any cached WaitingCompaction (pebble calls
// TrySchedule with a new pickedCompaction, making the old WC stale) and
// ensures the engine is in the unprobed list.
func (es *engineState) trySetWaitingLocked() {
	es.tryScheduleCount++
	if es.typ == EngineTypeRangeShared {
		es.tryInvalidateRememberedStateLocked()
	}
	if es.waiting {
		return
	}
	es.waiting = true
	if es.typ == EngineTypeRangeShared && !es.inUnprobed {
		es.scheduler.mu.unprobed = append(es.scheduler.mu.unprobed, es)
		es.inUnprobed = true
	}
}

// Register implements pebble.CompactionScheduler.
func (es *engineState) Register(_ int, db pebble.DBForCompaction) {
	s := es.scheduler
	s.mu.Lock()
	defer s.mu.Unlock()
	es.db = db
	es.registered = true
	switch es.typ {
	case EngineTypeStoreLocal:
		s.mu.storeLocal = es
	case EngineTypeRangeFlusher:
		s.mu.rangeFlusher = es
	case EngineTypeRangeShared:
		s.mu.engines[es.id] = es
	}
	if ctx := s.opts.LogCtx; ctx != nil {
		log.Storage.Infof(ctx, "compaction scheduler: registered %s engine (rs-engines=%d)",
			es.typ, len(s.mu.engines))
	}
}

// Unregister implements pebble.CompactionScheduler. It waits for any
// in-progress granting to complete before returning.
func (es *engineState) Unregister() {
	s := es.scheduler
	s.mu.Lock()
	defer s.mu.Unlock()
	es.registered = false
	switch es.typ {
	case EngineTypeStoreLocal:
		if s.mu.storeLocal == es {
			s.mu.storeLocal = nil
		}
	case EngineTypeRangeFlusher:
		if s.mu.rangeFlusher == es {
			s.mu.rangeFlusher = nil
		}
	case EngineTypeRangeShared:
		delete(s.mu.engines, es.id)
		if es.hasRemembered {
			s.removeFromHeapLocked(es)
		}
		if es.inUnprobed {
			s.removeFromUnprobedLocked(es)
		}
	}
	if ctx := s.opts.LogCtx; ctx != nil {
		log.Storage.Infof(ctx, "compaction scheduler: unregistered %s engine (rs-engines=%d)",
			es.typ, len(s.mu.engines))
	}
	// Wait for in-progress granting to finish so no more calls to
	// DBForCompaction methods arrive after Unregister returns.
	for s.mu.isGranting {
		s.mu.isGrantingCond.Wait()
	}
}

// TrySchedule implements pebble.CompactionScheduler.
func (es *engineState) TrySchedule() (bool, pebble.CompactionGrantHandle) {
	s := es.scheduler
	s.mu.Lock()
	defer s.mu.Unlock()
	if !es.registered {
		return false, nil
	}
	// If granting is in progress, defer to the granter.
	if s.mu.isGranting {
		es.trySetWaitingLocked()
		s.poke()
		return false, nil
	}
	perEngine := es.db.GetAllowedWithoutPermission()
	globalLimit := s.opts.GetMaxConcurrency()
	if es.running < perEngine && s.mu.globalRunning < globalLimit {
		es.running++
		s.mu.globalRunning++
		s.mu.runningByType[es.typ]++
		s.mu.stats.grants[es.typ]++
		es.tryInvalidateRememberedStateLocked()
		if ctx := s.opts.LogCtx; ctx != nil {
			log.Storage.VEventf(ctx, 2,
				"compaction grant (inline): %s running=%d/%d",
				es.typ, s.mu.globalRunning, globalLimit)
		}
		return true, &compactionHandle{es: es}
	}
	s.mu.stats.denials[es.typ]++
	es.trySetWaitingLocked()
	if ctx := s.opts.LogCtx; ctx != nil {
		log.Storage.VEventf(ctx, 3,
			"compaction denied: %s per-engine=%d/%d global=%d/%d",
			es.typ, es.running, perEngine, s.mu.globalRunning, globalLimit)
	}
	s.poke()
	return false, nil
}

// UpdateGetAllowedWithoutPermission implements pebble.CompactionScheduler.
func (es *engineState) UpdateGetAllowedWithoutPermission() {
	es.scheduler.poke()
}

// InvalidateRememberedWaitingState implements CompactionSchedulerPlus. Called
// after FlushSSTables to ensure stale cached WaitingCompaction data is
// discarded. If the engine is already waiting, it's re-added to the unprobed
// list so tryGrant re-probes with fresh data.
func (es *engineState) InvalidateRememberedWaitingState() {
	s := es.scheduler
	s.mu.Lock()
	defer s.mu.Unlock()
	es.tryInvalidateRememberedStateLocked()
	s.poke()
}

// rsEngineHeap is a min-heap of RS engineStates ordered by WaitingCompaction
// priority. Only RS engines with hasRemembered=true are in the heap.
// Lower Optional, higher Priority, higher Score = better.
type rsEngineHeap []*engineState

var _ heap.Interface = (*rsEngineHeap)(nil)

func (h rsEngineHeap) Len() int { return len(h) }

// Less returns true if h[i] is higher priority (should be scheduled first).
func (h rsEngineHeap) Less(i, j int) bool {
	return compareCompactions(
		h[i].remembered, EngineTypeRangeShared,
		h[j].remembered, EngineTypeRangeShared, 1.0) < 0
}

func (h rsEngineHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIndex = i
	h[j].heapIndex = j
}

func (h *rsEngineHeap) Push(x interface{}) {
	es := x.(*engineState)
	es.heapIndex = len(*h)
	*h = append(*h, es)
}

func (h *rsEngineHeap) Pop() interface{} {
	old := *h
	n := len(old)
	es := old[n-1]
	old[n-1] = nil // avoid memory leak
	es.heapIndex = -1
	*h = old[:n-1]
	return es
}
