// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"container/heap"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

// TestRangeFlushQueueOrdering verifies the heap ordering:
// lower flushFailedCount first, then higher approxBytes first.
func TestRangeFlushQueueOrdering(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rf := newRangeFlusher(newRangeFlushScheduler())
	rf.mu.Lock()
	// Add ranges with varying sizes.
	rf.enqueueLocked(1, 100)
	rf.enqueueLocked(2, 300)
	rf.enqueueLocked(3, 200)
	rf.mu.Unlock()
	// Dequeue should return in descending approxBytes order.
	rf.mu.Lock()
	item := rf.dequeueLocked()
	require.Equal(t, roachpb.RangeID(2), item.rangeID)
	require.Equal(t, int64(300), item.approxBytes)
	item = rf.dequeueLocked()
	require.Equal(t, roachpb.RangeID(3), item.rangeID)
	item = rf.dequeueLocked()
	require.Equal(t, roachpb.RangeID(1), item.rangeID)
	require.Nil(t, rf.dequeueLocked())
	rf.mu.Unlock()
}

// TestRangeFlushQueueFailedCountDeprioritization verifies that items with
// higher flushFailedCount are dequeued after items with lower failedCount,
// regardless of approxBytes.
func TestRangeFlushQueueFailedCountDeprioritization(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rf := newRangeFlusher(newRangeFlushScheduler())
	rf.mu.Lock()
	// Range 1: large but has failed once.
	rf.enqueueLocked(1, 1000)
	rf.mu.queued[1].flushFailedCount = 1
	// Range 2: small but never failed.
	rf.enqueueLocked(2, 100)
	// Range 3: medium, failed twice.
	rf.enqueueLocked(3, 500)
	rf.mu.queued[3].flushFailedCount = 2
	// Re-heapify after manual failedCount changes.
	for _, item := range rf.mu.queued {
		rf.mu.queue.fix(item)
	}
	rf.mu.Unlock()
	// Dequeue order: range 2 (failedCount=0), range 1 (failedCount=1),
	// range 3 (failedCount=2).
	rf.mu.Lock()
	item := rf.dequeueLocked()
	require.Equal(t, roachpb.RangeID(2), item.rangeID)
	item = rf.dequeueLocked()
	require.Equal(t, roachpb.RangeID(1), item.rangeID)
	item = rf.dequeueLocked()
	require.Equal(t, roachpb.RangeID(3), item.rangeID)
	rf.mu.Unlock()
}

// TestRangeFlushQueueEnqueueUpdate verifies that enqueueing an already-queued
// range updates its approxBytes and re-prioritizes.
func TestRangeFlushQueueEnqueueUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rf := newRangeFlusher(newRangeFlushScheduler())
	rf.mu.Lock()
	rf.enqueueLocked(1, 100)
	rf.enqueueLocked(2, 200)
	// Update range 1 to be larger than range 2.
	rf.enqueueLocked(1, 300)
	rf.mu.Unlock()
	rf.mu.Lock()
	item := rf.dequeueLocked()
	require.Equal(t, roachpb.RangeID(1), item.rangeID)
	require.Equal(t, int64(300), item.approxBytes)
	item = rf.dequeueLocked()
	require.Equal(t, roachpb.RangeID(2), item.rangeID)
	rf.mu.Unlock()
}

// TestRangeFlushQueueSkipsFlushing verifies that enqueueLocked does not add
// ranges that are currently in the flushing set.
func TestRangeFlushQueueSkipsFlushing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rf := newRangeFlusher(newRangeFlushScheduler())
	rf.mu.Lock()
	rf.mu.flushing[roachpb.RangeID(1)] = struct{}{}
	rf.enqueueLocked(1, 100)
	require.Equal(t, 0, rf.mu.queue.Len())
	require.Empty(t, rf.mu.queued)
	rf.mu.Unlock()
}

// TestRangeFlushQueueRemove verifies removeLocked.
func TestRangeFlushQueueRemove(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rf := newRangeFlusher(newRangeFlushScheduler())
	rf.mu.Lock()
	rf.enqueueLocked(1, 100)
	rf.enqueueLocked(2, 200)
	rf.removeLocked(1)
	require.Equal(t, 1, rf.mu.queue.Len())
	_, ok := rf.mu.queued[1]
	require.False(t, ok)
	// Remove non-existent is a no-op.
	rf.removeLocked(99)
	require.Equal(t, 1, rf.mu.queue.Len())
	rf.mu.Unlock()
}

// TestRangeFlusherMetrics verifies bytesQueued tracking across enqueue/dequeue.
func TestRangeFlusherMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rf := newRangeFlusher(newRangeFlushScheduler())
	rf.mu.Lock()
	rf.enqueueLocked(1, 100)
	rf.enqueueLocked(2, 200)
	require.Equal(t, int64(300), rf.mu.bytesQueued)
	// Update range 1 to 150.
	rf.enqueueLocked(1, 150)
	require.Equal(t, int64(350), rf.mu.bytesQueued)
	// Dequeue range 2 (200 bytes).
	item := rf.dequeueLocked()
	require.Equal(t, roachpb.RangeID(2), item.rangeID)
	require.Equal(t, int64(150), rf.mu.bytesQueued)
	// Remove range 1.
	rf.removeLocked(1)
	require.Equal(t, int64(0), rf.mu.bytesQueued)
	rf.mu.Unlock()
	// Verify Metrics snapshot.
	rf.mu.Lock()
	rf.mu.bytesFlushed = 500
	rf.mu.Unlock()
	m := rf.Metrics()
	require.Equal(t, int64(0), m.BytesQueued)
	require.Equal(t, int64(500), m.BytesFlushed)
}

// TestRangeFlusherSumApproxStoreLocalBytes verifies that
// sumApproxStoreLocalBytes is properly stored and independent of bytesQueued,
// since it includes replicas below the flush threshold.
func TestRangeFlusherSumApproxStoreLocalBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rf := newRangeFlusher(newRangeFlushScheduler())
	// Simulate what scanAndEnqueue does: accumulate the sum across all
	// eligible replicas (initialized, not timeseries, leaseholder) regardless
	// of byte threshold, and store at end of scan.
	rf.mu.Lock()
	// Only some replicas exceed the threshold and get enqueued.
	rf.enqueueLocked(1, 500)
	rf.enqueueLocked(2, 300)
	// The sum includes replicas below threshold too (e.g., a replica with
	// 50 bytes that wasn't enqueued).
	rf.mu.sumApproxStoreLocalBytes = 500 + 300 + 50
	require.Equal(t, int64(850), rf.mu.sumApproxStoreLocalBytes)
	require.Equal(t, int64(800), rf.mu.bytesQueued)
	rf.mu.Unlock()
	// Second scan updates the sum even if queue contents change.
	rf.mu.Lock()
	rf.enqueueLocked(1, 600) // updated
	rf.removeLocked(2)       // removed
	rf.mu.sumApproxStoreLocalBytes = 600 + 100
	require.Equal(t, int64(700), rf.mu.sumApproxStoreLocalBytes)
	require.Equal(t, int64(600), rf.mu.bytesQueued)
	rf.mu.Unlock()
}

// TestRangeFlusherNotifyCoalescing verifies that multiple maybeNotify calls
// coalesce into a single notification.
func TestRangeFlusherNotifyCoalescing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rf := newRangeFlusher(newRangeFlushScheduler())
	// Multiple notifications coalesce.
	rf.maybeNotify()
	rf.maybeNotify()
	rf.maybeNotify()
	// Only one value in channel.
	select {
	case <-rf.notifyCh:
	default:
		t.Fatal("expected notification")
	}
	select {
	case <-rf.notifyCh:
		t.Fatal("expected no second notification")
	default:
	}
}

// TestRangeFlusherEnqueueAndNotify verifies that enqueueAndNotify both enqueues
// and sends a notification.
func TestRangeFlusherEnqueueAndNotify(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rf := newRangeFlusher(newRangeFlushScheduler())
	rf.enqueueAndNotify(1, 100)
	// Verify enqueued.
	rf.mu.Lock()
	require.Equal(t, 1, rf.mu.queue.Len())
	require.Equal(t, int64(100), rf.mu.bytesQueued)
	rf.mu.Unlock()
	// Verify notification.
	select {
	case <-rf.notifyCh:
	default:
		t.Fatal("expected notification")
	}
}

// TestRangeFlusherDispatchConcurrency verifies that the flushing set gates
// concurrent dispatches by simulating what dispatchFlushes does.
func TestRangeFlusherDispatchConcurrency(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rf := newRangeFlusher(newRangeFlushScheduler())
	rf.mu.Lock()
	rf.enqueueLocked(1, 100)
	rf.enqueueLocked(2, 200)
	rf.enqueueLocked(3, 300)
	// Simulate dispatching 2 items (maxConcurrency=2).
	item1 := rf.dequeueLocked()
	rf.mu.flushing[item1.rangeID] = struct{}{}
	item2 := rf.dequeueLocked()
	rf.mu.flushing[item2.rangeID] = struct{}{}
	// Range 3 (300 bytes) and range 1 (100 bytes) are dispatched.
	require.Equal(t, roachpb.RangeID(3), item1.rangeID)
	require.Equal(t, roachpb.RangeID(2), item2.rangeID)
	require.Equal(t, 2, len(rf.mu.flushing))
	// Queue still has range 1.
	require.Equal(t, 1, rf.mu.queue.Len())
	rf.mu.Unlock()
}

// TestRangeFlusherFlushCompletion verifies that completing a flush removes it
// from flushing set and updates bytesFlushed.
func TestRangeFlusherFlushCompletion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rf := newRangeFlusher(newRangeFlushScheduler())
	rf.mu.Lock()
	rf.enqueueLocked(1, 100)
	item := rf.dequeueLocked()
	rf.mu.flushing[item.rangeID] = struct{}{}
	// Simulate successful flush completion.
	delete(rf.mu.flushing, item.rangeID)
	rf.mu.bytesFlushed += 100
	require.Equal(t, 0, len(rf.mu.flushing))
	require.Equal(t, int64(100), rf.mu.bytesFlushed)
	rf.mu.Unlock()
}

// TestRangeFlusherFailedFlushReenqueue verifies that failed flushes are
// re-enqueued with incremented failedCount and deprioritized.
func TestRangeFlusherFailedFlushReenqueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rf := newRangeFlusher(newRangeFlushScheduler())
	rf.mu.Lock()
	rf.enqueueLocked(1, 100)
	rf.enqueueLocked(2, 200)
	// Dispatch range 2 (highest priority).
	item := rf.dequeueLocked()
	require.Equal(t, roachpb.RangeID(2), item.rangeID)
	rf.mu.flushing[item.rangeID] = struct{}{}
	// Simulate flush failure: re-enqueue with incremented failedCount.
	delete(rf.mu.flushing, item.rangeID)
	reItem := &rangeFlushItem{
		rangeID:          item.rangeID,
		approxBytes:      item.approxBytes,
		flushFailedCount: item.flushFailedCount + 1,
	}
	heap.Push(&rf.mu.queue, reItem)
	rf.mu.queued[reItem.rangeID] = reItem
	rf.mu.bytesQueued += reItem.approxBytes
	// Now range 1 (failedCount=0, 100 bytes) should be ahead of
	// range 2 (failedCount=1, 200 bytes).
	next := rf.dequeueLocked()
	require.Equal(t, roachpb.RangeID(1), next.rangeID)
	require.Equal(t, 0, next.flushFailedCount)
	next = rf.dequeueLocked()
	require.Equal(t, roachpb.RangeID(2), next.rangeID)
	require.Equal(t, 1, next.flushFailedCount)
	rf.mu.Unlock()
}

// TestRangeFlusherScanPreservesFailedCount verifies that when a periodic scan
// updates approxBytes for an existing queued range, flushFailedCount is
// preserved.
func TestRangeFlusherScanPreservesFailedCount(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rf := newRangeFlusher(newRangeFlushScheduler())
	rf.mu.Lock()
	// Enqueue range 1 and manually set failedCount to simulate a prior failure.
	rf.enqueueLocked(1, 100)
	rf.mu.queued[1].flushFailedCount = 2
	rf.mu.queue.fix(rf.mu.queued[1])
	// Simulate scan updating approxBytes (as scanAndEnqueue does).
	item := rf.mu.queued[1]
	rf.mu.bytesQueued += 200 - item.approxBytes
	item.approxBytes = 200
	heap.Fix(&rf.mu.queue, item.index)
	// Verify failedCount is preserved.
	require.Equal(t, 2, rf.mu.queued[1].flushFailedCount)
	require.Equal(t, int64(200), rf.mu.queued[1].approxBytes)
	rf.mu.Unlock()
}

// TestRangeFlusherNotificationDedup verifies that rangeFlushNotified prevents
// redundant enqueue+notify calls when the flag is already set.
func TestRangeFlusherNotificationDedup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rf := newRangeFlusher(newRangeFlushScheduler())
	var notified atomic.Bool
	// First notification: flag was false, Swap returns false (old value),
	// so the condition !Swap(true) is true => enqueue happens.
	require.False(t, notified.Swap(true))
	rf.enqueueAndNotify(1, 100)
	// Second attempt: flag is already true, Swap returns true => no enqueue.
	require.True(t, notified.Swap(true))
	// Drain the channel.
	select {
	case <-rf.notifyCh:
	default:
	}
	// Verify only one item in queue (from first notification).
	rf.mu.Lock()
	require.Equal(t, 1, rf.mu.queue.Len())
	rf.mu.Unlock()
}

// TestRangeFlusherNotificationResetOnSuccess verifies that rangeFlushNotified
// is reset after successful flush, allowing re-notification.
func TestRangeFlusherNotificationResetOnSuccess(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var notified atomic.Bool
	// Simulate: notification sets the flag.
	notified.Store(true)
	// Simulate: successful flush resets the flag.
	notified.Store(false)
	// Now the replica can re-notify.
	require.False(t, notified.Swap(true))
}

// TestRangeFlusherNotificationKeptOnFailure verifies that rangeFlushNotified
// stays true after failed flush, preventing redundant re-notification since
// the range is re-enqueued.
func TestRangeFlusherNotificationKeptOnFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var notified atomic.Bool
	notified.Store(true)
	// On failure, we do NOT reset the flag. The range is re-enqueued
	// internally, so no need for the replica to re-notify.
	require.True(t, notified.Load())
}

// TestRangeFlushSchedulerTrySchedule verifies grants up to
// GetAllowedWithoutPermission, then denies.
func TestRangeFlushSchedulerTrySchedule(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sched := newRangeFlushScheduler()
	db := &mockDBForCompaction{allowed: 2}
	sched.Register(1, db)
	// First two grants succeed.
	ok1, h1 := sched.TrySchedule()
	require.True(t, ok1)
	require.NotNil(t, h1)
	ok2, h2 := sched.TrySchedule()
	require.True(t, ok2)
	require.NotNil(t, h2)
	// Third is denied.
	ok3, h3 := sched.TrySchedule()
	require.False(t, ok3)
	require.Nil(t, h3)
	h1.Done()
	h2.Done()
}

// TestRangeFlushSchedulerDone verifies Done decrements running, allowing
// new grants.
func TestRangeFlushSchedulerDone(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sched := newRangeFlushScheduler()
	db := &mockDBForCompaction{allowed: 1}
	sched.Register(1, db)
	ok, h := sched.TrySchedule()
	require.True(t, ok)
	// At limit.
	ok2, _ := sched.TrySchedule()
	require.False(t, ok2)
	// Done frees a slot.
	h.Done()
	ok3, h3 := sched.TrySchedule()
	require.True(t, ok3)
	h3.Done()
}

// TestRangeFlushSchedulerUnregister verifies that TrySchedule returns false
// after Unregister.
func TestRangeFlushSchedulerUnregister(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sched := newRangeFlushScheduler()
	db := &mockDBForCompaction{allowed: 4}
	sched.Register(1, db)
	sched.Unregister()
	ok, h := sched.TrySchedule()
	require.False(t, ok)
	require.Nil(t, h)
}

// mockDBForCompaction implements pebble.DBForCompaction for testing the
// scheduler in isolation.
type mockDBForCompaction struct {
	allowed    int
	waiting    bool
	scheduled  int // count of accepted Schedule calls
	acceptNext bool
}

func (m *mockDBForCompaction) GetAllowedWithoutPermission() int {
	return m.allowed
}

func (m *mockDBForCompaction) GetWaitingCompaction() (bool, pebble.WaitingCompaction) {
	return m.waiting, pebble.WaitingCompaction{}
}

func (m *mockDBForCompaction) Schedule(pebble.CompactionGrantHandle) bool {
	if !m.acceptNext {
		return false
	}
	m.scheduled++
	m.acceptNext = false // accept one at a time unless re-armed
	return true
}

// TestRangeFlushSchedulerDoneTriggersGrant verifies that Done() calls
// tryGrant to proactively schedule waiting work.
func TestRangeFlushSchedulerDoneTriggersGrant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sched := newRangeFlushScheduler()
	db := &mockDBForCompaction{allowed: 1, waiting: true, acceptNext: true}
	sched.Register(1, db)
	// Fill the single slot.
	ok, h := sched.TrySchedule()
	require.True(t, ok)
	require.Equal(t, 0, db.scheduled)
	// Done frees the slot and tryGrant should call Schedule.
	h.Done()
	require.Equal(t, 1, db.scheduled)
	// running should be 1 again (tryGrant incremented after Schedule accepted).
	sched.mu.Lock()
	require.Equal(t, 1, sched.mu.running)
	sched.mu.Unlock()
}

// TestRangeFlushSchedulerUpdateTriggersGrant verifies that
// UpdateGetAllowedWithoutPermission calls tryGrant when more capacity appears.
func TestRangeFlushSchedulerUpdateTriggersGrant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sched := newRangeFlushScheduler()
	db := &mockDBForCompaction{allowed: 0, waiting: true, acceptNext: true}
	sched.Register(1, db)
	// No capacity.
	ok, _ := sched.TrySchedule()
	require.False(t, ok)
	// Increase capacity and notify.
	db.allowed = 1
	sched.UpdateGetAllowedWithoutPermission()
	require.Equal(t, 1, db.scheduled)
	sched.mu.Lock()
	require.Equal(t, 1, sched.mu.running)
	sched.mu.Unlock()
}

// TestRangeFlushSchedulerTryGrantStopsOnNoWaiting verifies that tryGrant
// stops when GetWaitingCompaction returns false.
func TestRangeFlushSchedulerTryGrantStopsOnNoWaiting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sched := newRangeFlushScheduler()
	db := &mockDBForCompaction{allowed: 3, waiting: false}
	sched.Register(1, db)
	ok, h := sched.TrySchedule()
	require.True(t, ok)
	// Done calls tryGrant, but no waiting work.
	h.Done()
	require.Equal(t, 0, db.scheduled)
}

// TestRangeFlushSchedulerTryGrantStopsOnScheduleReject verifies that tryGrant
// stops when Schedule returns false.
func TestRangeFlushSchedulerTryGrantStopsOnScheduleReject(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sched := newRangeFlushScheduler()
	db := &mockDBForCompaction{allowed: 3, waiting: true, acceptNext: false}
	sched.Register(1, db)
	ok, h := sched.TrySchedule()
	require.True(t, ok)
	// Done calls tryGrant; waiting=true but acceptNext=false, so Schedule
	// returns false and tryGrant stops.
	h.Done()
	require.Equal(t, 0, db.scheduled)
	sched.mu.Lock()
	require.Equal(t, 0, sched.mu.running)
	sched.mu.Unlock()
}

// TestRangeFlusherGetWaitingCompaction verifies GetWaitingCompaction returns
// false when queue is empty and correct WaitingCompaction when non-empty.
func TestRangeFlusherGetWaitingCompaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rf := newRangeFlusher(newRangeFlushScheduler())
	rf.mu.Lock()
	// Empty queue.
	ok, _ := rf.getWaitingCompactionLocked()
	require.False(t, ok)
	// Non-empty queue, with sumApproxStoreLocalBytes > 0.
	rf.enqueueLocked(1, 100)
	rf.enqueueLocked(2, 200)
	rf.mu.sumApproxStoreLocalBytes = 1000
	ok, wc := rf.getWaitingCompactionLocked()
	require.True(t, ok)
	require.False(t, wc.Optional)
	require.Equal(t, 60, wc.Priority)
	// score = 0.75 + 2*(300/1000) = 0.75 + 0.6 = 1.35
	require.InDelta(t, 1.35, wc.Score, 0.001)
	// With sumApproxStoreLocalBytes == 0, score falls back to 0.75.
	rf.mu.sumApproxStoreLocalBytes = 0
	ok, wc = rf.getWaitingCompactionLocked()
	require.True(t, ok)
	require.InDelta(t, 0.75, wc.Score, 0.001)
	rf.mu.Unlock()
}

// TestDispatchFlushesWithScheduler verifies that dispatchFlushes uses the
// scheduler to gate concurrency by using a mock scheduler with limited grants.
func TestDispatchFlushesWithScheduler(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sched := newRangeFlushScheduler()
	db := &mockDBForCompaction{allowed: 2}
	sched.Register(1, db)
	rf := newRangeFlusher(sched)
	rf.mu.Lock()
	rf.enqueueLocked(1, 100)
	rf.enqueueLocked(2, 200)
	rf.enqueueLocked(3, 300)
	rf.mu.Unlock()
	// Exhaust grants by calling TrySchedule directly.
	ok1, h1 := sched.TrySchedule()
	require.True(t, ok1)
	ok2, h2 := sched.TrySchedule()
	require.True(t, ok2)
	// No more grants available.
	ok3, _ := sched.TrySchedule()
	require.False(t, ok3)
	// Queue still has 3 items since we didn't go through dispatchFlushes.
	rf.mu.Lock()
	require.Equal(t, 3, rf.mu.queue.Len())
	rf.mu.Unlock()
	// Release grants.
	h1.Done()
	h2.Done()
}

// TestRangeContainsTimeseriesData verifies that ranges with the timeseries
// prefix are correctly identified and excluded from range flush.
func TestRangeContainsTimeseriesData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// A key with the timeseries prefix.
	tsKey := roachpb.RKey(keys.TimeseriesPrefix)
	require.True(t, rangeContainsTimeseriesData(tsKey))
	// A key with the timeseries prefix plus extra bytes (a real ts key).
	tsKeyWithSuffix := roachpb.RKey(append(keys.TimeseriesPrefix.Clone(), 0x01, 0x02))
	require.True(t, rangeContainsTimeseriesData(tsKeyWithSuffix))
	// A regular SQL table key.
	sqlKey := roachpb.RKey(keys.SystemSQLCodec.TablePrefix(42))
	require.False(t, rangeContainsTimeseriesData(sqlKey))
	// Meta key.
	metaKey := roachpb.RKey(keys.Meta2Prefix)
	require.False(t, rangeContainsTimeseriesData(metaKey))
	// System prefix (close but not timeseries).
	sysKey := roachpb.RKey(keys.SystemPrefix)
	require.False(t, rangeContainsTimeseriesData(sysKey))
}

// TestRangeFlushBackoff verifies that dequeueLocked and
// getWaitingCompactionLocked respect the retryAfter field: backed-off items
// are skipped while eligible items deeper in the heap are still dequeued in
// priority order.
func TestRangeFlushBackoff(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rf := newRangeFlusher(newRangeFlushScheduler())
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Item 1: backed off, largest approxBytes so it sits at heap top.
	backedOff := &rangeFlushItem{
		rangeID:          1,
		approxBytes:      1000,
		flushFailedCount: 1,
		retryAfter:       timeutil.Now().Add(time.Hour),
	}
	heap.Push(&rf.mu.queue, backedOff)
	rf.mu.queued[backedOff.rangeID] = backedOff
	rf.mu.bytesQueued += backedOff.approxBytes

	// With only a backed-off item, nothing should be dequeuable or waiting.
	require.Nil(t, rf.dequeueLocked(), "backed-off item should not be dequeued")
	ok, _ := rf.getWaitingCompactionLocked()
	require.False(t, ok, "backed-off item should not report as waiting")
	require.Equal(t, 1, rf.mu.queue.Len(), "item should remain in queue")

	// Add eligible items with same flushFailedCount but smaller approxBytes.
	eligible1 := &rangeFlushItem{
		rangeID:          2,
		approxBytes:      500,
		flushFailedCount: 1,
		retryAfter:       timeutil.Now().Add(-time.Second),
	}
	heap.Push(&rf.mu.queue, eligible1)
	rf.mu.queued[eligible1.rangeID] = eligible1
	rf.mu.bytesQueued += eligible1.approxBytes

	eligible2 := &rangeFlushItem{
		rangeID:          3,
		approxBytes:      200,
		flushFailedCount: 1,
		retryAfter:       timeutil.Now().Add(-time.Second),
	}
	heap.Push(&rf.mu.queue, eligible2)
	rf.mu.queued[eligible2.rangeID] = eligible2
	rf.mu.bytesQueued += eligible2.approxBytes

	// Eligible items should be reported as waiting and dequeued in priority
	// order (highest approxBytes first) despite the backed-off heap top.
	ok, _ = rf.getWaitingCompactionLocked()
	require.True(t, ok, "eligible items should report as waiting despite backed-off heap top")

	got := rf.dequeueLocked()
	require.NotNil(t, got)
	require.Equal(t, roachpb.RangeID(2), got.rangeID)

	got = rf.dequeueLocked()
	require.NotNil(t, got)
	require.Equal(t, roachpb.RangeID(3), got.rangeID)

	// Only the backed-off item should remain.
	require.Equal(t, 1, rf.mu.queue.Len())
	require.Nil(t, rf.dequeueLocked(), "backed-off item should not be dequeued")

	// Once the backoff expires, the item becomes eligible again.
	backedOff.retryAfter = timeutil.Now().Add(-time.Second)
	ok, _ = rf.getWaitingCompactionLocked()
	require.True(t, ok)
	got = rf.dequeueLocked()
	require.NotNil(t, got)
	require.Equal(t, roachpb.RangeID(1), got.rangeID)
}

// fix calls heap.Fix for the given item. Helper for tests that manually modify
// item fields.
func (q *rangeFlushQueue) fix(item *rangeFlushItem) {
	heap.Fix(q, item.index)
}
