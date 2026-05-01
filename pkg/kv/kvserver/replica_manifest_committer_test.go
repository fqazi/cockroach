// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestFileNumStash_Available(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("empty stash", func(t *testing.T) {
		stash := fileNumStash{}
		require.Equal(t, 0, stash.available())
	})

	t.Run("stash with range", func(t *testing.T) {
		stash := fileNumStash{
			firstFileNum: 10,
			endFileNum:   20,
		}
		require.Equal(t, 10, stash.available())
	})

	t.Run("stash fully consumed", func(t *testing.T) {
		stash := fileNumStash{
			firstFileNum: 20,
			endFileNum:   20,
		}
		require.Equal(t, 0, stash.available())
	})

	t.Run("large range", func(t *testing.T) {
		stash := fileNumStash{
			firstFileNum: 1000,
			endFileNum:   2000,
		}
		require.Equal(t, 1000, stash.available())
	})
}

func TestFileNumStash_Allocate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	stash := fileNumStash{
		firstFileNum: storage.DiskFileNum(10),
		endFileNum:   storage.DiskFileNum(20),
	}

	// Allocate 3 file numbers.
	result := stash.allocate(3)
	require.Equal(t, []storage.DiskFileNum{10, 11, 12}, result)
	require.Equal(t, 7, stash.available())
	require.Equal(t, storage.DiskFileNum(13), stash.firstFileNum)

	// Allocate remaining 7 file numbers.
	result = stash.allocate(7)
	require.Equal(t, []storage.DiskFileNum{13, 14, 15, 16, 17, 18, 19}, result)
	require.Equal(t, 0, stash.available())
	require.Equal(t, storage.DiskFileNum(20), stash.firstFileNum)
}

func TestRangeFlush_MutualExclusion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	tc := testContext{manualClock: timeutil.NewManualTime(timeutil.Unix(0, 123))}
	cfg := TestStoreConfig(hlc.NewClockForTesting(tc.manualClock))
	cfg.TestingKnobs.DisableAutomaticLeaseRenewal = true
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	// Set ongoingFlush to true.
	tc.repl.rangeFlushMu.Lock()
	tc.repl.rangeFlushMu.ongoingFlush = true
	tc.repl.rangeFlushMu.Unlock()

	// RangeFlush should return an error.
	err := tc.repl.ManifestCommitter().RangeFlush()
	require.Error(t, err)
	require.Contains(t, err.Error(), "already in progress")

	// Clean up.
	tc.repl.rangeFlushMu.Lock()
	tc.repl.rangeFlushMu.ongoingFlush = false
	tc.repl.rangeFlushMu.Unlock()
}

// TestRangeFlush_StaleSnapshotDetection verifies that RangeFlush detects
// when rangeFlushMu.snapshot was set by a different FlushPrepare (e.g., a
// foreign FlushPrepare rerouted after lease transfer). The test injects a
// pre-existing snapshot with a wrong flushStartedCount and verifies that
// RangeFlush returns an error instead of using the stale snapshot.
func TestRangeFlush_StaleSnapshotDetection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	tc := testContext{manualClock: timeutil.NewManualTime(timeutil.Unix(0, 123))}
	cfg := TestStoreConfig(hlc.NewClockForTesting(tc.manualClock))
	cfg.TestingKnobs.DisableAutomaticLeaseRenewal = true
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	// Split at a user-space key so the FlushPrepare request passes key
	// validation (keys must be > LocalMax).
	splitKey := roachpb.Key("a")
	_, pErr := tc.SendWrapped(&kvpb.AdminSplitRequest{
		RequestHeader: kvpb.RequestHeader{Key: splitKey},
		SplitKey:      splitKey,
	})
	require.NoError(t, pErr.GoError())
	repl := tc.store.LookupReplica(roachpb.RKey(splitKey))
	require.NotNil(t, repl)

	// Inject a snapshot with a wrong flushStartedCount to simulate a
	// foreign FlushPrepare having stored its snapshot via
	// prepareLocalResult before the local FlushPrepare applies.
	staleSnap := tc.store.StateEngine().NewSnapshot()
	repl.rangeFlushMu.Lock()
	repl.rangeFlushMu.snapshot = staleSnap
	repl.rangeFlushMu.flushStartedCount = 999
	repl.rangeFlushMu.Unlock()

	// RangeFlush sends a FlushPrepare (FlushStartedCount becomes 1).
	// prepareLocalResult sees snapshot != nil and skips. RangeFlush
	// picks up the stale snapshot and detects 999 != 1.
	err := repl.ManifestCommitter().RangeFlush()
	require.Error(t, err)
	require.Contains(t, err.Error(), "wrong FlushPrepare")

	// Verify cleanup.
	repl.rangeFlushMu.Lock()
	require.Nil(t, repl.rangeFlushMu.snapshot)
	require.False(t, repl.rangeFlushMu.ongoingFlush)
	require.Equal(t, uint64(0), repl.rangeFlushMu.flushStartedCount)
	repl.rangeFlushMu.Unlock()
}

func TestRangeFlush_LeaseCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	tc := testContext{manualClock: timeutil.NewManualTime(timeutil.Unix(0, 123))}
	cfg := TestStoreConfig(hlc.NewClockForTesting(tc.manualClock))
	cfg.TestingKnobs.DisableAutomaticLeaseRenewal = true
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	// Transfer lease to a second replica so this one is not the leaseholder.
	secondReplica, err := tc.addBogusReplicaToRangeDesc(ctx)
	require.NoError(t, err)
	tc.manualClock.MustAdvanceTo(leaseExpiry(tc.repl))
	start := tc.Clock().NowAsClockTimestamp()
	err = sendLeaseRequest(tc.repl, &roachpb.Lease{
		ProposedTS: start,
		Start:      start,
		Expiration: start.ToTimestamp().Add(10, 0).Clone(),
		Replica:    secondReplica,
	})
	require.NoError(t, err)

	// RangeFlush should fail because we're not the leaseholder.
	err = tc.repl.ManifestCommitter().RangeFlush()
	require.Error(t, err)
	require.Contains(t, err.Error(), "not the leaseholder")
}

func TestFileNumStash_StashExtension(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Test extension: when new allocation is contiguous with existing stash,
	// only endFileNum is updated, preserving existing unconsumed numbers.
	t.Run("contiguous extension", func(t *testing.T) {
		stash := fileNumStash{
			firstFileNum: storage.DiskFileNum(10),
			endFileNum:   storage.DiskFileNum(15),
		}
		// Simulate: consumed some, got contiguous allocation [15, 25).
		// Since endFileNum == first (15 == 15), we extend.
		first := storage.DiskFileNum(15)
		end := storage.DiskFileNum(25)
		if stash.endFileNum == first {
			stash.endFileNum = end
		} else {
			stash.firstFileNum = first
			stash.endFileNum = end
		}
		// Stash now covers [10, 25).
		require.Equal(t, storage.DiskFileNum(10), stash.firstFileNum)
		require.Equal(t, storage.DiskFileNum(25), stash.endFileNum)
		require.Equal(t, 15, stash.available())
	})

	// Test non-contiguous: when new allocation is not contiguous (e.g., after
	// lease change), both firstFileNum and endFileNum are replaced.
	t.Run("non-contiguous replacement", func(t *testing.T) {
		stash := fileNumStash{
			firstFileNum: storage.DiskFileNum(10),
			endFileNum:   storage.DiskFileNum(15),
		}
		// Non-contiguous allocation [100, 200).
		first := storage.DiskFileNum(100)
		end := storage.DiskFileNum(200)
		if stash.endFileNum == first {
			stash.endFileNum = end
		} else {
			stash.firstFileNum = first
			stash.endFileNum = end
		}
		// Stash is completely replaced.
		require.Equal(t, storage.DiskFileNum(100), stash.firstFileNum)
		require.Equal(t, storage.DiskFileNum(200), stash.endFileNum)
		require.Equal(t, 100, stash.available())
	})
}
