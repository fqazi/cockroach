// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestNewCombinedBatch verifies that NewCombinedBatch creates a combined batch
// when an RSEngine is present and a plain batch when it is not.
func TestNewCombinedBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	tc := testContext{manualClock: timeutil.NewManualTime(timeutil.Unix(0, 123))}
	cfg := TestStoreConfig(hlc.NewClockForTesting(tc.manualClock))
	// These tests use mock RSEngines; disable the defaults from TestStoreConfig.
	cfg.BasaltFS = nil
	cfg.OpenRSEngine = nil
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	t.Run("without-rsengine", func(t *testing.T) {
		tc.repl.rsStateMu.RLock()
		require.Nil(t, tc.repl.rsStateMu.rsEngine)
		tc.repl.rsStateMu.RUnlock()
		batch, err := tc.repl.NewCombinedBatch(fs.BatchEvalReadCategory)
		require.NoError(t, err)
		defer batch.Close()
		require.True(t, batch.ConsistentIterators())
	})

	t.Run("with-rsengine", func(t *testing.T) {
		mockEngine := &mockRSEngine{
			manifestInfo: storage.ManifestInfo{
				Manifest: storage.FileNameAndNum{Name: "MANIFEST-000010", Num: 10},
			},
		}
		tc.repl.rsStateMu.Lock()
		tc.repl.rsStateMu.rsEngine = mockEngine
		tc.repl.rsStateMu.Unlock()
		defer func() {
			tc.repl.rsStateMu.Lock()
			tc.repl.rsStateMu.rsEngine = nil
			tc.repl.rsStateMu.Unlock()
		}()
		batch, err := tc.repl.NewCombinedBatch(fs.BatchEvalReadCategory)
		require.NoError(t, err)
		// Verify an RSEngine snapshot was created and ownership transferred
		// to the combined batch (not yet closed).
		snapshots := mockEngine.getSnapshots()
		require.Len(t, snapshots, 1)
		require.False(t, snapshots[0].closed)
		// Close the batch and verify the RSEngineSnapshot is closed.
		batch.Close()
		require.True(t, snapshots[0].closed)
	})
}

// TestNewCombinedReadOnly verifies that NewCombinedReadOnly creates a combined
// ReadWriter when an RSEngine is present and a plain ReadWriter when it is not.
func TestNewCombinedReadOnly(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	tc := testContext{manualClock: timeutil.NewManualTime(timeutil.Unix(0, 123))}
	cfg := TestStoreConfig(hlc.NewClockForTesting(tc.manualClock))
	// These tests use mock RSEngines; disable the defaults from TestStoreConfig.
	cfg.BasaltFS = nil
	cfg.OpenRSEngine = nil
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	t.Run("without-rsengine", func(t *testing.T) {
		rw, err := tc.repl.NewCombinedReadOnly(fs.BatchEvalReadCategory)
		require.NoError(t, err)
		defer rw.Close()
		require.True(t, rw.ConsistentIterators())
	})

	t.Run("with-rsengine", func(t *testing.T) {
		mockEngine := &mockRSEngine{
			manifestInfo: storage.ManifestInfo{
				Manifest: storage.FileNameAndNum{Name: "MANIFEST-000020", Num: 20},
			},
		}
		tc.repl.rsStateMu.Lock()
		tc.repl.rsStateMu.rsEngine = mockEngine
		tc.repl.rsStateMu.Unlock()
		defer func() {
			tc.repl.rsStateMu.Lock()
			tc.repl.rsStateMu.rsEngine = nil
			tc.repl.rsStateMu.Unlock()
		}()
		rw, err := tc.repl.NewCombinedReadOnly(fs.BatchEvalReadCategory)
		require.NoError(t, err)
		snapshots := mockEngine.getSnapshots()
		require.Len(t, snapshots, 1)
		require.False(t, snapshots[0].closed)
		rw.Close()
		require.True(t, snapshots[0].closed)
	})
}

// TestNewCombinedSnapshot verifies that NewCombinedSnapshot creates a combined
// Reader when an RSEngine is present and a plain Reader when it is not.
func TestNewCombinedSnapshot(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	tc := testContext{manualClock: timeutil.NewManualTime(timeutil.Unix(0, 123))}
	cfg := TestStoreConfig(hlc.NewClockForTesting(tc.manualClock))
	// These tests use mock RSEngines; disable the defaults from TestStoreConfig.
	cfg.BasaltFS = nil
	cfg.OpenRSEngine = nil
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	t.Run("without-rsengine", func(t *testing.T) {
		snap := tc.repl.NewCombinedSnapshot()
		defer snap.Close()
		// Basic sanity: the snapshot should be usable.
		require.True(t, snap.ConsistentIterators())
	})

	t.Run("with-rsengine", func(t *testing.T) {
		mockEngine := &mockRSEngine{
			manifestInfo: storage.ManifestInfo{
				Manifest: storage.FileNameAndNum{Name: "MANIFEST-000030", Num: 30},
			},
		}
		tc.repl.rsStateMu.Lock()
		tc.repl.rsStateMu.rsEngine = mockEngine
		tc.repl.rsStateMu.Unlock()
		defer func() {
			tc.repl.rsStateMu.Lock()
			tc.repl.rsStateMu.rsEngine = nil
			tc.repl.rsStateMu.Unlock()
		}()
		snap := tc.repl.NewCombinedSnapshot()
		snapshots := mockEngine.getSnapshots()
		require.Len(t, snapshots, 1)
		require.False(t, snapshots[0].closed)
		snap.Close()
		require.True(t, snapshots[0].closed)
	})
}
