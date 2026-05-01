// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// Note on test approach: These tests use direct injection of mockRSEngine into
// Replica.rsStateMu.rsEngine because they test behavior after an RSEngine is
// established. The StoreConfig.OpenRSEngine injection is useful when testing
// the full initialization path with non-zero RSManifestNum.

// mockRSEngineSnapshot implements storage.RSEngineSnapshot for testing.
type mockRSEngineSnapshot struct {
	engine       *mockRSEngine
	manifestInfo storage.ManifestInfo
	closed       bool
}

var _ storage.RSEngineSnapshot = (*mockRSEngineSnapshot)(nil)

func (s *mockRSEngineSnapshot) ManifestInfo() storage.ManifestInfo { return s.manifestInfo }
func (s *mockRSEngineSnapshot) ManifestNum() storage.DiskFileNum   { return s.manifestInfo.Manifest.Num }
func (s *mockRSEngineSnapshot) Clone() storage.RSEngineSnapshot {
	s.engine.mu.Lock()
	defer s.engine.mu.Unlock()
	c := &mockRSEngineSnapshot{engine: s.engine, manifestInfo: s.manifestInfo}
	s.engine.mu.snapshots = append(s.engine.mu.snapshots, c)
	return c
}
func (s *mockRSEngineSnapshot) Split(
	ctx context.Context, splitKey roachpb.Key, rhsDir string,
) (lhsManifest storage.FileNameAndNum, rhs storage.ManifestInfo, nextFileNum uint64, err error) {
	return storage.FileNameAndNum{}, storage.ManifestInfo{}, 0, nil
}
func (s *mockRSEngineSnapshot) Merge(
	ctx context.Context, rhs storage.RSEngineSnapshot,
) (merged storage.ManifestInfo, nextFileNum uint64, err error) {
	return storage.ManifestInfo{}, 0, nil
}
func (s *mockRSEngineSnapshot) Close() { s.closed = true }

// mockRSEngine is a mock implementation of storage.RSEngine for testing
// CompactionToggle behavior during lease transitions.
type mockRSEngine struct {
	manifestInfo storage.ManifestInfo
	mu           struct {
		sync.Mutex
		compactionToggleCalls []bool
		snapshots             []*mockRSEngineSnapshot
	}
}

var _ storage.RSEngine = (*mockRSEngine)(nil)

func (m *mockRSEngine) CompactionToggle(enable bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.compactionToggleCalls = append(m.mu.compactionToggleCalls, enable)
}

func (m *mockRSEngine) getCompactionToggleCalls() []bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]bool, len(m.mu.compactionToggleCalls))
	copy(result, m.mu.compactionToggleCalls)
	return result
}

func (m *mockRSEngine) EnableUnreferencedFileDeletion() {}

func (m *mockRSEngine) WaitForOngoingManifestChanges() {}

func (m *mockRSEngine) CurrentManifestNum() storage.DiskFileNum {
	return m.manifestInfo.Manifest.Num
}

func (m *mockRSEngine) FlushSSTables(
	scratchNames []string, flushCommit *storage.FlushCommitInfo,
) error {
	return nil
}

func (m *mockRSEngine) AddSSTables(scratchNames []string) error { return nil }

func (m *mockRSEngine) Ref() {}

func (m *mockRSEngine) Unref() {}

func (m *mockRSEngine) NewSnapshot() storage.RSEngineSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	snap := &mockRSEngineSnapshot{engine: m, manifestInfo: m.manifestInfo}
	m.mu.snapshots = append(m.mu.snapshots, snap)
	return snap
}

func (m *mockRSEngine) getSnapshots() []*mockRSEngineSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]*mockRSEngineSnapshot, len(m.mu.snapshots))
	copy(result, m.mu.snapshots)
	return result
}

func (m *mockRSEngine) PrepareExternalManifest(manifestNum storage.DiskFileNum) error {
	return nil
}

func (m *mockRSEngine) InstallPreparedManifest(manifestNum storage.DiskFileNum) {
	panic("mockRSEngine does not support in-place manifest install")
}

func (m *mockRSEngine) Quiesce() {}

func (m *mockRSEngine) Close() {}

func (m *mockRSEngine) TestingInnerEngine() storage.InnerRSEngine { return nil }

// TestCompactionToggleOnLeaseTransition verifies that CompactionToggle is
// called on the RSEngine when a replica acquires or loses the lease.
func TestCompactionToggleOnLeaseTransition(t *testing.T) {
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
	cfg.TestingKnobs.DisableAutomaticLeaseRenewal = true
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	// Add a second replica descriptor for lease transfer tests.
	secondReplica, err := tc.addBogusReplicaToRangeDesc(ctx)
	require.NoError(t, err)

	// Inject the mock RSEngine into the replica.
	mockEngine := &mockRSEngine{}
	tc.repl.rsStateMu.Lock()
	tc.repl.rsStateMu.rsEngine = mockEngine
	tc.repl.rsStateMu.Unlock()

	// The replica should be the leaseholder at this point. Let's verify
	// CompactionToggle was called when the lease was initially acquired.
	// Note: The initial lease acquisition happens during store startup,
	// before we inject the mock. So we'll test lease transitions instead.

	// Clear any existing calls.
	mockEngine.mu.Lock()
	mockEngine.mu.compactionToggleCalls = nil
	mockEngine.mu.Unlock()

	// Transfer the lease away to another replica.
	tc.manualClock.MustAdvanceTo(leaseExpiry(tc.repl))
	start := tc.Clock().NowAsClockTimestamp()
	err = sendLeaseRequest(tc.repl, &roachpb.Lease{
		ProposedTS: start,
		Start:      start,
		Expiration: start.ToTimestamp().Add(10, 0).Clone(),
		Replica:    secondReplica,
	})
	require.NoError(t, err)

	// Verify CompactionToggle(false) was called when we lost the lease.
	calls := mockEngine.getCompactionToggleCalls()
	require.NotEmpty(t, calls, "expected CompactionToggle to be called when losing lease")
	require.False(t, calls[len(calls)-1], "expected CompactionToggle(false) when losing lease")

	// Now acquire the lease back.
	tc.manualClock.MustAdvanceTo(leaseExpiry(tc.repl))
	start = tc.Clock().NowAsClockTimestamp()
	thisReplica, err := tc.repl.GetReplicaDescriptor()
	require.NoError(t, err)
	err = sendLeaseRequest(tc.repl, &roachpb.Lease{
		ProposedTS: start,
		Start:      start,
		Expiration: start.ToTimestamp().Add(10, 0).Clone(),
		Replica:    thisReplica,
	})
	require.NoError(t, err)

	// Verify CompactionToggle(true) was called when we acquired the lease.
	calls = mockEngine.getCompactionToggleCalls()
	require.NotEmpty(t, calls, "expected CompactionToggle to be called when acquiring lease")
	require.True(t, calls[len(calls)-1], "expected CompactionToggle(true) when acquiring lease")
}

// TestCompactionToggleIdempotent verifies that CompactionToggle can be called
// multiple times with the same value (idempotent behavior).
func TestCompactionToggleIdempotent(t *testing.T) {
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
	cfg.TestingKnobs.DisableAutomaticLeaseRenewal = true
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	// Inject the mock RSEngine into the replica.
	mockEngine := &mockRSEngine{}
	tc.repl.rsStateMu.Lock()
	tc.repl.rsStateMu.rsEngine = mockEngine
	tc.repl.rsStateMu.Unlock()

	// Clear any existing calls.
	mockEngine.mu.Lock()
	mockEngine.mu.compactionToggleCalls = nil
	mockEngine.mu.Unlock()

	// Request a lease extension (same leaseholder). This should still call
	// CompactionToggle with the same value (idempotent).
	tc.manualClock.MustAdvanceTo(leaseExpiry(tc.repl))
	start := tc.Clock().NowAsClockTimestamp()
	thisReplica, err := tc.repl.GetReplicaDescriptor()
	require.NoError(t, err)
	err = sendLeaseRequest(tc.repl, &roachpb.Lease{
		ProposedTS: start,
		Start:      start,
		Expiration: start.ToTimestamp().Add(10, 0).Clone(),
		Replica:    thisReplica,
	})
	require.NoError(t, err)

	// Verify CompactionToggle was called (idempotent - we're still the leaseholder).
	calls := mockEngine.getCompactionToggleCalls()
	require.NotEmpty(t, calls, "expected CompactionToggle to be called on lease extension")
	require.True(t, calls[len(calls)-1], "expected CompactionToggle(true) since we're still leaseholder")
}

// TestPrepareSplitManifestsNoRSEngine verifies that prepareSplitManifests
// returns nil when the replica has no RSEngine.
func TestPrepareSplitManifestsNoRSEngine(t *testing.T) {
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

	// Ensure no RSEngine is set (default state).
	tc.repl.rsStateMu.Lock()
	require.Nil(t, tc.repl.rsStateMu.rsEngine)
	tc.repl.rsStateMu.Unlock()

	// Create a fake RHS descriptor.
	rhsDesc := &roachpb.RangeDescriptor{
		RangeID:  tc.repl.RangeID + 1,
		StartKey: roachpb.RKey("m"),
		EndKey:   roachpb.RKey("z"),
	}

	// prepareSplitManifests should return nil when there's no RSEngine.
	manifestInfo, err := tc.repl.prepareSplitManifests(ctx, roachpb.Key("m"), rhsDesc)
	require.NoError(t, err)
	require.Nil(t, manifestInfo)
}

// TestPrepareSplitManifestsFailsEarlyWithoutBasaltFS verifies that
// prepareSplitManifests fails early when BasaltFS is not configured, without
// disabling compactions (since there's no work to do).
func TestPrepareSplitManifestsFailsEarlyWithoutBasaltFS(t *testing.T) {
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

	// Inject mock RSEngine with manifest info (RSEngine tracks its own manifest number).
	mockEngine := &mockRSEngine{
		manifestInfo: storage.ManifestInfo{
			Manifest: storage.FileNameAndNum{Name: "MANIFEST-000010", Num: 10},
		},
	}
	tc.repl.rsStateMu.Lock()
	tc.repl.rsStateMu.rsEngine = mockEngine
	tc.repl.rsStateMu.Unlock()

	// Create a fake RHS descriptor with the same replica as the LHS.
	desc := tc.repl.Desc()
	rhsDesc := &roachpb.RangeDescriptor{
		RangeID:  tc.repl.RangeID + 1,
		StartKey: roachpb.RKey("m"),
		EndKey:   roachpb.RKey("z"),
	}
	for _, repl := range desc.Replicas().Descriptors() {
		rhsDesc.AddReplica(repl.NodeID, repl.StoreID, repl.Type)
	}

	// Call prepareSplitManifests. This will fail early because BasaltFS is not
	// configured. The refactored code checks BasaltFS before disabling
	// compactions to avoid unnecessary toggle calls.
	_, err := tc.repl.prepareSplitManifests(ctx, roachpb.Key("m"), rhsDesc)
	// Expected to fail because BasaltFS is not configured.
	require.Error(t, err)
	require.Contains(t, err.Error(), "basaltFS is not configured")

	// Verify CompactionToggle was NOT called - we fail early without toggling.
	calls := mockEngine.getCompactionToggleCalls()
	require.Empty(t, calls, "expected no CompactionToggle calls when BasaltFS is not configured")
}

// TestEnableCompactionsAfterSplit verifies that enableCompactionsAfterSplit
// re-enables compactions on the RSEngine.
func TestEnableCompactionsAfterSplit(t *testing.T) {
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

	// Inject mock RSEngine.
	mockEngine := &mockRSEngine{}
	tc.repl.rsStateMu.Lock()
	tc.repl.rsStateMu.rsEngine = mockEngine
	tc.repl.rsStateMu.Unlock()

	// Clear any previous calls.
	mockEngine.mu.Lock()
	mockEngine.mu.compactionToggleCalls = nil
	mockEngine.mu.Unlock()

	// Call enableCompactionsAfterSplit.
	tc.repl.enableCompactionsAfterSplit()

	// Verify CompactionToggle(true) was called.
	calls := mockEngine.getCompactionToggleCalls()
	require.Len(t, calls, 1)
	require.True(t, calls[0], "expected CompactionToggle(true)")
}

// TestEnableCompactionsAfterSplitNoRSEngine verifies that enableCompactionsAfterSplit
// is a no-op when there's no RSEngine.
func TestEnableCompactionsAfterSplitNoRSEngine(t *testing.T) {
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

	// Ensure no RSEngine is set.
	tc.repl.rsStateMu.Lock()
	require.Nil(t, tc.repl.rsStateMu.rsEngine)
	tc.repl.rsStateMu.Unlock()

	// This should not panic.
	tc.repl.enableCompactionsAfterSplit()
}

// TestGetSnapshotWithRSEngine verifies that GetSnapshot captures an RSEngineSnapshot
// when the replica has an RSEngine configured.
func TestGetSnapshotWithRSEngine(t *testing.T) {
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

	// Create a mock RSEngine with specific manifest info.
	mockEngine := &mockRSEngine{
		manifestInfo: storage.ManifestInfo{
			Manifest: storage.FileNameAndNum{Name: "MANIFEST-000123", Num: 123},
			Files: []storage.FileNameAndNum{
				{Name: "000100.sst", Num: 100},
				{Name: "000101.sst", Num: 101},
			},
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

	// Get a snapshot.
	snap, err := tc.repl.GetSnapshot(ctx, uuid.NewV4())
	require.NoError(t, err)
	defer snap.Close()

	// Verify the RSEngineSnapshot was captured with correct manifest info.
	require.NotNil(t, snap.rsEngineSnap, "expected rsEngineSnap to be non-nil")
	require.Equal(t, uint64(123), snap.RSManifestDiskFileNum, "expected manifest num 123")
	require.True(t, snap.CanHaveDormantRangeDel, "expected CanHaveDormantRangeDel when rsEngine is non-nil")
	info := snap.rsEngineSnap.ManifestInfo()
	require.Equal(t, "MANIFEST-000123", info.Manifest.Name)
	require.Equal(t, storage.DiskFileNum(123), info.Manifest.Num)
	require.Len(t, info.Files, 2)
}

// TestGetSnapshotWithoutRSEngine verifies that GetSnapshot works correctly
// when no RSEngine is configured.
func TestGetSnapshotWithoutRSEngine(t *testing.T) {
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

	// Ensure no RSEngine is set (default state).
	tc.repl.rsStateMu.Lock()
	require.Nil(t, tc.repl.rsStateMu.rsEngine)
	tc.repl.rsStateMu.Unlock()

	// Get a snapshot.
	snap, err := tc.repl.GetSnapshot(ctx, uuid.NewV4())
	require.NoError(t, err)
	defer snap.Close()

	// Verify rsEngineSnap is nil and RSManifestDiskFileNum is NoManifestNum.
	require.Nil(t, snap.rsEngineSnap, "expected rsEngineSnap to be nil when no RSEngine")
	require.Equal(t, uint64(storage.NoManifestNum), snap.RSManifestDiskFileNum, "expected NoManifestNum")
	require.False(t, snap.CanHaveDormantRangeDel, "expected no CanHaveDormantRangeDel without rsEngine")
}

// TestOutgoingSnapshotClose verifies that Close properly releases RSEngineSnapshot.
func TestOutgoingSnapshotClose(t *testing.T) {
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

	// Create a mock RSEngine.
	mockEngine := &mockRSEngine{
		manifestInfo: storage.ManifestInfo{
			Manifest: storage.FileNameAndNum{Name: "MANIFEST-000050", Num: 50},
		},
	}
	tc.repl.rsStateMu.Lock()
	tc.repl.rsStateMu.rsEngine = mockEngine
	tc.repl.rsStateMu.Unlock()
	// Remove the mock engine before teardown so background goroutines don't
	// create new combined readers (with Pebble snapshots) that would leak.
	defer func() {
		tc.repl.rsStateMu.Lock()
		tc.repl.rsStateMu.rsEngine = nil
		tc.repl.rsStateMu.Unlock()
	}()

	// Get a snapshot.
	snap, err := tc.repl.GetSnapshot(ctx, uuid.NewV4())
	require.NoError(t, err)

	// Find the snapshot created by GetSnapshot. Background operations may also
	// create snapshots via combined engine methods, so we can't rely on exact
	// counts. Instead, verify that at least one open snapshot exists that matches
	// the manifest number from our OutgoingSnapshot.
	snapshots := mockEngine.getSnapshots()
	require.NotEmpty(t, snapshots, "expected at least one snapshot")
	var mockSnap *mockRSEngineSnapshot
	for _, s := range snapshots {
		if !s.closed && s.manifestInfo.Manifest.Num == storage.DiskFileNum(snap.RSManifestDiskFileNum) {
			mockSnap = s
			break
		}
	}
	require.NotNil(t, mockSnap, "expected to find open snapshot matching GetSnapshot result")
	require.False(t, mockSnap.closed, "snapshot should not be closed yet")

	// Close the OutgoingSnapshot.
	snap.Close()

	// Verify the RSEngineSnapshot was closed.
	require.True(t, mockSnap.closed, "RSEngineSnapshot should be closed")
}

// TestCreateSnapshotHardlinks verifies that createSnapshotHardlinks creates
// the correct hardlinks for the RS manifest and files.
func TestCreateSnapshotHardlinks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Create an in-memory filesystem.
	memFS := vfs.NewMem()
	srcStoreID := roachpb.StoreID(1)
	srcRangeID := roachpb.RangeID(100)
	srcReplicaID := roachpb.ReplicaID(1)
	dstStoreID := roachpb.StoreID(2)
	dstRangeID := roachpb.RangeID(100)
	dstReplicaID := roachpb.ReplicaID(2)
	// Create source directory and files.
	srcDir := BasaltDir(memFS, srcStoreID, srcRangeID, srcReplicaID)
	require.NoError(t, memFS.MkdirAll(srcDir, 0755))
	// Create manifest file.
	manifestName := "MANIFEST-000050"
	manifestPath := memFS.PathJoin(srcDir, manifestName)
	f, err := memFS.Create(manifestPath, vfs.WriteCategoryUnspecified)
	require.NoError(t, err)
	_, err = f.Write([]byte("manifest data"))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	// Create SSTable files.
	sstFiles := []storage.FileNameAndNum{
		{Name: "000100.sst", Num: 100},
		{Name: "000101.sst", Num: 101},
	}
	for _, sst := range sstFiles {
		sstPath := memFS.PathJoin(srcDir, sst.Name)
		f, err := memFS.Create(sstPath, vfs.WriteCategoryUnspecified)
		require.NoError(t, err)
		_, err = f.Write([]byte("sst data"))
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}
	manifestInfo := storage.ManifestInfo{
		Manifest: storage.FileNameAndNum{Name: manifestName, Num: 50},
		Files:    sstFiles,
	}
	// Call createSnapshotHardlinks.
	err = createSnapshotHardlinks(
		memFS,
		srcStoreID, srcRangeID, srcReplicaID,
		dstStoreID, dstRangeID, dstReplicaID,
		manifestInfo,
	)
	require.NoError(t, err)
	// Verify destination directory was created.
	dstDir := BasaltDir(memFS, dstStoreID, dstRangeID, dstReplicaID)
	stat, err := memFS.Stat(dstDir)
	require.NoError(t, err)
	require.True(t, stat.IsDir())
	// Verify manifest was linked.
	dstManifestPath := memFS.PathJoin(dstDir, manifestName)
	stat, err = memFS.Stat(dstManifestPath)
	require.NoError(t, err)
	require.False(t, stat.IsDir())
	// Verify SSTable files were linked.
	for _, sst := range sstFiles {
		dstSstPath := memFS.PathJoin(dstDir, sst.Name)
		stat, err := memFS.Stat(dstSstPath)
		require.NoError(t, err)
		require.False(t, stat.IsDir())
	}
}
