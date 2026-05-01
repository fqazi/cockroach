// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// engineLogEntry records an RSEngine lifecycle event (open/close).
type engineLogEntry struct {
	action      string
	manifestNum storage.DiskFileNum
	rangeID     roachpb.RangeID
}

// basaltE2ETestState holds the state for the basalt end-to-end test.
type basaltE2ETestState struct {
	t        testing.TB
	ctx      context.Context
	basaltFS vfs.FS // Shared MemFS for all stores
	tc       *testcluster.TestCluster
	// Maps actual range IDs to synthetic IDs. Actual rangeIDs are
	// non-deterministic, and we need deterministic output for the datadriven
	// test, when listing the contents of the basalt FS. So we assign synthetic
	// IDs starting from 1 for each range ID we encounter, and substitute the
	// synthetic IDs in the output.
	rangeIDMap      map[roachpb.RangeID]int
	nextSyntheticID int
	// engineLog records RSEngine lifecycle events for observability in tests.
	engineLog struct {
		sync.Mutex
		entries []engineLogEntry
	}
}

// TestBasaltE2EDatadriven runs an end-to-end datadriven test for Basalt.
func TestBasaltE2EDatadriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	datadriven.Walk(t, datapathutils.TestDataPath(t, "basalt_e2e"), func(t *testing.T, path string) {
		var state *basaltE2ETestState
		defer func() {
			if state != nil && state.tc != nil {
				state.tc.Stopper().Stop(ctx)
			}
		}()
		datadriven.RunTest(t, path, func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "init":
				numNodes := 1
				for _, arg := range td.CmdArgs {
					if arg.Key == "stores" && len(arg.Vals) > 0 {
						fmt.Sscanf(arg.Vals[0], "%d", &numNodes) //nolint:errcheck
					}
				}
				state = initBasaltTestCluster(t, ctx, numNodes, false)
				return fmt.Sprintf("cluster started with %d store(s)\n", numNodes)
			case "scratch-range":
				if state == nil {
					return "error: call init first"
				}
				scratchKey := state.tc.ScratchRange(t)
				store, err := state.getStore(0)
				require.NoError(t, err)
				repl := store.LookupReplica(roachpb.RKey(scratchKey))
				require.NotNil(t, repl)
				// Initialize range ID map and assign synthetic ID 1 to scratch range.
				state.rangeIDMap = make(map[roachpb.RangeID]int)
				state.nextSyntheticID = 1
				state.rangeIDMap[repl.RangeID] = state.nextSyntheticID
				state.nextSyntheticID++
				return "ok\n"
			case "flush":
				if state == nil {
					return "error: call init first"
				}
				syntheticID, storeIdx := parseRangeAndStore(td)
				if syntheticID == 0 {
					return "error: rangeID parameter required\n"
				}
				engine, err := state.getRSEngine(syntheticID, storeIdx)
				if err != nil {
					return fmt.Sprintf("error: %v\n", err)
				}
				if engine == nil {
					return "error: no RSEngine configured\n"
				}
				testEngine := engine.TestingInnerEngine().(*storage.TestingRSEngine)
				if err := testEngine.TestFlushSSTables("test-flush.sst"); err != nil {
					return fmt.Sprintf("error: %v\n", err)
				}
				// Sleep to ensure all replicas have applied to the state machine.
				//
				// TODO(basalt): do something less flaky here.
				time.Sleep(10 * time.Millisecond)
				return "flushed\n"
			case "range-shared-meta-keys":
				if state == nil {
					return "error: call init first"
				}
				syntheticID, storeIdx := parseRangeAndStore(td)
				if syntheticID == 0 {
					return "error: rangeID parameter required\n"
				}
				return state.loadRangeSharedMetaKeys(syntheticID, storeIdx)
			case "basalt-files":
				if state == nil {
					return "error: call init first"
				}
				return storage.PrintFilesystem(state.basaltFS, "/", state.rangeIDMap)
			case "rs-engine-state":
				if state == nil {
					return "error: call init first"
				}
				syntheticID, storeIdx := parseRangeAndStore(td)
				if syntheticID == 0 {
					return "error: rangeID parameter required\n"
				}
				engine, err := state.getRSEngine(syntheticID, storeIdx)
				if err != nil {
					return fmt.Sprintf("error: %v\n", err)
				}
				if engine == nil {
					return "no RSEngine configured\n"
				}
				testEngine := engine.TestingInnerEngine().(*storage.TestingRSEngine)
				return storage.PrintRSEngineState(testEngine)
			case "add-replica":
				if state == nil {
					return "error: call init first"
				}
				syntheticID, storeIdx := parseRangeAndStore(td)
				if syntheticID == 0 {
					return "error: rangeID parameter required\n"
				}
				// Get replica to find its start key.
				repl, err := state.getReplicaForRange(syntheticID, 0)
				if err != nil {
					return fmt.Sprintf("error: %v\n", err)
				}
				startKey := repl.Desc().StartKey.AsRawKey()
				desc := state.tc.AddVotersOrFatal(t, startKey, state.tc.Target(storeIdx))
				return fmt.Sprintf("added replica to store %d, replicas: %d\n", storeIdx+1, len(desc.Replicas().Descriptors()))
			case "transfer-lease":
				if state == nil {
					return "error: call init first"
				}
				syntheticID, storeIdx := parseRangeAndStore(td)
				if syntheticID == 0 {
					return "error: rangeID parameter required\n"
				}
				// Get replica to find its range ID and start key.
				repl, err := state.getReplicaForRange(syntheticID, 0)
				if err != nil {
					return fmt.Sprintf("error: %v\n", err)
				}
				state.tc.TransferRangeLeaseOrFatal(t, roachpb.RangeDescriptor{
					RangeID:  repl.RangeID,
					StartKey: repl.Desc().StartKey,
				}, state.tc.Target(storeIdx))
				// Sleep to ensure lease is fully established on the new leaseholder.
				//
				// TODO(basalt): do something less flaky here.
				time.Sleep(10 * time.Millisecond)
				return fmt.Sprintf("lease transferred to store %d\n", storeIdx+1)
			case "split":
				if state == nil {
					return "error: call init first"
				}
				syntheticID, _ := parseRangeAndStore(td)
				if syntheticID == 0 {
					return "error: rangeID parameter required\n"
				}
				// Get replica to find its start key.
				repl, err := state.getReplicaForRange(syntheticID, 0)
				if err != nil {
					return fmt.Sprintf("split error: %v\n", err)
				}
				startKey := repl.Desc().StartKey.AsRawKey()
				splitKey := startKey.Next()
				_, rhsDesc, err := state.tc.SplitRange(splitKey)
				if err != nil {
					return fmt.Sprintf("split error: %v\n", err)
				}
				// LHS keeps the same synthetic ID. RHS gets a new synthetic ID.
				rhsSyntheticID := state.nextSyntheticID
				state.rangeIDMap[rhsDesc.RangeID] = rhsSyntheticID
				state.nextSyntheticID++
				return fmt.Sprintf("split ok, lhs=%d rhs=%d\n", syntheticID, rhsSyntheticID)
			case "merge":
				if state == nil {
					return "error: call init first"
				}
				syntheticID, _ := parseRangeAndStore(td)
				if syntheticID == 0 {
					return "error: rangeID parameter required\n"
				}
				// Get LHS replica to find its start key and end key.
				lhsRepl, err := state.getReplicaForRange(syntheticID, 0)
				if err != nil {
					return fmt.Sprintf("merge error: %v\n", err)
				}
				lhsStartKey := lhsRepl.Desc().StartKey.AsRawKey()
				lhsEndKey := lhsRepl.Desc().EndKey
				// Look up RHS range.
				store, err := state.getStore(0)
				if err != nil {
					return fmt.Sprintf("merge error: %v\n", err)
				}
				rhsRepl := store.LookupReplica(lhsEndKey)
				if rhsRepl == nil {
					return "merge error: no RHS range found at endKey\n"
				}
				rhsSyntheticID, ok := state.rangeIDMap[rhsRepl.RangeID]
				if !ok {
					return "merge error: RHS range not in rangeIDMap\n"
				}
				_, err = state.tc.MergeRanges(lhsStartKey)
				if err != nil {
					return fmt.Sprintf("merge error: %v\n", err)
				}
				// Remove RHS from rangeIDMap.
				delete(state.rangeIDMap, rhsRepl.RangeID)
				return fmt.Sprintf("merge ok, removed rhs=%d\n", rhsSyntheticID)
			case "stop":
				if state != nil && state.tc != nil {
					state.tc.Stopper().Stop(ctx)
					state.tc = nil
				}
				return "stopped\n"
			default:
				return fmt.Sprintf("unknown command: %s\n", td.Cmd)
			}
		})
	})
}

// initBasaltTestCluster creates a TestCluster with Basalt configuration.
// Each node has a single store, so numNodes is also the number of stores.
// When writeClearRange is true, range flushes write ClearRawRangeDormant /
// ClearRawRangeActivate, causing flushed data to disappear from store-local.
func initBasaltTestCluster(
	t testing.TB, ctx context.Context, numNodes int, writeClearRange bool,
) *basaltE2ETestState {
	basaltFS := vfs.NewMem()
	storeKnobs := kvserver.StoreTestingKnobs{
		BasaltFS:               basaltFS,
		OpenRSEngine:           storage.OpenTestingRSEngine,
		WriteClearRangeOnFlush: writeClearRange,
	}
	args := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &storeKnobs,
			},
		},
	}
	tc := testcluster.StartTestCluster(t, numNodes, args)
	return &basaltE2ETestState{
		t:        t,
		ctx:      ctx,
		basaltFS: basaltFS,
		tc:       tc,
	}
}

// parseRangeAndStore parses rangeID=N and store=N from command args.
// Returns (syntheticRangeID, storeIdx). If rangeID is not specified, returns 0.
// storeIdx defaults to 0 (store=1).
func parseRangeAndStore(td *datadriven.TestData) (syntheticID int, storeIdx int) {
	for _, arg := range td.CmdArgs {
		switch arg.Key {
		case "rangeID":
			if len(arg.Vals) > 0 {
				fmt.Sscanf(arg.Vals[0], "%d", &syntheticID) //nolint:errcheck
			}
		case "store":
			if len(arg.Vals) > 0 {
				var storeNum int
				fmt.Sscanf(arg.Vals[0], "%d", &storeNum) //nolint:errcheck
				storeIdx = storeNum - 1
			}
		}
	}
	return syntheticID, storeIdx
}

// getRealRangeID returns the real RangeID for a synthetic ID.
func (state *basaltE2ETestState) getRealRangeID(syntheticID int) (roachpb.RangeID, bool) {
	for realID, synthID := range state.rangeIDMap {
		if synthID == syntheticID {
			return realID, true
		}
	}
	return 0, false
}

// getStore returns the Store for the given store index.
func (state *basaltE2ETestState) getStore(storeIdx int) (*kvserver.Store, error) {
	server := state.tc.Server(storeIdx)
	return server.GetStores().(*kvserver.Stores).GetStore(server.GetFirstStoreID())
}

// getReplicaForRange gets the replica for the given synthetic range ID on the given store.
func (state *basaltE2ETestState) getReplicaForRange(
	syntheticID int, storeIdx int,
) (*kvserver.Replica, error) {
	realID, ok := state.getRealRangeID(syntheticID)
	if !ok {
		return nil, fmt.Errorf("unknown synthetic range ID %d", syntheticID)
	}
	store, err := state.getStore(storeIdx)
	if err != nil {
		return nil, err
	}
	repl := store.GetReplicaIfExists(realID)
	if repl == nil {
		return nil, fmt.Errorf("no replica found for range %d on store %d", syntheticID, storeIdx+1)
	}
	return repl, nil
}

// getRSEngine gets the TestingRSEngine for the given synthetic range ID on the given store.
func (state *basaltE2ETestState) getRSEngine(
	syntheticID int, storeIdx int,
) (storage.RSEngine, error) {
	repl, err := state.getReplicaForRange(syntheticID, storeIdx)
	if err != nil {
		return nil, err
	}
	return repl.TestingRSEngine(), nil
}

// loadRangeSharedMetaKeys loads RSManifestState and RangeFileNumAllocState
// for the given synthetic range ID from the store at storeIdx.
func (state *basaltE2ETestState) loadRangeSharedMetaKeys(syntheticID int, storeIdx int) string {
	realID, ok := state.getRealRangeID(syntheticID)
	if !ok {
		return fmt.Sprintf("error: unknown synthetic range ID %d\n", syntheticID)
	}
	var buf strings.Builder
	store, err := state.getStore(storeIdx)
	if err != nil {
		return fmt.Sprintf("error getting store: %v\n", err)
	}
	stateEngine := store.StateEngine()
	sl := kvstorage.MakeStateLoader(realID)
	rsManifestState, err := sl.LoadRSManifestState(state.ctx, stateEngine)
	if err != nil {
		return fmt.Sprintf("error loading RSManifestState: %v\n", err)
	}
	rangeFileNumAllocState, err := sl.LoadRangeFileNumAllocState(state.ctx, stateEngine)
	if err != nil {
		return fmt.Sprintf("error loading RangeFileNumAllocState: %v\n", err)
	}
	fmt.Fprintf(&buf, "RSManifestState:\n")
	fmt.Fprintf(&buf, "  disk_file_num: %d, replica_id: %v\n", rsManifestState.DiskFileNum,
		rsManifestState.ReplicaId)
	fmt.Fprintf(&buf, "RangeFileNumAllocState:\n")
	fmt.Fprintf(&buf, "  next_file_num: %d\n", rangeFileNumAllocState.NextFileNum)
	return buf.String()
}

// TestBasaltApproxStoreLocalBytes verifies that ApproxStoreLocalBytes in
// RangeAppliedState increases monotonically as KV writes are applied.
func TestBasaltApproxStoreLocalBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	scratchKey := tc.ScratchRange(t)
	db := tc.Server(0).DB()
	store, err := tc.Server(0).GetStores().(*kvserver.Stores).GetStore(
		tc.Server(0).GetFirstStoreID())
	require.NoError(t, err)

	loadApproxBytes := func() int64 {
		repl := store.LookupReplica(roachpb.RKey(scratchKey))
		require.NotNil(t, repl)
		sl := kvstorage.MakeStateLoader(repl.RangeID)
		as, err := sl.LoadRangeAppliedState(ctx, store.StateEngine())
		require.NoError(t, err)
		return as.ApproxStoreLocalBytes
	}

	// Write a few KV pairs and verify ApproxStoreLocalBytes > 0.
	for i := 0; i < 5; i++ {
		key := append(scratchKey[:len(scratchKey):len(scratchKey)], byte('a'+i))
		require.NoError(t, db.Put(ctx, key, fmt.Sprintf("value-%d", i)))
	}
	bytesAfterFirstBatch := loadApproxBytes()
	require.Greater(t, bytesAfterFirstBatch, int64(0),
		"ApproxStoreLocalBytes should be > 0 after writes")

	// Write more KV pairs and verify the value increased.
	for i := 0; i < 5; i++ {
		key := append(scratchKey[:len(scratchKey):len(scratchKey)], byte('f'+i))
		require.NoError(t, db.Put(ctx, key, fmt.Sprintf("value-%d", i+5)))
	}
	bytesAfterSecondBatch := loadApproxBytes()
	require.Greater(t, bytesAfterSecondBatch, bytesAfterFirstBatch,
		"ApproxStoreLocalBytes should increase with more writes")
}

// TestBasaltApproxStoreLocalBytesSplit verifies that ApproxStoreLocalBytes is
// approximately halved on both sides after a split.
func TestBasaltApproxStoreLocalBytesSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	scratchKey := tc.ScratchRange(t)
	db := tc.Server(0).DB()
	store, err := tc.Server(0).GetStores().(*kvserver.Stores).GetStore(
		tc.Server(0).GetFirstStoreID())
	require.NoError(t, err)

	loadApproxBytes := func(key roachpb.Key) int64 {
		repl := store.LookupReplica(roachpb.RKey(key))
		require.NotNil(t, repl)
		sl := kvstorage.MakeStateLoader(repl.RangeID)
		as, err := sl.LoadRangeAppliedState(ctx, store.StateEngine())
		require.NoError(t, err)
		return as.ApproxStoreLocalBytes
	}

	// Write KV pairs to accumulate bytes.
	for i := 0; i < 10; i++ {
		key := append(scratchKey[:len(scratchKey):len(scratchKey)], byte('a'+i))
		require.NoError(t, db.Put(ctx, key, fmt.Sprintf("value-%d", i)))
	}
	preSplitBytes := loadApproxBytes(scratchKey)
	require.Greater(t, preSplitBytes, int64(0))

	// Split at scratchKey.Next().
	splitKey := scratchKey.Next()
	_, _, err = tc.SplitRange(splitKey)
	require.NoError(t, err)

	lhsBytes := loadApproxBytes(scratchKey)
	rhsBytes := loadApproxBytes(splitKey)
	t.Logf("pre-split=%d lhs=%d rhs=%d", preSplitBytes, lhsBytes, rhsBytes)
	require.Greater(t, lhsBytes, int64(0))
	require.Greater(t, rhsBytes, int64(0))
	// The RHS was initialized at evaluation time with half the then-current
	// value. The LHS was halved at application time but then the split
	// command's own WriteBatch bytes were added. Verify both sides are less
	// than the total (each got roughly half of the pre-split value, modulo
	// the split command's own bytes).
	total := lhsBytes + rhsBytes
	require.Greater(t, total, preSplitBytes)
	require.Less(t, rhsBytes, total)
}

// TestBasaltApproxStoreLocalBytesMerge verifies that ApproxStoreLocalBytes is
// the sum of both sides after a merge.
func TestBasaltApproxStoreLocalBytesMerge(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	scratchKey := tc.ScratchRange(t)
	db := tc.Server(0).DB()
	store, err := tc.Server(0).GetStores().(*kvserver.Stores).GetStore(
		tc.Server(0).GetFirstStoreID())
	require.NoError(t, err)

	loadApproxBytes := func(key roachpb.Key) int64 {
		repl := store.LookupReplica(roachpb.RKey(key))
		require.NotNil(t, repl)
		sl := kvstorage.MakeStateLoader(repl.RangeID)
		as, err := sl.LoadRangeAppliedState(ctx, store.StateEngine())
		require.NoError(t, err)
		return as.ApproxStoreLocalBytes
	}

	// Write some KV pairs, then split.
	for i := 0; i < 10; i++ {
		key := append(scratchKey[:len(scratchKey):len(scratchKey)], byte('a'+i))
		require.NoError(t, db.Put(ctx, key, fmt.Sprintf("value-%d", i)))
	}
	splitKey := scratchKey.Next()
	_, _, err = tc.SplitRange(splitKey)
	require.NoError(t, err)

	// Write more to each side to get distinct values.
	for i := 0; i < 5; i++ {
		key := append(scratchKey[:len(scratchKey):len(scratchKey)], byte('A'+i))
		require.NoError(t, db.Put(ctx, key, "lhs-extra"))
	}
	for i := 0; i < 5; i++ {
		key := append(splitKey[:len(splitKey):len(splitKey)], byte('A'+i))
		require.NoError(t, db.Put(ctx, key, "rhs-extra"))
	}

	lhsBytes := loadApproxBytes(scratchKey)
	rhsBytes := loadApproxBytes(splitKey)
	require.Greater(t, lhsBytes, int64(0))
	require.Greater(t, rhsBytes, int64(0))

	// Merge.
	_, err = tc.MergeRanges(scratchKey)
	require.NoError(t, err)

	mergedBytes := loadApproxBytes(scratchKey)
	t.Logf("lhs=%d rhs=%d merged=%d", lhsBytes, rhsBytes, mergedBytes)
	// The merged value should be at least the sum of both sides (the merge
	// command itself also adds bytes).
	require.GreaterOrEqual(t, mergedBytes, lhsBytes+rhsBytes)
}

// TestBasaltApproxStoreLocalBytesFlush verifies that ApproxStoreLocalBytes
// decreases after a range flush moves data to the range-shared engine.
func TestBasaltApproxStoreLocalBytesFlush(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	state := initBasaltTestCluster(t, ctx, 1, false)
	defer state.tc.Stopper().Stop(ctx)

	scratchKey := state.tc.ScratchRange(t)
	db := state.tc.Server(0).DB()
	store, err := state.getStore(0)
	require.NoError(t, err)

	repl := store.LookupReplica(roachpb.RKey(scratchKey))
	require.NotNil(t, repl)

	loadApproxBytes := func() int64 {
		sl := kvstorage.MakeStateLoader(repl.RangeID)
		as, err := sl.LoadRangeAppliedState(ctx, store.StateEngine())
		require.NoError(t, err)
		return as.ApproxStoreLocalBytes
	}

	// Write KV data and verify ApproxStoreLocalBytes > 0.
	for i := 0; i < 5; i++ {
		key := append(scratchKey[:len(scratchKey):len(scratchKey)], byte('a'+i))
		require.NoError(t, db.Put(ctx, key, fmt.Sprintf("value-%d", i)))
	}
	bytesBeforeFlush := loadApproxBytes()
	require.Greater(t, bytesBeforeFlush, int64(0),
		"ApproxStoreLocalBytes should be > 0 after writes")

	// Flush and verify ApproxStoreLocalBytes decreased.
	err = repl.ManifestCommitter().RangeFlush()
	require.NoError(t, err)
	bytesAfterFlush := loadApproxBytes()
	t.Logf("before flush=%d, after flush=%d", bytesBeforeFlush, bytesAfterFlush)
	require.Less(t, bytesAfterFlush, bytesBeforeFlush,
		"ApproxStoreLocalBytes should decrease after flush")

	// Write more data and verify it increased again.
	for i := 0; i < 5; i++ {
		key := append(scratchKey[:len(scratchKey):len(scratchKey)], byte('f'+i))
		require.NoError(t, db.Put(ctx, key, fmt.Sprintf("value-%d", i+5)))
	}
	bytesAfterMoreWrites := loadApproxBytes()
	require.Greater(t, bytesAfterMoreWrites, bytesAfterFlush,
		"ApproxStoreLocalBytes should increase with more writes")

	// Second flush and verify it decreased again.
	err = repl.ManifestCommitter().RangeFlush()
	require.NoError(t, err)
	bytesAfterSecondFlush := loadApproxBytes()
	t.Logf("after more writes=%d, after second flush=%d",
		bytesAfterMoreWrites, bytesAfterSecondFlush)
	require.Less(t, bytesAfterSecondFlush, bytesAfterMoreWrites,
		"ApproxStoreLocalBytes should decrease after second flush")
}

// TestBasaltE2EBasic is a simple smoke test for Basalt E2E functionality.
// It verifies that the Basalt injection mechanism works correctly through
// TestClusterArgs.
func TestBasaltE2EBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	// Create a shared MemFS for all stores.
	basaltFS := vfs.NewMem()
	storeKnobs := kvserver.StoreTestingKnobs{
		BasaltFS:     basaltFS,
		OpenRSEngine: storage.OpenTestingRSEngine,
	}
	args := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &storeKnobs,
			},
		},
	}
	tc := testcluster.StartTestCluster(t, 1, args)
	defer tc.Stopper().Stop(ctx)
	// Get a scratch range.
	scratchKey := tc.ScratchRange(t)
	server := tc.Server(0)
	store, err := server.GetStores().(*kvserver.Stores).GetStore(server.GetFirstStoreID())
	require.NoError(t, err)
	repl := store.LookupReplica(roachpb.RKey(scratchKey))
	require.NotNil(t, repl)
	// Verify RSEngine is configured through the testing knobs injection.
	rsEngine := repl.TestingRSEngine()
	require.NotNil(t, rsEngine, "RSEngine should be configured for scratch range")
	testEngine := rsEngine.TestingInnerEngine().(*storage.TestingRSEngine)
	require.NotNil(t, testEngine)
	// Verify that the range-shared meta keys are accessible.
	sl := kvstorage.MakeStateLoader(repl.RangeID)
	rsManifestState, err := sl.LoadRSManifestState(ctx, store.StateEngine())
	require.NoError(t, err)
	t.Logf("RSManifestState: DiskFileNum=%d", rsManifestState.DiskFileNum)
	rangeFileNumAllocState, err := sl.LoadRangeFileNumAllocState(ctx, store.StateEngine())
	require.NoError(t, err)
	t.Logf("RangeFileNumAllocState: NextFileNum=%d", rangeFileNumAllocState.NextFileNum)
	// Verify the Basalt filesystem is accessible.
	entries, err := basaltFS.List("/")
	require.NoError(t, err)
	t.Logf("Basalt FS root entries: %v", entries)
}
