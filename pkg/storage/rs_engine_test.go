// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// mockManifestChangeCommitter implements ManifestChangeCommitter for testing.
// It logs all calls to a strings.Builder for verification.
type mockManifestChangeCommitter struct {
	mu struct {
		sync.Mutex
		log                   strings.Builder
		nextFileNum           DiskFileNum
		lastInstalledManifest DiskFileNum
	}
}

func newMockManifestChangeCommitter(startFileNum DiskFileNum) *mockManifestChangeCommitter {
	m := &mockManifestChangeCommitter{}
	m.mu.nextFileNum = startFileNum
	return m
}

func (m *mockManifestChangeCommitter) GetFileNums(count int) ([]DiskFileNum, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	start := m.mu.nextFileNum
	result := make([]DiskFileNum, count)
	for i := range count {
		result[i] = m.mu.nextFileNum
		m.mu.nextFileNum++
	}
	fmt.Fprintf(&m.mu.log, "GetFileNums(%d) => [%d, %d)\n", count, start, m.mu.nextFileNum)
	return result, nil
}

func (m *mockManifestChangeCommitter) InstallNewManifest(
	currentManifestNum DiskFileNum, manifestInfo ManifestInfo, ingestHandle interface{},
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.lastInstalledManifest = manifestInfo.Manifest.Num
	fmt.Fprintf(&m.mu.log, "InstallNewManifest(current=%d, new=%s/%d, files=%v",
		currentManifestNum, manifestInfo.Manifest.Name, manifestInfo.Manifest.Num, manifestInfo.Files)
	if ingestHandle != nil {
		flushCommit := ingestHandle.(*FlushCommitInfo)
		if flushCommit != nil {
			fmt.Fprintf(&m.mu.log, ", flushCommit={ExpectedFlushStartedCount:%d, ActivateSpans:%v}",
				flushCommit.ExpectedFlushStartedCount, flushCommit.ActivateSpans)
		}
	}
	fmt.Fprintf(&m.mu.log, ")\n")
	return nil
}

func (m *mockManifestChangeCommitter) getLog() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mu.log.String()
}

func (m *mockManifestChangeCommitter) clearLog() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.log.Reset()
}

func (m *mockManifestChangeCommitter) getLastInstalledManifest() DiskFileNum {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mu.lastInstalledManifest
}

// TestOpenTestingRSEngine verifies basic OpenTestingRSEngine functionality.
func TestOpenTestingRSEngine(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	fs := vfs.NewMem()
	committer := newMockManifestChangeCommitter(100)
	opts := RSEngineOptions{
		manifestChangeCommitter: committer,
		basaltFS:                fs,
		basaltDir:               "/basalt",
		basaltScratchPathDir:    "/scratch",
	}
	// Open fresh engine (manifest=0).
	rsEngine, err := OpenTestingRSEngine(0, opts)
	require.NoError(t, err)
	engine := rsEngine.(*TestingRSEngine)
	require.NotNil(t, engine)
	require.Equal(t, NoManifestNum, engine.currentManifestNum())
	// Verify directories created.
	stat, err := fs.Stat("/basalt")
	require.NoError(t, err)
	require.True(t, stat.IsDir())
	stat, err = fs.Stat("/scratch")
	require.NoError(t, err)
	require.True(t, stat.IsDir())
	// Verify engine holds ref on manifest.
	engine.mu.Lock()
	require.Equal(t, 1, engine.mu.manifests[0].refCount)
	engine.mu.Unlock()
	engine.closeInner()
}

// TestRefUnrefBlocksClose verifies that Close() blocks while a Ref is held
// and completes after Unref().
func TestRefUnrefBlocksClose(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	fs := vfs.NewMem()
	committer := newMockManifestChangeCommitter(100)
	opts := RSEngineOptions{
		manifestChangeCommitter: committer,
		basaltFS:                fs,
		basaltDir:               "/basalt",
		basaltScratchPathDir:    "/scratch",
	}
	rsEngine, err := OpenTestingRSEngine(0, opts)
	require.NoError(t, err)
	engine := rsEngine.(*TestingRSEngine)

	// Take an external ref.
	engine.ref()

	// Start async close — it should block.
	engine.closeWaitingCh = make(chan struct{})
	doneCh := make(chan struct{})
	go func() {
		engine.closeInner()
		close(doneCh)
	}()
	<-engine.closeWaitingCh

	// Verify Close is blocked.
	time.Sleep(time.Millisecond)
	select {
	case <-doneCh:
		t.Fatal("Close() completed while Ref is held")
	default:
	}

	// Release the ref — Close should complete.
	engine.unref()
	<-doneCh
}

// testingRSEngineState holds state for the datadriven test.
type testingRSEngineState struct {
	t         testing.TB
	fs        vfs.FS
	committer *mockManifestChangeCommitter
	// engines maps rangeID name (e.g. "r1") to engine.
	engines map[string]*TestingRSEngine
	// snapshots maps snapshot name (e.g. "r1-159") to snapshot.
	snapshots map[string]*TestingRSEngineSnapshot
	// asyncCloses tracks async close operations by rangeID.
	asyncCloses map[string]chan struct{}
}

func newTestingRSEngineState(t testing.TB, startFileNum DiskFileNum) *testingRSEngineState {
	return &testingRSEngineState{
		t:           t,
		fs:          vfs.NewMem(),
		committer:   newMockManifestChangeCommitter(startFileNum),
		engines:     make(map[string]*TestingRSEngine),
		snapshots:   make(map[string]*TestingRSEngineSnapshot),
		asyncCloses: make(map[string]chan struct{}),
	}
}

func (s *testingRSEngineState) basaltDir(rangeID string) string {
	return fmt.Sprintf("/%s", rangeID)
}

func (s *testingRSEngineState) scratchDir(rangeID string) string {
	return fmt.Sprintf("/%s-scratch", rangeID)
}

func (s *testingRSEngineState) snapshotName(rangeID string, manifestNum DiskFileNum) string {
	return fmt.Sprintf("%s-%d", rangeID, manifestNum)
}

// openEngine is a wrapper that calls OpenTestingRSEngine and uses require.NoError.
func (s *testingRSEngineState) openEngine(
	rangeID string, manifestNum DiskFileNum,
) *TestingRSEngine {
	opts := RSEngineOptions{
		manifestChangeCommitter: s.committer,
		basaltFS:                s.fs,
		basaltDir:               s.basaltDir(rangeID),
		basaltScratchPathDir:    s.scratchDir(rangeID),
	}
	rsEngine, err := OpenTestingRSEngine(manifestNum, opts)
	require.NoError(s.t, err)
	return rsEngine.(*TestingRSEngine)
}

// TestTestingRSEngineDatadriven runs datadriven tests for TestingRSEngine.
func TestTestingRSEngineDatadriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	var state *testingRSEngineState
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "testing_rs_engine"),
		func(t *testing.T, td *datadriven.TestData) string {
			var buf strings.Builder
			switch td.Cmd {
			case "init":
				// Initialize fresh test state.
				startFileNum := DiskFileNum(100)
				for _, arg := range td.CmdArgs {
					if arg.Key == "start-file-num" && len(arg.Vals) > 0 {
						var n int
						fmt.Sscanf(arg.Vals[0], "%d", &n) //nolint:errcheck
						startFileNum = DiskFileNum(n)
					}
				}
				state = newTestingRSEngineState(t, startFileNum)
				return ""

			case "open":
				if state == nil {
					return "error: call init first"
				}
				var rangeID string
				var manifestNum DiskFileNum
				for _, arg := range td.CmdArgs {
					switch arg.Key {
					case "range":
						if len(arg.Vals) > 0 {
							rangeID = arg.Vals[0]
						}
					case "manifest":
						if len(arg.Vals) > 0 {
							var n int
							fmt.Sscanf(arg.Vals[0], "%d", &n) //nolint:errcheck
							manifestNum = DiskFileNum(n)
						}
					}
				}
				if rangeID == "" {
					return "error: range= required"
				}
				engine := state.openEngine(rangeID, manifestNum)
				state.engines[rangeID] = engine
				fmt.Fprintf(&buf, "opened %s: manifest=%d\n", rangeID, engine.currentManifestNum())
				return buf.String()

			case "flush":
				if state == nil {
					return "error: call init first"
				}
				var rangeID string
				fileName := "test.sst"
				for _, arg := range td.CmdArgs {
					switch arg.Key {
					case "range":
						if len(arg.Vals) > 0 {
							rangeID = arg.Vals[0]
						}
					case "file":
						if len(arg.Vals) > 0 {
							fileName = arg.Vals[0]
						}
					}
				}
				if rangeID == "" {
					return "error: range= required"
				}
				engine, ok := state.engines[rangeID]
				if !ok {
					return fmt.Sprintf("error: engine %s not found", rangeID)
				}
				state.committer.clearLog()
				err := engine.TestFlushSSTables(fileName)
				if err != nil {
					return fmt.Sprintf("error: %v", err)
				}
				buf.WriteString(state.committer.getLog())
				// Close and reopen with new manifest from committer.
				engine.closeInner()
				newManifestNum := state.committer.getLastInstalledManifest()
				state.engines[rangeID] = state.openEngine(rangeID, newManifestNum)
				fmt.Fprintf(&buf, "reopened %s: manifest=%d\n", rangeID, newManifestNum)
				return buf.String()

			case "snapshot":
				if state == nil {
					return "error: call init first"
				}
				var rangeID string
				for _, arg := range td.CmdArgs {
					if arg.Key == "range" && len(arg.Vals) > 0 {
						rangeID = arg.Vals[0]
					}
				}
				if rangeID == "" {
					return "error: range= required"
				}
				engine, ok := state.engines[rangeID]
				if !ok {
					return fmt.Sprintf("error: engine %s not found", rangeID)
				}
				snap := engine.newSnapshot().(*TestingRSEngineSnapshot)
				snapName := state.snapshotName(rangeID, snap.ManifestNum())
				state.snapshots[snapName] = snap
				fmt.Fprintf(&buf, "snapshot %s\n", snapName)
				return buf.String()

			case "close-snapshot":
				if state == nil {
					return "error: call init first"
				}
				var snapName string
				for _, arg := range td.CmdArgs {
					if arg.Key == "name" && len(arg.Vals) > 0 {
						snapName = arg.Vals[0]
					}
				}
				if snapName == "" {
					return "error: name= required"
				}
				snap, ok := state.snapshots[snapName]
				if !ok {
					return fmt.Sprintf("error: snapshot %s not found", snapName)
				}
				snap.Close()
				delete(state.snapshots, snapName)
				fmt.Fprintf(&buf, "closed: %s\n", snapName)
				return buf.String()

			case "split":
				if state == nil {
					return "error: call init first"
				}
				var lhsRangeID, rhsRangeID, snapName, splitKey string
				for _, arg := range td.CmdArgs {
					switch arg.Key {
					case "snapshot":
						if len(arg.Vals) > 0 {
							snapName = arg.Vals[0]
						}
					case "key":
						if len(arg.Vals) > 0 {
							splitKey = arg.Vals[0]
						}
					case "lhs-range":
						if len(arg.Vals) > 0 {
							lhsRangeID = arg.Vals[0]
						}
					case "rhs-range":
						if len(arg.Vals) > 0 {
							rhsRangeID = arg.Vals[0]
						}
					}
				}
				if snapName == "" || splitKey == "" || rhsRangeID == "" {
					return "error: snapshot=, key=, rhs-range= required"
				}
				snap, ok := state.snapshots[snapName]
				if !ok {
					return fmt.Sprintf("error: snapshot %s not found", snapName)
				}
				// Find LHS range from snapshot if not specified.
				if lhsRangeID == "" {
					// Parse from snapshot name: r1-159 -> r1
					parts := strings.Split(snapName, "-")
					if len(parts) >= 1 {
						lhsRangeID = parts[0]
					}
				}
				rhsDir := state.basaltDir(rhsRangeID)
				state.committer.clearLog()
				lhsManifest, rhsInfo, _, err := snap.Split(context.Background(),
					roachpb.Key(splitKey), rhsDir)
				if err != nil {
					return fmt.Sprintf("error: %v", err)
				}
				buf.WriteString(state.committer.getLog())
				fmt.Fprintf(&buf, "lhs-manifest: %s/%d\n", lhsManifest.Name, lhsManifest.Num)
				fmt.Fprintf(&buf, "rhs-manifest: %s/%d\n", rhsInfo.Manifest.Name, rhsInfo.Manifest.Num)
				fmt.Fprintf(&buf, "rhs-files: %v\n", rhsInfo.Files)
				// Close LHS and reopen with new manifest.
				lhsEngine := state.engines[lhsRangeID]
				// First close snapshot.
				snap.Close()
				delete(state.snapshots, snapName)
				lhsEngine.closeInner()
				state.engines[lhsRangeID] = state.openEngine(lhsRangeID, lhsManifest.Num)
				// Open RHS with new manifest.
				state.engines[rhsRangeID] = state.openEngine(rhsRangeID, rhsInfo.Manifest.Num)
				fmt.Fprintf(&buf, "reopened %s: manifest=%d\n", lhsRangeID, lhsManifest.Num)
				fmt.Fprintf(&buf, "opened %s: manifest=%d\n", rhsRangeID, rhsInfo.Manifest.Num)
				return buf.String()

			case "merge":
				if state == nil {
					return "error: call init first"
				}
				var lhsSnapName, rhsSnapName string
				for _, arg := range td.CmdArgs {
					switch arg.Key {
					case "lhs-snapshot":
						if len(arg.Vals) > 0 {
							lhsSnapName = arg.Vals[0]
						}
					case "rhs-snapshot":
						if len(arg.Vals) > 0 {
							rhsSnapName = arg.Vals[0]
						}
					}
				}
				if lhsSnapName == "" || rhsSnapName == "" {
					return "error: lhs-snapshot=, rhs-snapshot= required"
				}
				lhsSnap, ok := state.snapshots[lhsSnapName]
				if !ok {
					return fmt.Sprintf("error: lhs snapshot %s not found", lhsSnapName)
				}
				rhsSnap, ok := state.snapshots[rhsSnapName]
				if !ok {
					return fmt.Sprintf("error: rhs snapshot %s not found", rhsSnapName)
				}
				// Parse range IDs from snapshot names.
				lhsParts := strings.Split(lhsSnapName, "-")
				rhsParts := strings.Split(rhsSnapName, "-")
				lhsRangeID := lhsParts[0]
				rhsRangeID := rhsParts[0]
				state.committer.clearLog()
				merged, _, err := lhsSnap.Merge(context.Background(), rhsSnap)
				if err != nil {
					return fmt.Sprintf("error: %v", err)
				}
				buf.WriteString(state.committer.getLog())
				fmt.Fprintf(&buf, "merged-manifest: %s/%d\n", merged.Manifest.Name, merged.Manifest.Num)
				fmt.Fprintf(&buf, "merged-files: %v\n", merged.Files)
				// Close both snapshots.
				lhsSnap.Close()
				delete(state.snapshots, lhsSnapName)
				rhsSnap.Close()
				delete(state.snapshots, rhsSnapName)
				// Close RHS engine.
				rhsEngine := state.engines[rhsRangeID]
				rhsEngine.closeInner()
				delete(state.engines, rhsRangeID)
				// Close LHS and reopen with merged manifest.
				lhsEngine := state.engines[lhsRangeID]
				lhsEngine.closeInner()
				state.engines[lhsRangeID] = state.openEngine(lhsRangeID, merged.Manifest.Num)
				fmt.Fprintf(&buf, "reopened %s: manifest=%d\n", lhsRangeID, merged.Manifest.Num)
				return buf.String()

			case "state":
				if state == nil {
					return "error: call init first"
				}
				var rangeID string
				for _, arg := range td.CmdArgs {
					if arg.Key == "range" && len(arg.Vals) > 0 {
						rangeID = arg.Vals[0]
					}
				}
				if rangeID == "" {
					return "error: range= required"
				}
				engine, ok := state.engines[rangeID]
				if !ok {
					return fmt.Sprintf("error: engine %s not found", rangeID)
				}
				return PrintRSEngineState(engine)

			case "files":
				if state == nil {
					return "error: call init first"
				}
				dir := "/"
				for _, arg := range td.CmdArgs {
					if arg.Key == "dir" && len(arg.Vals) > 0 {
						dir = arg.Vals[0]
					}
				}
				return PrintFilesystem(state.fs, dir, nil)

			case "close":
				if state == nil {
					return "error: call init first"
				}
				var rangeID string
				for _, arg := range td.CmdArgs {
					if arg.Key == "range" && len(arg.Vals) > 0 {
						rangeID = arg.Vals[0]
					}
				}
				if rangeID == "" {
					return "error: range= required"
				}
				engine, ok := state.engines[rangeID]
				if !ok {
					return fmt.Sprintf("error: engine %s not found", rangeID)
				}
				engine.closeInner()
				delete(state.engines, rangeID)
				fmt.Fprintf(&buf, "closed: %s\n", rangeID)
				return buf.String()

			case "async-close":
				// Starts Close() in a goroutine and waits until it's blocked
				// waiting for snapshots to drain. Use wait-close to join.
				if state == nil {
					return "error: call init first"
				}
				var rangeID string
				for _, arg := range td.CmdArgs {
					if arg.Key == "range" && len(arg.Vals) > 0 {
						rangeID = arg.Vals[0]
					}
				}
				if rangeID == "" {
					return "error: range= required"
				}
				engine, ok := state.engines[rangeID]
				if !ok {
					return fmt.Sprintf("error: engine %s not found", rangeID)
				}
				if _, ok := state.asyncCloses[rangeID]; ok {
					return fmt.Sprintf("error: async close already in progress for %s", rangeID)
				}
				// Set up hook channel and done channel.
				engine.closeWaitingCh = make(chan struct{})
				doneCh := make(chan struct{})
				state.asyncCloses[rangeID] = doneCh
				go func() {
					engine.closeInner()
					close(doneCh)
				}()
				// Wait for Close() to signal it reached the wait point.
				<-engine.closeWaitingCh
				// Verify Close() is actually blocked by sleeping and checking
				// that doneCh is still not signaled. If Close() completes during
				// this window, it means it didn't actually block.
				time.Sleep(time.Millisecond)
				select {
				case <-doneCh:
					return fmt.Sprintf("error: close for %s completed without blocking", rangeID)
				default:
				}
				fmt.Fprintf(&buf, "async-close started: %s (blocked waiting for snapshots)\n", rangeID)
				return buf.String()

			case "wait-close":
				// Waits for async-close to complete.
				if state == nil {
					return "error: call init first"
				}
				var rangeID string
				for _, arg := range td.CmdArgs {
					if arg.Key == "range" && len(arg.Vals) > 0 {
						rangeID = arg.Vals[0]
					}
				}
				if rangeID == "" {
					return "error: range= required"
				}
				doneCh, ok := state.asyncCloses[rangeID]
				if !ok {
					return fmt.Sprintf("error: no async close in progress for %s", rangeID)
				}
				<-doneCh
				delete(state.asyncCloses, rangeID)
				delete(state.engines, rangeID)
				fmt.Fprintf(&buf, "async-close completed: %s\n", rangeID)
				return buf.String()

			default:
				return fmt.Sprintf("unknown command: %s", td.Cmd)
			}
		})
}

// buildSST creates a valid SST file at path with the given key-value pairs.
// Keys are encoded as MVCC keys (no timestamp) to be compatible with
// EngineComparer.
func buildSST(fs vfs.FS, path string, kvs [][2]string) error {
	f, err := fs.Create(path, vfs.WriteCategoryUnspecified)
	if err != nil {
		return err
	}
	writerOpts := sstable.WriterOptions{
		TableFormat: pebble.FormatNewest.MaxTableFormat(),
		Comparer:    &EngineComparer,
		KeySchema:   KeySchemas[0],
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), writerOpts)
	for _, kv := range kvs {
		encodedKey := EncodeMVCCKeyPrefix(roachpb.Key(kv[0]))
		if err := w.Set(encodedKey, []byte(kv[1])); err != nil {
			_ = w.Close()
			return err
		}
	}
	return w.Close()
}

// pebbleRSEngineTestState holds state for the pebble RSEngine datadriven test.
type pebbleRSEngineTestState struct {
	t         testing.TB
	fs        vfs.FS
	committer *mockManifestChangeCommitter
	// committers holds per-range committers. When a range is opened with
	// start-file-num, it gets its own committer that allocates file numbers
	// independently — mimicking production where each range has its own
	// RangeFileNumAllocState. This enables tests to exercise file number
	// collisions across ranges (e.g., both ranges allocating file 110 after
	// a split with nextFileNum=110).
	committers map[string]*mockManifestChangeCommitter
	engines    map[string]InnerRSEngine
	snapshots  map[string]RSEngineSnapshot
}

func newPebbleRSEngineTestState(t testing.TB, startFileNum DiskFileNum) *pebbleRSEngineTestState {
	return &pebbleRSEngineTestState{
		t:          t,
		fs:         vfs.NewMem(),
		committer:  newMockManifestChangeCommitter(startFileNum),
		committers: make(map[string]*mockManifestChangeCommitter),
		engines:    make(map[string]InnerRSEngine),
		snapshots:  make(map[string]RSEngineSnapshot),
	}
}

// getCommitter returns the per-range committer if one exists, otherwise the
// shared committer.
func (s *pebbleRSEngineTestState) getCommitter(rangeID string) *mockManifestChangeCommitter {
	if c, ok := s.committers[rangeID]; ok {
		return c
	}
	return s.committer
}

func (s *pebbleRSEngineTestState) basaltDir(rangeID string) string {
	return fmt.Sprintf("/%s", rangeID)
}

func (s *pebbleRSEngineTestState) scratchDir(rangeID string) string {
	return fmt.Sprintf("/%s-scratch", rangeID)
}

func (s *pebbleRSEngineTestState) snapshotName(rangeID string, manifestNum DiskFileNum) string {
	return fmt.Sprintf("%s-%d", rangeID, manifestNum)
}

func (s *pebbleRSEngineTestState) openEngine(
	rangeID string, manifestNum DiskFileNum,
) InnerRSEngine {
	opts := RSEngineOptions{
		manifestChangeCommitter: s.getCommitter(rangeID),
		basaltFS:                s.fs,
		basaltDir:               s.basaltDir(rangeID),
		basaltScratchPathDir:    s.scratchDir(rangeID),
		logCtx:                  context.Background(),
		// Pin ProcessID for deterministic scratch dir names. The trailing
		// per-DB instance suffix is normalized separately at print time; see
		// normalizeScratchDir in rs_engine_testing.go.
		testingProcessID: "1111",
	}
	rsEngine, err := OpenRSEngine(manifestNum, opts)
	require.NoError(s.t, err)
	return rsEngine
}

// TestPebbleRSEngineSplitDatadriven runs datadriven tests for pebbleRSEngine
// Split. Unlike TestTestingRSEngineDatadriven which uses the mock
// TestingRSEngine, this test exercises the real pebble-backed engine with
// actual SSTs and pebble's SplitLSM API.
func TestPebbleRSEngineSplitDatadriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	var state *pebbleRSEngineTestState
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "pebble_rs_engine_split"),
		func(t *testing.T, td *datadriven.TestData) string {
			var buf strings.Builder
			switch td.Cmd {
			case "init":
				startFileNum := DiskFileNum(100)
				for _, arg := range td.CmdArgs {
					if arg.Key == "start-file-num" && len(arg.Vals) > 0 {
						var n int
						fmt.Sscanf(arg.Vals[0], "%d", &n) //nolint:errcheck
						startFileNum = DiskFileNum(n)
					}
				}
				state = newPebbleRSEngineTestState(t, startFileNum)
				return ""

			case "open":
				if state == nil {
					return "error: call init first"
				}
				var rangeID string
				var manifestNum DiskFileNum
				var startFileNum DiskFileNum
				hasStartFileNum := false
				for _, arg := range td.CmdArgs {
					switch arg.Key {
					case "range":
						if len(arg.Vals) > 0 {
							rangeID = arg.Vals[0]
						}
					case "manifest":
						if len(arg.Vals) > 0 {
							var n int
							fmt.Sscanf(arg.Vals[0], "%d", &n) //nolint:errcheck
							manifestNum = DiskFileNum(n)
						}
					case "start-file-num":
						if len(arg.Vals) > 0 {
							var n int
							fmt.Sscanf(arg.Vals[0], "%d", &n) //nolint:errcheck
							startFileNum = DiskFileNum(n)
							hasStartFileNum = true
						}
					}
				}
				if rangeID == "" {
					return "error: range= required"
				}
				if hasStartFileNum {
					state.committers[rangeID] = newMockManifestChangeCommitter(startFileNum)
				}
				engine := state.openEngine(rangeID, manifestNum)
				state.engines[rangeID] = engine
				fmt.Fprintf(&buf, "opened %s: manifest=%d\n", rangeID, engine.currentManifestNum())
				return buf.String()

			case "ingest":
				if state == nil {
					return "error: call init first"
				}
				var rangeID string
				for _, arg := range td.CmdArgs {
					if arg.Key == "range" && len(arg.Vals) > 0 {
						rangeID = arg.Vals[0]
					}
				}
				if rangeID == "" {
					return "error: range= required"
				}
				engine, ok := state.engines[rangeID]
				if !ok {
					return fmt.Sprintf("error: engine %s not found", rangeID)
				}
				// Parse key-value pairs from input.
				var kvs [][2]string
				for _, line := range strings.Split(td.Input, "\n") {
					line = strings.TrimSpace(line)
					if line == "" {
						continue
					}
					parts := strings.SplitN(line, " ", 3)
					if len(parts) < 3 || parts[0] != "set" {
						return fmt.Sprintf("unsupported: %s (expected 'set <key> <value>')", line)
					}
					kvs = append(kvs, [2]string{parts[1], parts[2]})
				}
				// Build SST in scratch dir.
				scratchPath := state.fs.PathJoin(state.scratchDir(rangeID), "test.sst")
				if err := buildSST(state.fs, scratchPath, kvs); err != nil {
					return fmt.Sprintf("error building SST: %v", err)
				}
				committer := state.getCommitter(rangeID)
				committer.clearLog()
				if err := engine.flushSSTables([]string{"test.sst"}, nil); err != nil {
					return fmt.Sprintf("error: %v", err)
				}
				buf.WriteString(committer.getLog())
				// Simulate the apply-path install on the leader: stage the
				// manifest just written to disk, then promote it to current.
				manifestNum := committer.getLastInstalledManifest()
				if err := engine.prepareExternalManifest(manifestNum); err != nil {
					return fmt.Sprintf("error preparing manifest: %v", err)
				}
				engine.installPreparedManifest(manifestNum)
				fmt.Fprintf(&buf, "ingested: manifest=%d\n", manifestNum)
				return buf.String()

			case "snapshot":
				if state == nil {
					return "error: call init first"
				}
				var rangeID string
				for _, arg := range td.CmdArgs {
					if arg.Key == "range" && len(arg.Vals) > 0 {
						rangeID = arg.Vals[0]
					}
				}
				if rangeID == "" {
					return "error: range= required"
				}
				engine, ok := state.engines[rangeID]
				if !ok {
					return fmt.Sprintf("error: engine %s not found", rangeID)
				}
				snap := engine.newSnapshot()
				snapName := state.snapshotName(rangeID, snap.ManifestNum())
				state.snapshots[snapName] = snap
				fmt.Fprintf(&buf, "snapshot %s\n", snapName)
				return buf.String()

			case "close-snapshot":
				if state == nil {
					return "error: call init first"
				}
				var snapName string
				for _, arg := range td.CmdArgs {
					if arg.Key == "name" && len(arg.Vals) > 0 {
						snapName = arg.Vals[0]
					}
				}
				if snapName == "" {
					return "error: name= required"
				}
				snap, ok := state.snapshots[snapName]
				if !ok {
					return fmt.Sprintf("error: snapshot %s not found", snapName)
				}
				snap.Close()
				delete(state.snapshots, snapName)
				fmt.Fprintf(&buf, "closed: %s\n", snapName)
				return buf.String()

			case "split":
				if state == nil {
					return "error: call init first"
				}
				var lhsRangeID, rhsRangeID, snapName, splitKey string
				for _, arg := range td.CmdArgs {
					switch arg.Key {
					case "snapshot":
						if len(arg.Vals) > 0 {
							snapName = arg.Vals[0]
						}
					case "key":
						if len(arg.Vals) > 0 {
							splitKey = arg.Vals[0]
						}
					case "lhs-range":
						if len(arg.Vals) > 0 {
							lhsRangeID = arg.Vals[0]
						}
					case "rhs-range":
						if len(arg.Vals) > 0 {
							rhsRangeID = arg.Vals[0]
						}
					}
				}
				if snapName == "" || splitKey == "" || rhsRangeID == "" {
					return "error: snapshot=, key=, rhs-range= required"
				}
				snap, ok := state.snapshots[snapName]
				if !ok {
					return fmt.Sprintf("error: snapshot %s not found", snapName)
				}
				// Extract LHS range from snapshot name if not specified.
				if lhsRangeID == "" {
					parts := strings.Split(snapName, "-")
					if len(parts) >= 1 {
						lhsRangeID = parts[0]
					}
				}
				rhsDir := state.basaltDir(rhsRangeID)
				lhsManifest, rhsInfo, nextFileNum, err := snap.Split(context.Background(),
					roachpb.Key(splitKey), rhsDir)
				if err != nil {
					return fmt.Sprintf("error: %v", err)
				}

				if lhsManifest.Num == 0 {
					// NoOp: empty LSM or all tables on LHS.
					fmt.Fprintf(&buf, "no-op\n")
					return buf.String()
				}

				fmt.Fprintf(&buf, "lhs-manifest: %s/%d\n", lhsManifest.Name, lhsManifest.Num)
				fmt.Fprintf(&buf, "rhs-manifest: %s/%d\n", rhsInfo.Manifest.Name, rhsInfo.Manifest.Num)
				fmt.Fprintf(&buf, "rhs-files: [%d files]\n", len(rhsInfo.Files))
				fmt.Fprintf(&buf, "next-file-num: %d\n", nextFileNum)

				// Close snapshot.
				snap.Close()
				delete(state.snapshots, snapName)

				// Close LHS and reopen with new manifest.
				lhsEngine := state.engines[lhsRangeID]
				lhsEngine.closeInner()
				state.engines[lhsRangeID] = state.openEngine(lhsRangeID, lhsManifest.Num)

				// Open RHS with new manifest.
				state.engines[rhsRangeID] = state.openEngine(rhsRangeID, rhsInfo.Manifest.Num)

				fmt.Fprintf(&buf, "reopened %s: manifest=%d\n", lhsRangeID, lhsManifest.Num)
				fmt.Fprintf(&buf, "opened %s: manifest=%d\n", rhsRangeID, rhsInfo.Manifest.Num)
				return buf.String()

			case "file-names":
				if state == nil {
					return "error: call init first"
				}
				dir := "/"
				for _, arg := range td.CmdArgs {
					if arg.Key == "dir" && len(arg.Vals) > 0 {
						dir = arg.Vals[0]
					}
				}
				// Wait for Pebble's async obsolete file cleanup to complete
				// so the listing is deterministic.
				for _, eng := range state.engines {
					eng.(*pebbleRSEngine).db.TestOnlyWaitForCleaning()
				}
				return PrintFileNames(state.fs, dir, nil)

			case "scan":
				if state == nil {
					return "error: call init first"
				}
				var rangeID string
				for _, arg := range td.CmdArgs {
					if arg.Key == "range" && len(arg.Vals) > 0 {
						rangeID = arg.Vals[0]
					}
				}
				if rangeID == "" {
					return "error: range= required"
				}
				engine, ok := state.engines[rangeID]
				if !ok {
					return fmt.Sprintf("error: engine %s not found", rangeID)
				}
				db := engine.(*pebbleRSEngine).db
				iter, err := db.NewIter(nil)
				if err != nil {
					return fmt.Sprintf("error: %v", err)
				}
				for valid := iter.First(); valid; valid = iter.Next() {
					key, err := DecodeMVCCKey(iter.Key())
					if err != nil {
						fmt.Fprintf(&buf, "error decoding key: %v\n", err)
						continue
					}
					fmt.Fprintf(&buf, "%s = %s\n", key.Key, iter.Value())
				}
				iterErr := iter.Error()
				if err := iter.Close(); err != nil {
					return fmt.Sprintf("error closing iterator: %v", err)
				}
				if iterErr != nil {
					return fmt.Sprintf("error iterating: %v", iterErr)
				}
				if buf.Len() == 0 {
					fmt.Fprintf(&buf, "<empty>\n")
				}
				return buf.String()

			case "close":
				if state == nil {
					return "error: call init first"
				}
				var rangeID string
				for _, arg := range td.CmdArgs {
					if arg.Key == "range" && len(arg.Vals) > 0 {
						rangeID = arg.Vals[0]
					}
				}
				if rangeID == "" {
					return "error: range= required"
				}
				engine, ok := state.engines[rangeID]
				if !ok {
					return fmt.Sprintf("error: engine %s not found", rangeID)
				}
				engine.closeInner()
				delete(state.engines, rangeID)
				fmt.Fprintf(&buf, "closed: %s\n", rangeID)
				return buf.String()

			default:
				return fmt.Sprintf("unknown command: %s", td.Cmd)
			}
		})
}

// pebbleRSEngineRunCommand runs a single datadriven command against a
// pebbleRSEngineTestState. Used by TestPebbleRSEngineMergeDatadriven.
func pebbleRSEngineRunCommand(
	t *testing.T, td *datadriven.TestData, state **pebbleRSEngineTestState,
) string {
	var buf strings.Builder
	switch td.Cmd {
	case "init":
		startFileNum := DiskFileNum(100)
		for _, arg := range td.CmdArgs {
			if arg.Key == "start-file-num" && len(arg.Vals) > 0 {
				var n int
				fmt.Sscanf(arg.Vals[0], "%d", &n) //nolint:errcheck
				startFileNum = DiskFileNum(n)
			}
		}
		*state = newPebbleRSEngineTestState(t, startFileNum)
		return ""

	case "open":
		if *state == nil {
			return "error: call init first"
		}
		var rangeID string
		var manifestNum DiskFileNum
		var startFileNum DiskFileNum
		hasStartFileNum := false
		for _, arg := range td.CmdArgs {
			switch arg.Key {
			case "range":
				if len(arg.Vals) > 0 {
					rangeID = arg.Vals[0]
				}
			case "manifest":
				if len(arg.Vals) > 0 {
					var n int
					fmt.Sscanf(arg.Vals[0], "%d", &n) //nolint:errcheck
					manifestNum = DiskFileNum(n)
				}
			case "start-file-num":
				if len(arg.Vals) > 0 {
					var n int
					fmt.Sscanf(arg.Vals[0], "%d", &n) //nolint:errcheck
					startFileNum = DiskFileNum(n)
					hasStartFileNum = true
				}
			}
		}
		if rangeID == "" {
			return "error: range= required"
		}
		// Create per-range committer if start-file-num specified.
		if hasStartFileNum {
			(*state).committers[rangeID] = newMockManifestChangeCommitter(startFileNum)
		}
		engine := (*state).openEngine(rangeID, manifestNum)
		(*state).engines[rangeID] = engine
		fmt.Fprintf(&buf, "opened %s: manifest=%d\n", rangeID, engine.currentManifestNum())
		return buf.String()

	case "ingest":
		if *state == nil {
			return "error: call init first"
		}
		var rangeID string
		for _, arg := range td.CmdArgs {
			if arg.Key == "range" && len(arg.Vals) > 0 {
				rangeID = arg.Vals[0]
			}
		}
		if rangeID == "" {
			return "error: range= required"
		}
		engine, ok := (*state).engines[rangeID]
		if !ok {
			return fmt.Sprintf("error: engine %s not found", rangeID)
		}
		// Parse key-value pairs from input.
		var kvs [][2]string
		for _, line := range strings.Split(td.Input, "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			parts := strings.SplitN(line, " ", 3)
			if len(parts) < 3 || parts[0] != "set" {
				return fmt.Sprintf("unsupported: %s (expected 'set <key> <value>')", line)
			}
			kvs = append(kvs, [2]string{parts[1], parts[2]})
		}
		// Build SST in scratch dir.
		scratchPath := (*state).fs.PathJoin((*state).scratchDir(rangeID), "test.sst")
		if err := buildSST((*state).fs, scratchPath, kvs); err != nil {
			return fmt.Sprintf("error building SST: %v", err)
		}
		committer := (*state).getCommitter(rangeID)
		committer.clearLog()
		if err := engine.flushSSTables([]string{"test.sst"}, nil); err != nil {
			return fmt.Sprintf("error: %v", err)
		}
		buf.WriteString(committer.getLog())
		// Simulate the apply-path install on the leader: stage the
		// manifest just written to disk, then promote it to current.
		manifestNum := committer.getLastInstalledManifest()
		if err := engine.prepareExternalManifest(manifestNum); err != nil {
			return fmt.Sprintf("error preparing manifest: %v", err)
		}
		engine.installPreparedManifest(manifestNum)
		fmt.Fprintf(&buf, "ingested: manifest=%d\n", manifestNum)
		return buf.String()

	case "snapshot":
		if *state == nil {
			return "error: call init first"
		}
		var rangeID string
		for _, arg := range td.CmdArgs {
			if arg.Key == "range" && len(arg.Vals) > 0 {
				rangeID = arg.Vals[0]
			}
		}
		if rangeID == "" {
			return "error: range= required"
		}
		engine, ok := (*state).engines[rangeID]
		if !ok {
			return fmt.Sprintf("error: engine %s not found", rangeID)
		}
		snap := engine.newSnapshot()
		snapName := (*state).snapshotName(rangeID, snap.ManifestNum())
		(*state).snapshots[snapName] = snap
		fmt.Fprintf(&buf, "snapshot %s\n", snapName)
		return buf.String()

	case "close-snapshot":
		if *state == nil {
			return "error: call init first"
		}
		var snapName string
		for _, arg := range td.CmdArgs {
			if arg.Key == "name" && len(arg.Vals) > 0 {
				snapName = arg.Vals[0]
			}
		}
		if snapName == "" {
			return "error: name= required"
		}
		snap, ok := (*state).snapshots[snapName]
		if !ok {
			return fmt.Sprintf("error: snapshot %s not found", snapName)
		}
		snap.Close()
		delete((*state).snapshots, snapName)
		fmt.Fprintf(&buf, "closed: %s\n", snapName)
		return buf.String()

	case "split":
		if *state == nil {
			return "error: call init first"
		}
		var lhsRangeID, rhsRangeID, snapName, splitKey string
		for _, arg := range td.CmdArgs {
			switch arg.Key {
			case "snapshot":
				if len(arg.Vals) > 0 {
					snapName = arg.Vals[0]
				}
			case "key":
				if len(arg.Vals) > 0 {
					splitKey = arg.Vals[0]
				}
			case "lhs-range":
				if len(arg.Vals) > 0 {
					lhsRangeID = arg.Vals[0]
				}
			case "rhs-range":
				if len(arg.Vals) > 0 {
					rhsRangeID = arg.Vals[0]
				}
			}
		}
		if snapName == "" || splitKey == "" || rhsRangeID == "" {
			return "error: snapshot=, key=, rhs-range= required"
		}
		snap, ok := (*state).snapshots[snapName]
		if !ok {
			return fmt.Sprintf("error: snapshot %s not found", snapName)
		}
		// Extract LHS range from snapshot name if not specified.
		if lhsRangeID == "" {
			parts := strings.Split(snapName, "-")
			if len(parts) >= 1 {
				lhsRangeID = parts[0]
			}
		}
		rhsDir := (*state).basaltDir(rhsRangeID)
		lhsManifest, rhsInfo, nextFileNum, err := snap.Split(context.Background(),
			roachpb.Key(splitKey), rhsDir)
		if err != nil {
			return fmt.Sprintf("error: %v", err)
		}

		if lhsManifest.Num == 0 {
			// NoOp: empty LSM or all tables on LHS.
			fmt.Fprintf(&buf, "no-op\n")
			return buf.String()
		}

		fmt.Fprintf(&buf, "lhs-manifest: %s/%d\n", lhsManifest.Name, lhsManifest.Num)
		fmt.Fprintf(&buf, "rhs-manifest: %s/%d\n", rhsInfo.Manifest.Name, rhsInfo.Manifest.Num)
		fmt.Fprintf(&buf, "rhs-files: [%d files]\n", len(rhsInfo.Files))
		fmt.Fprintf(&buf, "next-file-num: %d\n", nextFileNum)

		// Close snapshot.
		snap.Close()
		delete((*state).snapshots, snapName)

		// Close LHS and reopen with new manifest.
		lhsEngine := (*state).engines[lhsRangeID]
		lhsEngine.closeInner()
		(*state).engines[lhsRangeID] = (*state).openEngine(lhsRangeID, lhsManifest.Num)

		// Open RHS with new manifest.
		(*state).engines[rhsRangeID] = (*state).openEngine(rhsRangeID, rhsInfo.Manifest.Num)

		fmt.Fprintf(&buf, "reopened %s: manifest=%d\n", lhsRangeID, lhsManifest.Num)
		fmt.Fprintf(&buf, "opened %s: manifest=%d\n", rhsRangeID, rhsInfo.Manifest.Num)
		return buf.String()

	case "merge":
		if *state == nil {
			return "error: call init first"
		}
		var lhsSnapName, rhsSnapName string
		for _, arg := range td.CmdArgs {
			switch arg.Key {
			case "lhs-snapshot":
				if len(arg.Vals) > 0 {
					lhsSnapName = arg.Vals[0]
				}
			case "rhs-snapshot":
				if len(arg.Vals) > 0 {
					rhsSnapName = arg.Vals[0]
				}
			}
		}
		if lhsSnapName == "" || rhsSnapName == "" {
			return "error: lhs-snapshot=, rhs-snapshot= required"
		}
		lhsSnap, ok := (*state).snapshots[lhsSnapName]
		if !ok {
			return fmt.Sprintf("error: lhs snapshot %s not found", lhsSnapName)
		}
		rhsSnap, ok := (*state).snapshots[rhsSnapName]
		if !ok {
			return fmt.Sprintf("error: rhs snapshot %s not found", rhsSnapName)
		}
		// Parse range IDs from snapshot names.
		lhsParts := strings.Split(lhsSnapName, "-")
		rhsParts := strings.Split(rhsSnapName, "-")
		lhsRangeID := lhsParts[0]
		rhsRangeID := rhsParts[0]

		merged, nextFileNum, err := lhsSnap.Merge(context.Background(), rhsSnap)
		if err != nil {
			return fmt.Sprintf("error: %v", err)
		}

		if merged.Manifest.Num == 0 {
			// NoOp: RHS has no tables.
			// Close snapshots.
			lhsSnap.Close()
			delete((*state).snapshots, lhsSnapName)
			rhsSnap.Close()
			delete((*state).snapshots, rhsSnapName)
			fmt.Fprintf(&buf, "no-op\n")
			return buf.String()
		}

		fmt.Fprintf(&buf, "merged-manifest: %s/%d\n", merged.Manifest.Name, merged.Manifest.Num)
		fmt.Fprintf(&buf, "merged-files: [%d files]\n", len(merged.Files))
		fmt.Fprintf(&buf, "next-file-num: %d\n", nextFileNum)

		// Close both snapshots.
		lhsSnap.Close()
		delete((*state).snapshots, lhsSnapName)
		rhsSnap.Close()
		delete((*state).snapshots, rhsSnapName)

		// Close RHS engine.
		rhsEngine := (*state).engines[rhsRangeID]
		rhsEngine.closeInner()
		delete((*state).engines, rhsRangeID)

		// Close LHS and reopen with merged manifest.
		lhsEngine := (*state).engines[lhsRangeID]
		lhsEngine.closeInner()
		(*state).engines[lhsRangeID] = (*state).openEngine(lhsRangeID, merged.Manifest.Num)
		fmt.Fprintf(&buf, "reopened %s: manifest=%d\n", lhsRangeID, merged.Manifest.Num)
		return buf.String()

	case "file-names":
		if *state == nil {
			return "error: call init first"
		}
		dir := "/"
		for _, arg := range td.CmdArgs {
			if arg.Key == "dir" && len(arg.Vals) > 0 {
				dir = arg.Vals[0]
			}
		}
		// Wait for Pebble's async obsolete file cleanup to complete
		// so the listing is deterministic.
		for _, eng := range (*state).engines {
			eng.(*pebbleRSEngine).db.TestOnlyWaitForCleaning()
		}
		return PrintFileNames((*state).fs, dir, nil)

	case "scan":
		if *state == nil {
			return "error: call init first"
		}
		var rangeID string
		for _, arg := range td.CmdArgs {
			if arg.Key == "range" && len(arg.Vals) > 0 {
				rangeID = arg.Vals[0]
			}
		}
		if rangeID == "" {
			return "error: range= required"
		}
		engine, ok := (*state).engines[rangeID]
		if !ok {
			return fmt.Sprintf("error: engine %s not found", rangeID)
		}
		db := engine.(*pebbleRSEngine).db
		iter, err := db.NewIter(nil)
		if err != nil {
			return fmt.Sprintf("error: %v", err)
		}
		for valid := iter.First(); valid; valid = iter.Next() {
			key, err := DecodeMVCCKey(iter.Key())
			if err != nil {
				fmt.Fprintf(&buf, "error decoding key: %v\n", err)
				continue
			}
			fmt.Fprintf(&buf, "%s = %s\n", key.Key, iter.Value())
		}
		iterErr := iter.Error()
		if err := iter.Close(); err != nil {
			return fmt.Sprintf("error closing iterator: %v", err)
		}
		if iterErr != nil {
			return fmt.Sprintf("error iterating: %v", iterErr)
		}
		if buf.Len() == 0 {
			fmt.Fprintf(&buf, "<empty>\n")
		}
		return buf.String()

	case "close":
		if *state == nil {
			return "error: call init first"
		}
		var rangeID string
		for _, arg := range td.CmdArgs {
			if arg.Key == "range" && len(arg.Vals) > 0 {
				rangeID = arg.Vals[0]
			}
		}
		if rangeID == "" {
			return "error: range= required"
		}
		engine, ok := (*state).engines[rangeID]
		if !ok {
			return fmt.Sprintf("error: engine %s not found", rangeID)
		}
		engine.closeInner()
		delete((*state).engines, rangeID)
		fmt.Fprintf(&buf, "closed: %s\n", rangeID)
		return buf.String()

	default:
		return fmt.Sprintf("unknown command: %s", td.Cmd)
	}
}

// TestPebbleRSEngineMergeDatadriven runs datadriven tests for pebbleRSEngine
// Merge. Exercises the real pebble-backed engine with actual SSTs and pebble's
// MergeLSM API.
func TestPebbleRSEngineMergeDatadriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	var state *pebbleRSEngineTestState
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "pebble_rs_engine_merge"),
		func(t *testing.T, td *datadriven.TestData) string {
			return pebbleRSEngineRunCommand(t, td, &state)
		})
}

// TestPebbleRSEngineCompactionInstall verifies that when Pebble runs a
// compaction on a range-shared LSM, the resulting manifest is installed
// via ManifestChangeCommitter.InstallNewManifest.
func TestPebbleRSEngineCompactionInstall(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	fs := vfs.NewMem()
	committer := newMockManifestChangeCommitter(100)
	opts := RSEngineOptions{
		manifestChangeCommitter: committer,
		basaltFS:                fs,
		basaltDir:               "/r1",
		basaltScratchPathDir:    "/r1-scratch",
		logCtx:                  context.Background(),
	}
	engine, err := OpenRSEngine(0, opts)
	require.NoError(t, err)
	defer engine.closeInner()

	// Ingest overlapping SSTs to build up L0 files. All SSTs contain the
	// same key so they can't all be placed at the same level.
	for i := 0; i < 20; i++ {
		scratchPath := fs.PathJoin("/r1-scratch", "test.sst")
		require.NoError(t, buildSST(fs, scratchPath, [][2]string{
			{"a", fmt.Sprintf("v%d", i)},
		}))
		committer.clearLog()
		require.NoError(t, engine.flushSSTables([]string{"test.sst"}, nil))
		if manifestNum := committer.getLastInstalledManifest(); manifestNum != NoManifestNum {
			require.NoError(t, engine.prepareExternalManifest(manifestNum))
			engine.installPreparedManifest(manifestNum)
		}
	}

	// Enable compactions. With L0 files accumulated, Pebble should
	// schedule and run a compaction.
	committer.clearLog()
	engine.compactionToggle(true)

	// Wait for the compaction to complete. Pebble calls GetFileNums (via
	// FileNumAllocator) during the compaction to allocate file
	// numbers, then the result goroutine calls InstallNewManifest.
	require.Eventually(t, func() bool {
		return strings.Contains(committer.getLog(), "InstallNewManifest")
	}, 5*time.Second, 10*time.Millisecond,
		"expected compaction to trigger InstallNewManifest")

	installLog := committer.getLog()
	require.Contains(t, installLog, "GetFileNums",
		"compaction installs should call GetFileNums")
}
