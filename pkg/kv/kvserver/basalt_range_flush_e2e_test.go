// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// sealRequiredFS wraps a vfs.FS to simulate BasaltFS's seal semantics:
// files created with Create() buffer writes locally. The buffered data
// is flushed to the underlying FS only on Close(), simulating the
// requirement that files must be sealed (closed) before their data is
// visible to other file handles, Stat(), or through Links.
type sealRequiredFS struct {
	vfs.FS
}

// Create wraps the underlying Create to return a sealRequiredFile that
// buffers writes until Close.
func (fs *sealRequiredFS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	f, err := fs.FS.Create(name, category)
	if err != nil {
		return nil, err
	}
	return &sealRequiredFile{File: f}, nil
}

// ReuseForWrite wraps the underlying ReuseForWrite to return a
// sealRequiredFile that buffers writes until Close.
func (fs *sealRequiredFS) ReuseForWrite(
	oldname, newname string, category vfs.DiskWriteCategory,
) (vfs.File, error) {
	f, err := fs.FS.ReuseForWrite(oldname, newname, category)
	if err != nil {
		return nil, err
	}
	return &sealRequiredFile{File: f}, nil
}

// sealRequiredFile buffers all Write calls. The buffered data is flushed
// to the underlying file only on Close, making the data invisible to
// readers (including via Link) until the file is sealed.
type sealRequiredFile struct {
	vfs.File
	buf bytes.Buffer
}

func (f *sealRequiredFile) Write(p []byte) (int, error) {
	return f.buf.Write(p)
}

func (f *sealRequiredFile) WriteAt(p []byte, off int64) (int, error) {
	return 0, errors.New("WriteAt not supported on sealRequiredFile")
}

func (f *sealRequiredFile) Close() error {
	if f.buf.Len() > 0 {
		if _, err := f.File.Write(f.buf.Bytes()); err != nil {
			return err
		}
	}
	return f.File.Close()
}

// TestBasaltRangeFlushDatadriven exercises the full RangeFlush path using
// real MVCC data and verifies the flushed SST contents in the range-shared
// engine.
func TestBasaltRangeFlushDatadriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	datadriven.Walk(t, datapathutils.TestDataPath(t, "basalt_range_flush_e2e"), func(t *testing.T, path string) {
		var state *basaltE2ETestState
		// Track last-seen ApproxStoreLocalBytes per range for delta reporting.
		prevApproxBytes := make(map[roachpb.RangeID]int64)
		defer func() {
			if state != nil && state.tc != nil {
				state.tc.Stopper().Stop(ctx)
			}
		}()
		datadriven.RunTest(t, path, func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "init":
				var engineType string
				numNodes := 1
				for _, arg := range td.CmdArgs {
					switch arg.Key {
					case "engine":
						if len(arg.Vals) > 0 {
							engineType = arg.Vals[0]
						}
					case "stores":
						if len(arg.Vals) > 0 {
							fmt.Sscanf(arg.Vals[0], "%d", &numNodes) //nolint:errcheck
						}
					}
				}
				state = initBasaltRangeFlushTestCluster(t, ctx, engineType, numNodes)
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
				state.rangeIDMap = make(map[roachpb.RangeID]int)
				state.nextSyntheticID = 1
				state.rangeIDMap[repl.RangeID] = state.nextSyntheticID
				state.nextSyntheticID++
				return "ok\n"

			case "put":
				if state == nil {
					return "error: call init first"
				}
				var key, value string
				for _, arg := range td.CmdArgs {
					switch arg.Key {
					case "key":
						if len(arg.Vals) > 0 {
							key = arg.Vals[0]
						}
					case "value":
						if len(arg.Vals) > 0 {
							value = arg.Vals[0]
						}
					}
				}
				if key == "" {
					return "error: key parameter required\n"
				}
				// Get the scratch range start key and append the user key.
				repl, err := state.getReplicaForRange(1, 0)
				if err != nil {
					return fmt.Sprintf("error: %v\n", err)
				}
				startKey := repl.Desc().StartKey.AsRawKey()
				fullKey := append(startKey[:len(startKey):len(startKey)], []byte(key)...)
				db := state.tc.Server(0).DB()
				if err := db.Put(ctx, fullKey, value); err != nil {
					return fmt.Sprintf("error: %v\n", err)
				}
				return "ok\n"

			case "range-flush":
				if state == nil {
					return "error: call init first"
				}
				syntheticID, storeIdx := parseRangeAndStore(td)
				if syntheticID == 0 {
					return "error: rangeID parameter required\n"
				}
				repl, err := state.getReplicaForRange(syntheticID, storeIdx)
				if err != nil {
					return fmt.Sprintf("error: %v\n", err)
				}
				if err := repl.ManifestCommitter().RangeFlush(); err != nil {
					return fmt.Sprintf("error: %v\n", err)
				}
				return "flushed\n"

			case "add-replica":
				if state == nil {
					return "error: call init first"
				}
				syntheticID, storeIdx := parseRangeAndStore(td)
				if syntheticID == 0 {
					return "error: rangeID parameter required\n"
				}
				repl, err := state.getReplicaForRange(syntheticID, 0)
				if err != nil {
					return fmt.Sprintf("error: %v\n", err)
				}
				startKey := repl.Desc().StartKey.AsRawKey()
				desc := state.tc.AddVotersOrFatal(
					t, startKey, state.tc.Target(storeIdx),
				)
				return fmt.Sprintf(
					"added replica to store %d, replicas: %d\n",
					storeIdx+1, len(desc.Replicas().Descriptors()),
				)

			case "scan-rs-engine":
				if state == nil {
					return "error: call init first"
				}
				syntheticID, storeIdx := parseRangeAndStore(td)
				if syntheticID == 0 {
					return "error: rangeID parameter required\n"
				}
				return scanRSEngineSSTs(t, state, syntheticID, storeIdx)

			case "approx-store-local-bytes":
				if state == nil {
					return "error: call init first"
				}
				syntheticID, storeIdx := parseRangeAndStore(td)
				if syntheticID == 0 {
					return "error: rangeID parameter required\n"
				}
				repl, err := state.getReplicaForRange(syntheticID, storeIdx)
				if err != nil {
					return fmt.Sprintf("error: %v\n", err)
				}
				store, err := state.getStore(storeIdx)
				if err != nil {
					return fmt.Sprintf("error: %v\n", err)
				}
				sl := kvstorage.MakeStateLoader(repl.RangeID)
				as, err := sl.LoadRangeAppliedState(ctx, store.StateEngine())
				if err != nil {
					return fmt.Sprintf("error: %v\n", err)
				}
				cur := as.ApproxStoreLocalBytes
				var sign string
				if cur > 0 {
					sign = "positive"
				} else if cur == 0 {
					sign = "zero"
				} else {
					sign = "negative"
				}
				prev, hasPrev := prevApproxBytes[repl.RangeID]
				prevApproxBytes[repl.RangeID] = cur
				if !hasPrev {
					return fmt.Sprintf("%s\n", sign)
				}
				var delta string
				if cur > prev {
					delta = "increased"
				} else if cur < prev {
					delta = "decreased"
				} else {
					delta = "unchanged"
				}
				return fmt.Sprintf("%s(%s)\n", sign, delta)

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
				return storage.PrintFileNames(state.basaltFS, "/", state.rangeIDMap)

			case "engine-lifecycle":
				if state == nil {
					return "error: call init first"
				}
				syntheticID, _ := parseRangeAndStore(td)
				if syntheticID == 0 {
					return "error: rangeID parameter required\n"
				}
				state.engineLog.Lock()
				entries := make([]engineLogEntry, len(state.engineLog.entries))
				copy(entries, state.engineLog.entries)
				state.engineLog.Unlock()
				var buf strings.Builder
				for _, e := range entries {
					synthID, ok := state.rangeIDMap[e.rangeID]
					if !ok || synthID != syntheticID {
						continue
					}
					fmt.Fprintf(&buf, "%s(manifest=%d)\n", e.action, e.manifestNum)
				}
				if buf.Len() == 0 {
					return "no lifecycle events\n"
				}
				return buf.String()

			case "wait-for-manifest":
				// Wait for a follower's RSEngine to reach the expected manifest
				// number. The manifest propagates asynchronously via Raft, so
				// polling is needed after leaseholder-initiated flushes.
				if state == nil {
					return "error: call init first"
				}
				syntheticID, storeIdx := parseRangeAndStore(td)
				if syntheticID == 0 {
					return "error: rangeID parameter required\n"
				}
				var expectManifest uint64
				for _, arg := range td.CmdArgs {
					if arg.Key == "manifest" && len(arg.Vals) > 0 {
						fmt.Sscanf(arg.Vals[0], "%d", &expectManifest) //nolint:errcheck
					}
				}
				if expectManifest == 0 {
					return "error: manifest parameter required\n"
				}
				testutils.SucceedsSoon(t, func() error {
					engine, err := state.getRSEngine(syntheticID, storeIdx)
					if err != nil {
						return err
					}
					if engine == nil {
						return fmt.Errorf("no RSEngine configured yet")
					}
					cur := uint64(engine.CurrentManifestNum())
					if cur != expectManifest {
						return fmt.Errorf(
							"manifest %d != expected %d", cur, expectManifest,
						)
					}
					return nil
				})
				return "ok\n"

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
				if testEngine, ok := engine.TestingInnerEngine().(*storage.TestingRSEngine); ok {
					return storage.PrintRSEngineState(testEngine)
				}
				// For pebble engine, print a summary from the snapshot.
				snap := engine.NewSnapshot()
				defer snap.Close()
				info := snap.ManifestInfo()
				var buf strings.Builder
				fmt.Fprintf(&buf, "current-manifest: %d\n", snap.ManifestNum())
				fmt.Fprintf(&buf, "files: %d\n", len(info.Files))
				return buf.String()

			default:
				return fmt.Sprintf("unknown command: %s\n", td.Cmd)
			}
		})
	})
}

// initBasaltRangeFlushTestCluster creates a TestCluster configured for range
// flush testing. engineType selects the RSEngine implementation: "pebble" uses
// storage.OpenRSEngine (real pebble), "pebble-seal" uses OpenRSEngine with a
// sealRequiredFS wrapper that simulates BasaltFS seal semantics, anything else
// uses the default storage.OpenTestingRSEngine.
func initBasaltRangeFlushTestCluster(
	t testing.TB, ctx context.Context, engineType string, numNodes int,
) *basaltE2ETestState {
	state := &basaltE2ETestState{
		t:        t,
		ctx:      ctx,
		basaltFS: vfs.NewMem(),
	}
	var baseOpenFunc storage.OpenRSEngineFunc
	switch engineType {
	case "pebble":
		baseOpenFunc = storage.OpenRSEngine
	case "pebble-seal":
		baseOpenFunc = storage.OpenRSEngine
		state.basaltFS = &sealRequiredFS{FS: vfs.NewMem()}
	default:
		baseOpenFunc = storage.OpenTestingRSEngine
	}
	openRSEngine := func(
		manifestNum storage.DiskFileNum, opts storage.RSEngineOptions,
	) (storage.InnerRSEngine, error) {
		engine, err := baseOpenFunc(manifestNum, opts)
		if err == nil {
			if rangeID, ok := parseRangeIDFromBasaltDir(opts.BasaltDir()); ok {
				state.engineLog.Lock()
				state.engineLog.entries = append(state.engineLog.entries, engineLogEntry{
					action:      "open",
					manifestNum: manifestNum,
					rangeID:     rangeID,
				})
				state.engineLog.Unlock()
			}
		}
		return engine, err
	}
	storeKnobs := kvserver.StoreTestingKnobs{
		BasaltFS:               state.basaltFS,
		OpenRSEngine:           openRSEngine,
		WriteClearRangeOnFlush: true,
	}
	args := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &storeKnobs,
			},
		},
	}
	state.tc = testcluster.StartTestCluster(t, numNodes, args)
	return state
}

// parseRangeIDFromBasaltDir extracts the range ID from a BasaltDir path.
// The path format is s<storeID>/ranges/r<rangeID>:<replicaID>.
func parseRangeIDFromBasaltDir(basaltDir string) (roachpb.RangeID, bool) {
	lastSlash := strings.LastIndex(basaltDir, "/")
	if lastSlash < 0 {
		return 0, false
	}
	base := basaltDir[lastSlash+1:]
	var rangeID, replicaID int
	if n, _ := fmt.Sscanf(base, "r%d:%d", &rangeID, &replicaID); n >= 1 {
		return roachpb.RangeID(rangeID), true
	}
	return 0, false
}

// scanRSEngineSSTs reads all SST files from the range-shared engine,
// decodes the MVCC keys, and prints them grouped by SST file. Within
// each SST, keys are printed in iteration order. Only point keys
// within the range's user key span are printed. The user key is
// printed relative to the range start key for readability.
func scanRSEngineSSTs(
	t *testing.T, state *basaltE2ETestState, syntheticID int, storeIdx int,
) string {
	t.Helper()
	repl, err := state.getReplicaForRange(syntheticID, storeIdx)
	if err != nil {
		return fmt.Sprintf("error: %v\n", err)
	}
	engine, err := state.getRSEngine(syntheticID, storeIdx)
	if err != nil {
		return fmt.Sprintf("error: %v\n", err)
	}
	if engine == nil {
		return "no RSEngine configured\n"
	}
	snap := engine.NewSnapshot()
	defer snap.Close()
	manifestInfo := snap.ManifestInfo()

	// Get the range start key to strip from output.
	startKey := repl.Desc().StartKey.AsRawKey()

	store, err := state.getStore(storeIdx)
	require.NoError(t, err)
	basaltFS := state.basaltFS
	basaltDir := kvserver.BasaltDir(
		basaltFS, store.StoreID(), repl.RangeID, repl.ReplicaID(),
	)

	var buf strings.Builder
	for _, fileInfo := range manifestInfo.Files {
		fmt.Fprintf(&buf, "%s:\n", fileInfo.Name)
		sstPath := basaltFS.PathJoin(basaltDir, fileInfo.Name)
		f, err := basaltFS.Open(sstPath)
		if err != nil {
			fmt.Fprintf(&buf, "  error opening: %v\n", err)
			continue
		}
		readable, err := objstorage.NewSimpleReadable(f)
		if err != nil {
			fmt.Fprintf(&buf, "  error creating readable: %v\n", err)
			continue
		}
		readerOpts := storage.DefaultPebbleOptions().MakeReaderOptions()
		readerOpts.Mergers = map[string]*pebble.Merger{
			"cockroach_merge_operator": storage.MVCCMerger,
			"pebble.concatenate":       pebble.DefaultMerger,
		}
		readerOpts.Comparers = map[string]*pebble.Comparer{
			storage.EngineComparer.Name:  &storage.EngineComparer,
			"leveldb.BytewiseComparator": pebble.DefaultComparer,
		}
		reader, err := sstable.NewReader(context.Background(), readable, readerOpts)
		if err != nil {
			fmt.Fprintf(&buf, "  error creating reader: %v\n", err)
			continue
		}
		iter, err := reader.NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
		if err != nil {
			reader.Close() //nolint:errcheck
			fmt.Fprintf(&buf, "  error creating iter: %v\n", err)
			continue
		}
		for sstKV := iter.First(); sstKV != nil; sstKV = iter.Next() {
			mvccKey, err := storage.DecodeMVCCKey(sstKV.K.UserKey)
			if err != nil {
				continue // skip non-MVCC keys
			}
			if mvccKey.Key.Compare(startKey) < 0 {
				continue
			}
			val, _, err := sstKV.V.Value(nil)
			if err != nil {
				iter.Close()   //nolint:errcheck
				reader.Close() //nolint:errcheck
				return fmt.Sprintf("error reading value: %v\n", err)
			}
			userKey := string(mvccKey.Key[len(startKey):])
			var roachVal roachpb.Value
			roachVal.RawBytes = val
			valueBytes, err := roachVal.GetBytes()
			if err != nil {
				fmt.Fprintf(&buf, "  %s => %x\n", userKey, val)
			} else {
				fmt.Fprintf(&buf, "  %s => %s\n", userKey, string(valueBytes))
			}
		}
		if err := iter.Close(); err != nil {
			reader.Close() //nolint:errcheck
			return fmt.Sprintf("error closing iter: %v\n", err)
		}
		reader.Close() //nolint:errcheck
	}
	if buf.Len() == 0 {
		return "no SSTs found\n"
	}
	return buf.String()
}
