// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"context"
	"io"
	"os"
	"strconv"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/cockroachkvs"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/tablefilters/bloom"
	"github.com/cockroachdb/pebble/vfs"
)

// ErrRSEngineClosed is returned when an operation is attempted on a closed
// RSEngine. Callers can use errors.Is to detect this condition and retry.
var ErrRSEngineClosed = errors.New("engine is closed")

// TODO(basalt): export Pebble's base.DiskFileNum type and use that instead of
// this one.
type DiskFileNum uint64

// NoManifestNum indicates that no Raft-allocated manifest exists yet. A fresh
// RSEngine opened with this value has no data; Pebble creates an internal
// bootstrap manifest (MANIFEST-000000 for range-shared LSMs) but CRDB does not
// track it. The first InstallPreparedManifest call replaces this with a real
// Raft-allocated number.
// Code throughout kvserver checks for this sentinel to skip snapshot
// transmission, hardlink creation, and engine opening when no RS data exists.
const NoManifestNum DiskFileNum = 0

// Manifest basic distributed lifecycle (failures possible at every point):
//
// written-to-basalt-path-for-local-store-rangeid-replicaid =>
// hardlinked-to-basalt-path-for-all-replicas => proposed-via-raft =>
// applied-to-current-pointer => current-pointer-application-durable.
//
// The first step is the responsibility of the InnerRSEngine, and the rest are
// the responsibility of the higher layer (kvserver).
//
// The current-pointer is in the store-local engine. Only when it is durable and
// there are no older quiesced DBs can the old manifests and unneeded files be
// GCed (also subject to no open iterators/snapshots etc., which is the current
// Pebble behavior). The responsibility of GC lies with the InnerRSEngine when it is
// informed that it is safe to do it via enableUnreferencedFileDeletion().
//
// Manifest lifecycle from the perspective of a InnerRSEngine:
//
// An InnerRSEngine at the leaseholder sees the most action. But it doesn't know
// or care about leases. All it knows that the capability to install a new
// manifest has been externalized to a different entity. But care has to be
// taken to ensure that manifest updates are serialized, in that the next
// manifest being proposed is an immediate successor of a particular manifest,
// and the next manifest can only be installed if the immediate successor
// relationship is maintained.
//
// We will see in the interfaces below how this predecessor relationship is
// conveyed to the external layer in the parameter to
// ManifestChangeCommitter.InstallNewManifest, and is available to the
// external layer when it calls split or merge on a RSEngineSnapshot.
//
// Causes of manifest changes:
//
// Compactions: the range shared engine can have many concurrent compactions
// ongoing that don't conflict with each other, and an ongoing range flush.
//
// For a compaction, Pebble internally writes output files to a scratch
// directory with temporary file numbers. When the compaction completes,
// Pebble calls ManifestChangeCommitter.GetFileNums (via the
// FileNumAllocator callback) to allocate globally-coordinated
// file numbers, and remaps and hardlinks the output files to BasaltDir.
// Pebble then calls InstallNewManifest.
//
// Range flush: the output files are written to BasaltScratchPathDir by the
// caller of RSEngine.FlushSSTables. The flush calls DB.Ingest, which calls
// ManifestChangeCommitter.{GetFileNums,InstallNewManifest}.
//
// AddSSTables (index backfill) follows the same pattern as range flush:
// files are provided in the scratch directory, and the engine calls
// ManifestChangeCommitter.{GetFileNums,InstallNewManifest}.
//
// Split, Merge.

// FlushCommitInfo carries flush-commit-specific parameters through
// FlushSSTables and InstallNewManifest. These are not used for
// compaction manifest installs, or for AddSSTables.
type FlushCommitInfo struct {
	ExpectedFlushStartedCount uint64
	// ActivateSpans are the key spans over which ClearRawRangeActivate
	// is written, activating dormant deletions from flush prepare.
	// Currently just the user key span.
	ActivateSpans []roachpb.Span
	// FlushedApproxStoreLocalBytes is the ApproxStoreLocalBytes value
	// captured at flush prepare time. Subtracted from the range's
	// ApproxStoreLocalBytes on flush commit.
	FlushedApproxStoreLocalBytes int64
}

// ManifestChangeCommitter is the interface provided to the range shared
// engine for manifest changes. The committer can return an error for various
// reasons, including when no longer the range leaseholder.
type ManifestChangeCommitter interface {
	// GetFileNums is used to fetch filenums for compactions, ingest, flush,
	// Split, Merge. All these calls originate in Pebble. They should be totally
	// ordered in the manner that Pebble expects the new manifests to be installed
	// (safety does not rely on the ordering, but we want to avoid wasted work).
	GetFileNums(count int) ([]DiskFileNum, error)
	// InstallNewManifest is called by pebble.DB after it has:
	// 1. Hardlinked files from scratch to its own directory
	// 2. Created the manifest in its own directory
	//
	// The ManifestChangeCommitter:
	// 1. Hardlinks files to all OTHER replicas: thisStore/dir/name → otherStore/dir/name
	// 2. Raft-commits the manifest change
	// 3. Updates the current-pointer in the state machine
	//
	// The currentManifestNum is the manifest number being replaced, passed
	// explicitly by the RSEngine. The manifestInfo.Files contains the new
	// files (basenames) that need to be hardlinked to other replicas.
	//
	// Used for compactions, flush, add-sstable. Split/merge installation for
	// other replicas does not originate in Pebble, hence does not use
	// InstallNewManifest.
	//
	// ingestHandle is non-nil for flush commits and carries *FlushCommitInfo.
	InstallNewManifest(
		currentManifestNum DiskFileNum, manifestInfo ManifestInfo, ingestHandle interface{}) error
}

type FileNameAndNum struct {
	// Name is the basename of the file, without any directory components.
	Name string
	Num  DiskFileNum
}

type ManifestInfo struct {
	Manifest FileNameAndNum
	Files    []FileNameAndNum
}

// pebbleManifestCommitter adapts a CRDB ManifestChangeCommitter to Pebble's
// ManifestCommitter interface. Pebble invokes InstallNewManifest from inside
// UpdateVersionLocked once the new manifest is on disk; this adapter
// translates Pebble's DiskFileNum-based ManifestInstallInfo into the
// basename-bearing ManifestInfo expected by the cross-replica commit path,
// and forwards the opaque ingestHandle (e.g. *FlushCommitInfo) unchanged.
type pebbleManifestCommitter struct {
	inner ManifestChangeCommitter
}

// InstallNewManifest implements pebble.ManifestCommitter.
func (p *pebbleManifestCommitter) InstallNewManifest(
	currentManifestNum pebble.DiskFileNum, info pebble.ManifestInstallInfo, ingestHandle interface{},
) error {
	files := make([]FileNameAndNum, len(info.NewTables))
	for i, num := range info.NewTables {
		dn := DiskFileNum(num)
		files[i] = FileNameAndNum{Name: formatSSTName(dn), Num: dn}
	}
	manifestNum := DiskFileNum(info.NewManifestFileNum)
	mi := ManifestInfo{
		Manifest: FileNameAndNum{
			Name: formatManifestName(manifestNum),
			Num:  manifestNum,
		},
		Files: files,
	}
	return p.inner.InstallNewManifest(DiskFileNum(currentManifestNum), mi, ingestHandle)
}

// RSEngineOptions holds options for opening an InnerRSEngine. Fields are
// package-private; RSEngineContainerOptions is the public API for callers
// outside pkg/storage.
type RSEngineOptions struct {
	manifestChangeCommitter ManifestChangeCommitter
	basaltFS                vfs.FS
	// basaltDir is the directory containing the range-shared engine data files.
	// In practice, this will be something akin to
	// <clusterID>/s<storeID>/r<rangeID>:<replicaID>.
	basaltDir string
	// basaltScratchPathDir is a directory to use for scratch files. It is a
	// directory that is cleared on restart. Every RSEngine has its own
	// directory, so there is no risk of cross-engine name collisions. In
	// practice, this will be something akin to
	// <clusterID>/s<storeID>/scratch/r<rangeID>:<replicaID>.
	basaltScratchPathDir string
	// compactionScheduler, if non-nil, is used for pebble compaction
	// scheduling.
	compactionScheduler CompactionSchedulerPlus
	// logCtx is the context used for Pebble's logger. It should carry
	// logging tags (e.g. node, store, range) from the caller.
	logCtx context.Context
	// testingProcessID, if non-empty, overrides the os.Getpid() value used to
	// name Pebble's per-process scratch directory. Tests set this to a fixed
	// value so listings of the on-disk file tree are deterministic.
	testingProcessID string
}

// BasaltDir returns the directory containing the range-shared engine data
// files.
func (o RSEngineOptions) BasaltDir() string {
	return o.basaltDir
}

// InnerRSEngine is the interface for the underlying range-shared engine
// implementation. Methods are unexported because only rsEngineContainer
// (within this package) calls them. External packages use the exported
// RSEngine container interface instead.
//
// The type name is exported so that OpenRSEngineFunc and
// RSEngine.TestingInnerEngine can reference it as a return type.
type InnerRSEngine interface {
	// compactionToggle is called to enable or disable compactions. Compactions
	// are enabled only at the range leaseholder. The higher layer will also
	// disable compactions during split and merge operations.
	compactionToggle(enable bool)
	// enableUnreferencedFileDeletion is called when the engine will never be
	// reopened pointing to an older manifest, and there are no older quiesced
	// engines.
	//
	// https://github.com/cockroachlabs/basalt/issues/289 should obviate the
	// need for durability detection, if we go with the WAG solution.
	enableUnreferencedFileDeletion()
	// waitForOngoingManifestChanges blocks until ongoing flushSSTables,
	// addSSTables, and compaction InstallNewManifest calls complete. Called
	// after compactionToggle(false) to drain pending work before Split/Merge.
	// Best-effort: new operations may start if compactions are re-enabled.
	// Safety relies on higher layer doing the split/merge ensuring that the
	// manifest num(s) of the range(s) involved in the split/merge have not
	// changed since the caller called RSEngineSnapshot.{Split,Merge}.
	waitForOngoingManifestChanges()
	// currentManifestNum returns the DiskFileNum of the current manifest.
	currentManifestNum() DiskFileNum
	// flushSSTables is used for non-overlapping sstables that do not contain
	// multiple key-value pairs for the same userkey. The callee assigns a single
	// seqnum to each sstable. The scratchNames are basenames in the scratch
	// directory. The engine hardlinks these to its real directory before calling
	// InstallNewManifest.
	//
	// flushCommit carries the expected FlushStartedCount and RANGEDEL_TURN_ON
	// spans through to the Raft request via InstallNewManifest.
	flushSSTables(scratchNames []string, flushCommit *FlushCommitInfo) error
	// addSSTables is used for non-overlapping sstables that do not contain
	// multiple key-value pairs for the same userkey. For example, for index
	// backfill. The callee assigns a single seqnum to each sstable. The
	// scratchNames are basenames in the scratch directory. The engine hardlinks
	// these to its real directory before calling InstallNewManifest.
	addSSTables(scratchNames []string) error
	// ref increments the external reference count, which prevents closeInner from
	// completing.
	ref()
	// unref decrements the external reference count. Must be paired with a
	// prior ref call. closeInner blocks until all ref/unref pairs complete.
	unref()
	// newSnapshot creates a new RSEngineSnapshot at the current manifest.
	newSnapshot() RSEngineSnapshot
	// prepareExternalManifest reads a manifest file (previously hardlinked into
	// BasaltDir by the leaseholder) from disk, builds the in-memory state, and
	// stages it as a candidate. The caller guarantees that the manifestNum is the
	// one immediately succeeding currentManifestNum.
	prepareExternalManifest(manifestNum DiskFileNum) error
	// installPreparedManifest promotes the prepared candidate version to current.
	// Must only be called after prepareExternalManifest returned with no error.
	// After this call, currentManifestNum() returns manifestNum.
	installPreparedManifest(manifestNum DiskFileNum)
	// quiesce prevents new background work (flushes, compactions) from starting.
	// The DB remains open so that outstanding snapshots continue to work, and new
	// snapshots can be created. In-flight compactions finish naturally, but will
	// likely fail since the manifestNum of this DB has already been superceded.
	// quiesce does NOT wait for outstanding ref/unref pairs to drain. Callers
	// must still call closeInner() afterward to drain refs and release resources.
	quiesce()
	// closeInner closes the engine, releasing all resources. This method blocks
	// until all outstanding RSEngineSnapshots are closed, and all ref/unref
	// pairs complete.
	closeInner()
}

type RSEngineSnapshot interface {
	ManifestInfo() ManifestInfo
	// ManifestNum returns the DiskFileNum of the manifest of the snapshot.
	ManifestNum() DiskFileNum
	// Clone creates a new RSEngineSnapshot that shares the same pinned manifest
	// state.
	Clone() RSEngineSnapshot
	// Split is called before the split 2PC transaction starts. The caller
	// disables compactions and drains (WaitForOngoingManifestChanges) before
	// taking a snapshot and calling Split. The snapshot pins the manifest
	// state so Split operates on a consistent view.
	//
	// The Split method (using parent RSEngine's directory info):
	// 1. Creates the new LHS manifest in its own directory
	// 2. Hardlinks RHS files and manifest to rhsDir
	//
	// Returns:
	// - lhsManifest: FileNameAndNum for the new LHS manifest
	// - rhs: ManifestInfo with manifest and files in rhsDir
	//
	// The caller hardlinks to other replicas:
	// - LHS manifest: thisStore/lhsDir/name → otherStore/lhsDir/name
	// - RHS manifest+files: thisStore/rhsDir/name → otherStore/rhsDir/name
	//
	// And the caller is responsible for all the other actions of the split,
	// and updating the current pointers of both ranges in all replicas, via
	// the split 2PC transaction. If the 2PC transaction fails, the caller
	// enables compactions again and can retry later.
	Split(ctx context.Context, splitKey roachpb.Key, rhsDir string) (
		lhsManifest FileNameAndNum, rhs ManifestInfo, nextFileNum uint64, err error)
	// Merge is called before the merge 2PC transaction starts. The caller
	// disables compactions and drains (WaitForOngoingManifestChanges) on both
	// engines before taking snapshots. The LHS snapshot's Merge method is
	// called with the RHS snapshot.
	//
	// The LHS RSEngineSnapshot (using parent RSEngine's directory info):
	// 1. Queries RHS snapshot for its directory and filenames
	// 2. Hardlinks RHS files into LHS directory with new renumbered names
	// 3. Creates the new merged manifest
	//
	// Returns:
	// - merged: ManifestInfo with new manifest and renumbered files
	// - nextFileNum: file number high-water mark after MergeLSM's internal
	//   allocations (manifests, renumbered tables, virtual table nums). The
	//   caller must advance the RangeFileNumAllocState past this value to
	//   avoid collisions. Zero if no-op.
	//
	// The caller hardlinks to other replicas:
	// - thisStore/mergedDir/name → otherStore/mergedDir/name
	//
	// If the 2PC transaction fails, the caller enables compactions again
	// and can retry later.
	Merge(ctx context.Context, rhs RSEngineSnapshot) (merged ManifestInfo, nextFileNum uint64, err error)
	// Close releases the snapshot.
	Close()
}

// OpenRSEngineFunc is a function type for opening an InnerRSEngine. It allows
// injection of TestingRSEngine for testing.
type OpenRSEngineFunc func(manifestNum DiskFileNum, opts RSEngineOptions) (InnerRSEngine, error)

// RSEngine is the interface for range-shared engines (rsEngineContainer is the
// real implementation).
//
// A RSEngine is opened once per Replica (when basaltFS is configured) and
// closed when the Replica is destroyed. Internally, it is a container that
// manages one active underlying InnerRSEngine and tracks quiesced engines from
// past manifest changes. Each InnerRSEngine wraps a pebble.DB.
//
// InnerRSEngine Lifecycle (pebbleRSEngine is the real implementation):
//
// Multiple InnerRSEngines can co-exist for the same range, since a new
// InnerRSEngine may need to be opened when a new manifest is installed, while
// older InnerRSEngines can be concurrently serving reads. All but the latest
// InnerRSEngine must have returned from Quiesce before opening a new
// InnerRSEngine. This ensures the other InnerRSEngines are no longer writing to
// the same directory. Quiescing delegates to the underlying pebble.DB.
//
// A quiesced InnerRSEngine will soon have Close called on it. Since there may
// be concurrent CockroachDB operations with a RSEngineSnapshots using that
// InnerRSEngine, it is the responsibility of the InnerRSEngine wrapper to
// ensure that all RSEngineSnapshots are closed before calling pebble.DB.Close.
// DB.Close can further wait for operations it has started to complete.
//
// In some cases, we can transition from one manifest.Version to another without
// quiescing and closing. This is attempted using the PrepareExternalManifest
// and InstallPreparedManifest pair by using
// InnerRSEngine.{prepareExternalManifest,installPreparedManifest}.
//
// Since there can always be quiesced pebble.DBs that have not completed Close,
// the current active DB cannot start cleaning up old unreferenced files. It is
// safe to do this when there are zero quiesced DBs. To illustrate: if there are
// quiesced DBs using manifest-nums 10 and 25, and the DB with manifest-num 10
// finishes Close, we cannot yet start cleaning up filenums <= 10, since they
// may be referenced by manifest-num 25. Since the active DB has limited
// history, we simply wait until there are zero quiesced DBs. There is a risk
// here that with very frequent manifest installs that we keep transitioning to
// new active DBs and there are always non-zero quiesced DBs. We accept this
// risk since most manifest transitions will be able to use
// InnerRSEngine.{prepareExternalManifest,installPreparedManifest} and won't
// result in quiesced DBs.
//
// Synchronization:
//
// This InnerRSEngine container uses an internal activeMu RWMutex to protect the
// active engine pointer. Short operations (CompactionToggle, NewSnapshot,
// CurrentManifestNum) hold activeMu.RLock for their duration. Long operations
// (FlushSSTables, AddSSTables, WaitForOngoingManifestChanges) hold
// activeMu.RLock briefly to load the active pointer and ref the underlying
// engine, then release activeMu.RLock. InstallPreparedManifest holds
// activeMu.Lock (exclusive) when swapping the active pointer.
//
// Lock ordering: Replica.rsStateMu < container.activeMu.
//
// Two hazards are addressed:
//   - H1 (active engine swap during long op): long ops ref the specific
//     underlying engine under activeMu.RLock, preventing its Close from
//     completing after a swap.
//   - H2 (container Close during long op): container-level Ref/Unref.
//     Close waits for all container refs to drain before closing engines.
type RSEngine interface {
	// CompactionToggle is called to enable or disable compactions. Compactions
	// are enabled only at the range leaseholder. The higher layer will also
	// disable compactions during split and merge operations.
	CompactionToggle(enable bool)
	// WaitForOngoingManifestChanges blocks until ongoing FlushSSTables,
	// AddSSTables, and compaction InstallNewManifest calls complete. Called
	// after CompactionToggle(false) to drain pending work before Split/Merge.
	// Long operation: refs the active engine internally.
	WaitForOngoingManifestChanges()
	// CurrentManifestNum returns the DiskFileNum of the current manifest.
	CurrentManifestNum() DiskFileNum
	// FlushSSTables is used for non-overlapping sstables that do not contain
	// multiple key-value pairs for the same userkey. The callee assigns a single
	// seqnum to each sstable. The scratchNames are basenames in the scratch
	// directory. The RSEngine hardlinks these to its real directory before
	// calling InstallNewManifest.
	//
	// flushCommit carries the expected FlushStartedCount and RANGEDEL_TURN_ON
	// spans through to the Raft request via InstallNewManifest.
	//
	// Long operation: refs the active engine internally.
	FlushSSTables(scratchNames []string, flushCommit *FlushCommitInfo) error
	// AddSSTables is used for non-overlapping sstables that do not contain
	// multiple key-value pairs for the same userkey. For example, for index
	// backfill. The callee assigns a single seqnum to each sstable.
	// The scratchNames are basenames in the scratch directory. The RSEngine
	// hardlinks these to its real directory before calling InstallNewManifest.
	//
	// Long operation: refs the active engine internally.
	AddSSTables(scratchNames []string) error
	// Ref increments the container's external reference count. Close blocks
	// until all Ref/Unref pairs complete. Callers that hold a reference to
	// the RSEngine and want to prevent it from being closed (e.g. during
	// split/merge operations) should call Ref.
	Ref()
	// Unref decrements the container's external reference count. Must be
	// paired with a prior Ref call.
	Unref()
	// NewSnapshot creates a new RSEngineSnapshot at the current manifest of
	// the active engine.
	NewSnapshot() RSEngineSnapshot
	// PrepareExternalManifest prepares for a manifest transition to
	// manifestNum. The caller guarantees that manifestNum immediately
	// succeeds CurrentManifestNum.
	//
	// The container opens a new underlying engine at manifestNum and quiesces
	// the current active engine. The new engine is stored internally and
	// installed by InstallPreparedManifest.
	//
	// TODO(basalt): add fast path via the underlying engine's
	// prepareExternalManifest/installPreparedManifest, which avoids opening
	// a new engine when the manifest delta is small.
	//
	// Panics if called when a previous PrepareExternalManifest has not been
	// consumed by InstallPreparedManifest. Prepare/Install must be paired
	// with no intervening Prepare.
	//
	// This is a long operation.
	PrepareExternalManifest(manifestNum DiskFileNum) error
	// InstallPreparedManifest swaps the active engine to the one prepared by
	// PrepareExternalManifest. Must only be called after
	// PrepareExternalManifest returned with no error. After this call,
	// CurrentManifestNum() returns manifestNum.
	//
	// The old active engine is added to the quiesced list and closed
	// asynchronously. When all quiesced engines have been closed,
	// EnableUnreferencedFileDeletion is called on the active engine.
	InstallPreparedManifest(manifestNum DiskFileNum)
	// Close closes the container and all underlying engines (active and
	// quiesced). Blocks until all container-level Ref/Unref pairs complete,
	// then quiesces and closes the active engine and waits for all quiesced
	// engine close goroutines to finish. It also waits for all the engines
	// RSEngineSnapshots to be closed.
	Close()
	// TestingInnerEngine returns the active underlying InnerRSEngine. For
	// use in tests that need to inspect engine internals.
	TestingInnerEngine() InnerRSEngine
}

// rsNoLockFS wraps a vfs.FS and disables Lock operations. Pebble calls Lock
// during Open to prevent two DB instances on the same directory, but
// RSEngine intentionally overlaps old and new DBs during manifest swaps:
// Quiesce stops background work on the old DB while a new DB opens on the
// same directory, and the old DB is closed asynchronously after refs drain.
// File locking must be disabled to allow this two-DB overlap.
//
// TODO(basalt): Once the kvserver switches to the two-phase manifest
// install path (PrepareExternalManifest/InstallPreparedManifest) and no
// longer opens a new DB per manifest change, remove this wrapper and
// re-enable locking.
type rsNoLockFS struct {
	vfs.FS
}

// AtomicRename implements vfs.AtomicRenamer, delegating to the underlying FS.
func (f rsNoLockFS) AtomicRename() bool {
	if ar, ok := f.FS.(vfs.AtomicRenamer); ok {
		return ar.AtomicRename()
	}
	return false
}

// Lock returns a no-op closer, effectively disabling file locking.
func (rsNoLockFS) Lock(name string) (io.Closer, error) {
	return io.NopCloser(nil), nil
}

// OpenRSEngine opens an InnerRSEngine backed by a real pebble.DB in
// range-shared mode. The pebble instance is opened with no WAL, no automatic
// compactions, and LSMKindRangeShared so that manifest changes are staged as
// candidates and installed explicitly.
//
// When manifestNum != NoManifestNum, InitialManifestNum is set so that pebble.Open replays
// the specified manifest during recovery. SSTs referenced in the manifest
// survive orphan cleanup, so no staging or post-open manifest installation is
// needed.
func OpenRSEngine(manifestNum DiskFileNum, opts RSEngineOptions) (InnerRSEngine, error) {
	// Ensure scratch directory exists (still needed for compaction/flush output).
	if err := opts.basaltFS.MkdirAll(opts.basaltScratchPathDir, 0755); err != nil {
		return nil, errors.Wrap(err, "creating BasaltScratchPathDir")
	}

	// Initialize all fields in pebbleRSEngine, except for db.
	eng := &pebbleRSEngine{
		opts: opts,
	}
	eng.mu.manifestNum = uint64(manifestNum)
	eng.mu.cond = sync.NewCond(&eng.mu.Mutex)
	eng.opMu.cond = sync.NewCond(&eng.opMu.Mutex)

	// Create the pebble.DB.
	pebbleOpts := &pebble.Options{
		Comparer: &EngineComparer,
		// Use pebble.DefaultMerger because the flush SST writer
		// (MakeSSTWriter) doesn't explicitly set a merger, so SSTs are
		// written with the default merger name "pebble.concatenate".
		Merger:                      pebble.DefaultMerger,
		FS:                          rsNoLockFS{opts.basaltFS},
		FormatMajorVersion:          pebble.FormatNewest,
		KeySchema:                   DefaultKeySchema,
		KeySchemas:                  sstable.MakeKeySchemas(KeySchemas...),
		DisableAutomaticCompactions: true,
		DisableWAL:                  true,
		DisableTableStats:           true,
		LoggerAndTracer:             pebbleLogger{ctx: opts.logCtx, depth: 1},
		BlockPropertyCollectors:     cockroachkvs.BlockPropertyCollectors,
	}
	pebbleOpts.Levels[0] = pebble.LevelOptions{
		BlockSize:         32 << 10,
		IndexBlockSize:    256 << 10,
		TableFilterPolicy: func() pebble.TableFilterPolicy { return bloom.FilterPolicy(10) },
	}
	pebbleOpts.Levels[0].EnsureL0Defaults()
	for i := 1; i < len(pebbleOpts.Levels); i++ {
		l := &pebbleOpts.Levels[i]
		l.BlockSize = 32 << 10
		l.IndexBlockSize = 256 << 10
		l.TableFilterPolicy = func() pebble.TableFilterPolicy { return bloom.FilterPolicy(10) }
		l.EnsureL1PlusDefaults(&pebbleOpts.Levels[i-1])
	}
	el := pebble.MakeLoggingEventListener(pebbleLogger{
		ctx:   opts.logCtx,
		depth: 2, // skip over the EventListener stack frame
	})

	pebbleOpts.EventListener = &el
	// TODO(basalt): Support value separation in range-shared LSMs. Blob file
	// IDs require global coordination which is not yet implemented.
	pebbleOpts.Experimental.ValueSeparationPolicy = func() pebble.ValueSeparationPolicy {
		return pebble.ValueSeparationPolicy{}
	}
	pebbleOpts.Experimental.LSMKind = pebble.LSMKindRangeShared
	if opts.testingProcessID != "" {
		pebbleOpts.Experimental.ProcessID = opts.testingProcessID
	} else {
		pebbleOpts.Experimental.ProcessID = strconv.Itoa(os.Getpid())
	}
	if manifestNum != NoManifestNum {
		pebbleOpts.Experimental.InitialManifestNum = pebble.DiskFileNum(manifestNum)
	}
	// Wire up the external file number allocator so that Pebble uses
	// Raft-allocated file numbers for compaction outputs, the same path
	// used by flushes via GetFileNums.
	pebbleOpts.Experimental.FileNumAllocator = func(count int) ([]pebble.DiskFileNum, error) {
		fileNums, err := opts.manifestChangeCommitter.GetFileNums(count)
		if err != nil {
			return nil, err
		}
		pebbleNums := make([]pebble.DiskFileNum, len(fileNums))
		for i, n := range fileNums {
			pebbleNums[i] = pebble.DiskFileNum(n)
		}
		return pebbleNums, nil
	}
	// Wire the ManifestCommitter so Pebble drives manifest installs (for
	// flushes, compactions, and ingests) through the cross-replica commit
	// path inside UpdateVersionLocked.
	pebbleOpts.Experimental.ManifestCommitter = &pebbleManifestCommitter{
		inner: opts.manifestChangeCommitter,
	}
	csPlus := opts.compactionScheduler
	if csPlus != nil {
		pebbleOpts.Experimental.CompactionScheduler = func() pebble.CompactionScheduler {
			return csPlus
		}
	}
	db, err := pebble.Open(opts.basaltDir, pebbleOpts)
	if err != nil {
		return nil, err
	}
	eng.db = db
	return eng, nil
}

// combinedReader wraps a Reader and an RSEngineSnapshot so that closing the
// combined reader also closes the RSEngineSnapshot.
type combinedReader struct {
	// The Reader has its own ref to the LSMVersionHandle contained in the
	// rsSnapshot. However, we need to ensure that the Reader ref does not outlive
	// the RSEngineSnapshot, so we keep it here, in order to close the snapshot
	// after the Reader is closed.
	Reader
	rsSnapshot RSEngineSnapshot
}

func (cr combinedReader) Close() {
	cr.Reader.Close()
	cr.rsSnapshot.Close()
}

// combinedReadWriter wraps a ReadWriter and an RSEngineSnapshot so that
// closing the combined reader-writer also closes the RSEngineSnapshot.
type combinedReadWriter struct {
	ReadWriter
	// The ReadWriter has its own ref to the LSMVersionHandle contained in the
	// rsSnapshot. However, we need to ensure that the ReadWriter ref does not
	// outlive the RSEngineSnapshot, so we keep it here, in order to close the
	// snapshot after the ReadWriter is closed.
	rsSnapshot RSEngineSnapshot
}

func (crw combinedReadWriter) Close() {
	crw.ReadWriter.Close()
	crw.rsSnapshot.Close()
}

// combinedBatch wraps a Batch and an RSEngineSnapshot so that closing the
// combined batch also closes the RSEngineSnapshot.
type combinedBatch struct {
	Batch
	// The batch has its own ref to the LSMVersionHandle contained in the
	// rsSnapshot. However, we need to ensure that the batch ref does not outlive
	// the RSEngineSnapshot, so we keep it here, in order to close the snapshot
	// after the batch is closed.
	rsSnapshot RSEngineSnapshot
}

func (cb combinedBatch) Close() {
	cb.Batch.Close()
	cb.rsSnapshot.Close()
}

// MakeCombinedReader creates a Reader that sees the combined state of the
// store-local engine and the range-shared engine. The combined reader takes
// ownership of the RSEngineSnapshot and closes it on Close.
func MakeCombinedReader(
	storeLocal ReaderWithCombinedIteration, rangeShared RSEngineSnapshot,
) Reader {
	if rsSnap, ok := rangeShared.(*pebbleRSEngineSnapshot); ok && rsSnap.versionHandle.IsSet() {
		storeLocal.SetSecondaryLSM(rsSnap.versionHandle.Clone())
	}
	return combinedReader{Reader: storeLocal, rsSnapshot: rangeShared}
}

// MakeCombinedReaderWriter creates a ReadWriter that sees the combined state
// of the store-local engine and the range-shared engine for reads, while
// writes go to the store-local engine. The combined reader-writer takes
// ownership of the RSEngineSnapshot and closes it on Close.
func MakeCombinedReaderWriter(
	storeLocal ReadWriterWithCombinedIteration, rangeShared RSEngineSnapshot,
) ReadWriter {
	if rsSnap, ok := rangeShared.(*pebbleRSEngineSnapshot); ok && rsSnap.versionHandle.IsSet() {
		storeLocal.SetSecondaryLSM(rsSnap.versionHandle.Clone())
	}
	return combinedReadWriter{ReadWriter: storeLocal, rsSnapshot: rangeShared}
}

// MakeCombinedBatch creates a Batch that sees the combined state of the
// store-local engine and the range-shared engine for reads, while writes go
// to the store-local batch. The combined batch takes ownership of the
// RSEngineSnapshot and closes it on Close.
func MakeCombinedBatch(storeLocal Batch, rangeShared RSEngineSnapshot) Batch {
	if rsSnap, ok := rangeShared.(*pebbleRSEngineSnapshot); ok && rsSnap.versionHandle.IsSet() {
		storeLocal.SetSecondaryLSM(rsSnap.versionHandle.Clone())
	}
	return combinedBatch{Batch: storeLocal, rsSnapshot: rangeShared}
}

// TODO(basalt): the Replica.rsStateMu acquisition needs to be narrowed, and
// installing a new manifest that makes a small delta needs to be made
// cheaper. Note the the batch application immediately makes the rangedel-on
// visible to someone pinning iterators, so the batch application needs the
// mutex protection. But with RSE, the store-local engine for the state machine
// has no WAL, so there is no IO involved in batch application -- so we don't
// need to worry about this.
// - PinEngineStateForIterators: will be split into two parts that first pin the
//   state by grabbing a ref to the readState. This happens under the mutex. The
//   iter creation will happen later.
//
// - Instead of opening a new RSEngine, we will have a two step replacement of
//   the manifest. First a prepare step which happens without the mutex, where
//   Pebble has the new version but it is not yet installed. Then an install step
//   under the mutex.
