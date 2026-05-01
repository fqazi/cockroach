// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

// pebbleRSEngine implements RSEngine backed by a real pebble.DB opened in
// range-shared mode (LSMKindRangeShared). It delegates manifest management
// operations to the underlying pebble.DB; flush, ingest, and compaction
// manifest installs are driven by Pebble itself via the
// Experimental.ManifestCommitter hook configured in OpenRSEngine.
//
// The engine uses a two-mutex design:
//   - mu: protects lifecycle state (externalRefs, db pointer)
//   - opMu: serializes Split/Merge against each other and against Quiesce/Close.
//     Pebble-driven operations (flush, ingest, compaction install) coordinate
//     internally and do not acquire opMu.
type pebbleRSEngine struct {
	opts RSEngineOptions
	// db is non-nil until Close. Writes (setting to nil) must hold both
	// opMu and mu, in that order. Reads may hold either.
	db *pebble.DB
	mu struct {
		sync.Mutex
		cond         *sync.Cond
		externalRefs int
		// manifestNum tracks the current manifest number. Set during
		// OpenRSEngine and updated by InstallPreparedManifest.
		manifestNum uint64
	}
	// opMu serializes Split/Merge operations. At most one such operation
	// can be ongoing at a time.
	opMu struct {
		sync.Mutex
		cond      *sync.Cond
		ongoingOp bool
		// closedForOngoingOps is set to true by Quiesce (to stop background
		// work) and by Close (in case Quiesce was not called). When true,
		// beginOp returns ErrRSEngineClosed.
		closedForOngoingOps bool
	}
}

// currentManifestNum returns the DiskFileNum of the current manifest.
func (e *pebbleRSEngine) currentManifestNum() DiskFileNum {
	e.mu.Lock()
	defer e.mu.Unlock()
	return DiskFileNum(e.mu.manifestNum)
}

// prepareExternalManifest reads a manifest file from BasaltDir and stages it
// as a candidate version.
func (e *pebbleRSEngine) prepareExternalManifest(manifestNum DiskFileNum) error {
	e.mu.Lock()
	db := e.db
	if db == nil {
		e.mu.Unlock()
		panic("PrepareExternalManifest called after Close")
	}
	e.mu.externalRefs++
	e.mu.Unlock()
	defer e.unref()
	return db.PrepareExternalManifest(pebble.DiskFileNum(manifestNum))
}

// installPreparedManifest promotes the prepared candidate version to current.
func (e *pebbleRSEngine) installPreparedManifest(manifestNum DiskFileNum) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.db == nil {
		panic("InstallPreparedManifest called after Close")
	}
	if err := e.db.InstallPreparedManifest(pebble.DiskFileNum(manifestNum)); err != nil {
		panic(fmt.Sprintf("InstallPreparedManifest(%d): %v", manifestNum, err))
	}
	e.mu.manifestNum = uint64(manifestNum)
}

// compactionToggle enables or disables automatic compactions on the underlying
// pebble DB. Only the leaseholder enables compactions; they are disabled during
// split/merge operations.
func (e *pebbleRSEngine) compactionToggle(enable bool) {
	// Read e.db under e.mu to avoid a nil dereference if Close() runs
	// concurrently (Close sets e.db = nil under e.mu). If db is non-nil,
	// SetAutomaticCompactions is safe even if Close runs concurrently
	// because it checks d.closed first.
	e.mu.Lock()
	db := e.db
	e.mu.Unlock()
	if db != nil {
		db.SetAutomaticCompactions(enable)
	}
}

// enableUnreferencedFileDeletion lifts pebble's gate on file deletion. The
// underlying pebble.DB is opened with deletion gated (LSMKindRangeShared);
// rsEngineContainer calls this on the active engine once the quiesced-engine
// list empties, signaling that no older instance still references the files.
func (e *pebbleRSEngine) enableUnreferencedFileDeletion() {
	e.mu.Lock()
	db := e.db
	e.mu.Unlock()
	if db != nil {
		db.EnableUnreferencedFileDeletion()
	}
}

// waitForOngoingManifestChanges blocks until any ongoing Split/Merge
// completes.
func (e *pebbleRSEngine) waitForOngoingManifestChanges() {
	e.opMu.Lock()
	defer e.opMu.Unlock()
	for e.opMu.ongoingOp && !e.opMu.closedForOngoingOps {
		e.opMu.cond.Wait()
	}
}

// beginOp serializes Split/Merge against each other and against
// Quiesce/Close. Returns an error if the engine is closed.
func (e *pebbleRSEngine) beginOp() error {
	e.opMu.Lock()
	defer e.opMu.Unlock()
	for e.opMu.ongoingOp && !e.opMu.closedForOngoingOps {
		e.opMu.cond.Wait()
	}
	if e.opMu.closedForOngoingOps {
		return ErrRSEngineClosed
	}
	e.opMu.ongoingOp = true
	return nil
}

// endOp marks the current Split/Merge as complete.
func (e *pebbleRSEngine) endOp() {
	e.opMu.Lock()
	defer e.opMu.Unlock()
	e.opMu.ongoingOp = false
	e.opMu.cond.Signal()
}

// flushSSTables ingests a single SST from the scratch directory into the
// pebble DB and stages a candidate manifest for installation via Raft. A
// flush always produces exactly one SST; multi-file ingests go through
// addSSTables.
func (e *pebbleRSEngine) flushSSTables(scratchNames []string, flushCommit *FlushCommitInfo) error {
	if len(scratchNames) != 1 {
		return errors.AssertionFailedf("FlushSSTables expects exactly 1 file, got %d", len(scratchNames))
	}
	return e.ingest(scratchNames, flushCommit)
}

// addSSTables ingests one or more non-overlapping SSTs from the scratch
// directory into the pebble DB and stages a candidate manifest for
// installation via Raft. addSSTables passes a nil ingestHandle since the
// AddSSTables path has no flush-commit-specific state to plumb to
// InstallNewManifest.
func (e *pebbleRSEngine) addSSTables(scratchNames []string) error {
	return e.ingest(scratchNames, nil)
}

// ingest is the shared body of flushSSTables and addSSTables. It is a thin
// wrapper around (*pebble.DB).IngestWithHandle: Pebble allocates file numbers
// via Experimental.FileNumAllocator, links the scratch SSTs into the DB
// directory, writes the new manifest, and then drives the cross-replica
// commit through Experimental.ManifestCommitter (wired in OpenRSEngine to
// pebbleManifestCommitter, which delegates to the embedder's
// ManifestChangeCommitter). ingestHandle is forwarded to the committer so
// flush-commit metadata (*FlushCommitInfo) can travel in the same Raft
// proposal that flips the manifest pointer; the add-sstables path passes nil.
func (e *pebbleRSEngine) ingest(scratchNames []string, ingestHandle interface{}) error {
	paths := make([]string, len(scratchNames))
	for i, name := range scratchNames {
		paths[i] = e.opts.basaltFS.PathJoin(e.opts.basaltScratchPathDir, name)
	}
	return e.db.IngestWithHandle(e.opts.logCtx, paths, ingestHandle)
}

// newSnapshot creates a snapshot of the current manifest state by acquiring a
// lightweight VersionHandle from pebble. The handle holds a ref on the current
// LSM version, keeping its file metadata alive without copying file lists
// eagerly. The caller must call Close on the returned snapshot to release the
// version ref and the engine ref.
func (e *pebbleRSEngine) newSnapshot() RSEngineSnapshot {
	e.mu.Lock()
	db := e.db
	e.mu.externalRefs++
	e.mu.Unlock()
	var vh pebble.LSMVersionHandle
	if db != nil {
		vh = db.NewLSMVersionHandle()
	}
	// The VersionHandle carries the manifest file number atomically with the
	// version reference. Pebble names the bootstrap manifest MANIFEST-000000
	// for range-shared LSMs, so a filenum of 0 naturally maps to NoManifestNum
	// without any separate predicate or read of external state.
	manifestNum := NoManifestNum
	if vh.IsSet() {
		manifestNum = DiskFileNum(vh.ManifestFileNum())
	}
	return &pebbleRSEngineSnapshot{
		engine:        e,
		manifestNum:   manifestNum,
		versionHandle: vh,
	}
}

// pebbleRSEngineSnapshot implements RSEngineSnapshot for the real pebble
// RSEngine. It holds a pebble.VersionHandle to keep the LSM version's file
// metadata alive. The file list is built from the handle on each
// ManifestInfo call; in production ManifestInfo is called at most once
// per snapshot (when sending an outgoing range snapshot).
type pebbleRSEngineSnapshot struct {
	engine        *pebbleRSEngine
	manifestNum   DiskFileNum
	versionHandle pebble.LSMVersionHandle
	closed        bool
}

var _ RSEngineSnapshot = (*pebbleRSEngineSnapshot)(nil)

func (s *pebbleRSEngineSnapshot) ManifestInfo() ManifestInfo {
	var files []FileNameAndNum
	if s.versionHandle.IsSet() {
		dfns := s.versionHandle.TableFileNums()
		files = make([]FileNameAndNum, len(dfns))
		for i, dfn := range dfns {
			num := DiskFileNum(dfn)
			files[i] = FileNameAndNum{
				Name: formatSSTName(num),
				Num:  num,
			}
		}
	}
	return ManifestInfo{
		Manifest: FileNameAndNum{
			Name: formatManifestName(s.manifestNum),
			Num:  s.manifestNum,
		},
		Files: files,
	}
}

func (s *pebbleRSEngineSnapshot) ManifestNum() DiskFileNum {
	return s.manifestNum
}

func (s *pebbleRSEngineSnapshot) Clone() RSEngineSnapshot {
	s.engine.ref()
	return &pebbleRSEngineSnapshot{
		engine:        s.engine,
		manifestNum:   s.manifestNum,
		versionHandle: s.versionHandle.Clone(),
	}
}

// Split creates new manifest state for LHS and RHS after a range split.
// It calls pebble's SplitLSM to classify tables relative to splitKey,
// virtualize straddling files, hardlink right-side files into rhsDir, and
// write two manifest snapshots (one for LHS, one for RHS).
func (s *pebbleRSEngineSnapshot) Split(
	ctx context.Context, splitKey roachpb.Key, rhsDir string,
) (lhsManifest FileNameAndNum, rhs ManifestInfo, nextFileNum uint64, err error) {
	if err := s.engine.beginOp(); err != nil {
		return FileNameAndNum{}, ManifestInfo{}, 0, err
	}
	defer s.engine.endOp()
	result, err := s.engine.db.SplitLSM(EncodeMVCCKeyPrefix(splitKey), rhsDir)
	if err != nil {
		return FileNameAndNum{}, ManifestInfo{}, 0, errors.Wrap(err, "SplitLSM")
	}

	// NoOp means all tables are on the LHS (or the LSM is empty). No
	// manifests or files were created. Return zero values so the caller
	// can detect this and skip hardlinking/manifest updates.
	if result.NoOp {
		return FileNameAndNum{}, ManifestInfo{}, 0, nil
	}

	lhsManifestNum := DiskFileNum(result.LHSManifestFileNum)
	rhsManifestNum := DiskFileNum(result.RHSManifestFileNum)
	if lhsManifestNum < s.manifestNum {
		// Alternatively, this could be in Pebble.
		return FileNameAndNum{}, ManifestInfo{}, 0,
			errors.Errorf("lhsManifestNum %d < s.manifestNum %d", lhsManifestNum, s.manifestNum)
	}
	if rhsManifestNum < s.manifestNum {
		// Alternatively, this could be in Pebble.
		return FileNameAndNum{}, ManifestInfo{}, 0,
			errors.Errorf("rhsManifestNum %d < s.manifestNum %d", rhsManifestNum, s.manifestNum)
	}

	// Build RHS ManifestInfo from the SplitLSM result.
	rhsFiles := make([]FileNameAndNum, 0, len(result.RHSFiles))
	for _, rhsDFN := range result.RHSFiles {
		num := DiskFileNum(rhsDFN)
		rhsFiles = append(rhsFiles, FileNameAndNum{
			Name: formatSSTName(num),
			Num:  num,
		})
	}
	return FileNameAndNum{
			Name: formatManifestName(lhsManifestNum),
			Num:  lhsManifestNum,
		}, ManifestInfo{
			Manifest: FileNameAndNum{
				Name: formatManifestName(rhsManifestNum),
				Num:  rhsManifestNum,
			},
			Files: rhsFiles,
		}, result.NextFileNum, nil
}

func (s *pebbleRSEngineSnapshot) Merge(
	ctx context.Context, rhs RSEngineSnapshot,
) (merged ManifestInfo, nextFileNum uint64, err error) {
	if err := s.engine.beginOp(); err != nil {
		return ManifestInfo{}, 0, err
	}
	defer s.engine.endOp()

	rhsSnap, ok := rhs.(*pebbleRSEngineSnapshot)
	if !ok {
		return ManifestInfo{}, 0, errors.AssertionFailedf(
			"expected *pebbleRSEngineSnapshot, got %T", rhs)
	}

	result, err := s.engine.db.MergeLSM(rhsSnap.engine.db)
	if err != nil {
		return ManifestInfo{}, 0, errors.Wrap(err, "MergeLSM")
	}

	// NoOp means the RHS has no tables. No manifest or files were created.
	// Return zero values so the caller can detect this and skip
	// hardlinking/manifest updates.
	if result.NoOp {
		return ManifestInfo{}, 0, nil
	}

	manifestNum := DiskFileNum(result.ManifestFileNum)
	if manifestNum < s.manifestNum {
		// Alternatively, this could be in Pebble.
		return ManifestInfo{}, 0,
			errors.Errorf("merged manifestNum %d < s.manifestNum %d", manifestNum, s.manifestNum)
	}
	// Build Files slice from the renumbered files map (new DiskFileNums).
	files := make([]FileNameAndNum, 0, len(result.RenumberedFiles))
	for _, newDFN := range result.RenumberedFiles {
		num := DiskFileNum(newDFN)
		files = append(files, FileNameAndNum{
			Name: formatSSTName(num),
			Num:  num,
		})
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].Num < files[j].Num
	})

	return ManifestInfo{
		Manifest: FileNameAndNum{
			Name: formatManifestName(manifestNum),
			Num:  manifestNum,
		},
		Files: files,
	}, result.NextFileNum, nil
}

func (s *pebbleRSEngineSnapshot) Close() {
	if s.closed {
		return
	}
	s.closed = true
	s.versionHandle.Close()
	s.engine.unref()
}

// ref increments the external reference count.
func (e *pebbleRSEngine) ref() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.mu.externalRefs++
}

// unref decrements the external reference count. Signals closeInner() when it
// reaches 0.
func (e *pebbleRSEngine) unref() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.mu.externalRefs--
	if e.mu.externalRefs < 0 {
		panic("externalRefs went negative")
	}
	e.mu.cond.Signal()
}

// quiesce prevents new background work (flushes, compactions) from starting
// on the underlying pebble.DB, without closing it. The DB remains open so
// that outstanding VersionHandles (from newSnapshot) can continue to read
// file metadata. In-flight compactions finish naturally, but their output
// remains isolated to the scratch directory, so it won't interfere with a new
// DB opened on the same store.
//
// closeInner must still be called afterward to wait for outstanding refs to
// drain and release all resources.
func (e *pebbleRSEngine) quiesce() {
	e.opMu.Lock()
	e.opMu.closedForOngoingOps = true
	e.opMu.cond.Broadcast()
	e.opMu.Unlock()

	e.mu.Lock()
	db := e.db
	e.mu.Unlock()
	if db != nil {
		db.Quiesce()
	}
}

// closeInner nils db, waits for external refs to drain (all VersionHandles
// released), then closes the underlying pebble.DB. The caller must call
// quiesce first to stop background work before closeInner blocks waiting for
// refs.
func (e *pebbleRSEngine) closeInner() {
	// Set closedForOngoingOps and nil db in one opMu hold. Lock ordering
	// is opMu before mu.
	e.opMu.Lock()
	e.opMu.closedForOngoingOps = true
	e.opMu.cond.Broadcast()
	e.mu.Lock()
	db := e.db
	e.db = nil
	e.mu.Unlock()
	e.opMu.Unlock()
	// Wait for outstanding refs (snapshots, PrepareExternalManifest) to
	// drain. Unref only acquires mu, so no deadlock with opMu.
	e.mu.Lock()
	for e.mu.externalRefs > 0 {
		e.mu.cond.Wait()
	}
	e.mu.Unlock()
	if db != nil {
		if err := db.Close(); err != nil {
			panic(fmt.Sprintf("pebbleRSEngine.Close: %v", err))
		}
	}
}
