// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
)

// PrintFilesystem pretty-prints directory contents recursively.
// For directories, it prints a trailing slash and recurses. For files, it
// prints just the filename (no contents, to keep output concise).
//
// If rangeIDMap is non-nil, directory names matching the pattern r<num>:<num>
// will have their range ID (first number) replaced with the mapped synthetic
// value. This makes test output deterministic when range IDs vary between runs.
func PrintFilesystem(fs vfs.FS, dir string, rangeIDMap map[roachpb.RangeID]int) string {
	var buf strings.Builder
	printFilesystemRecursive(fs, dir, "", &buf, rangeIDMap)
	return buf.String()
}

func printFilesystemRecursive(
	fs vfs.FS, dir, prefix string, buf *strings.Builder, rangeIDMap map[roachpb.RangeID]int,
) {
	entries, err := fs.List(dir)
	if err != nil {
		fmt.Fprintf(buf, "%serror: %v\n", prefix, err)
		return
	}
	slices.Sort(entries)
	for _, entry := range entries {
		path := fs.PathJoin(dir, entry)
		stat, err := fs.Stat(path)
		if err != nil {
			fmt.Fprintf(buf, "%s%s: error\n", prefix, entry)
			continue
		}
		if stat.IsDir() {
			// Check if this is a range directory pattern r<rangeID>:<replicaID>.
			if rangeID, ok := parseRangeDir(entry); ok {
				// If map is non-nil and rangeID not in map, skip entirely.
				if rangeIDMap != nil {
					if _, inMap := rangeIDMap[rangeID]; !inMap {
						continue
					}
				}
			}
			displayEntry := remapRangeID(entry, rangeIDMap)
			fmt.Fprintf(buf, "%s%s/\n", prefix, displayEntry)
			printFilesystemRecursive(fs, path, prefix+displayEntry+"/", buf, rangeIDMap)
		} else {
			displayEntry := remapRangeID(entry, rangeIDMap)
			fmt.Fprintf(buf, "%s%s:\n", prefix, displayEntry)
			// Print file contents with indentation.
			f, err := fs.Open(path)
			if err != nil {
				fmt.Fprintf(buf, "%s  error: %v\n", prefix, err)
				continue
			}
			data, err := io.ReadAll(f)
			f.Close()
			if err != nil {
				fmt.Fprintf(buf, "%s  error: %v\n", prefix, err)
				continue
			}
			for _, line := range strings.Split(string(data), "\n") {
				if line != "" {
					fmt.Fprintf(buf, "%s  %s\n", prefix, line)
				}
			}
		}
	}
}

// PrintFileNames lists all file and directory names in the filesystem rooted at
// dir, without printing file contents. This is useful for tests that write real
// SST files whose binary content is non-deterministic.
//
// To keep listings stable, two normalizations are applied:
//   - Range IDs in directory names like "r82:1" are remapped via rangeIDMap
//     (when non-nil); see remapRangeID.
//   - The trailing instance suffix on pebble's per-DB scratch dir names is
//     rewritten to "1"; see normalizeScratchDir.
func PrintFileNames(fs vfs.FS, dir string, rangeIDMap map[roachpb.RangeID]int) string {
	var buf strings.Builder
	printFileNamesRecursive(fs, dir, &buf, rangeIDMap)
	return buf.String()
}

func printFileNamesRecursive(
	fs vfs.FS, dir string, buf *strings.Builder, rangeIDMap map[roachpb.RangeID]int,
) {
	entries, err := fs.List(dir)
	if err != nil {
		fmt.Fprintf(buf, "error: %v\n", err)
		return
	}
	slices.Sort(entries)
	for _, entry := range entries {
		path := fs.PathJoin(dir, entry)
		stat, err := fs.Stat(path)
		if err != nil {
			continue
		}
		if stat.IsDir() {
			if rangeID, ok := parseRangeDir(entry); ok {
				if rangeIDMap != nil {
					if _, inMap := rangeIDMap[rangeID]; !inMap {
						continue
					}
				}
			}
			displayEntry := normalizeScratchDir(remapRangeID(entry, rangeIDMap))
			fmt.Fprintf(buf, "%s/\n", displayEntry)
			printFileNamesRecursive(fs, path, buf, rangeIDMap)
		} else {
			displayEntry := remapRangeID(entry, rangeIDMap)
			fmt.Fprintf(buf, "%s\n", displayEntry)
		}
	}
}

// parseRangeDir parses a directory name matching pattern r<rangeID>:<replicaID>.
// Returns the rangeID and true if the pattern matches, otherwise returns 0, false.
func parseRangeDir(name string) (roachpb.RangeID, bool) {
	var rangeID, replicaID int
	if n, _ := fmt.Sscanf(name, "r%d:%d", &rangeID, &replicaID); n == 2 {
		return roachpb.RangeID(rangeID), true
	}
	return 0, false
}

// normalizeScratchDir rewrites the trailing instance suffix on pebble scratch
// dir names (scratch-{ProcessID}-{instance}) to "1". The instance comes from
// a process-global counter that keeps climbing under `go test -count=N`; the
// test only cares that the dir exists.
func normalizeScratchDir(name string) string {
	if !strings.HasPrefix(name, "scratch-") {
		return name
	}
	i := strings.LastIndexByte(name, '-')
	if i <= len("scratch-") {
		return name
	}
	for _, c := range name[i+1:] {
		if c < '0' || c > '9' {
			return name
		}
	}
	return name[:i+1] + "1"
}

// remapRangeID replaces range IDs in directory names like "r82:1" with their
// synthetic mapped values. If the map is nil or the range ID is not found,
// the original string is returned unchanged.
func remapRangeID(name string, rangeIDMap map[roachpb.RangeID]int) string {
	if rangeIDMap == nil {
		return name
	}
	// Parse pattern: r<rangeID>:<replicaID>
	var rangeID, replicaID int
	if n, _ := fmt.Sscanf(name, "r%d:%d", &rangeID, &replicaID); n == 2 {
		if synthetic, ok := rangeIDMap[roachpb.RangeID(rangeID)]; ok {
			return fmt.Sprintf("r%d:%d", synthetic, replicaID)
		}
	}
	return name
}

// PrintRSEngineState prints the internal manifest state of a TestingRSEngine.
// It shows the current manifest number and all tracked manifests with their
// reference counts and SSTable lists.
func PrintRSEngineState(engine *TestingRSEngine) string {
	var buf strings.Builder
	engine.mu.Lock()
	defer engine.mu.Unlock()
	fmt.Fprintf(&buf, "current-manifest: %d\n", engine.mu.currentManifestNum)
	var nums []DiskFileNum
	for num := range engine.mu.manifests {
		nums = append(nums, num)
	}
	slices.Sort(nums)
	for _, num := range nums {
		ms := engine.mu.manifests[num]
		fmt.Fprintf(&buf, "manifest %d: refcount=%d sstables=%v\n",
			num, ms.refCount, ms.sstables)
	}
	return buf.String()
}

// manifestState tracks the refcount and sstables for a manifest.
type manifestState struct {
	refCount int
	// Immutable after creation.
	sstables []string // sorted list of sstable basenames
}

// snapshotInfo tracks an open snapshot for debugging.
type snapshotInfo struct {
	id         uint64
	stack      string
	createTime time.Time
}

// TestingRSEngine implements RSEngine for testing without a real Pebble engine.
// It stores manifest state in memory and writes human-readable manifest files
// to the filesystem. SSTables are tracked by name only (contents are opaque).
//
// The engine uses a two-mutex design:
//   - mu: protects internal state (manifests map, currentManifestNum)
//   - opMu: serializes manifest-changing operations (FlushSSTables, Split, Merge)
//     ensuring at most one such operation is ongoing at a time
//
// The engine holds a reference on the current manifest. Snapshots increment
// the refcount; closing them decrements it. Close() waits for all external
// refs (snapshots) to drain before returning.
type TestingRSEngine struct {
	// mu protects internal state. Never hold mu when calling ManifestChangeCommitter.
	mu struct {
		sync.Mutex
		currentManifestNum DiskFileNum
		// manifests tracks manifest state by manifest number. Each manifest has
		// a refcount and a list of sstable basenames.
		manifests map[DiskFileNum]*manifestState
		// cond is signalled when a refCount is decremented. Close waits on this.
		cond *sync.Cond
		// externalRefs tracks callers that have called Ref() but not yet
		// Unref(). Close() waits for this to reach 0.
		externalRefs int
		// noOpLog logs no-op method calls for testing verification.
		noOpLog strings.Builder
		// Snapshot tracking for debugging.
		nextSnapshotID uint64
		openSnapshots  map[uint64]*snapshotInfo
	}
	// opMu serializes manifest-changing operations. At most one operation can
	// be ongoing at a time.
	opMu struct {
		sync.Mutex
		cond      *sync.Cond
		ongoingOp bool
		closed    bool
	}
	opts RSEngineOptions
	// closeWaitingCh is an optional test hook. If non-nil, Close() closes this
	// channel when it starts waiting for manifests to drain, allowing tests to
	// deterministically verify blocking behavior.
	closeWaitingCh chan struct{}
}

// OpenTestingRSEngine creates a new TestingRSEngine. It matches the
// OpenRSEngineFunc signature so it can be injected into production code.
// If manifestNum != NoManifestNum, loads existing manifest from BasaltDir.
func OpenTestingRSEngine(manifestNum DiskFileNum, opts RSEngineOptions) (InnerRSEngine, error) {
	if opts.manifestChangeCommitter == nil {
		return nil, errors.New("ManifestChangeCommitter is required")
	}
	if opts.basaltFS == nil {
		return nil, errors.New("BasaltFS is required")
	}
	if opts.basaltDir == "" {
		return nil, errors.New("BasaltDir is required")
	}
	if opts.basaltScratchPathDir == "" {
		return nil, errors.New("BasaltScratchPathDir is required")
	}
	e := &TestingRSEngine{
		opts: opts,
	}
	e.mu.manifests = make(map[DiskFileNum]*manifestState)
	e.mu.openSnapshots = make(map[uint64]*snapshotInfo)
	e.opMu.cond = sync.NewCond(&e.opMu.Mutex)
	e.mu.cond = sync.NewCond(&e.mu.Mutex)
	// Create directories if they don't exist.
	if err := opts.basaltFS.MkdirAll(opts.basaltDir, 0755); err != nil {
		return nil, errors.Wrap(err, "creating BasaltDir")
	}
	if err := opts.basaltFS.MkdirAll(opts.basaltScratchPathDir, 0755); err != nil {
		return nil, errors.Wrap(err, "creating BasaltScratchPathDir")
	}
	// Load existing manifest if specified.
	if manifestNum != NoManifestNum {
		sstables, err := readManifestFile(opts.basaltFS, opts.basaltDir, manifestNum)
		if err != nil {
			return nil, err
		}
		e.mu.manifests[manifestNum] = &manifestState{
			refCount: 1, // engine holds a ref
			sstables: sstables,
		}
	} else {
		// Fresh engine with no manifest yet.
		e.mu.manifests[0] = &manifestState{
			refCount: 1, // engine holds a ref
			sstables: nil,
		}
	}
	e.mu.currentManifestNum = manifestNum
	return e, nil
}

// formatManifestName returns the manifest filename for a DiskFileNum.
// Format: MANIFEST-NNNNNN (6-digit zero-padded).
func formatManifestName(num DiskFileNum) string {
	return fmt.Sprintf("MANIFEST-%06d", num)
}

// formatSSTName returns the SST filename for a DiskFileNum.
// Format: NNNNNN.sst (6-digit zero-padded).
func formatSSTName(num DiskFileNum) string {
	return fmt.Sprintf("%06d.sst", num)
}

// readManifestFile reads a human-readable manifest file and returns the
// list of sstable basenames. Validates that the manifest number matches
// the expected value.
// Manifest format:
//
//	manifest:<num>
//	000040.sst
//	000041.sst
func readManifestFile(fs vfs.FS, basaltDir string, manifestNum DiskFileNum) ([]string, error) {
	manifestName := formatManifestName(manifestNum)
	path := fs.PathJoin(basaltDir, manifestName)
	f, err := fs.Open(path)
	if err != nil {
		return nil, errors.Wrapf(err, "opening manifest %s", path)
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, errors.Wrapf(err, "reading manifest %s", path)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) == 0 {
		return nil, errors.Newf("empty manifest file %s", path)
	}
	// First line should be "manifest:<num>".
	if !strings.HasPrefix(lines[0], "manifest:") {
		return nil, errors.Newf("invalid manifest header in %s: %s", path, lines[0])
	}
	var parsedNum DiskFileNum
	if _, err := fmt.Sscanf(lines[0], "manifest:%d", &parsedNum); err != nil {
		return nil, errors.Wrapf(err, "parsing manifest number from %s", lines[0])
	}
	if parsedNum != manifestNum {
		return nil, errors.Newf("manifest number mismatch in %s: expected %d, got %d",
			path, manifestNum, parsedNum)
	}
	// Remaining lines are sstable names.
	sstables := make([]string, 0, len(lines)-1)
	for _, line := range lines[1:] {
		line = strings.TrimSpace(line)
		if line != "" {
			sstables = append(sstables, line)
		}
	}
	if !slices.IsSorted(sstables) {
		return nil, errors.Newf("sstables not sorted in %s: %v", path, sstables)
	}
	return sstables, nil
}

// writeManifestFile writes a human-readable manifest file.
func writeManifestFile(
	fs vfs.FS, basaltDir string, manifestNum DiskFileNum, sstables []string,
) error {
	var buf strings.Builder
	fmt.Fprintf(&buf, "manifest:%d\n", manifestNum)
	for _, sst := range sstables {
		fmt.Fprintf(&buf, "%s\n", sst)
	}
	manifestName := formatManifestName(manifestNum)
	path := fs.PathJoin(basaltDir, manifestName)
	f, err := fs.Create(path, vfs.WriteCategoryUnspecified)
	if err != nil {
		return errors.Wrapf(err, "creating manifest %s", path)
	}
	if _, err := f.Write([]byte(buf.String())); err != nil {
		_ = f.Close()
		return errors.Wrapf(err, "writing manifest %s", path)
	}
	if err := f.Close(); err != nil {
		return errors.Wrapf(err, "closing manifest %s", path)
	}
	return nil
}

// currentManifestNum returns the current manifest number.
func (e *TestingRSEngine) currentManifestNum() DiskFileNum {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.mu.currentManifestNum
}

// compactionToggle is a no-op for the testing engine.
func (e *TestingRSEngine) compactionToggle(enable bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	fmt.Fprintf(&e.mu.noOpLog, "CompactionToggle(%v)\n", enable)
}

// enableUnreferencedFileDeletion is a no-op for the testing engine.
func (e *TestingRSEngine) enableUnreferencedFileDeletion() {
	e.mu.Lock()
	defer e.mu.Unlock()
	fmt.Fprintf(&e.mu.noOpLog, "EnableUnreferencedFileDeletion\n")
}

// waitForOngoingManifestChanges waits for any ongoing manifest changes.
func (e *TestingRSEngine) waitForOngoingManifestChanges() {
	e.mu.Lock()
	fmt.Fprintf(&e.mu.noOpLog, "started WaitForOngoingManifestChanges()\n") // nolint:deferunlockcheck
	e.mu.Unlock()                                                           // nolint:deferunlockcheck
	e.opMu.Lock()
	for e.opMu.ongoingOp && !e.opMu.closed { // nolint:deferunlockcheck
		e.opMu.cond.Wait()
	}
	e.opMu.Unlock() // nolint:deferunlockcheck
	e.mu.Lock()
	fmt.Fprintf(&e.mu.noOpLog, "finished WaitForOngoingManifestChanges()\n") // nolint:deferunlockcheck
	e.mu.Unlock()                                                            // nolint:deferunlockcheck
}

func (e *TestingRSEngine) getNoOpLog() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.mu.noOpLog.String()
}

func (e *TestingRSEngine) clearNoOpLog() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.mu.noOpLog.Reset()
}

// beginOp waits until no operation is ongoing, then marks an operation as
// started. Returns error if the engine is closed.
func (e *TestingRSEngine) beginOp() error {
	e.opMu.Lock()
	defer e.opMu.Unlock()
	for e.opMu.ongoingOp && !e.opMu.closed {
		e.opMu.cond.Wait()
	}
	if e.opMu.closed {
		return ErrRSEngineClosed
	}
	e.opMu.ongoingOp = true
	return nil
}

// endOp marks the current operation as complete.
func (e *TestingRSEngine) endOp() {
	e.opMu.Lock()
	defer e.opMu.Unlock()
	e.opMu.ongoingOp = false
	e.opMu.cond.Broadcast()
}

// flushSSTables flushes SSTs to the filesystem and creates a new manifest.
func (e *TestingRSEngine) flushSSTables(scratchNames []string, flushCommit *FlushCommitInfo) error {
	if len(scratchNames) != 1 {
		return errors.AssertionFailedf("FlushSSTables expects exactly 1 file, got %d", len(scratchNames))
	}
	if err := e.beginOp(); err != nil {
		return err
	}
	defer e.endOp()
	snap := e.newSnapshot().(*TestingRSEngineSnapshot)
	defer snap.Close()
	// Verify scratch file exists.
	scratchPath := e.opts.basaltFS.PathJoin(e.opts.basaltScratchPathDir, scratchNames[0])
	if _, err := e.opts.basaltFS.Stat(scratchPath); err != nil {
		return errors.Wrapf(err, "scratch file %s not found", scratchPath)
	}
	// Get file numbers: one for the new manifest, one for the new sstable.
	count := max(60, 2)
	fileNums, err := e.opts.manifestChangeCommitter.GetFileNums(count)
	if err != nil {
		return err
	}
	// Use the highest for manifest, next highest for sstable.
	newManifestNum := fileNums[len(fileNums)-1]
	newSSTNum := fileNums[len(fileNums)-2]
	newSSTName := formatSSTName(newSSTNum)
	// Hardlink SST from scratch to BasaltDir.
	dstSSTPath := e.opts.basaltFS.PathJoin(e.opts.basaltDir, newSSTName)
	if err := e.opts.basaltFS.Link(scratchPath, dstSSTPath); err != nil {
		return errors.Wrapf(err, "linking SST %s to %s", scratchPath, dstSSTPath)
	}
	// Build new sstable list: current + new.
	sstables := append([]string(nil), snap.sstables...)
	sstables = append(sstables, newSSTName)
	if !slices.IsSorted(sstables) {
		return errors.AssertionFailedf("sstables not sorted after append: %v", sstables)
	}
	// Write manifest file.
	if err := writeManifestFile(e.opts.basaltFS, e.opts.basaltDir, newManifestNum, sstables); err != nil {
		return err
	}
	// Install new manifest via Raft. Do NOT update internal state — the engine
	// will be closed and reopened with the new manifest number after this
	// returns.
	manifestInfo := ManifestInfo{
		Manifest: FileNameAndNum{Name: formatManifestName(newManifestNum), Num: newManifestNum},
		Files:    []FileNameAndNum{{Name: newSSTName, Num: newSSTNum}},
	}
	if err := e.opts.manifestChangeCommitter.InstallNewManifest(snap.manifestNum, manifestInfo, flushCommit); err != nil {
		return err
	}
	// Match Pebble's contract: on a successful ingest, the scratch source file
	// is owned by the engine and removed.
	if err := e.opts.basaltFS.Remove(scratchPath); err != nil {
		return errors.Wrapf(err, "removing scratch file %s after flush", scratchPath)
	}
	return nil
}

// addSSTables is not implemented for testing engine.
func (e *TestingRSEngine) addSSTables(scratchNames []string) error {
	return nil
}

// ref increments the external reference count.
func (e *TestingRSEngine) ref() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.mu.externalRefs++
}

// unref decrements the external reference count. Panics if the count goes
// negative. Signals closeInner() when the count reaches 0.
func (e *TestingRSEngine) unref() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.mu.externalRefs--
	if e.mu.externalRefs < 0 {
		panic("externalRefs went negative")
	}
	e.mu.cond.Signal()
}

// newSnapshot returns a new snapshot of the engine.
func (e *TestingRSEngine) newSnapshot() RSEngineSnapshot {
	// Capture stack trace for debugging snapshot leaks.
	buf := make([]byte, 4096)
	n := runtime.Stack(buf, false)
	stack := string(buf[:n])
	e.mu.Lock()
	defer e.mu.Unlock()
	manifestNum := e.mu.currentManifestNum
	state := e.mu.manifests[manifestNum]
	// State must exist since engine holds a ref.
	state.refCount++
	// Track snapshot for debugging.
	e.mu.nextSnapshotID++
	id := e.mu.nextSnapshotID
	e.mu.openSnapshots[id] = &snapshotInfo{
		id:         id,
		stack:      stack,
		createTime: time.Now(),
	}
	return &TestingRSEngineSnapshot{
		engine:      e,
		manifestNum: manifestNum,
		sstables:    state.sstables,
		snapshotID:  id,
	}
}

// quiesce prevents new operations from starting, matching the behavioral
// semantics of pebbleRSEngine.quiesce. The in-memory engine has no directory
// exclusivity constraint, but callers (e.g. rsEngineContainer) rely on
// quiesce to make beginOp return ErrRSEngineClosed.
func (e *TestingRSEngine) quiesce() {
	e.opMu.Lock()
	e.opMu.closed = true
	e.opMu.cond.Broadcast()
	e.opMu.Unlock()
}

// closeInner closes the engine, waiting for all snapshots to be released.
func (e *TestingRSEngine) closeInner() {
	// Mark as closed and wait for ongoing operation to complete.
	func() {
		e.opMu.Lock()
		defer e.opMu.Unlock()
		e.opMu.closed = true
		for e.opMu.ongoingOp {
			e.opMu.cond.Wait()
		}
	}()
	func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		state := e.mu.manifests[e.mu.currentManifestNum]
		state.refCount--
		if state.refCount == 0 {
			delete(e.mu.manifests, e.mu.currentManifestNum)
		}
	}()
	// Signal test hook before waiting.
	if e.closeWaitingCh != nil {
		close(e.closeWaitingCh)
	}
	// Wait for all manifests to be removed and externalRefs to reach 0.
	startTime := time.Now()
	for !e.closeCleanupDone(&startTime) {
		time.Sleep(100 * time.Millisecond)
	}
}

// closeCleanupDone checks whether all manifests and external refs have
// been released. If blocked for too long, it logs diagnostic info about
// open snapshots. Returns true when cleanup is complete.
func (e *TestingRSEngine) closeCleanupDone(startTime *time.Time) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.mu.manifests) == 0 && e.mu.externalRefs == 0 {
		return true
	}
	if elapsed := time.Since(*startTime); elapsed > 5*time.Second &&
		(len(e.mu.openSnapshots) > 0 || e.mu.externalRefs > 0) {
		var buf strings.Builder
		fmt.Fprintf(&buf, "TestingRSEngine.Close() blocked for %v waiting for "+
			"%d open snapshot(s), %d external ref(s):\n",
			elapsed, len(e.mu.openSnapshots), e.mu.externalRefs)
		for id, info := range e.mu.openSnapshots {
			fmt.Fprintf(&buf, "  snapshot %d (created %v ago):\n%s\n",
				id, time.Since(info.createTime), info.stack)
		}
		fmt.Print(buf.String())
		*startTime = time.Now()
	}
	return false
}

// prepareExternalManifest reads a manifest file from BasaltDir and stages it
// as a candidate. Currently a no-op since TestingRSEngine uses open/close.
func (e *TestingRSEngine) prepareExternalManifest(manifestNum DiskFileNum) error {
	return nil
}

// installPreparedManifest promotes the prepared candidate version to current.
// Currently unused since TestingRSEngine uses open/close.
func (e *TestingRSEngine) installPreparedManifest(manifestNum DiskFileNum) {
	panic("TestingRSEngine does not support in-place manifest install")
}

// TestFlushSSTables is a test helper that creates a scratch file and calls FlushSSTables.
func (e *TestingRSEngine) TestFlushSSTables(scratchFileName string) error {
	e.mu.Lock()
	currentManifestNum := e.mu.currentManifestNum
	e.mu.Unlock()
	// Write a simple content to scratch file.
	content := fmt.Sprintf("previous-manifest:%d", currentManifestNum)
	scratchPath := e.opts.basaltFS.PathJoin(e.opts.basaltScratchPathDir, scratchFileName)
	f, err := e.opts.basaltFS.Create(scratchPath, vfs.WriteCategoryUnspecified)
	if err != nil {
		return err
	}
	if _, err := f.Write([]byte(content)); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	flushErr := e.flushSSTables([]string{scratchFileName}, nil)
	if err := e.opts.basaltFS.Remove(scratchPath); err != nil && flushErr == nil {
		return errors.Wrap(err, "removing scratch file")
	}
	return flushErr
}

// TestingRSEngineSnapshot implements RSEngineSnapshot for testing.
type TestingRSEngineSnapshot struct {
	engine      *TestingRSEngine
	manifestNum DiskFileNum
	sstables    []string
	snapshotID  uint64
	mu          struct {
		sync.Mutex
		closed bool
	}
}

var _ RSEngineSnapshot = (*TestingRSEngineSnapshot)(nil)

// ManifestInfo returns the manifest info for this snapshot.
func (s *TestingRSEngineSnapshot) ManifestInfo() ManifestInfo {
	if s.mu.closed {
		panic("snapshot is closed")
	}
	files := make([]FileNameAndNum, len(s.sstables))
	for i, name := range s.sstables {
		// Parse DiskFileNum from name (e.g., "000042.sst" -> 42).
		var num DiskFileNum
		fmt.Sscanf(name, "%d.sst", &num) //nolint:errcheck
		files[i] = FileNameAndNum{Name: name, Num: num}
	}
	return ManifestInfo{
		Manifest: FileNameAndNum{
			Name: formatManifestName(s.manifestNum),
			Num:  s.manifestNum,
		},
		Files: files,
	}
}

// ManifestNum returns the manifest number for this snapshot.
func (s *TestingRSEngineSnapshot) ManifestNum() DiskFileNum {
	return s.manifestNum
}

// Clone creates a fully independent RSEngineSnapshot that shares the same
// pinned manifest state. The clone gets its own manifest ref and snapshotID.
func (s *TestingRSEngineSnapshot) Clone() RSEngineSnapshot {
	buf := make([]byte, 4096)
	n := runtime.Stack(buf, false)
	stack := string(buf[:n])
	e := s.engine
	e.mu.Lock()
	defer e.mu.Unlock()
	state := e.mu.manifests[s.manifestNum]
	state.refCount++
	e.mu.nextSnapshotID++
	id := e.mu.nextSnapshotID
	e.mu.openSnapshots[id] = &snapshotInfo{
		id:         id,
		stack:      stack,
		createTime: time.Now(),
	}
	return &TestingRSEngineSnapshot{
		engine:      e,
		manifestNum: s.manifestNum,
		sstables:    s.sstables,
		snapshotID:  id,
	}
}

// Split splits the snapshot into LHS and RHS manifests. Returns 0 for
// nextFileNum because TestingRSEngine allocates file numbers via GetFileNums
// (the Raft allocator), so the allocator already knows about them.
func (s *TestingRSEngineSnapshot) Split(
	ctx context.Context, splitKey roachpb.Key, rhsDir string,
) (lhsManifest FileNameAndNum, rhs ManifestInfo, nextFileNum uint64, err error) {
	if s.mu.closed {
		panic("snapshot is closed")
	}
	if err := s.engine.beginOp(); err != nil {
		return FileNameAndNum{}, ManifestInfo{}, 0, err
	}
	defer s.engine.endOp()
	// Get file numbers for new manifests.
	count := max(60, 1+len(s.sstables))
	fileNums, err := s.engine.opts.manifestChangeCommitter.GetFileNums(count)
	if err != nil {
		return FileNameAndNum{}, ManifestInfo{}, 0, err
	}
	// Use same manifest number for both LHS and RHS (split shares the same version).
	newManifestNum := fileNums[len(fileNums)-1]
	manifestName := formatManifestName(newManifestNum)
	// Create LHS manifest in BasaltDir.
	if err := writeManifestFile(s.engine.opts.basaltFS, s.engine.opts.basaltDir,
		newManifestNum, s.sstables); err != nil {
		return FileNameAndNum{}, ManifestInfo{}, 0, err
	}
	// Create RHS directory and manifest.
	if err := s.engine.opts.basaltFS.MkdirAll(rhsDir, 0755); err != nil {
		return FileNameAndNum{}, ManifestInfo{}, 0, errors.Wrap(err, "creating RHS directory")
	}
	if err := writeManifestFile(s.engine.opts.basaltFS, rhsDir, newManifestNum, s.sstables); err != nil {
		return FileNameAndNum{}, ManifestInfo{}, 0, err
	}
	// Hardlink sstables from LHS to RHS.
	rhsFiles := make([]FileNameAndNum, len(s.sstables))
	for i, sstName := range s.sstables {
		srcPath := s.engine.opts.basaltFS.PathJoin(s.engine.opts.basaltDir, sstName)
		dstPath := s.engine.opts.basaltFS.PathJoin(rhsDir, sstName)
		if err := s.engine.opts.basaltFS.Link(srcPath, dstPath); err != nil {
			return FileNameAndNum{}, ManifestInfo{}, 0, errors.Wrapf(err, "linking SST %s to %s", srcPath, dstPath)
		}
		var num DiskFileNum
		fmt.Sscanf(sstName, "%d.sst", &num) //nolint:errcheck
		rhsFiles[i] = FileNameAndNum{Name: sstName, Num: num}
	}
	lhsManifest = FileNameAndNum{Name: manifestName, Num: newManifestNum}
	rhs = ManifestInfo{
		Manifest: FileNameAndNum{Name: manifestName, Num: newManifestNum},
		Files:    rhsFiles,
	}
	return lhsManifest, rhs, 0, nil
}

// Merge merges this snapshot with RHS snapshot. Returns 0 for nextFileNum
// because TestingRSEngine allocates file numbers via GetFileNums (the Raft
// allocator), so the allocator already knows about them.
func (s *TestingRSEngineSnapshot) Merge(
	ctx context.Context, rhs RSEngineSnapshot,
) (merged ManifestInfo, nextFileNum uint64, err error) {
	rhsSnap, ok := rhs.(*TestingRSEngineSnapshot)
	if !ok {
		return ManifestInfo{}, 0, errors.AssertionFailedf("expected *TestingRSEngineSnapshot, got %T", rhs)
	}
	// Acquire operation lock on both engines.
	if err := s.engine.beginOp(); err != nil {
		return ManifestInfo{}, 0, err
	}
	defer s.engine.endOp()
	if err := rhsSnap.engine.beginOp(); err != nil {
		return ManifestInfo{}, 0, err
	}
	defer rhsSnap.engine.endOp()
	rhsInfo := rhs.ManifestInfo()
	// Get file numbers: one for manifest, one for each RHS sstable.
	count := max(60, 1+len(rhsInfo.Files))
	fileNums, err := s.engine.opts.manifestChangeCommitter.GetFileNums(count)
	if err != nil {
		return ManifestInfo{}, 0, err
	}
	newManifestNum := fileNums[len(fileNums)-1]
	// Renumber RHS sstables and hardlink to LHS directory.
	newFiles := make([]FileNameAndNum, len(rhsInfo.Files))
	allSSTables := append([]string{}, s.sstables...)
	for i, rhsFile := range rhsInfo.Files {
		newNum := fileNums[i]
		newName := formatSSTName(newNum)
		// Hardlink from RHS directory to LHS directory.
		srcPath := rhsSnap.engine.opts.basaltFS.PathJoin(rhsSnap.engine.opts.basaltDir, rhsFile.Name)
		dstPath := s.engine.opts.basaltFS.PathJoin(s.engine.opts.basaltDir, newName)
		if err := s.engine.opts.basaltFS.Link(srcPath, dstPath); err != nil {
			return ManifestInfo{}, 0, errors.Wrapf(err, "linking SST %s to %s", srcPath, dstPath)
		}
		newFiles[i] = FileNameAndNum{Name: newName, Num: newNum}
		allSSTables = append(allSSTables, newName)
	}
	if !slices.IsSorted(allSSTables) {
		return ManifestInfo{}, 0, errors.AssertionFailedf("sstables not sorted after merge: %v", allSSTables)
	}
	// Write merged manifest.
	if err := writeManifestFile(s.engine.opts.basaltFS, s.engine.opts.basaltDir,
		newManifestNum, allSSTables); err != nil {
		return ManifestInfo{}, 0, err
	}
	return ManifestInfo{
		Manifest: FileNameAndNum{Name: formatManifestName(newManifestNum), Num: newManifestNum},
		Files:    newFiles,
	}, 0, nil
}

// Close releases the manifest ref held by this snapshot.
func (s *TestingRSEngineSnapshot) Close() {
	s.mu.Lock()
	if s.mu.closed {
		s.mu.Unlock()
		return
	}
	s.mu.closed = true
	s.mu.Unlock()
	s.releaseManifestRef()
}

// releaseManifestRef releases the manifest ref held by this snapshot.
// Called when the snapshot's internal refcount reaches 0.
func (s *TestingRSEngineSnapshot) releaseManifestRef() {
	s.engine.mu.Lock()
	defer s.engine.mu.Unlock()
	// Remove from snapshot tracking.
	delete(s.engine.mu.openSnapshots, s.snapshotID)
	state, ok := s.engine.mu.manifests[s.manifestNum]
	if !ok {
		panic("expected to find manifest")
	}
	state.refCount--
	// Remove from map if unreferenced.
	if state.refCount == 0 {
		delete(s.engine.mu.manifests, s.manifestNum)
	}
	s.engine.mu.cond.Signal()
}
