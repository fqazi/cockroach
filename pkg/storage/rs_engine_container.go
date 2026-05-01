// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/pebble/vfs"
)

// rsEngineContainer implements RSEngine by managing one active InnerRSEngine
// and tracking quiesced engines from past manifest swaps. See the RSEngine
// interface comment for synchronization details.
type rsEngineContainer struct {
	containerOpts RSEngineContainerOptions
	openEngine    OpenRSEngineFunc
	compactionCS  *MultiEngineCompactionScheduler
	stopper       *stop.Stopper
	// activeMu protects the active engine pointer, the pending engine from
	// PrepareExternalManifest, and the quiesced engine list. Short operations
	// hold RLock for their duration; long operations hold RLock briefly to
	// load and ref the active engine. InstallPreparedManifest and
	// removeQuiesced hold Lock.
	activeMu struct {
		syncutil.RWMutex
		active        InnerRSEngine
		pendingEngine InnerRSEngine
		quiesced      []InnerRSEngine
	}
	// mu protects container-level ref counting and closed state.
	mu struct {
		syncutil.Mutex
		cond         *sync.Cond
		externalRefs int
		closed       bool
	}
	closeWG sync.WaitGroup
}

var _ RSEngine = (*rsEngineContainer)(nil)

// RSEngineContainerOptions holds options for creating an rsEngineContainer.
type RSEngineContainerOptions struct {
	ManifestChangeCommitter ManifestChangeCommitter
	BasaltFS                vfs.FS
	// BasaltDir is the directory containing the range-shared engine data files.
	BasaltDir string
	// BasaltScratchPathDir is a directory to use for scratch files.
	BasaltScratchPathDir string
	// LogCtx is the context used for Pebble's logger. It should carry
	// logging tags (e.g. node, store, range) from the caller.
	LogCtx context.Context
	// TestingProcessID, if non-empty, overrides the os.Getpid() value used to
	// name Pebble's per-process scratch directory.
	TestingProcessID string
	// OpenRSEngineFunc opens a new underlying RSEngine.
	OpenRSEngineFunc OpenRSEngineFunc
	// CompactionScheduler, if non-nil, is used to call OpeningEngine when
	// creating new underlying engines.
	CompactionScheduler *MultiEngineCompactionScheduler
	// Stopper is used to run async close goroutines for quiesced engines.
	Stopper *stop.Stopper
}

// makeRSEngineOptions builds RSEngineOptions from the container's fields,
// optionally setting the compaction scheduler from the
// MultiEngineCompactionScheduler.
func (opts *RSEngineContainerOptions) makeRSEngineOptions() RSEngineOptions {
	rsOpts := RSEngineOptions{
		manifestChangeCommitter: opts.ManifestChangeCommitter,
		basaltFS:                opts.BasaltFS,
		basaltDir:               opts.BasaltDir,
		basaltScratchPathDir:    opts.BasaltScratchPathDir,
		logCtx:                  opts.LogCtx,
		testingProcessID:        opts.TestingProcessID,
	}
	if opts.CompactionScheduler != nil {
		rsOpts.compactionScheduler = opts.CompactionScheduler.OpeningEngine(EngineTypeRangeShared)
	}
	return rsOpts
}

// NewRSEngineContainer creates a new rsEngineContainer, opening the initial
// underlying engine at manifestNum.
func NewRSEngineContainer(
	manifestNum DiskFileNum, containerOpts RSEngineContainerOptions,
) (RSEngine, error) {
	opts := containerOpts.makeRSEngineOptions()
	initial, err := containerOpts.OpenRSEngineFunc(manifestNum, opts)
	if err != nil {
		return nil, err
	}
	c := &rsEngineContainer{
		containerOpts: containerOpts,
		openEngine:    containerOpts.OpenRSEngineFunc,
		compactionCS:  containerOpts.CompactionScheduler,
		stopper:       containerOpts.Stopper,
	}
	c.activeMu.active = initial
	c.mu.cond = sync.NewCond(&c.mu.Mutex)
	return c, nil
}

// acquireActive loads the active engine under activeMu.RLock and refs it.
// The caller must call engine.unref() when done. Used by long operations
// to protect against H1 (active engine swap).
func (c *rsEngineContainer) acquireActive() InnerRSEngine {
	c.activeMu.RLock()
	active := c.activeMu.active
	active.ref()
	c.activeMu.RUnlock()
	return active
}

// CurrentManifestNum delegates to the active engine (short operation).
func (c *rsEngineContainer) CurrentManifestNum() DiskFileNum {
	c.activeMu.RLock()
	defer c.activeMu.RUnlock()
	return c.activeMu.active.currentManifestNum()
}

// CompactionToggle delegates to the active engine (short operation).
func (c *rsEngineContainer) CompactionToggle(enable bool) {
	c.activeMu.RLock()
	defer c.activeMu.RUnlock()
	c.activeMu.active.compactionToggle(enable)
}

// WaitForOngoingManifestChanges delegates to the active engine (long
// operation).
func (c *rsEngineContainer) WaitForOngoingManifestChanges() {
	active := c.acquireActive()
	defer active.unref()
	active.waitForOngoingManifestChanges()
}

// FlushSSTables delegates to the active engine (long operation).
func (c *rsEngineContainer) FlushSSTables(
	scratchNames []string, flushCommit *FlushCommitInfo,
) error {
	active := c.acquireActive()
	defer active.unref()
	return active.flushSSTables(scratchNames, flushCommit)
}

// AddSSTables delegates to the active engine (long operation).
func (c *rsEngineContainer) AddSSTables(scratchNames []string) error {
	active := c.acquireActive()
	defer active.unref()
	return active.addSSTables(scratchNames)
}

// Ref increments the container's external reference count.
func (c *rsEngineContainer) Ref() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.externalRefs++
}

// Unref decrements the container's external reference count. Signals Close
// when it reaches 0.
func (c *rsEngineContainer) Unref() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.externalRefs--
	if c.mu.externalRefs < 0 {
		panic("rsEngineContainer: externalRefs went negative")
	}
	c.mu.cond.Signal()
}

// NewSnapshot delegates to the active engine (short operation).
func (c *rsEngineContainer) NewSnapshot() RSEngineSnapshot {
	c.activeMu.RLock()
	defer c.activeMu.RUnlock()
	return c.activeMu.active.newSnapshot()
}

// PrepareExternalManifest opens a new underlying engine at manifestNum and
// quiesces the current active engine. The new engine is stored as
// pendingEngine for InstallPreparedManifest.
func (c *rsEngineContainer) PrepareExternalManifest(manifestNum DiskFileNum) error {
	c.activeMu.RLock()
	if c.activeMu.pendingEngine != nil {
		c.activeMu.RUnlock()
		panic("rsEngineContainer: PrepareExternalManifest called with pending engine already set")
	}
	active := c.activeMu.active
	active.ref()
	c.activeMu.RUnlock()
	newOpts := c.containerOpts.makeRSEngineOptions()
	newEngine, err := c.openEngine(manifestNum, newOpts)
	if err != nil {
		active.unref()
		return err
	}
	active.quiesce()
	active.unref()
	// Store pending engine. activeMu.RLock is sufficient because raftMu
	// serializes Prepare/Install calls — no concurrent writer.
	c.activeMu.RLock()
	c.activeMu.pendingEngine = newEngine
	c.activeMu.RUnlock()
	return nil
}

// InstallPreparedManifest swaps the active engine to the pendingEngine
// prepared by PrepareExternalManifest. The old active engine is added to
// the quiesced list and closed asynchronously via the Stopper.
func (c *rsEngineContainer) InstallPreparedManifest(manifestNum DiskFileNum) {
	c.activeMu.Lock()
	pending := c.activeMu.pendingEngine
	if pending == nil {
		c.activeMu.Unlock()
		panic("rsEngineContainer: InstallPreparedManifest called without pending engine")
	}
	old := c.activeMu.active
	c.activeMu.active = pending
	c.activeMu.pendingEngine = nil
	c.activeMu.quiesced = append(c.activeMu.quiesced, old)
	c.activeMu.Unlock()
	c.closeWG.Add(1)
	ctx := c.containerOpts.LogCtx
	if err := c.stopper.RunAsyncTask(ctx, "close-quiesced-rsengine", func(ctx context.Context) {
		defer c.closeWG.Done()
		old.closeInner()
		c.removeQuiesced(old)
	}); err != nil {
		old.closeInner()
		c.removeQuiesced(old)
		c.closeWG.Done()
	}
}

// removeQuiesced removes a closed engine from the quiesced list. When the
// list becomes empty, refs the active engine and calls
// EnableUnreferencedFileDeletion on it. The ref ensures the call targets
// the correct engine even if a swap occurs concurrently.
func (c *rsEngineContainer) removeQuiesced(engine InnerRSEngine) {
	c.activeMu.Lock()
	for i, e := range c.activeMu.quiesced {
		if e == engine {
			c.activeMu.quiesced = append(c.activeMu.quiesced[:i], c.activeMu.quiesced[i+1:]...)
			break
		}
	}
	var activeToNotify InnerRSEngine
	if len(c.activeMu.quiesced) == 0 && c.activeMu.active != nil {
		activeToNotify = c.activeMu.active
		activeToNotify.ref()
	}
	c.activeMu.Unlock()
	if activeToNotify != nil {
		activeToNotify.enableUnreferencedFileDeletion()
		activeToNotify.unref()
	}
}

// Close closes the container and all engines. Waits for container-level refs
// to drain, then quiesces and closes the active engine, and waits for all
// quiesced engine close goroutines to finish.
func (c *rsEngineContainer) Close() {
	c.mu.Lock()
	c.mu.closed = true
	for c.mu.externalRefs > 0 {
		c.mu.cond.Wait()
	}
	c.mu.Unlock()
	c.activeMu.Lock()
	active := c.activeMu.active
	c.activeMu.active = nil
	pending := c.activeMu.pendingEngine
	c.activeMu.pendingEngine = nil
	c.activeMu.Unlock()
	if active != nil {
		active.quiesce()
		active.closeInner()
	}
	if pending != nil {
		pending.quiesce()
		pending.closeInner()
	}
	c.closeWG.Wait()
}

// TestingInnerEngine returns the active underlying InnerRSEngine. For use
// in tests that need to inspect engine internals (e.g. PrintRSEngineState).
func (c *rsEngineContainer) TestingInnerEngine() InnerRSEngine {
	c.activeMu.RLock()
	defer c.activeMu.RUnlock()
	return c.activeMu.active
}

// String returns a debug string describing the container's state.
func (c *rsEngineContainer) String() string {
	c.activeMu.RLock()
	activeManifest := DiskFileNum(0)
	if c.activeMu.active != nil {
		activeManifest = c.activeMu.active.currentManifestNum()
	}
	hasPending := c.activeMu.pendingEngine != nil
	numQuiesced := len(c.activeMu.quiesced)
	c.activeMu.RUnlock()
	c.mu.Lock()
	refs := c.mu.externalRefs
	closed := c.mu.closed
	c.mu.Unlock()
	return fmt.Sprintf("rsEngineContainer{active=%d, pending=%v, quiesced=%d, refs=%d, closed=%v}",
		activeManifest, hasPending, numQuiesced, refs, closed)
}
