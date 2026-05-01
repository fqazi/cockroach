// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// newTestContainer creates a container with a TestingRSEngine for testing.
func newTestContainer(
	t *testing.T, fs vfs.FS, basaltDir string,
) (RSEngine, *mockManifestChangeCommitter, *stop.Stopper) {
	t.Helper()
	committer := newMockManifestChangeCommitter(100)
	stopper := stop.NewStopper()
	containerOpts := RSEngineContainerOptions{
		ManifestChangeCommitter: committer,
		BasaltFS:                fs,
		BasaltDir:               basaltDir,
		BasaltScratchPathDir:    basaltDir + "/scratch",
		LogCtx:                  context.Background(),
		OpenRSEngineFunc:        OpenTestingRSEngine,
		Stopper:                 stopper,
	}
	c, err := NewRSEngineContainer(0, containerOpts)
	require.NoError(t, err)
	return c, committer, stopper
}

// initialEngine returns the active TestingRSEngine from a container.
func initialEngine(t *testing.T, c RSEngine) *TestingRSEngine {
	t.Helper()
	container := c.(*rsEngineContainer)
	container.activeMu.RLock()
	defer container.activeMu.RUnlock()
	return container.activeMu.active.(*TestingRSEngine)
}

func TestContainerBasicDelegation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	fs := vfs.NewMem()
	c, _, stopper := newTestContainer(t, fs, "/basalt")
	defer stopper.Stop(context.Background())
	defer c.Close()
	engine := initialEngine(t, c)
	require.Equal(t, NoManifestNum, c.CurrentManifestNum())
	c.CompactionToggle(true)
	require.Contains(t, engine.getNoOpLog(), "CompactionToggle(true)")
	engine.clearNoOpLog()
	c.WaitForOngoingManifestChanges()
	require.Contains(t, engine.getNoOpLog(), "WaitForOngoingManifestChanges")
	snap := c.NewSnapshot()
	require.NotNil(t, snap)
	require.Equal(t, NoManifestNum, snap.ManifestNum())
	snap.Close()
}

func TestContainerRefUnref(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	fs := vfs.NewMem()
	c, _, stopper := newTestContainer(t, fs, "/basalt")
	c.Ref()
	c.Ref()
	c.Unref()
	c.Unref()
	c.Close()
	stopper.Stop(context.Background())
}

func TestContainerRefBlocksClose(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	fs := vfs.NewMem()
	c, _, stopper := newTestContainer(t, fs, "/basalt")
	defer stopper.Stop(context.Background())
	c.Ref()
	closeDone := make(chan struct{})
	go func() {
		c.Close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
		t.Fatal("Close returned before Unref")
	case <-time.After(50 * time.Millisecond):
	}
	c.Unref()
	select {
	case <-closeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not complete after Unref")
	}
}

func TestContainerUnrefPanicsOnNegative(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	fs := vfs.NewMem()
	c, _, stopper := newTestContainer(t, fs, "/basalt")
	defer stopper.Stop(context.Background())
	defer c.Close()
	require.Panics(t, func() { c.Unref() })
}

func TestContainerPrepareInstall(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	fs := vfs.NewMem()
	c, committer, stopper := newTestContainer(t, fs, "/basalt")
	defer stopper.Stop(context.Background())
	defer c.Close()
	engine := initialEngine(t, c)
	err := engine.TestFlushSSTables("flush1.sst")
	require.NoError(t, err)
	newManifestNum := committer.getLastInstalledManifest()
	require.NotEqual(t, NoManifestNum, newManifestNum)
	err = c.PrepareExternalManifest(newManifestNum)
	require.NoError(t, err)
	require.Equal(t, NoManifestNum, c.CurrentManifestNum())
	c.InstallPreparedManifest(newManifestNum)
	require.Equal(t, newManifestNum, c.CurrentManifestNum())
	container := c.(*rsEngineContainer)
	container.closeWG.Wait()
	container.activeMu.RLock()
	require.Empty(t, container.activeMu.quiesced)
	container.activeMu.RUnlock()
}

func TestContainerDoublePrepare(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	fs := vfs.NewMem()
	c, committer, stopper := newTestContainer(t, fs, "/basalt")
	defer stopper.Stop(context.Background())
	defer c.Close()
	engine := initialEngine(t, c)
	err := engine.TestFlushSSTables("flush1.sst")
	require.NoError(t, err)
	manifestNum := committer.getLastInstalledManifest()
	err = c.PrepareExternalManifest(manifestNum)
	require.NoError(t, err)
	require.Panics(t, func() {
		_ = c.PrepareExternalManifest(manifestNum)
	})
	c.InstallPreparedManifest(manifestNum)
}

func TestContainerInstallWithoutPreparePanics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	fs := vfs.NewMem()
	c, _, stopper := newTestContainer(t, fs, "/basalt")
	defer stopper.Stop(context.Background())
	defer c.Close()
	require.Panics(t, func() {
		c.InstallPreparedManifest(42)
	})
}

func TestContainerLongOpDuringSwap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	fs := vfs.NewMem()
	c, committer, stopper := newTestContainer(t, fs, "/basalt")
	defer stopper.Stop(context.Background())
	defer c.Close()
	container := c.(*rsEngineContainer)
	engine := initialEngine(t, c)
	// Simulate a long operation holding a ref on the active engine.
	container.activeMu.RLock()
	active := container.activeMu.active
	active.ref()
	container.activeMu.RUnlock()
	err := engine.TestFlushSSTables("flush1.sst")
	require.NoError(t, err)
	manifestNum := committer.getLastInstalledManifest()
	err = c.PrepareExternalManifest(manifestNum)
	require.NoError(t, err)
	c.InstallPreparedManifest(manifestNum)
	// Old engine is quiesced but not yet closed (ref held).
	container.activeMu.RLock()
	require.Len(t, container.activeMu.quiesced, 1)
	container.activeMu.RUnlock()
	require.Equal(t, manifestNum, c.CurrentManifestNum())
	// Release ref — old engine closes and quiesced list drains.
	active.unref()
	container.closeWG.Wait()
	container.activeMu.RLock()
	require.Empty(t, container.activeMu.quiesced)
	container.activeMu.RUnlock()
}

func TestContainerEnableUnreferencedFileDeletionOnEmpty(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	fs := vfs.NewMem()
	c, committer, stopper := newTestContainer(t, fs, "/basalt")
	defer stopper.Stop(context.Background())
	defer c.Close()
	container := c.(*rsEngineContainer)
	engine := initialEngine(t, c)
	err := engine.TestFlushSSTables("flush1.sst")
	require.NoError(t, err)
	manifestNum := committer.getLastInstalledManifest()
	err = c.PrepareExternalManifest(manifestNum)
	require.NoError(t, err)
	c.InstallPreparedManifest(manifestNum)
	container.closeWG.Wait()
	newEngine := initialEngine(t, c)
	require.Contains(t, newEngine.getNoOpLog(), "EnableUnreferencedFileDeletion")
}

func TestContainerConcurrentShortOps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	fs := vfs.NewMem()
	c, _, stopper := newTestContainer(t, fs, "/basalt")
	defer stopper.Stop(context.Background())
	defer c.Close()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = c.CurrentManifestNum()
				c.CompactionToggle(true)
				snap := c.NewSnapshot()
				snap.Close()
			}
		}()
	}
	wg.Wait()
}

func TestContainerCloseClosesActiveAndPending(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	fs := vfs.NewMem()
	c, committer, stopper := newTestContainer(t, fs, "/basalt")
	defer stopper.Stop(context.Background())
	engine := initialEngine(t, c)
	err := engine.TestFlushSSTables("flush1.sst")
	require.NoError(t, err)
	manifestNum := committer.getLastInstalledManifest()
	err = c.PrepareExternalManifest(manifestNum)
	require.NoError(t, err)
	c.Close()
}

func TestContainerMultipleSwaps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	fs := vfs.NewMem()
	c, committer, stopper := newTestContainer(t, fs, "/basalt")
	defer stopper.Stop(context.Background())
	defer c.Close()
	container := c.(*rsEngineContainer)
	engine := initialEngine(t, c)
	err := engine.TestFlushSSTables("flush1.sst")
	require.NoError(t, err)
	num1 := committer.getLastInstalledManifest()
	err = c.PrepareExternalManifest(num1)
	require.NoError(t, err)
	c.InstallPreparedManifest(num1)
	container.closeWG.Wait()
	require.Equal(t, num1, c.CurrentManifestNum())
	engine2 := initialEngine(t, c)
	err = engine2.TestFlushSSTables("flush2.sst")
	require.NoError(t, err)
	num2 := committer.getLastInstalledManifest()
	require.NotEqual(t, num1, num2)
	err = c.PrepareExternalManifest(num2)
	require.NoError(t, err)
	c.InstallPreparedManifest(num2)
	container.closeWG.Wait()
	require.Equal(t, num2, c.CurrentManifestNum())
	container.activeMu.RLock()
	require.Empty(t, container.activeMu.quiesced)
	container.activeMu.RUnlock()
}

func TestContainerFlushOnQuiescedEngineReturnsError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	fs := vfs.NewMem()
	c, committer, stopper := newTestContainer(t, fs, "/basalt")
	defer stopper.Stop(context.Background())
	defer c.Close()
	container := c.(*rsEngineContainer)
	engine := initialEngine(t, c)
	container.activeMu.RLock()
	oldActive := container.activeMu.active.(*TestingRSEngine)
	oldActive.ref()
	container.activeMu.RUnlock()
	err := engine.TestFlushSSTables("flush1.sst")
	require.NoError(t, err)
	manifestNum := committer.getLastInstalledManifest()
	err = c.PrepareExternalManifest(manifestNum)
	require.NoError(t, err)
	c.InstallPreparedManifest(manifestNum)
	err = oldActive.flushSSTables([]string{"should-fail.sst"}, nil)
	require.ErrorIs(t, err, ErrRSEngineClosed)
	oldActive.unref()
	container.closeWG.Wait()
}

func TestContainerSnapshotOnOldEngineAfterSwap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	fs := vfs.NewMem()
	c, committer, stopper := newTestContainer(t, fs, "/basalt")
	defer stopper.Stop(context.Background())
	defer c.Close()
	container := c.(*rsEngineContainer)
	container.activeMu.RLock()
	oldActive := container.activeMu.active
	oldActive.ref()
	container.activeMu.RUnlock()
	snap := oldActive.newSnapshot()
	engine := initialEngine(t, c)
	err := engine.TestFlushSSTables("flush1.sst")
	require.NoError(t, err)
	manifestNum := committer.getLastInstalledManifest()
	err = c.PrepareExternalManifest(manifestNum)
	require.NoError(t, err)
	c.InstallPreparedManifest(manifestNum)
	require.Equal(t, NoManifestNum, snap.ManifestNum())
	newSnap := c.NewSnapshot()
	require.Equal(t, manifestNum, newSnap.ManifestNum())
	newSnap.Close()
	snap.Close()
	oldActive.unref()
	container.closeWG.Wait()
}

func TestContainerString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	fs := vfs.NewMem()
	c, _, stopper := newTestContainer(t, fs, "/basalt")
	defer stopper.Stop(context.Background())
	defer c.Close()
	s := c.(*rsEngineContainer).String()
	require.Contains(t, s, "rsEngineContainer")
	require.Contains(t, s, "active=0")
	require.Contains(t, s, "pending=false")
	require.Contains(t, s, "quiesced=0")
	require.Contains(t, s, "refs=0")
	require.Contains(t, s, "closed=false")
}

func TestContainerConcurrentSwapAndOps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	fs := vfs.NewMem()
	c, committer, stopper := newTestContainer(t, fs, "/basalt")
	defer stopper.Stop(context.Background())
	defer c.Close()
	engine := initialEngine(t, c)
	err := engine.TestFlushSSTables("flush1.sst")
	require.NoError(t, err)
	manifestNum := committer.getLastInstalledManifest()
	var opsDone atomic.Int64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			_ = c.CurrentManifestNum()
			snap := c.NewSnapshot()
			snap.Close()
			opsDone.Add(1)
		}
	}()
	for opsDone.Load() < 10 {
		time.Sleep(time.Millisecond)
	}
	err = c.PrepareExternalManifest(manifestNum)
	require.NoError(t, err)
	c.InstallPreparedManifest(manifestNum)
	wg.Wait()
	require.Equal(t, manifestNum, c.CurrentManifestNum())
}
