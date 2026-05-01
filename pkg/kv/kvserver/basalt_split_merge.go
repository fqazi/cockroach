// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// splitManifestInfo holds the manifest information needed for a split
// transaction. It captures the state at the time of manifest preparation.
type splitManifestInfo struct {
	// lhsManifest is the new manifest for the LHS range after the split.
	// The RSEngine creates this in its own directory.
	lhsManifest storage.FileNameAndNum
	// rhsManifest contains the manifest and files for the RHS range.
	// The RSEngine creates these in the rhsDir.
	rhsManifest storage.ManifestInfo
	// nextFileNum is the file number high-water mark from pebble's SplitLSM.
	// Both LHS and RHS RangeFileNumAllocState.NextFileNum must be advanced
	// past this value to avoid collisions with internally allocated file
	// numbers.
	nextFileNum uint64
	// currentManifestNum is the manifest number at the time of preparation.
	// This is used to verify no manifest changes occurred before the split.
	currentManifestNum uint64
	// descGeneration is the descriptor generation at the time of preparation.
	descGeneration roachpb.RangeGeneration
}

// prepareSplitManifests prepares a range-shared manifest for a split.
// Called before the split 2PC transaction starts. Returns nil splitManifestInfo
// if the range does not have a range-shared LSM.
//
// The caller must call enableCompactionsAfterSplit when the split completes or
// fails to re-enable compactions.
func (r *Replica) prepareSplitManifests(
	ctx context.Context, splitKey roachpb.Key, rhsDesc *roachpb.RangeDescriptor,
) (*splitManifestInfo, error) {
	// Step 1: Get RSEngine reference (brief lock). Ref() prevents the engine
	// from being closed while we use it outside the lock.
	r.rsStateMu.RLock()
	rsEngine := r.rsStateMu.rsEngine
	if rsEngine != nil {
		rsEngine.Ref()
	}
	r.rsStateMu.RUnlock()
	if rsEngine == nil {
		log.KvDistribution.Infof(ctx, "split: no range-shared LSM, skipping RS split")
		return nil, nil // no range-shared LSM
	}
	defer rsEngine.Unref()
	log.KvDistribution.Infof(ctx, "split: preparing range-shared split manifests (splitKey=%s, rhs=r%d)", splitKey, rhsDesc.RangeID)
	// Step 2: Check basaltFS before any engine operations.
	basaltFS := r.store.cfg.BasaltFS
	if basaltFS == nil {
		return nil, errors.New("basaltFS is not configured")
	}
	// Construct RHS directory for this store's replica. The RSEngine will
	// create files here for the RHS.
	thisRepl, ok := rhsDesc.GetReplicaDescriptor(r.store.StoreID())
	if !ok {
		return nil, errors.Errorf("replica for store %d not found in RHS descriptor", r.store.StoreID())
	}
	rhsDir := BasaltDir(basaltFS, thisRepl.StoreID, rhsDesc.RangeID, thisRepl.ReplicaID)
	// Step 3: Disable compactions and drain (BEFORE mutex, best-effort).
	rsEngine.CompactionToggle(false)
	rsEngine.WaitForOngoingManifestChanges()
	// Step 4a: Capture descriptor for later verification. We do this before
	// taking the snapshot since the split will fail if the descriptor changes
	// so that later check will also ensure that the descriptor is consistent
	// with the snapshot. Replica.Desc() needs Replica.mu, which is ordered
	// before Replica.rsStateMu, so we can't grab the descriptor while holding
	// Replica.rsStateMu.
	desc := r.Desc() // nolint:deferunlockcheck
	// Step 4b: Take snapshot.
	rsSnap := rsEngine.NewSnapshot() // nolint:deferunlockcheck
	// Step 5: Call Split on snapshot OUTSIDE mutex (long operation).
	lhs, rhs, nextFileNum, err := rsSnap.Split(ctx, splitKey, rhsDir)
	if err != nil {
		rsSnap.Close()
		rsEngine.CompactionToggle(true)
		// If the RSEngine was closed (e.g. by a concurrent split replacing it)
		// or we lost the lease (e.g. due to a lease transfer during joint config
		// exit in maybeLeaveAtomicChangeReplicas), mark the error as retryable so
		// executeAdminCommandWithDescriptor retries the split. On retry,
		// redirectOnOrAcquireLease will redirect to the actual leaseholder.
		if errors.Is(err, storage.ErrRSEngineClosed) ||
			errors.Is(err, errManifestCommitterNotLeaseholder) {
			return nil, errors.Mark(
				errors.Wrap(err, "preparing split manifests"),
				errMarkCanRetryReplicationChangeWithUpdatedDesc,
			)
		}
		return nil, errors.Wrap(err, "preparing split manifests")
	}
	// No-op: all tables are on the LHS (or LSM is empty). No manifests or
	// files were created, so skip hardlinking and return nil to tell the
	// split transaction to skip RS manifest updates.
	if lhs.Num == 0 {
		rsSnap.Close()
		rsEngine.CompactionToggle(true)
		log.KvDistribution.Infof(ctx, "split: RS split no-op (all tables on LHS or LSM empty)")
		return nil, nil
	}
	currentManifest := uint64(rsSnap.ManifestNum())
	// Step 6: Create hardlinks OUTSIDE mutex (IO intensive).
	if err := r.createSplitHardlinks(ctx, lhs, rhs, desc, rhsDesc); err != nil {
		rsSnap.Close()
		rsEngine.CompactionToggle(true)
		return nil, err
	}
	rsSnap.Close()
	log.KvDistribution.Infof(ctx, "split: RS split prepared (splitKey=%s, rhs=r%d, lhs manifest=%d, rhs manifest=%d, nextFileNum=%d)",
		splitKey, rhsDesc.RangeID, lhs.Num, rhs.Manifest.Num, nextFileNum)
	return &splitManifestInfo{
		lhsManifest:        lhs,
		rhsManifest:        rhs,
		nextFileNum:        nextFileNum,
		currentManifestNum: currentManifest,
		descGeneration:     desc.Generation,
	}, nil
}

// createSplitHardlinks creates hardlinks for split manifests in OTHER replica
// directories. The RSEngine already created files in this replica's directories.
// LHS keeps existing files with an updated manifest. RHS gets a new directory
// with hardlinks to shared SSTables.
func (r *Replica) createSplitHardlinks(
	ctx context.Context,
	lhsManifest storage.FileNameAndNum,
	rhs storage.ManifestInfo,
	desc *roachpb.RangeDescriptor,
	rhsDesc *roachpb.RangeDescriptor,
) error {
	basaltFS := r.store.cfg.BasaltFS
	if basaltFS == nil {
		return errors.New("basaltFS is not configured")
	}
	thisStoreID := r.store.StoreID()
	// Get this replica's directories to use as source for hardlinks.
	thisLhsDir := BasaltDir(basaltFS, thisStoreID, r.RangeID, r.replicaID)
	thisRhsRepl, ok := rhsDesc.GetReplicaDescriptor(thisStoreID)
	if !ok {
		return errors.Errorf("replica for store %d not found in RHS descriptor", thisStoreID)
	}
	thisRhsDir := BasaltDir(basaltFS, thisRhsRepl.StoreID, rhsDesc.RangeID, thisRhsRepl.ReplicaID)
	for _, repl := range desc.Replicas().Descriptors() {
		if repl.StoreID == thisStoreID {
			continue // skip this replica - RSEngine already handled it
		}
		// LHS: create directory if needed (the replica may not have been
		// initialized yet) and link the new manifest.
		lhsDir := BasaltDir(basaltFS, repl.StoreID, r.RangeID, repl.ReplicaID)
		if err := basaltFS.MkdirAll(lhsDir, 0755); err != nil {
			return errors.Wrap(err, "creating LHS directory")
		}
		srcManifest := basaltFS.PathJoin(thisLhsDir, lhsManifest.Name)
		dstManifest := basaltFS.PathJoin(lhsDir, lhsManifest.Name)
		if err := basaltFS.Link(srcManifest, dstManifest); err != nil {
			return errors.Wrap(err, "linking LHS manifest")
		}
		// RHS: create directory and hardlink manifest + files.
		rhsRepl, ok := rhsDesc.GetReplicaDescriptor(repl.StoreID)
		if !ok {
			return errors.Errorf("replica for store %d not found in RHS descriptor", repl.StoreID)
		}
		rhsDir := BasaltDir(basaltFS, rhsRepl.StoreID, rhsDesc.RangeID, rhsRepl.ReplicaID)
		if err := basaltFS.MkdirAll(rhsDir, 0755); err != nil {
			return errors.Wrap(err, "creating RHS directory")
		}
		if err := linkManifestAndFiles(basaltFS, thisRhsDir, rhsDir, rhs); err != nil {
			return errors.Wrap(err, "linking RHS files")
		}
	}
	// Seal pending registry entries so followers can read the encryption
	// metadata for the hardlinked split files.
	if sealer, ok := basaltFS.(fs.RegistrySealer); ok {
		sealer.SealPendingRegistryEntries()
	}
	return nil
}

// enableCompactionsAfterSplit re-enables compactions after split completes or
// fails. This must be called regardless of whether the split succeeds.
func (r *Replica) enableCompactionsAfterSplit() {
	r.rsStateMu.Lock()
	defer r.rsStateMu.Unlock()
	if r.rsStateMu.rsEngine != nil {
		r.rsStateMu.rsEngine.CompactionToggle(true)
	}
}

// mergeManifestInfo holds the manifest information needed for a merge
// transaction. It captures the state at the time of manifest preparation.
type mergeManifestInfo struct {
	// mergedManifest is the new manifest for the merged range.
	// The RSEngine creates this in its own directory along with renumbered files.
	mergedManifest storage.ManifestInfo
	// nextFileNum is the file number high-water mark from pebble's MergeLSM.
	// The LHS RangeFileNumAllocState.NextFileNum must be advanced past this
	// value to avoid collisions with internally allocated file numbers.
	nextFileNum uint64
	// lhsManifestNum is the LHS manifest number at the time of preparation.
	lhsManifestNum uint64
	// rhsManifestNum is the RHS manifest number at the time of preparation.
	rhsManifestNum uint64
	// lhsDescGeneration is the LHS descriptor generation at preparation.
	lhsDescGeneration roachpb.RangeGeneration
	// rhsRangeID is the RHS range ID for cleanup.
	rhsRangeID roachpb.RangeID
}

// prepareMergeManifests prepares range-shared manifests for a merge.
// Called before the merge 2PC transaction starts. Returns nil mergeManifestInfo
// if neither range has a range-shared LSM.
//
// The caller must call enableCompactionsAfterMerge when the merge completes or
// fails to re-enable compactions on both ranges.
func (r *Replica) prepareMergeManifests(
	ctx context.Context, rhsDesc *roachpb.RangeDescriptor,
) (*mergeManifestInfo, error) {
	// Get the RHS replica. Since replicas are collocated for merges, it should
	// be on the same store.
	rhsRepl := r.store.GetReplicaIfExists(rhsDesc.RangeID)
	if rhsRepl == nil {
		return nil, errors.Errorf("RHS replica %d not found on store", rhsDesc.RangeID)
	}
	// Step 1: Get RSEngine references (brief locks). Ref() prevents the
	// container from being closed while we use it outside the locks.
	// TODO(basalt): verify that the LHS cannot be destroyed during
	// AdminMerge (it holds the lease and executeAdminCommandWithDescriptor
	// checks IsDestroyed).
	r.rsStateMu.RLock()
	lhsEngine := r.rsStateMu.rsEngine
	if lhsEngine != nil {
		lhsEngine.Ref()
	}
	r.rsStateMu.RUnlock()
	if lhsEngine != nil {
		defer lhsEngine.Unref()
	}
	// TODO(basalt): use Store.GetRSEngineIfExists for RHS access to
	// atomically load + Ref the container, avoiding TOCTOU if the RHS
	// replica is destroyed concurrently.
	rhsRepl.rsStateMu.RLock()
	rhsEngine := rhsRepl.rsStateMu.rsEngine
	if rhsEngine != nil {
		rhsEngine.Ref()
	}
	rhsRepl.rsStateMu.RUnlock()
	if rhsEngine != nil {
		defer rhsEngine.Unref()
	}
	if lhsEngine == nil && rhsEngine == nil {
		log.KvDistribution.Infof(ctx, "merge: no range-shared LSM on LHS and RHS, skipping RS merge")
		return nil, nil
	}
	if (lhsEngine == nil) != (rhsEngine == nil) {
		return nil, errors.Errorf("merge: mismatched LHS and RHS engines")
	}
	log.KvDistribution.Infof(ctx, "merge: preparing range-shared merge manifests (LHS r%d + RHS r%d)", r.RangeID, rhsDesc.RangeID)
	// Step 2: Disable compactions and drain on both (BEFORE mutex, best-effort).
	lhsEngine.CompactionToggle(false)
	lhsEngine.WaitForOngoingManifestChanges()
	rhsEngine.CompactionToggle(false)
	rhsEngine.WaitForOngoingManifestChanges()
	// Step 3a: Capture LHS descriptor for later verification. We do this before
	// taking the snapshot since the merge will fail if the descriptor changes
	// so that later check will also ensure that the descriptor is consistent
	// with the snapshot. Replica.Desc() needs Replica.mu, which is ordered
	// before Replica.rsStateMu, so we can't grab the descriptor while holding
	// Replica.rsStateMu.
	lhsDesc := r.Desc() // nolint:deferunlockcheck
	// Step 3b: Take snapshots.
	lhsSnap := lhsEngine.NewSnapshot() // nolint:deferunlockcheck
	defer lhsSnap.Close()
	rhsSnap := rhsEngine.NewSnapshot() // nolint:deferunlockcheck
	defer rhsSnap.Close()
	// Step 4: Call Merge on LHS snapshot OUTSIDE mutex (long operation).
	merged, nextFileNum, err := lhsSnap.Merge(ctx, rhsSnap)
	if err != nil {
		lhsEngine.CompactionToggle(true)
		rhsEngine.CompactionToggle(true)
		// If either RSEngine was closed (e.g. by a concurrent split replacing
		// it) or we lost the lease (e.g. due to a lease transfer during joint
		// config exit), mark the error as retryable so the caller retries with
		// updated descriptors and fresh engines.
		if errors.Is(err, storage.ErrRSEngineClosed) ||
			errors.Is(err, errManifestCommitterNotLeaseholder) {
			return nil, errors.Mark(
				errors.Wrap(err, "preparing merge manifests"),
				errMarkCanRetryReplicationChangeWithUpdatedDesc,
			)
		}
		return nil, errors.Wrap(err, "preparing merge manifests")
	}
	// No-op: RHS has no tables. No manifest or files were created, so skip
	// hardlinking and return nil to tell the merge transaction to skip RS
	// manifest updates.
	if merged.Manifest.Num == 0 {
		lhsEngine.CompactionToggle(true)
		rhsEngine.CompactionToggle(true)
		log.KvDistribution.Infof(ctx, "merge: RS merge no-op (RHS has no tables)")
		return nil, nil
	}
	lhsManifestNum := uint64(lhsSnap.ManifestNum())
	rhsManifestNum := uint64(rhsSnap.ManifestNum())
	// Step 5: Create hardlinks OUTSIDE mutex (IO intensive).
	if err := r.createMergeHardlinks(ctx, merged, lhsDesc, rhsDesc); err != nil {
		lhsEngine.CompactionToggle(true)
		rhsEngine.CompactionToggle(true)
		return nil, err
	}
	log.KvDistribution.Infof(ctx, "merge: RS merge prepared (manifest=%d, nextFileNum=%d, files=%d)",
		merged.Manifest.Num, nextFileNum, len(merged.Files))
	return &mergeManifestInfo{
		mergedManifest:    merged,
		nextFileNum:       nextFileNum,
		lhsManifestNum:    lhsManifestNum,
		rhsManifestNum:    rhsManifestNum,
		lhsDescGeneration: lhsDesc.Generation,
		rhsRangeID:        rhsDesc.RangeID,
	}, nil
}

// createMergeHardlinks creates hardlinks for the merged manifest and files in
// OTHER replica directories. The RSEngine already created files in this
// replica's directory.
func (r *Replica) createMergeHardlinks(
	ctx context.Context, merged storage.ManifestInfo, lhsDesc, rhsDesc *roachpb.RangeDescriptor,
) error {
	basaltFS := r.store.cfg.BasaltFS
	if basaltFS == nil {
		return errors.New("basaltFS is not configured")
	}
	thisStoreID := r.store.StoreID()
	// Get this replica's LHS directory to use as source for hardlinks.
	thisLhsDir := BasaltDir(basaltFS, thisStoreID, r.RangeID, r.replicaID)
	for _, repl := range lhsDesc.Replicas().Descriptors() {
		if repl.StoreID == thisStoreID {
			continue // skip this replica - RSEngine already handled it
		}
		lhsDir := BasaltDir(basaltFS, repl.StoreID, r.RangeID, repl.ReplicaID)
		if err := basaltFS.MkdirAll(lhsDir, 0755); err != nil {
			return errors.Wrap(err, "creating merge directory")
		}
		if err := linkManifestAndFiles(basaltFS, thisLhsDir, lhsDir, merged); err != nil {
			return errors.Wrap(err, "linking merged files")
		}
	}
	// Seal pending registry entries so followers can read the encryption
	// metadata for the hardlinked merge files.
	if sealer, ok := basaltFS.(fs.RegistrySealer); ok {
		sealer.SealPendingRegistryEntries()
	}
	return nil
}

// enableCompactionsAfterMerge re-enables compactions on both LHS and RHS after
// merge completes or fails. This must be called regardless of success.
func (r *Replica) enableCompactionsAfterMerge(rhsRangeID roachpb.RangeID) {
	func() {
		r.rsStateMu.Lock()
		defer r.rsStateMu.Unlock()
		if r.rsStateMu.rsEngine != nil {
			r.rsStateMu.rsEngine.CompactionToggle(true)
		}
	}()
	if rhsRepl := r.store.GetReplicaIfExists(rhsRangeID); rhsRepl != nil {
		func() {
			rhsRepl.rsStateMu.Lock()
			defer rhsRepl.rsStateMu.Unlock()
			if rhsRepl.rsStateMu.rsEngine != nil {
				rhsRepl.rsStateMu.rsEngine.CompactionToggle(true)
			}
		}()
	}
}
