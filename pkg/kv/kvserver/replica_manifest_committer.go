// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/rangekey"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

// fileNumStash holds pre-allocated file numbers tied to a lease sequence. The
// stash represents the range [firstFileNum, endFileNum). leaseSeq is a lower
// bound on the lease used to acquire the files in the stash.
type fileNumStash struct {
	syncutil.Mutex
	leaseSeq     roachpb.LeaseSequence
	firstFileNum storage.DiskFileNum // inclusive
	endFileNum   storage.DiskFileNum // exclusive
}

// available returns the number of file numbers available in the stash.
func (s *fileNumStash) available() int {
	return int(s.endFileNum - s.firstFileNum)
}

// allocate allocates count file numbers from the stash.
//
// REQUIRES: s.available() >= count.
func (s *fileNumStash) allocate(count int) []storage.DiskFileNum {
	result := make([]storage.DiskFileNum, count)
	for i := 0; i < count; i++ {
		result[i] = s.firstFileNum + storage.DiskFileNum(i)
	}
	s.firstFileNum += storage.DiskFileNum(count)
	if s.firstFileNum > s.endFileNum {
		log.KvExec.Fatalf(context.Background(),
			"allocated more file numbers than available in stash: allocated up to %d, but end is %d",
			s.firstFileNum, s.endFileNum)
	}
	return result
}

// errManifestCommitterNotLeaseholder is returned by the
// ReplicaManifestCommitter when the replica is no longer the leaseholder. This
// can happen if the lease is transferred during a split or merge that exits a
// joint configuration. Callers should treat this as a retryable error.
var errManifestCommitterNotLeaseholder = errors.New("not the leaseholder")

// ReplicaManifestCommitter is a type alias for Replica that implements
// storage.ManifestChangeCommitter for the range-shared LSM.
type ReplicaManifestCommitter Replica

// replica returns the underlying Replica.
func (c *ReplicaManifestCommitter) replica() *Replica {
	return (*Replica)(c)
}

// GetFileNums allocates file numbers for the range-shared LSM.
// It implements the storage.ManifestChangeCommitter interface.
func (c *ReplicaManifestCommitter) GetFileNums(count int) ([]storage.DiskFileNum, error) {
	ctx := context.TODO()
	r := c.replica()
	leaseReplicaID, leaseSeq := c.currentLease()
	if leaseReplicaID != r.ReplicaID() {
		return nil, errManifestCommitterNotLeaseholder
	}
	r.rsFileNumStash.Lock()
	defer r.rsFileNumStash.Unlock()
	// Clear stash if lease changed.
	if r.rsFileNumStash.leaseSeq != leaseSeq {
		// It is possible that by the time we allocate filenums, the lease has been
		// lost and reacquired, and the allocated filenums are from the latest
		// lease, but we will believe they are from an old lease and discard them
		// from the stash in the future. This is safe. Safety relies on the return
		// value from GetFileNums being a contiguous sequence. This ensures that it
		// is not possible for:
		//
		// change-A gets: 10, 11, 15
		// change-B gets: 13, 14
		//
		// The hazard with such interleaved sequences would be if change-B (say a
		// compaction) committed first and then change-A (say the LHS of a split),
		// then we would no longer satisfy the manifest and associated filenum
		// invariant.
		//
		// In comparison, if we get:
		//
		// change-A gets: 10, 11, 12
		// change-B gets: 13, 14
		//
		// And change-B commit firsts, the change-A will be rejected since the
		// manifest-num must monotonically increase.
		r.rsFileNumStash.leaseSeq = leaseSeq
		r.rsFileNumStash.firstFileNum = 0
		r.rsFileNumStash.endFileNum = 0
	}
	// If we have enough cached, return from stash.
	if r.rsFileNumStash.available() >= count {
		return r.rsFileNumStash.allocate(count), nil
	}
	// Allocate more: requested count + 100 (to reduce Raft round trips).
	toAllocate := count + 100
	// Send AllocateFileNumsForRangeRequest. We are holding the mutex here, and
	// this could take long. It doesn't matter since all calls to GetFileNums
	// are serialized: flush calls are serialized by the RSEngine's opMu, and
	// compaction calls (via FileNumAllocator) are serialized by
	// Pebble's d.mu. Flushes and compactions don't overlap because flushes
	// drain all in-progress compactions before calling GetFileNums.
	first, end, err := c.allocateFromRaft(ctx, toAllocate)
	if err != nil {
		return nil, err
	}
	if r.rsFileNumStash.endFileNum == first {
		// Common case when the stash was not empty, but was insufficient, and
		// since the lease has not changed, the new allocation is contiguous with
		// the stash.
		r.rsFileNumStash.endFileNum = end
	} else {
		r.rsFileNumStash.firstFileNum = first
		r.rsFileNumStash.endFileNum = end
	}
	if r.rsFileNumStash.available() < count {
		log.KvExec.Fatalf(ctx, "allocated %d file numbers, but only %d available in stash",
			toAllocate, r.rsFileNumStash.available())
	}
	return r.rsFileNumStash.allocate(count), nil
}

// InstallNewManifest installs a new manifest file and new files referenced by
// the manifest. It implements the storage.ManifestChangeCommitter interface.
//
// The RSEngine has already hardlinked files from scratch to this replica's
// directory. This method:
//  1. Reads current RSManifestState and RangeDescriptor
//  2. Creates hardlinks for all files in OTHER replica directories
//  3. Sends a SetRangeSharedManifestNumRequest via Raft to atomically update
//     the manifest pointer across all replicas
//
// The expected_desc_generation check in the Raft command guards against
// replica/keyspan changes that would invalidate the hardlinks.
//
// NB: since we close and open RSEngines under the same Replica, it is
// possible that this call is happening from a RSEngine that is not the
// current one. This is fine since the currentManifestNum of an old RSEngine
// will be stale and the SetRangeSharedManifestNumRequest will fail.
// Additionally, it is possible that an old RSEngine that is waiting to be
// closed has called InstallNewManifest on a Replica that is also obsolete,
// since the replica has been removed. This is also ok, since the descriptor
// generation will be stale and the the SetRangeSharedManifestNumRequest will
// fail.
func (c *ReplicaManifestCommitter) InstallNewManifest(
	currentManifestNum storage.DiskFileNum,
	manifestInfo storage.ManifestInfo,
	ingestHandle interface{},
) error {
	if manifestInfo.Manifest.Num < currentManifestNum {
		// Alternatively, this could be in Pebble.
		return errors.Errorf("manifest number %d is less than current manifest number %d",
			manifestInfo.Manifest.Num, currentManifestNum)
	}
	ctx := context.TODO()
	r := c.replica()
	// r.Desc() returns a pointer to an immutable RangeDescriptor. When changes
	// happen (splits/merges/replica changes), the entire pointer is atomically
	// replaced with a new RangeDescriptor. The struct we receive won't change.
	desc := r.Desc()
	basaltFS := r.store.cfg.BasaltFS
	if basaltFS == nil {
		return errors.New("basaltFS is not configured")
	}
	// Create hardlinks for OTHER replicas. The RSEngine already created files
	// in this replica's directory.
	thisStoreID := r.store.StoreID()
	thisDir := BasaltDir(basaltFS, thisStoreID, r.RangeID, r.replicaID)
	for _, repl := range desc.Replicas().Descriptors() {
		if repl.StoreID == thisStoreID {
			continue // skip this replica - RSEngine already handled it
		}
		targetDir := BasaltDir(basaltFS, repl.StoreID, r.RangeID, repl.ReplicaID)
		if err := linkManifestAndFiles(basaltFS, thisDir, targetDir, manifestInfo); err != nil {
			if !oserror.IsNotExist(err) {
				return errors.Wrap(err, "linking manifest and files")
			}
			// Target directory doesn't exist yet — the replica was recently
			// added and the snapshot creating its directory hasn't been sent.
			// Create it and retry.
			if mkErr := basaltFS.MkdirAll(targetDir, 0755); mkErr != nil {
				return errors.Wrap(mkErr, "creating target directory")
			}
			if err := linkManifestAndFiles(basaltFS, thisDir, targetDir, manifestInfo); err != nil {
				return errors.Wrap(err, "linking manifest and files after mkdir")
			}
		}
	}
	// Seal any pending file registry entries so that followers can read
	// the encryption metadata for the hardlinked files. On basaltfs,
	// files are only visible to other nodes after sealing.
	if sealer, ok := basaltFS.(fs.RegistrySealer); ok {
		sealer.SealPendingRegistryEntries()
	}
	// Send SetRangeSharedManifestNumRequest via Raft.
	req := &kvpb.SetRangeSharedManifestNumRequest{
		RequestHeader: kvpb.RequestHeader{
			Key:    desc.StartKey.AsRawKey(),
			EndKey: desc.EndKey.AsRawKey(),
		},
		ExpectedManifestNum:    uint64(currentManifestNum),
		ExpectedDescGeneration: desc.Generation,
		NextManifestNum:        uint64(manifestInfo.Manifest.Num),
		ManifestName:           manifestInfo.Manifest.Name,
	}
	// Look up the MANIFEST's FileEntry from the encrypted FS and
	// include it in the Raft proposal. On shared storage, the sealed
	// registry file may not be visible to followers for many seconds
	// after sealing, but the Raft proposal arrives immediately. By
	// carrying the entry through Raft, followers can install it into
	// their local registry before opening the RSEngine.
	if fep, ok := basaltFS.(fs.FileEntryProvider); ok {
		manifestPath := basaltFS.PathJoin(thisDir, manifestInfo.Manifest.Name)
		if entry := fep.GetFileEntry(manifestPath); entry != nil {
			var err error
			req.ManifestFileEntry, err = protoutil.Marshal(entry)
			if err != nil {
				return errors.Wrap(err, "marshaling manifest file entry")
			}
		}
	}
	if ingestHandle != nil {
		flushCommit := ingestHandle.(*storage.FlushCommitInfo)
		if flushCommit != nil {
			req.IsFlushCommit = true
			req.ExpectedFlushStartedCount = flushCommit.ExpectedFlushStartedCount
			req.ActivateSpans = flushCommit.ActivateSpans
			req.FlushedApproxStoreLocalBytes = flushCommit.FlushedApproxStoreLocalBytes
			// Verify each ActivateSpan is within the range bounds.
			for _, sp := range flushCommit.ActivateSpans {
				if sp.Key.Compare(desc.StartKey.AsRawKey()) < 0 ||
					sp.EndKey.Compare(desc.EndKey.AsRawKey()) > 0 {
					return errors.Errorf(
						"ActivateSpan %s outside range bounds [%s, %s)",
						sp, desc.StartKey, desc.EndKey)
				}
			}
		}
	}
	_, pErr := kv.SendWrappedWith(
		ctx,
		r.store.DB().NonTransactionalSender(),
		kvpb.Header{RangeID: r.RangeID},
		req,
	)
	if pErr != nil {
		return pErr.GoError()
	}
	return nil
}

// currentLease returns the current lease holder's replica ID and sequence.
func (c *ReplicaManifestCommitter) currentLease() (roachpb.ReplicaID, roachpb.LeaseSequence) {
	r := c.replica()
	r.mu.RLock()
	defer r.mu.RUnlock()
	lease := r.shMu.state.Lease
	return lease.Replica.ReplicaID, lease.Sequence
}

// allocateFromRaft sends an AllocateFileNumsForRangeRequest through Raft.
func (c *ReplicaManifestCommitter) allocateFromRaft(
	ctx context.Context, count int,
) (first, end storage.DiskFileNum, err error) {
	r := c.replica()
	desc := r.Desc()
	req := &kvpb.AllocateFileNumsForRangeRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: desc.StartKey.AsRawKey(),
		},
		Count: uint32(count),
	}
	resp, pErr := kv.SendWrappedWith(
		ctx,
		r.store.DB().NonTransactionalSender(),
		kvpb.Header{RangeID: r.RangeID},
		req,
	)
	if pErr != nil {
		return 0, 0, pErr.GoError()
	}
	allocResp := resp.(*kvpb.AllocateFileNumsForRangeResponse)
	return storage.DiskFileNum(allocResp.FirstFileNum),
		storage.DiskFileNum(allocResp.EndFileNum), nil
}

// ManifestCommitter returns the replica as a ManifestCommitter, which
// implements storage.ManifestChangeCommitter for the range-shared LSM.
func (r *Replica) ManifestCommitter() *ReplicaManifestCommitter {
	return (*ReplicaManifestCommitter)(r)
}

// Verify that ReplicaManifestCommitter implements storage.ManifestChangeCommitter.
var _ storage.ManifestChangeCommitter = (*ReplicaManifestCommitter)(nil)

// RangeFlush performs a range flush, moving data from the store-local engine
// to the range-shared engine. Must be called on the leaseholder.
//
// Uses r.AnnotateCtx(context.Background()) internally — does not accept an
// external context. The context must not be cancelled except on Replica
// removal.
//
// Cancellation hazard: if Flush A is cancelled and Flush B starts before
// Flush A's Raft command applies, prepareLocalResult sees ongoingFlush=true
// (from B) and stores Flush A's snapshot. Flush B would pick up a stale
// snapshot. Restricting cancellation to Replica removal prevents this since
// no Flush B can start on a removed Replica. The flushStartedCount
// validation (see rangeFlushMu comment) provides defense-in-depth for
// this hazard.
func (c *ReplicaManifestCommitter) RangeFlush() error {
	r := c.replica()
	ctx := r.AnnotateCtx(context.Background())
	// Check lease.
	leaseReplicaID, _ := c.currentLease()
	if leaseReplicaID != r.ReplicaID() {
		return errManifestCommitterNotLeaseholder
	}
	// Acquire rangeFlushMu, set ongoingFlush, and capture a unique flush
	// counter for the scratch filename.
	flushCount, err := func() (uint64, error) {
		r.rangeFlushMu.Lock()
		defer r.rangeFlushMu.Unlock()
		if r.rangeFlushMu.ongoingFlush {
			return 0, errors.New("range flush already in progress")
		}
		r.rangeFlushMu.ongoingFlush = true
		r.rangeFlushMu.flushCount++
		return r.rangeFlushMu.flushCount, nil
	}()
	if err != nil {
		return err
	}
	defer func() {
		r.rangeFlushMu.Lock()
		defer r.rangeFlushMu.Unlock()
		if r.rangeFlushMu.snapshot != nil {
			r.rangeFlushMu.snapshot.Close()
			r.rangeFlushMu.snapshot = nil
		}
		r.rangeFlushMu.flushStartedCount = 0
		r.rangeFlushMu.ongoingFlush = false
	}()
	// Send RangeFlushPrepareRequest. Save the descriptor used for prepare so
	// we can validate the span hasn't changed before committing.
	prepareDesc := r.Desc()
	// Compute dormant/activate spans if the testing knob is set. These
	// spans are used for ClearRawRangeDormant (prepare) and
	// ClearRawRangeActivate (commit).
	var clearSpans []roachpb.Span
	if r.store.TestingKnobs().WriteClearRangeOnFlush {
		clearSpans = rditer.Select(prepareDesc.RangeID, rditer.SelectOpts{
			Ranged: rditer.SelectRangedOptions{
				RSpan:    prepareDesc.RSpan(),
				UserKeys: true,
			},
		})
	}
	prepReq := &kvpb.RangeFlushPrepareRequest{
		RequestHeader: kvpb.RequestHeader{
			Key:    prepareDesc.StartKey.AsRawKey(),
			EndKey: prepareDesc.EndKey.AsRawKey(),
		},
		DormantSpans: clearSpans,
	}
	prepResp, pErr := kv.SendWrappedWith(
		ctx,
		r.store.DB().NonTransactionalSender(),
		kvpb.Header{RangeID: r.RangeID},
		prepReq,
	)
	if pErr != nil {
		return pErr.GoError()
	}
	flushPrepResp := prepResp.(*kvpb.RangeFlushPrepareResponse)
	// Pick up snapshot, flushStartedCount, and approxStoreLocalBytes set
	// by prepareLocalResult.
	r.rangeFlushMu.Lock()
	snap := r.rangeFlushMu.snapshot
	r.rangeFlushMu.snapshot = nil
	storedFlushStartedCount := r.rangeFlushMu.flushStartedCount
	flushedApproxStoreLocalBytes := r.rangeFlushMu.approxStoreLocalBytes
	r.rangeFlushMu.Unlock()
	if snap == nil {
		return errors.New("range flush snapshot not available")
	}
	defer snap.Close()
	if storedFlushStartedCount != flushPrepResp.FlushStartedCount {
		return errors.Errorf(
			"range flush snapshot from wrong FlushPrepare: stored FlushStartedCount %d, expected %d",
			storedFlushStartedCount, flushPrepResp.FlushStartedCount,
		)
	}
	basaltFS := r.store.cfg.BasaltFS
	if basaltFS == nil {
		return errors.New("basaltFS not configured")
	}
	// Create SST file in scratch directory with a unique name.
	scratchDir := BasaltScratchDir(basaltFS, r.store.StoreID(), r.RangeID, r.replicaID)
	sstName := fmt.Sprintf("flush-%d-%d.sst", r.RangeID, flushCount)
	sstPath := basaltFS.PathJoin(scratchDir, sstName)
	f, err := basaltFS.Create(sstPath, vfs.WriteCategoryUnspecified)
	if err != nil {
		return errors.Wrap(err, "creating flush SST file")
	}
	writable := objstorageprovider.NewFileWritable(f)
	sstWriter := storage.MakeSSTWriter(writable, sstable.WriterOptions{
		Comparer:    &storage.EngineComparer,
		KeySchema:   storage.KeySchemas[0],
		TableFormat: pebble.FormatNewest.MaxTableFormat(),
	})
	defer sstWriter.Close()
	// Iterate snapshot and write to SST.
	err = rditer.IterateReplicaKeySpansShared(
		ctx,
		prepareDesc,
		r.store.ClusterSettings(),
		uuid.UUID{}, // unused
		snap,
		func(key *pebble.InternalKey, val pebble.LazyValue, _ pebble.IteratorLevel, _ pebble.DormantRelation) error {
			v, _, err := val.Value(nil)
			if err != nil {
				return err
			}
			return sstWriter.PutInternalPointKey(key, v)
		},
		func(start, end []byte, _ pebble.SeqNum, _ pebble.DormantRelation) error {
			return sstWriter.ClearRawEncodedRange(start, end)
		},
		// NB: since visitDormantRangeDel is nil, at most one point is exposed for
		// each user-key.
		nil, /* visitDormantRangeDel */
		func(_, _ []byte, _ []rangekey.Key) error {
			return nil // don't flush range keys
		},
		nil, // no shared files in store-local snapshot
		nil, // no external files in store-local snapshot
	)
	if err != nil {
		return errors.Wrap(err, "iterating snapshot")
	}
	if err := sstWriter.Finish(); err != nil {
		return errors.Wrap(err, "finishing flush SST")
	}
	// Acquire the RSEngine just before FlushSSTables and hold a Ref to
	// prevent it from being closed while we're using it. If no RSEngine
	// The container is always created during replica init when basaltFS is
	// configured. No lazy-open needed.
	rsEngine := r.rsStateMu.rsEngine
	if rsEngine == nil {
		return errors.New("rsEngine not configured")
	}
	rsEngine.Ref()
	defer rsEngine.Unref()
	// Validate that the range span hasn't changed since the prepare request.
	// The range could have split and merged many times since the
	// RANGEDEL_OFF was written, but if the span is again the same, the
	// SSTable is a valid representation of what should be flushed.
	currentDesc := r.Desc()
	if !prepareDesc.StartKey.Equal(currentDesc.StartKey) ||
		!prepareDesc.EndKey.Equal(currentDesc.EndKey) {
		return errors.New("range span changed since flush prepare")
	}
	flushCommit := storage.FlushCommitInfo{
		ExpectedFlushStartedCount:    flushPrepResp.FlushStartedCount,
		ActivateSpans:                clearSpans,
		FlushedApproxStoreLocalBytes: flushedApproxStoreLocalBytes,
	}
	flushErr := rsEngine.FlushSSTables([]string{sstName}, &flushCommit)
	if flushErr != nil {
		// On a successful flush the engine consumes the scratch SST. Clean it
		// up here only on the error path.
		if err := basaltFS.Remove(sstPath); err != nil {
			log.Ops.Warningf(ctx, "removing scratch file %s after flush error: %v", sstPath, err)
		}
	}
	return flushErr
}
