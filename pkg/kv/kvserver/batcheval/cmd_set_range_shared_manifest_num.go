// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/lockspanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

func init() {
	RegisterReadWriteCommand(kvpb.SetRangeSharedManifestNum, declareKeysSetRangeSharedManifestNum, SetRangeSharedManifestNum)
}

func declareKeysSetRangeSharedManifestNum(
	rs ImmutableRangeState,
	_ *kvpb.Header,
	_ kvpb.Request,
	latchSpans *spanset.SpanSet,
	lockSpans *lockspanset.LockSpanSet,
	_ time.Duration,
) error {
	descSpan := roachpb.Span{Key: keys.RangeDescriptorKey(rs.GetStartKey())}
	// Latch on the RangeDescriptorKey, the same key that splits/merges latch on.
	// This ensures mutual exclusion with splits and merges.
	latchSpans.AddNonMVCC(spanset.SpanReadWrite, descSpan)
	// Declare a lock span so the concurrency manager creates a lockTableGuard.
	// The command reads the RangeDescriptor via MVCCGetProto, which can encounter
	// a LockConflictError from a concurrent split/merge intent. Without this
	// declaration, HandleLockConflictError fatals due to a missing guard.
	lockSpans.Add(lock.None, descSpan)
	return nil
}

// SetRangeSharedManifestNum updates the range-shared manifest number. This
// command is used by InstallNewManifest to atomically switch to a new manifest
// after hardlinking files to all replica directories.
//
// The command performs the following checks before updating:
//   - The expected manifest number must match the current RSManifestState
//   - The expected descriptor generation must match the current RangeDescriptor
//
// These checks ensure that no split/merge/replica changes have occurred since
// the hardlinks were created. Importantly, this command reads the descriptor
// from storage (not just in-memory) to detect concurrent split/merge
// transactions that have written a provisional descriptor (intent) but haven't
// committed yet. A consistent read will return WriteIntentError if there's an
// uncommitted intent, causing this request to return an error.
func SetRangeSharedManifestNum(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.SetRangeSharedManifestNumRequest)
	rangeID := cArgs.EvalCtx.GetRangeID()

	// Read current RSManifestState.
	rsManifestKey := keys.RangeSharedManifestNumKey(rangeID)
	var rsState kvserverpb.RSManifestState
	if _, err := storage.MVCCGetProto(
		ctx, readWriter, rsManifestKey, hlc.Timestamp{}, &rsState, storage.MVCCGetOptions{
			ReadCategory: fs.BatchEvalReadCategory,
		},
	); err != nil {
		return result.Result{}, errors.Wrap(err, "reading RSManifestState")
	}
	// Check expected manifest number. A value of 0 in the stored state means
	// this is the first manifest for the range.
	if rsState.DiskFileNum != args.ExpectedManifestNum {
		return result.Result{}, errors.Errorf(
			"manifest number mismatch: expected %d, found %d",
			args.ExpectedManifestNum, rsState.DiskFileNum)
	}
	// Read RangeDescriptor from storage using a consistent MVCC read. This is
	// critical for detecting concurrent split/merge transactions: if another
	// transaction has written a provisional descriptor (intent), this read will
	// return WriteIntentError, causing this request to return an error. Using
	// EvalCtx.Desc() would only see the committed descriptor and miss the
	// concurrent transaction.
	descKey := keys.RangeDescriptorKey(cArgs.EvalCtx.Desc().StartKey)
	var desc roachpb.RangeDescriptor
	if _, err := storage.MVCCGetProto(
		ctx, readWriter, descKey, cArgs.Header.Timestamp, &desc, storage.MVCCGetOptions{
			ReadCategory: fs.BatchEvalReadCategory,
		},
	); err != nil {
		return result.Result{}, errors.Wrap(err, "reading RangeDescriptor")
	}
	if desc.Generation != args.ExpectedDescGeneration {
		return result.Result{}, errors.Errorf(
			"descriptor generation mismatch: expected %d, found %d (range may have split/merged)",
			args.ExpectedDescGeneration, desc.Generation)
	}

	// When this is a flush commit, verify FlushStartedCount matches the value
	// returned by the corresponding flush prepare. If another prepare has
	// intervened, the count will have been incremented and this commit must fail.
	if args.IsFlushCommit {
		sl := kvstorage.MakeStateLoader(rangeID)
		as, err := sl.LoadRangeAppliedState(ctx, readWriter)
		if err != nil {
			return result.Result{}, errors.Wrap(err, "loading RangeAppliedState")
		}
		if as.FlushStartedCount != args.ExpectedFlushStartedCount {
			return result.Result{}, errors.Errorf(
				"flush started count mismatch: expected %d, found %d",
				args.ExpectedFlushStartedCount, as.FlushStartedCount)
		}
	}

	for _, sp := range args.ActivateSpans {
		if err := readWriter.ClearRawRangeActivate(sp.Key, sp.EndKey); err != nil {
			return result.Result{}, err
		}
	}

	// Set up the replicated result to install the new manifest. The actual
	// RSManifestState write happens during application in runPostAddTriggersReplicaOnly,
	// since each replica writes its own replicaID to the state.
	var res result.Result
	rsInstall := &kvserverpb.RSManifestInstall{
		NextManifestNum:   args.NextManifestNum,
		ManifestFileEntry: args.ManifestFileEntry,
		ManifestName:      args.ManifestName,
	}
	if args.IsFlushCommit {
		rsInstall.FlushedApproxStoreLocalBytes = args.FlushedApproxStoreLocalBytes
	}
	// No IncrementFlushStartedCount here — that's handled by flush prepare.
	res.Replicated.RSManifestInstall = rsInstall

	return res, nil
}
