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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/lockspanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// Manifest installation needs to ensure that the range span, range replicas,
// and latest manifest have not changed since the caller computed the new
// manifest (as the successor to the latest manifest) and hardlinked it at all
// replicas. This is accomplished by a combination of concurrency control and
// confirmation of assumptions.
//
// Concurrency control: All manifest installation serializes on the
// RangeDescriptor. Non-transactional updates (range flush and compactions)
// write-latch the RangeDescriptor during evaluation of
// SetRangeSharedManifestNumRequest. Transactional changes are splits and
// merges: these already acquire a lock on the RangeDescriptor by writing a
// new RangeDescriptor.
//
// Confirmation of assumptions:
//   - SetRangeSharedManifestNumRequest confirms there is no provisional
//     RangeDescriptor and the committed RangeDescriptor has the expected
//     Generation. It also confirms that the current manifest num is the
//     expected one.
//   - The 2PC transactions, after locking the RangeDescriptor, use
//     CheckRangeSharedManifestNumRequest to confirm that the generation and
//     current manifest num are the expected ones. Note that they don't look
//     at the provisional value, since they have already installed one. They
//     use EvalCtx.Desc(), which is the current committed descriptor.

// SkipDescGenerationCheck is a sentinel value for
// CheckRangeSharedManifestNumRequest.ExpectedDescGeneration that skips the
// descriptor generation check.
const SkipDescGenerationCheck roachpb.RangeGeneration = -1

func init() {
	RegisterReadOnlyCommand(
		kvpb.CheckRangeSharedManifestNum, declareKeysCheckRangeSharedManifestNum, CheckRangeSharedManifestNum)
}

func declareKeysCheckRangeSharedManifestNum(
	rs ImmutableRangeState,
	_ *kvpb.Header,
	_ kvpb.Request,
	latchSpans *spanset.SpanSet,
	_ *lockspanset.LockSpanSet,
	_ time.Duration,
) error {
	// Read-only latch on the RangeDescriptorKey. Since this is called within
	// a 2PC transaction that already holds a write lock on the descriptor,
	// this latch primarily ensures proper ordering with other operations.
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{
		Key: keys.RangeDescriptorKey(rs.GetStartKey()),
	})
	return nil
}

// CheckRangeSharedManifestNum verifies that the range-shared manifest number
// and descriptor generation match expected values. This is used by split/merge
// transactions after they have locked the RangeDescriptor to confirm that no
// concurrent manifest changes have occurred since manifest preparation.
//
// Unlike SetRangeSharedManifestNum, this command uses EvalCtx.Desc() to get
// the committed descriptor rather than reading from storage. This is correct
// because the calling transaction has already written a provisional descriptor
// (locking the key), so a storage read would see its own intent. We want to
// verify the committed state matches what was observed during preparation.
func CheckRangeSharedManifestNum(
	ctx context.Context, reader storage.Reader, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.CheckRangeSharedManifestNumRequest)
	rangeID := cArgs.EvalCtx.GetRangeID()

	// Read current RSManifestState.
	rsManifestKey := keys.RangeSharedManifestNumKey(rangeID)
	var rsState kvserverpb.RSManifestState
	if _, err := storage.MVCCGetProto(
		ctx, reader, rsManifestKey, hlc.Timestamp{}, &rsState, storage.MVCCGetOptions{
			ReadCategory: fs.BatchEvalReadCategory,
		},
	); err != nil {
		return result.Result{}, errors.Wrap(err, "reading RSManifestState")
	}
	// Check expected manifest number.
	if rsState.DiskFileNum != args.ExpectedManifestNum {
		return result.Result{}, errors.Errorf(
			"manifest number mismatch: expected %d, found %d",
			args.ExpectedManifestNum, rsState.DiskFileNum)
	}
	// Check descriptor generation using EvalCtx.Desc() - the committed
	// descriptor. The calling transaction has already written a provisional
	// descriptor, so we intentionally check the committed value to verify
	// nothing changed between manifest preparation and locking.
	if args.ExpectedDescGeneration != SkipDescGenerationCheck {
		desc := cArgs.EvalCtx.Desc()
		if desc.Generation != args.ExpectedDescGeneration {
			return result.Result{}, errors.Errorf(
				"descriptor generation mismatch: expected %d, found %d (range may have split/merged)",
				args.ExpectedDescGeneration, desc.Generation)
		}
	}
	return result.Result{}, nil
}
