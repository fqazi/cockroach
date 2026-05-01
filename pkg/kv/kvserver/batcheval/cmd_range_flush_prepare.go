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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/lockspanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/errors"
)

func init() {
	RegisterReadWriteCommand(kvpb.RangeFlushPrepare, declareKeysRangeFlushPrepare, RangeFlushPrepare)
}

func declareKeysRangeFlushPrepare(
	rs ImmutableRangeState,
	_ *kvpb.Header,
	_ kvpb.Request,
	latchSpans *spanset.SpanSet,
	_ *lockspanset.LockSpanSet,
	_ time.Duration,
) error {
	// Latch on RangeDescriptorKey to serialize with flush commit
	// (SetRangeSharedManifestNum) and with splits/merges. This is necessary
	// for the FlushStartedCount protocol and the dormant/activate protocol.
	//
	// FlushStartedCount hazard: the prepare increments FlushStartedCount
	// (via IncrementFlushStartedCount in the replicated result), and the
	// corresponding commit verifies the count hasn't changed. Without
	// latching, a second prepare could be proposed to Raft after the
	// commit evaluates (passing the count check) but before it applies.
	// Since Raft applies commands in log order, the second prepare would
	// apply first, incrementing the count. The commit would then apply
	// with a stale count check, proceeding when it should have failed.
	//
	// Premature activation hazard: if two prepares write dormant clears
	// at different seqnums (S1 and S2, where S2 > S1), a commit that
	// was built from S1's snapshot would write ClearRawRangeActivate,
	// which activates ALL overlapping dormants — including S2's. This
	// deletes data between S1 and S2 from the store-local engine. That
	// data is not in S1's SST (snapshot predates it) and S2's flush
	// hasn't committed yet, so the data is lost.
	//
	// The latch serializes prepare and commit evaluations on the same
	// leaseholder. Since proposals enter the Raft log in evaluation
	// order (on the same proposer), the application order matches,
	// preventing a prepare from slipping between a commit's evaluation
	// and application.
	//
	// Note: the prepare is also isAlone, which is required because the
	// snapshot is taken in prepareLocalResult after the batch applies.
	// If other requests followed the prepare in the same batch, the
	// snapshot would include their writes — data that the dormant
	// clears don't cover, breaking the flush boundary invariant.
	latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{
		Key: keys.RangeDescriptorKey(rs.GetStartKey()),
	})
	return nil
}

// RangeFlushPrepare increments FlushStartedCount in RangeAppliedState and
// signals prepareLocalResult to take a store-local snapshot.
func RangeFlushPrepare(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.RangeFlushPrepareRequest)
	reply := resp.(*kvpb.RangeFlushPrepareResponse)
	rangeID := cArgs.EvalCtx.GetRangeID()
	sl := kvstorage.MakeStateLoader(rangeID)
	as, err := sl.LoadRangeAppliedState(ctx, readWriter)
	if err != nil {
		return result.Result{}, errors.Wrap(err, "loading RangeAppliedState")
	}
	reply.FlushStartedCount = as.FlushStartedCount + 1
	for _, sp := range args.DormantSpans {
		if err := readWriter.ClearRawRangeDormant(sp.Key, sp.EndKey); err != nil {
			return result.Result{}, err
		}
	}
	var res result.Result
	res.Local.TakeStoreLocalSnapshot = true
	res.Replicated.IncrementFlushStartedCount = true
	return res, nil
}
