// Copyright 2024 The Cockroach Authors.
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

func init() {
	RegisterReadWriteCommand(kvpb.AllocateFileNumsForRange, declareKeysAllocateFileNums, AllocateFileNumsForRange)
}

func declareKeysAllocateFileNums(
	rs ImmutableRangeState,
	_ *kvpb.Header,
	_ kvpb.Request,
	latchSpans *spanset.SpanSet,
	_ *lockspanset.LockSpanSet,
	_ time.Duration,
) error {
	latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{
		Key: keys.RangeFileNumAllocKey(rs.GetRangeID()),
	})
	return nil
}

// AllocateFileNumsForRange allocates file numbers for the range-shared LSM.
// The allocated file numbers can be used for MANIFESTs and SSTables in the
// RSEngine. This command reads and updates the RangeFileNumAllocState stored
// at a replicated range-ID local key.
func AllocateFileNumsForRange(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.AllocateFileNumsForRangeRequest)
	reply := resp.(*kvpb.AllocateFileNumsForRangeResponse)
	rangeID := cArgs.EvalCtx.GetRangeID()

	key := keys.RangeFileNumAllocKey(rangeID)

	// Read current state.
	var state kvserverpb.RangeFileNumAllocState
	if _, err := storage.MVCCGetProto(
		ctx, readWriter, key, hlc.Timestamp{}, &state, storage.MVCCGetOptions{
			ReadCategory: fs.BatchEvalReadCategory,
		},
	); err != nil {
		return result.Result{}, errors.Wrap(err, "reading RangeFileNumAllocState")
	}

	// The state should have been initialized during range creation.
	if state.NextFileNum == 0 {
		return result.Result{}, errors.AssertionFailedf(
			"RangeFileNumAllocState not initialized for range %d", rangeID)
	}

	// Allocate numbers as [first, end).
	reply.FirstFileNum = state.NextFileNum
	reply.EndFileNum = state.NextFileNum + uint64(args.Count)
	state.NextFileNum = reply.EndFileNum

	// Write updated state.
	if err := storage.MVCCPutProto(
		ctx, readWriter, key, hlc.Timestamp{}, &state,
		storage.MVCCWriteOptions{Stats: cArgs.Stats, Category: fs.BatchEvalReadCategory},
	); err != nil {
		return result.Result{}, errors.Wrap(err, "writing RangeFileNumAllocState")
	}

	return result.Result{}, nil
}
