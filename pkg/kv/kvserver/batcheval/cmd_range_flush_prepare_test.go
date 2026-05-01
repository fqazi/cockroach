// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestRangeFlushPrepare(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	const rangeID = 12

	st := cluster.MakeTestingClusterSettings()
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	evalCtx := &MockEvalCtx{
		ClusterSettings: st,
		Desc:            &roachpb.RangeDescriptor{RangeID: rangeID},
	}
	sl := kvstorage.MakeStateLoader(rangeID)

	writeAppliedState := func(flushStartedCount uint64) {
		as := kvserverpb.RangeAppliedState{FlushStartedCount: flushStartedCount}
		require.NoError(t, storage.MVCCPutProto(ctx, eng,
			sl.RangeAppliedStateKey(), hlc.Timestamp{}, &as,
			storage.MVCCWriteOptions{}))
	}

	t.Run("default flush started count", func(t *testing.T) {
		writeAppliedState(0)
		req := &kvpb.RangeFlushPrepareRequest{}
		resp := &kvpb.RangeFlushPrepareResponse{}
		cArgs := CommandArgs{EvalCtx: evalCtx.EvalContext(), Args: req}
		res, err := RangeFlushPrepare(ctx, eng, cArgs, resp)
		require.NoError(t, err)
		require.Equal(t, uint64(1), resp.FlushStartedCount)
		require.True(t, res.Local.TakeStoreLocalSnapshot)
		require.True(t, res.Replicated.IncrementFlushStartedCount)
	})

	t.Run("non-zero flush started count", func(t *testing.T) {
		writeAppliedState(5)
		req := &kvpb.RangeFlushPrepareRequest{}
		resp := &kvpb.RangeFlushPrepareResponse{}
		cArgs := CommandArgs{EvalCtx: evalCtx.EvalContext(), Args: req}
		res, err := RangeFlushPrepare(ctx, eng, cArgs, resp)
		require.NoError(t, err)
		require.Equal(t, uint64(6), resp.FlushStartedCount)
		require.True(t, res.Local.TakeStoreLocalSnapshot)
		require.True(t, res.Replicated.IncrementFlushStartedCount)
	})

	t.Run("concurrent prepare invalidates prior prepare", func(t *testing.T) {
		// Two sequential prepares each increment FlushStartedCount. The first
		// prepare's returned value (6) won't match the state after the second
		// prepare applies (FlushStartedCount=7).
		writeAppliedState(5)
		resp1 := &kvpb.RangeFlushPrepareResponse{}
		cArgs1 := CommandArgs{EvalCtx: evalCtx.EvalContext(), Args: &kvpb.RangeFlushPrepareRequest{}}
		_, err := RangeFlushPrepare(ctx, eng, cArgs1, resp1)
		require.NoError(t, err)
		require.Equal(t, uint64(6), resp1.FlushStartedCount)
		// Simulate the first prepare's application incrementing the count.
		writeAppliedState(6)
		resp2 := &kvpb.RangeFlushPrepareResponse{}
		cArgs2 := CommandArgs{EvalCtx: evalCtx.EvalContext(), Args: &kvpb.RangeFlushPrepareRequest{}}
		_, err = RangeFlushPrepare(ctx, eng, cArgs2, resp2)
		require.NoError(t, err)
		require.Equal(t, uint64(7), resp2.FlushStartedCount)
		// First prepare's value (6) no longer matches the current state (7
		// after the second prepare applies). A flush commit with
		// ExpectedFlushStartedCount=6 would fail.
	})
}
