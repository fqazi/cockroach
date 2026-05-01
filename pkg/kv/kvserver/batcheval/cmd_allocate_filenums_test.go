// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
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

func TestAllocateFileNumsForRange(t *testing.T) {
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

	key := keys.RangeFileNumAllocKey(rangeID)

	t.Run("uninitialized state fails", func(t *testing.T) {
		req := &kvpb.AllocateFileNumsForRangeRequest{Count: 5}
		resp := &kvpb.AllocateFileNumsForRangeResponse{}
		cArgs := CommandArgs{EvalCtx: evalCtx.EvalContext(), Args: req}
		_, err := AllocateFileNumsForRange(ctx, eng, cArgs, resp)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not initialized")
	})

	// Initialize state.
	initState := kvserverpb.RangeFileNumAllocState{NextFileNum: kvstorage.InitialRangeFileNum}
	require.NoError(t, storage.MVCCPutProto(ctx, eng, key, hlc.Timestamp{}, &initState,
		storage.MVCCWriteOptions{}))

	t.Run("first allocation", func(t *testing.T) {
		req := &kvpb.AllocateFileNumsForRangeRequest{Count: 5}
		resp := &kvpb.AllocateFileNumsForRangeResponse{}
		cArgs := CommandArgs{EvalCtx: evalCtx.EvalContext(), Args: req}
		_, err := AllocateFileNumsForRange(ctx, eng, cArgs, resp)
		require.NoError(t, err)
		require.Equal(t, uint64(7), resp.FirstFileNum)
		require.Equal(t, uint64(12), resp.EndFileNum)
	})

	t.Run("subsequent allocation", func(t *testing.T) {
		req := &kvpb.AllocateFileNumsForRangeRequest{Count: 3}
		resp := &kvpb.AllocateFileNumsForRangeResponse{}
		cArgs := CommandArgs{EvalCtx: evalCtx.EvalContext(), Args: req}
		_, err := AllocateFileNumsForRange(ctx, eng, cArgs, resp)
		require.NoError(t, err)
		require.Equal(t, uint64(12), resp.FirstFileNum)
		require.Equal(t, uint64(15), resp.EndFileNum)
	})

	t.Run("state persistence", func(t *testing.T) {
		var state kvserverpb.RangeFileNumAllocState
		_, err := storage.MVCCGetProto(ctx, eng, key, hlc.Timestamp{}, &state,
			storage.MVCCGetOptions{})
		require.NoError(t, err)
		require.Equal(t, uint64(15), state.NextFileNum)
	})
}

func TestAllocateFileNumsForRange_ZeroCount(t *testing.T) {
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

	key := keys.RangeFileNumAllocKey(rangeID)

	// Initialize state.
	initState := kvserverpb.RangeFileNumAllocState{NextFileNum: kvstorage.InitialRangeFileNum}
	require.NoError(t, storage.MVCCPutProto(ctx, eng, key, hlc.Timestamp{}, &initState,
		storage.MVCCWriteOptions{}))

	// Allocate zero file numbers.
	req := &kvpb.AllocateFileNumsForRangeRequest{Count: 0}
	resp := &kvpb.AllocateFileNumsForRangeResponse{}
	cArgs := CommandArgs{EvalCtx: evalCtx.EvalContext(), Args: req}
	_, err := AllocateFileNumsForRange(ctx, eng, cArgs, resp)
	require.NoError(t, err)
	require.Equal(t, uint64(7), resp.FirstFileNum)
	require.Equal(t, uint64(7), resp.EndFileNum)

	// State should remain unchanged.
	var state kvserverpb.RangeFileNumAllocState
	_, err = storage.MVCCGetProto(ctx, eng, key, hlc.Timestamp{}, &state,
		storage.MVCCGetOptions{})
	require.NoError(t, err)
	require.Equal(t, uint64(7), state.NextFileNum)
}

func TestAllocateFileNumsForRange_LargeCount(t *testing.T) {
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

	key := keys.RangeFileNumAllocKey(rangeID)

	// Initialize state.
	initState := kvserverpb.RangeFileNumAllocState{NextFileNum: kvstorage.InitialRangeFileNum}
	require.NoError(t, storage.MVCCPutProto(ctx, eng, key, hlc.Timestamp{}, &initState,
		storage.MVCCWriteOptions{}))

	// Allocate a large number of file numbers.
	req := &kvpb.AllocateFileNumsForRangeRequest{Count: 1000}
	resp := &kvpb.AllocateFileNumsForRangeResponse{}
	cArgs := CommandArgs{EvalCtx: evalCtx.EvalContext(), Args: req}
	_, err := AllocateFileNumsForRange(ctx, eng, cArgs, resp)
	require.NoError(t, err)
	require.Equal(t, uint64(7), resp.FirstFileNum)
	require.Equal(t, uint64(1007), resp.EndFileNum)

	// State should be updated.
	var state kvserverpb.RangeFileNumAllocState
	_, err = storage.MVCCGetProto(ctx, eng, key, hlc.Timestamp{}, &state,
		storage.MVCCGetOptions{})
	require.NoError(t, err)
	require.Equal(t, uint64(1007), state.NextFileNum)
}
