// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
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

func TestSetRangeSharedManifestNum(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	const rangeID = 12
	const generation = roachpb.RangeGeneration(5)

	st := cluster.MakeTestingClusterSettings()
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	desc := &roachpb.RangeDescriptor{
		RangeID:    rangeID,
		StartKey:   roachpb.RKey("a"),
		EndKey:     roachpb.RKey("z"),
		Generation: generation,
	}
	evalCtx := &MockEvalCtx{
		ClusterSettings: st,
		Desc:            desc,
	}
	rsManifestKey := keys.RangeSharedManifestNumKey(rangeID)
	descKey := keys.RangeDescriptorKey(desc.StartKey)
	evalTS := hlc.Timestamp{WallTime: 100}

	// Write the RangeDescriptor to storage.
	require.NoError(t, storage.MVCCPutProto(ctx, eng, descKey, hlc.Timestamp{WallTime: 10},
		desc, storage.MVCCWriteOptions{}))

	t.Run("first manifest with no prior state", func(t *testing.T) {
		// No RSManifestState exists yet (DiskFileNum defaults to 0).
		req := &kvpb.SetRangeSharedManifestNumRequest{
			ExpectedManifestNum:    0,
			ExpectedDescGeneration: generation,
			NextManifestNum:        7,
		}
		resp := &kvpb.SetRangeSharedManifestNumResponse{}
		cArgs := CommandArgs{
			EvalCtx: evalCtx.EvalContext(),
			Args:    req,
			Header:  kvpb.Header{Timestamp: evalTS},
		}
		result, err := SetRangeSharedManifestNum(ctx, eng, cArgs, resp)
		require.NoError(t, err)
		require.NotNil(t, result.Replicated.RSManifestInstall)
		require.Equal(t, uint64(7), result.Replicated.RSManifestInstall.NextManifestNum)
	})

	// Initialize RSManifestState with manifest number 10.
	initState := kvserverpb.RSManifestState{DiskFileNum: 10, ReplicaId: 1}
	require.NoError(t, storage.MVCCBlindPutProto(ctx, eng, rsManifestKey, hlc.Timestamp{},
		&initState, storage.MVCCWriteOptions{}))

	t.Run("success with matching expected values", func(t *testing.T) {
		req := &kvpb.SetRangeSharedManifestNumRequest{
			ExpectedManifestNum:    10,
			ExpectedDescGeneration: generation,
			NextManifestNum:        15,
		}
		resp := &kvpb.SetRangeSharedManifestNumResponse{}
		cArgs := CommandArgs{
			EvalCtx: evalCtx.EvalContext(),
			Args:    req,
			Header:  kvpb.Header{Timestamp: evalTS},
		}
		result, err := SetRangeSharedManifestNum(ctx, eng, cArgs, resp)
		require.NoError(t, err)
		require.NotNil(t, result.Replicated.RSManifestInstall)
		require.Equal(t, uint64(15), result.Replicated.RSManifestInstall.NextManifestNum)
	})

	t.Run("failure on manifest num mismatch", func(t *testing.T) {
		req := &kvpb.SetRangeSharedManifestNumRequest{
			ExpectedManifestNum:    99, // wrong
			ExpectedDescGeneration: generation,
			NextManifestNum:        20,
		}
		resp := &kvpb.SetRangeSharedManifestNumResponse{}
		cArgs := CommandArgs{
			EvalCtx: evalCtx.EvalContext(),
			Args:    req,
			Header:  kvpb.Header{Timestamp: evalTS},
		}
		_, err := SetRangeSharedManifestNum(ctx, eng, cArgs, resp)
		require.Error(t, err)
		require.Contains(t, err.Error(), "manifest number mismatch")
	})

	t.Run("failure on generation mismatch", func(t *testing.T) {
		req := &kvpb.SetRangeSharedManifestNumRequest{
			ExpectedManifestNum:    10,
			ExpectedDescGeneration: 99, // wrong
			NextManifestNum:        20,
		}
		resp := &kvpb.SetRangeSharedManifestNumResponse{}
		cArgs := CommandArgs{
			EvalCtx: evalCtx.EvalContext(),
			Args:    req,
			Header:  kvpb.Header{Timestamp: evalTS},
		}
		_, err := SetRangeSharedManifestNum(ctx, eng, cArgs, resp)
		require.Error(t, err)
		require.Contains(t, err.Error(), "descriptor generation mismatch")
	})

	t.Run("detects concurrent split/merge intent", func(t *testing.T) {
		// Simulate a concurrent split/merge by writing a provisional descriptor
		// (intent) with a different generation.
		txn := roachpb.MakeTransaction(
			"concurrent-split", nil, isolation.Serializable, roachpb.NormalUserPriority,
			hlc.Timestamp{WallTime: 50}, 0, 1, 0, false,
		)
		newDesc := *desc
		newDesc.Generation = generation + 1
		require.NoError(t, storage.MVCCPutProto(ctx, eng, descKey, txn.ReadTimestamp,
			&newDesc, storage.MVCCWriteOptions{Txn: &txn}))
		req := &kvpb.SetRangeSharedManifestNumRequest{
			ExpectedManifestNum:    10,
			ExpectedDescGeneration: generation,
			NextManifestNum:        25,
		}
		resp := &kvpb.SetRangeSharedManifestNumResponse{}
		cArgs := CommandArgs{
			EvalCtx: evalCtx.EvalContext(),
			Args:    req,
			Header:  kvpb.Header{Timestamp: evalTS},
		}
		_, err := SetRangeSharedManifestNum(ctx, eng, cArgs, resp)
		// Should fail with LockConflictError due to the uncommitted intent.
		require.Error(t, err)
		var lcErr *kvpb.LockConflictError
		require.ErrorAs(t, err, &lcErr)
	})

	// Use a fresh engine for flush commit tests to avoid the intent from above.
	eng2 := storage.NewDefaultInMemForTesting()
	defer eng2.Close()

	// Write RangeDescriptor.
	require.NoError(t, storage.MVCCPutProto(ctx, eng2, descKey, hlc.Timestamp{WallTime: 10},
		desc, storage.MVCCWriteOptions{}))
	// Write RSManifestState with manifest number 10.
	initState2 := kvserverpb.RSManifestState{DiskFileNum: 10, ReplicaId: 1}
	require.NoError(t, storage.MVCCBlindPutProto(ctx, eng2, rsManifestKey, hlc.Timestamp{},
		&initState2, storage.MVCCWriteOptions{}))
	// Write RangeAppliedState with FlushStartedCount=1 (simulating one
	// prepare having incremented it).
	sl := kvstorage.MakeStateLoader(rangeID)
	as := kvserverpb.RangeAppliedState{FlushStartedCount: 1}
	require.NoError(t, storage.MVCCPutProto(ctx, eng2,
		sl.RangeAppliedStateKey(), hlc.Timestamp{}, &as, storage.MVCCWriteOptions{}))

	t.Run("flush commit with matching flush started count", func(t *testing.T) {
		activateSpans := []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")}}
		req := &kvpb.SetRangeSharedManifestNumRequest{
			ExpectedManifestNum:       10,
			ExpectedDescGeneration:    generation,
			NextManifestNum:           30,
			IsFlushCommit:             true,
			ExpectedFlushStartedCount: 1,
			ActivateSpans:             activateSpans,
		}
		resp := &kvpb.SetRangeSharedManifestNumResponse{}
		cArgs := CommandArgs{
			EvalCtx: evalCtx.EvalContext(),
			Args:    req,
			Header:  kvpb.Header{Timestamp: evalTS},
		}
		result, err := SetRangeSharedManifestNum(ctx, eng2, cArgs, resp)
		require.NoError(t, err)
		require.NotNil(t, result.Replicated.RSManifestInstall)
		require.Equal(t, uint64(30), result.Replicated.RSManifestInstall.NextManifestNum)
	})

	t.Run("flush commit with mismatched flush started count", func(t *testing.T) {
		req := &kvpb.SetRangeSharedManifestNumRequest{
			ExpectedManifestNum:       10,
			ExpectedDescGeneration:    generation,
			NextManifestNum:           31,
			IsFlushCommit:             true,
			ExpectedFlushStartedCount: 5, // wrong: stored FlushStartedCount is 1
		}
		resp := &kvpb.SetRangeSharedManifestNumResponse{}
		cArgs := CommandArgs{
			EvalCtx: evalCtx.EvalContext(),
			Args:    req,
			Header:  kvpb.Header{Timestamp: evalTS},
		}
		_, err := SetRangeSharedManifestNum(ctx, eng2, cArgs, resp)
		require.Error(t, err)
		require.Contains(t, err.Error(), "flush started count mismatch")
	})

	t.Run("flush commit fails when another prepare intervened", func(t *testing.T) {
		// Write FlushStartedCount=2 to simulate a second prepare having
		// incremented the count after the first prepare returned 1.
		as2 := kvserverpb.RangeAppliedState{FlushStartedCount: 2}
		require.NoError(t, storage.MVCCPutProto(ctx, eng2,
			sl.RangeAppliedStateKey(), hlc.Timestamp{}, &as2, storage.MVCCWriteOptions{}))
		req := &kvpb.SetRangeSharedManifestNumRequest{
			ExpectedManifestNum:       10,
			ExpectedDescGeneration:    generation,
			NextManifestNum:           33,
			IsFlushCommit:             true,
			ExpectedFlushStartedCount: 1, // stale: another prepare bumped to 2
		}
		resp := &kvpb.SetRangeSharedManifestNumResponse{}
		cArgs := CommandArgs{
			EvalCtx: evalCtx.EvalContext(),
			Args:    req,
			Header:  kvpb.Header{Timestamp: evalTS},
		}
		_, err := SetRangeSharedManifestNum(ctx, eng2, cArgs, resp)
		require.Error(t, err)
		require.Contains(t, err.Error(), "flush started count mismatch")
	})

	t.Run("compaction install ignores flush started count", func(t *testing.T) {
		req := &kvpb.SetRangeSharedManifestNumRequest{
			ExpectedManifestNum:    10,
			ExpectedDescGeneration: generation,
			NextManifestNum:        32,
			// IsFlushCommit is false (default).
		}
		resp := &kvpb.SetRangeSharedManifestNumResponse{}
		cArgs := CommandArgs{
			EvalCtx: evalCtx.EvalContext(),
			Args:    req,
			Header:  kvpb.Header{Timestamp: evalTS},
		}
		result, err := SetRangeSharedManifestNum(ctx, eng2, cArgs, resp)
		require.NoError(t, err)
		require.NotNil(t, result.Replicated.RSManifestInstall)
	})
}
