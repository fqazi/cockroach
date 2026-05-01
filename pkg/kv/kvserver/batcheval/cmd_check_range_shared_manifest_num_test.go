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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestCheckRangeSharedManifestNum(t *testing.T) {
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

	t.Run("success with no prior manifest state", func(t *testing.T) {
		// No RSManifestState exists yet (DiskFileNum defaults to 0).
		req := &kvpb.CheckRangeSharedManifestNumRequest{
			ExpectedManifestNum:    0,
			ExpectedDescGeneration: generation,
		}
		resp := &kvpb.CheckRangeSharedManifestNumResponse{}
		cArgs := CommandArgs{EvalCtx: evalCtx.EvalContext(), Args: req}
		_, err := CheckRangeSharedManifestNum(ctx, eng, cArgs, resp)
		require.NoError(t, err)
	})

	// Initialize RSManifestState with manifest number 10.
	initState := kvserverpb.RSManifestState{DiskFileNum: 10, ReplicaId: 1}
	require.NoError(t, storage.MVCCBlindPutProto(ctx, eng, rsManifestKey, hlc.Timestamp{},
		&initState, storage.MVCCWriteOptions{}))

	t.Run("success with matching expected values", func(t *testing.T) {
		req := &kvpb.CheckRangeSharedManifestNumRequest{
			ExpectedManifestNum:    10,
			ExpectedDescGeneration: generation,
		}
		resp := &kvpb.CheckRangeSharedManifestNumResponse{}
		cArgs := CommandArgs{EvalCtx: evalCtx.EvalContext(), Args: req}
		_, err := CheckRangeSharedManifestNum(ctx, eng, cArgs, resp)
		require.NoError(t, err)
	})

	t.Run("failure on manifest num mismatch", func(t *testing.T) {
		req := &kvpb.CheckRangeSharedManifestNumRequest{
			ExpectedManifestNum:    99, // wrong
			ExpectedDescGeneration: generation,
		}
		resp := &kvpb.CheckRangeSharedManifestNumResponse{}
		cArgs := CommandArgs{EvalCtx: evalCtx.EvalContext(), Args: req}
		_, err := CheckRangeSharedManifestNum(ctx, eng, cArgs, resp)
		require.Error(t, err)
		require.Contains(t, err.Error(), "manifest number mismatch")
	})

	t.Run("failure on generation mismatch", func(t *testing.T) {
		req := &kvpb.CheckRangeSharedManifestNumRequest{
			ExpectedManifestNum:    10,
			ExpectedDescGeneration: 99, // wrong
		}
		resp := &kvpb.CheckRangeSharedManifestNumResponse{}
		cArgs := CommandArgs{EvalCtx: evalCtx.EvalContext(), Args: req}
		_, err := CheckRangeSharedManifestNum(ctx, eng, cArgs, resp)
		require.Error(t, err)
		require.Contains(t, err.Error(), "descriptor generation mismatch")
	})

	t.Run("skip desc generation check with sentinel", func(t *testing.T) {
		// SkipDescGenerationCheck skips the generation check, so a mismatched
		// generation in the eval context should not cause failure.
		req := &kvpb.CheckRangeSharedManifestNumRequest{
			ExpectedManifestNum:    10,
			ExpectedDescGeneration: SkipDescGenerationCheck,
		}
		resp := &kvpb.CheckRangeSharedManifestNumResponse{}
		cArgs := CommandArgs{EvalCtx: evalCtx.EvalContext(), Args: req}
		_, err := CheckRangeSharedManifestNum(ctx, eng, cArgs, resp)
		require.NoError(t, err)
	})

	t.Run("skip desc generation check still checks manifest num", func(t *testing.T) {
		req := &kvpb.CheckRangeSharedManifestNumRequest{
			ExpectedManifestNum:    99, // wrong
			ExpectedDescGeneration: SkipDescGenerationCheck,
		}
		resp := &kvpb.CheckRangeSharedManifestNumResponse{}
		cArgs := CommandArgs{EvalCtx: evalCtx.EvalContext(), Args: req}
		_, err := CheckRangeSharedManifestNum(ctx, eng, cArgs, resp)
		require.Error(t, err)
		require.Contains(t, err.Error(), "manifest number mismatch")
	})

	t.Run("uses committed descriptor not provisional", func(t *testing.T) {
		// This test verifies that CheckRangeSharedManifestNum uses EvalCtx.Desc()
		// (the committed descriptor) rather than reading from storage.
		// In a real split/merge transaction, the caller would have written a
		// provisional descriptor, but we want to verify against the committed
		// state that was observed during manifest preparation.
		//
		// We simulate this by setting a different generation in the evalCtx
		// than what might be in storage. The command should use evalCtx.Desc().
		differentGenDesc := &roachpb.RangeDescriptor{
			RangeID:    rangeID,
			StartKey:   roachpb.RKey("a"),
			EndKey:     roachpb.RKey("z"),
			Generation: generation + 5, // different from storage
		}
		differentEvalCtx := &MockEvalCtx{
			ClusterSettings: st,
			Desc:            differentGenDesc,
		}
		req := &kvpb.CheckRangeSharedManifestNumRequest{
			ExpectedManifestNum:    10,
			ExpectedDescGeneration: generation + 5, // matches evalCtx.Desc()
		}
		resp := &kvpb.CheckRangeSharedManifestNumResponse{}
		cArgs := CommandArgs{EvalCtx: differentEvalCtx.EvalContext(), Args: req}
		_, err := CheckRangeSharedManifestNum(ctx, eng, cArgs, resp)
		require.NoError(t, err)
	})
}
