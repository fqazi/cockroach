// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestUninitializedReplicaState(t *testing.T) {
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()
	desc := roachpb.RangeDescriptor{RangeID: 123}
	exp, err := MakeStateLoader(desc.RangeID).Load(context.Background(), eng, &desc)
	require.NoError(t, err)
	act := UninitializedReplicaState(desc.RangeID)
	require.Equal(t, exp, act)
}

func TestRSManifestState(t *testing.T) {
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()
	ctx := context.Background()
	sl := MakeStateLoader(123)

	// Load returns empty state when key doesn't exist.
	state, err := sl.LoadRSManifestState(ctx, eng)
	require.NoError(t, err)
	require.Equal(t, kvserverpb.RSManifestState{}, state)

	// Set and load roundtrip.
	expected := kvserverpb.RSManifestState{DiskFileNum: 42}
	require.NoError(t, sl.SetRSManifestState(ctx, eng, expected))
	state, err = sl.LoadRSManifestState(ctx, eng)
	require.NoError(t, err)
	require.Equal(t, expected, state)

	// Update to new value.
	expected = kvserverpb.RSManifestState{DiskFileNum: 100}
	require.NoError(t, sl.SetRSManifestState(ctx, eng, expected))
	state, err = sl.LoadRSManifestState(ctx, eng)
	require.NoError(t, err)
	require.Equal(t, expected, state)
}
