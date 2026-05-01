// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestBasaltDir(t *testing.T) {
	result := BasaltDir(vfs.Default, roachpb.StoreID(1), roachpb.RangeID(42), roachpb.ReplicaID(3))
	require.Equal(t, "s1/ranges/r42:3", result)
}

func TestBasaltStoreRangesDir(t *testing.T) {
	result := BasaltStoreRangesDir(vfs.Default, roachpb.StoreID(1))
	require.Equal(t, "s1/ranges", result)
}

func TestBasaltScratchDir(t *testing.T) {
	result := BasaltScratchDir(vfs.Default, roachpb.StoreID(1), roachpb.RangeID(42), roachpb.ReplicaID(3))
	require.Equal(t, "s1/scratch/r42:3", result)
}
