// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvadmission"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/replica_rac2"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage/snaprecv"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/blockiter"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

// mockIncomingSnapshotStream implements incomingSnapshotStream for testing.
type mockIncomingSnapshotStream struct {
	requests []*kvserverpb.SnapshotRequest
	index    int
	sent     []*kvserverpb.SnapshotResponse
}

func (m *mockIncomingSnapshotStream) Send(resp *kvserverpb.SnapshotResponse) error {
	m.sent = append(m.sent, resp)
	return nil
}

func (m *mockIncomingSnapshotStream) Recv() (*kvserverpb.SnapshotRequest, error) {
	if m.index >= len(m.requests) {
		return nil, nil
	}
	req := m.requests[m.index]
	m.index++
	return req, nil
}

// mockKVAdmissionController is a minimal mock that returns nil for GetSnapshotQueue.
type mockKVAdmissionController struct{}

func (m *mockKVAdmissionController) AdmitKVWork(
	_ context.Context, _ roachpb.TenantID, _ roachpb.TenantID, _ *kvpb.BatchRequest,
) (kvadmission.Handle, error) {
	return kvadmission.Handle{}, nil
}

func (m *mockKVAdmissionController) AdmittedKVWorkDone(
	kvadmission.Handle, *kvadmission.StoreWriteBytes,
) {
}

func (m *mockKVAdmissionController) AdmitRangefeedRequest(
	_ roachpb.TenantID, _ *kvpb.RangeFeedRequest,
) *admission.Pacer {
	return nil
}

func (m *mockKVAdmissionController) SetTenantWeightProvider(
	kvadmission.TenantWeightProvider, *stop.Stopper,
) {
}

func (m *mockKVAdmissionController) SnapshotIngestedOrWritten(
	_ roachpb.StoreID, _ pebble.IngestOperationStats, _ uint64,
) {
}

func (m *mockKVAdmissionController) FollowerStoreWriteBytes(
	_ roachpb.StoreID, _ kvadmission.FollowerStoreWriteBytes,
) {
}

func (m *mockKVAdmissionController) AdmitRaftEntry(
	_ context.Context, _ roachpb.TenantID, _ roachpb.StoreID, _ roachpb.RangeID, _ raftpb.Entry,
) (admitted bool, err error) {
	return true, nil
}

func (m *mockKVAdmissionController) OnBypassed(_ roachpb.StoreID, _ roachpb.RangeID, _ int64) {
}

func (m *mockKVAdmissionController) OnDestroyRaftMuLocked(_ roachpb.StoreID, _ roachpb.RangeID) {
}

func (m *mockKVAdmissionController) Admit(
	_ context.Context, _ replica_rac2.EntryForAdmission,
) bool {
	return true
}

func (m *mockKVAdmissionController) GetSnapshotQueue(_ roachpb.StoreID) *admission.SnapshotQueue {
	return nil
}

func (m *mockKVAdmissionController) GetProvisionedBandwidth(_ roachpb.StoreID) int64 {
	return 0
}

// TestKVBatchSnapshotStrategyReceiveExternalReplicate tests the
// kvBatchSnapshotStrategy.Receive method with ExternalReplicate=true. This
// ensures the production code correctly handles DEL and other Pebble internal
// key kinds, when receiving external SST snapshots.
//
// The bug fixes covered by this test: (a) the receiver uses
// ExpectInternalKeys to decide whether ReadOne should accept internal
// key kinds like DEL and RANGEKEYDEL, (b) DEL keys were having their
// value retrieved using BatchReader.Value, which resulted in a panic.
func TestKVBatchSnapshotStrategyReceiveExternalReplicate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Create test store configuration.
	manual := timeutil.NewManualTime(timeutil.Unix(0, 123))
	cfg := TestStoreConfig(hlc.NewClockForTesting(manual))
	cfg.KVAdmissionController = &mockKVAdmissionController{}

	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	// Create a minimal Store with only the fields needed by Receive.
	testIdent := roachpb.StoreIdent{
		ClusterID: uuid.MakeV4(),
		NodeID:    1,
		StoreID:   1,
	}
	store := &Store{
		cfg:   cfg,
		Ident: &testIdent,
	}

	// Create range descriptor for the test.
	desc := &roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKey("d"),
		EndKey:   roachpb.RKeyMax,
	}

	// Create snapshot UUID.
	snapUUID := uuid.Must(uuid.FromBytes([]byte("foobar1234567890")))

	// Create SST snapshot storage and scratch space.
	sstSnapshotStorage := snaprecv.NewSSTSnapshotStorage(eng, rate.NewLimiter(rate.Inf, 0))
	scratch := sstSnapshotStorage.NewScratchSpace(desc.RangeID, snapUUID, nil)

	// Helper to create a batch repr with specified operations.
	makeBatchRepr := func(fn func(storage.WriteBatch)) []byte {
		batch := eng.NewWriteBatch()
		defer batch.Close()
		fn(batch)
		repr := batch.Repr()
		reprCopy := make([]byte, len(repr))
		copy(reprCopy, repr)
		return reprCopy
	}

	now := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}

	t.Run("external-replicate-with-del-succeeds", func(t *testing.T) {
		// Create a batch with a DEL operation (requires ExpectInternalKeys).
		mvccKey := storage.MVCCKey{Key: roachpb.Key("e"), Timestamp: now}
		encodedKey := storage.EncodeMVCCKey(mvccKey)
		kvBatch := makeBatchRepr(func(b storage.WriteBatch) {
			ik := pebble.InternalKey{
				UserKey: encodedKey,
				Trailer: pebble.MakeInternalKeyTrailer(0, pebble.InternalKeyKindDelete),
			}
			require.NoError(t, b.PutInternalPointKey(&ik, nil))
		})

		// Create the mock stream that returns the batch followed by a Final request.
		stream := &mockIncomingSnapshotStream{
			requests: []*kvserverpb.SnapshotRequest{
				{KVBatch: kvBatch},
				{Final: true},
			},
		}

		// Create the header with ExpectInternalKeys=true.
		header := kvserverpb.SnapshotRequest_Header{
			ExternalReplicate:  true,
			ExpectInternalKeys: true,
			State: kvserverpb.ReplicaState{
				Desc: desc,
			},
			RaftMessageRequest: kvserverpb.RaftMessageRequest{
				Message: raftpb.Message{
					Snapshot: &raftpb.Snapshot{
						Data: snapUUID.GetBytes(),
					},
				},
			},
		}

		// Create the kvBatchSnapshotStrategy.
		kvSS := &kvBatchSnapshotStrategy{
			st:      cfg.Settings,
			scratch: scratch,
		}

		// Call the actual Receive method - this tests the production code.
		inSnap, err := kvSS.Receive(ctx, store, stream, header, func(int64) {})
		require.NoError(t, err, "Receive should succeed with ExternalReplicate=true and DEL operation")
		require.Equal(t, snapUUID, inSnap.SnapUUID)
	})

	t.Run("no-external-with-del-fails", func(t *testing.T) {
		// Create a new scratch space for this subtest.
		scratch2 := sstSnapshotStorage.NewScratchSpace(desc.RangeID, uuid.MakeV4(), nil)

		// Create a batch with a DEL operation.
		mvccKey := storage.MVCCKey{Key: roachpb.Key("f"), Timestamp: now}
		encodedKey := storage.EncodeMVCCKey(mvccKey)
		kvBatch := makeBatchRepr(func(b storage.WriteBatch) {
			ik := pebble.InternalKey{
				UserKey: encodedKey,
				Trailer: pebble.MakeInternalKeyTrailer(0, pebble.InternalKeyKindDelete),
			}
			require.NoError(t, b.PutInternalPointKey(&ik, nil))
		})

		// Create the mock stream.
		stream := &mockIncomingSnapshotStream{
			requests: []*kvserverpb.SnapshotRequest{
				{KVBatch: kvBatch},
				{Final: true},
			},
		}

		// Create the header with ExternalReplicate=false and SharedReplicate=false.
		snapUUID2 := uuid.MakeV4()
		header := kvserverpb.SnapshotRequest_Header{
			SharedReplicate:   false,
			ExternalReplicate: false,
			State: kvserverpb.ReplicaState{
				Desc: desc,
			},
			RaftMessageRequest: kvserverpb.RaftMessageRequest{
				Message: raftpb.Message{
					Snapshot: &raftpb.Snapshot{
						Data: snapUUID2.GetBytes(),
					},
				},
			},
		}

		// Create the kvBatchSnapshotStrategy.
		kvSS := &kvBatchSnapshotStrategy{
			st:      cfg.Settings,
			scratch: scratch2,
		}

		// Call the actual Receive method - should fail.
		_, err := kvSS.Receive(ctx, store, stream, header, func(int64) {})
		require.Error(t, err, "Receive should fail with DEL operation when neither SharedReplicate nor ExternalReplicate is set")
		require.Contains(t, err.Error(), "unexpected batch entry key kind")
	})
}

func TestKVBatchSnapshotStrategyReceiveBatchReaderError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	manual := timeutil.NewManualTime(timeutil.Unix(0, 123))
	cfg := TestStoreConfig(hlc.NewClockForTesting(manual))
	cfg.KVAdmissionController = &mockKVAdmissionController{}

	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()
	testIdent := roachpb.StoreIdent{
		ClusterID: uuid.MakeV4(),
		NodeID:    1,
		StoreID:   1,
	}
	store := &Store{
		cfg:   cfg,
		Ident: &testIdent,
	}

	desc := &roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKey("a"),
		EndKey:   roachpb.RKeyMax,
	}

	snapUUID := uuid.Must(uuid.FromBytes([]byte("foobar1234567890")))
	sstSnapshotStorage := snaprecv.NewSSTSnapshotStorage(eng, rate.NewLimiter(rate.Inf, 0))
	scratch := sstSnapshotStorage.NewScratchSpace(desc.RangeID, snapUUID, nil)

	now := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}

	makeBatchRepr := func(fn func(storage.WriteBatch)) []byte {
		batch := eng.NewWriteBatch()
		defer batch.Close()
		fn(batch)
		repr := batch.Repr()
		reprCopy := make([]byte, len(repr))
		copy(reprCopy, repr)
		return reprCopy
	}

	kvBatch := makeBatchRepr(func(b storage.WriteBatch) {
		mvccKey := storage.MVCCKey{Key: roachpb.Key("b"), Timestamp: now}
		require.NoError(t, b.PutMVCC(mvccKey, storage.MVCCValue{Value: roachpb.MakeValueFromString("val")}))
		mvccKey2 := storage.MVCCKey{Key: roachpb.Key("c"), Timestamp: now}
		require.NoError(t, b.PutMVCC(mvccKey2, storage.MVCCValue{Value: roachpb.MakeValueFromString("val2")}))
	})

	corruptBatch := make([]byte, len(kvBatch)-5)
	copy(corruptBatch, kvBatch[:len(kvBatch)-5])

	stream := &mockIncomingSnapshotStream{
		requests: []*kvserverpb.SnapshotRequest{
			{KVBatch: corruptBatch},
			{Final: true},
		},
	}

	header := kvserverpb.SnapshotRequest_Header{
		State: kvserverpb.ReplicaState{
			Desc: desc,
		},
		RaftMessageRequest: kvserverpb.RaftMessageRequest{
			Message: raftpb.Message{
				Snapshot: &raftpb.Snapshot{
					Data: snapUUID.GetBytes(),
				},
			},
		},
	}
	kvSS := &kvBatchSnapshotStrategy{
		st:      cfg.Settings,
		scratch: scratch,
	}

	_, err := kvSS.Receive(ctx, store, stream, header, func(int64) {})
	require.Error(t, err, "Receive should return an error when batch reader encounters a corrupt batch")
}

// mockOutgoingSnapshotStream implements outgoingSnapshotStream for testing.
type mockOutgoingSnapshotStream struct {
	sentRequests []*kvserverpb.SnapshotRequest
}

func (m *mockOutgoingSnapshotStream) Send(req *kvserverpb.SnapshotRequest) error {
	m.sentRequests = append(m.sentRequests, req)
	return nil
}

func (m *mockOutgoingSnapshotStream) Recv() (*kvserverpb.SnapshotResponse, error) {
	return &kvserverpb.SnapshotResponse{Status: kvserverpb.SnapshotResponse_ACCEPTED}, nil
}

// TestKVBatchSnapshotStrategySendDormant tests that when
// CanHaveDormantRangeDel is true (even with RSManifestDiskFileNum == NoManifestNum,
// i.e. a flush hasn't completed yet), the Send path separates
// BelowDormant keys into BelowDormantKVBatch while normal keys and
// dormant range deletions go into KVBatch. With a small batch size,
// multiple SnapshotRequests are produced and HaveDormantRangeDel
// transitions from false to true.
func TestKVBatchSnapshotStrategySendDormant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	manual := timeutil.NewManualTime(timeutil.Unix(0, 123))
	cfg := TestStoreConfig(hlc.NewClockForTesting(manual))
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()
	// Write point keys at various user keys via batch.
	initBatch := eng.NewUnindexedBatch()
	for _, k := range []string{"d", "e", "f", "g"} {
		ek := storage.MVCCKey{Key: roachpb.Key(k), Timestamp: hlc.Timestamp{WallTime: 1}}
		val := storage.MVCCValue{Value: roachpb.MakeValueFromString(k + "1")}
		require.NoError(t, initBatch.PutMVCC(ek, val))
	}
	require.NoError(t, initBatch.Commit(true))
	initBatch.Close()
	// Write a dormant range deletion spanning [e, g).
	batch := eng.NewUnindexedBatch()
	require.NoError(t, batch.ClearRawRangeDormant(roachpb.Key("e"), roachpb.Key("g")))
	require.NoError(t, batch.Commit(true))
	batch.Close()
	// Write point keys above the dormant via a batch — these will be
	// AboveDormant because they have higher seqnums.
	aboveBatch := eng.NewUnindexedBatch()
	for _, k := range []string{"e", "f"} {
		ek := storage.MVCCKey{Key: roachpb.Key(k), Timestamp: hlc.Timestamp{WallTime: 2}}
		val := storage.MVCCValue{Value: roachpb.MakeValueFromString(k + "2")}
		require.NoError(t, aboveBatch.PutMVCC(ek, val))
	}
	require.NoError(t, aboveBatch.Commit(true))
	aboveBatch.Close()
	desc := &roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKey("d"),
		EndKey:   roachpb.RKey("h"),
	}
	// RSManifestDiskFileNum is NoManifestNum to simulate the case where
	// rsEngine exists but no flush has completed yet. Dormant range
	// deletions can still exist in the store-local LSM.
	header := kvserverpb.SnapshotRequest_Header{
		State: kvserverpb.ReplicaState{Desc: desc},
	}
	// countBatchKeys counts point keys and range-del keys in a batch repr,
	// and reports whether a dormant range deletion is present.
	countBatchKeys := func(t *testing.T, repr []byte) (points, rangeDels int, hasDormant bool) {
		t.Helper()
		reader, err := storage.NewBatchReader(repr)
		require.NoError(t, err)
		for reader.Next() {
			switch reader.KeyKind() {
			case pebble.InternalKeyKindRangeDeleteDormant:
				hasDormant = true
			case pebble.InternalKeyKindRangeKeyDelete, pebble.InternalKeyKindRangeKeySet,
				pebble.InternalKeyKindRangeKeyUnset, pebble.InternalKeyKindRangeDelete:
				rangeDels++
			default:
				points++
			}
		}
		require.NoError(t, reader.Error())
		return points, rangeDels, hasDormant
	}
	t.Run("large-batch", func(t *testing.T) {
		snap := eng.NewSnapshot()
		defer snap.Close()
		outSnap := &OutgoingSnapshot{
			State:                  kvserverpb.ReplicaState{Desc: desc},
			StateSnap:              snap,
			CanHaveDormantRangeDel: true,
		}
		stream := &mockOutgoingSnapshotStream{}
		kvSS := &kvBatchSnapshotStrategy{
			batchSize:     1 << 20,
			limiter:       rate.NewLimiter(rate.Inf, 1),
			st:            cfg.Settings,
			newWriteBatch: eng.NewWriteBatch,
		}
		_, err := kvSS.Send(ctx, stream, header, outSnap, func(int64) {})
		require.NoError(t, err)
		// All data fits in one batch.
		require.Len(t, stream.sentRequests, 1)
		req := stream.sentRequests[0]
		require.NotNil(t, req.KVBatch)
		require.NotNil(t, req.BelowDormantKVBatch)
		require.True(t, req.HaveDormantRangeDel)
		points, _, hasDormant := countBatchKeys(t, req.KVBatch)
		require.Equal(t, 4, points) // d@1, e@2, f@2, g@1
		require.True(t, hasDormant)
		belowPts, _, _ := countBatchKeys(t, req.BelowDormantKVBatch)
		require.Equal(t, 2, belowPts) // e@1, f@1
	})
	t.Run("small-batch", func(t *testing.T) {
		snap := eng.NewSnapshot()
		defer snap.Close()
		outSnap := &OutgoingSnapshot{
			State:                  kvserverpb.ReplicaState{Desc: desc},
			StateSnap:              snap,
			CanHaveDormantRangeDel: true,
		}
		stream := &mockOutgoingSnapshotStream{}
		kvSS := &kvBatchSnapshotStrategy{
			batchSize:     1, // force flush after every key
			limiter:       rate.NewLimiter(rate.Inf, 1),
			st:            cfg.Settings,
			newWriteBatch: eng.NewWriteBatch,
		}
		_, err := kvSS.Send(ctx, stream, header, outSnap, func(int64) {})
		require.NoError(t, err)
		require.Greater(t, len(stream.sentRequests), 1)
		// Verify HaveDormantRangeDel transitions from false to true.
		sawFalse := false
		sawTrue := false
		for _, req := range stream.sentRequests {
			if !req.HaveDormantRangeDel {
				sawFalse = true
				// Must not see false after true.
				require.False(t, sawTrue, "HaveDormantRangeDel went true then back to false")
			} else {
				sawTrue = true
			}
		}
		require.True(t, sawFalse, "expected at least one request with HaveDormantRangeDel=false")
		require.True(t, sawTrue, "expected at least one request with HaveDormantRangeDel=true")
		// Verify at least one request has nil KVBatch (the b==nil edge case
		// where only BelowDormant keys were flushed).
		hasNilKVBatch := false
		for _, req := range stream.sentRequests {
			if req.KVBatch == nil && len(req.BelowDormantKVBatch) > 0 {
				hasNilKVBatch = true
			}
		}
		require.True(t, hasNilKVBatch, "expected at least one request with nil KVBatch and non-nil BelowDormantKVBatch")
		// Verify all keys are accounted for across all requests.
		totalNormalPts := 0
		totalBelowPts := 0
		hasDormant := false
		for _, req := range stream.sentRequests {
			if req.KVBatch != nil {
				pts, _, dormant := countBatchKeys(t, req.KVBatch)
				totalNormalPts += pts
				hasDormant = hasDormant || dormant
			}
			if len(req.BelowDormantKVBatch) > 0 {
				pts, _, _ := countBatchKeys(t, req.BelowDormantKVBatch)
				totalBelowPts += pts
			}
		}
		require.Equal(t, 4, totalNormalPts) // d@1, e@2, f@2, g@1
		require.Equal(t, 2, totalBelowPts)  // e@1, f@1
		require.True(t, hasDormant)
	})
}

// TestKVBatchSnapshotStrategyReceiveBelowDormant tests that the Receive
// method processes BelowDormantKVBatch through MultiSSTWriter, producing
// stacked SSTs with lower SSTs for BelowDormant keys.
func TestKVBatchSnapshotStrategyReceiveBelowDormant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	manual := timeutil.NewManualTime(timeutil.Unix(0, 123))
	cfg := TestStoreConfig(hlc.NewClockForTesting(manual))
	cfg.KVAdmissionController = &mockKVAdmissionController{}
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()
	testIdent := roachpb.StoreIdent{
		ClusterID: uuid.MakeV4(),
		NodeID:    1,
		StoreID:   1,
	}
	store := &Store{
		cfg:   cfg,
		Ident: &testIdent,
	}
	desc := &roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKey("d"),
		EndKey:   roachpb.RKeyMax,
	}
	snapUUID := uuid.Must(uuid.FromBytes([]byte("foobar1234567890")))
	sstSnapshotStorage := snaprecv.NewSSTSnapshotStorage(eng, rate.NewLimiter(rate.Inf, 0))
	scratch := sstSnapshotStorage.NewScratchSpace(desc.RangeID, snapUUID, nil)
	// Create a normal KVBatch with a point key using PutRawMVCC to avoid
	// checksum issues (Receive verifies value checksums).
	normalBatch := eng.NewWriteBatch()
	mvccKey := storage.MVCCKey{Key: roachpb.Key("e"), Timestamp: hlc.Timestamp{WallTime: 2}}
	encodedVal, err := storage.EncodeMVCCValue(storage.MVCCValue{Value: roachpb.MakeValueFromString("e2")})
	require.NoError(t, err)
	require.NoError(t, normalBatch.PutRawMVCC(mvccKey, encodedVal))
	normalRepr := normalBatch.Repr()
	normalReprCopy := make([]byte, len(normalRepr))
	copy(normalReprCopy, normalRepr)
	normalBatch.Close()
	// Create a below-dormant KVBatch with a point key.
	belowBatch := eng.NewWriteBatch()
	belowKey := storage.MVCCKey{Key: roachpb.Key("e"), Timestamp: hlc.Timestamp{WallTime: 1}}
	belowVal, err := storage.EncodeMVCCValue(storage.MVCCValue{Value: roachpb.MakeValueFromString("e1")})
	require.NoError(t, err)
	require.NoError(t, belowBatch.PutRawMVCC(belowKey, belowVal))
	belowRepr := belowBatch.Repr()
	belowReprCopy := make([]byte, len(belowRepr))
	copy(belowReprCopy, belowRepr)
	belowBatch.Close()
	stream := &mockIncomingSnapshotStream{
		requests: []*kvserverpb.SnapshotRequest{
			{
				KVBatch:             normalReprCopy,
				BelowDormantKVBatch: belowReprCopy,
				HaveDormantRangeDel: true,
			},
			{Final: true},
		},
	}
	header := kvserverpb.SnapshotRequest_Header{
		State:              kvserverpb.ReplicaState{Desc: desc},
		ExpectInternalKeys: true,
		RaftMessageRequest: kvserverpb.RaftMessageRequest{
			Message: raftpb.Message{
				Snapshot: &raftpb.Snapshot{Data: snapUUID.GetBytes()},
			},
		},
	}
	kvSS := &kvBatchSnapshotStrategy{
		st:      cfg.Settings,
		scratch: scratch,
	}
	inSnap, err := kvSS.Receive(ctx, store, stream, header, func(int64) {})
	require.NoError(t, err)
	require.Equal(t, snapUUID, inSnap.SnapUUID)
	// Verify stacked SSTs contain a lower SST for the last MVCC SST.
	hasLower := false
	for _, sst := range inSnap.stackedSSTs {
		if sst.LowerSST.Path != "" {
			hasLower = true
		}
	}
	require.True(t, hasLower, "stacked SSTs should have a lower SST for BelowDormant keys")
}

// TestKVBatchSnapshotStrategyReceiveMultipleBelowDormant tests that
// receiving multiple BelowDormantKVBatch messages works correctly.
func TestKVBatchSnapshotStrategyReceiveMultipleBelowDormant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	manual := timeutil.NewManualTime(timeutil.Unix(0, 123))
	cfg := TestStoreConfig(hlc.NewClockForTesting(manual))
	cfg.KVAdmissionController = &mockKVAdmissionController{}
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()
	testIdent := roachpb.StoreIdent{
		ClusterID: uuid.MakeV4(),
		NodeID:    1,
		StoreID:   1,
	}
	store := &Store{
		cfg:   cfg,
		Ident: &testIdent,
	}
	desc := &roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKey("d"),
		EndKey:   roachpb.RKeyMax,
	}
	snapUUID := uuid.Must(uuid.FromBytes([]byte("foobar1234567890")))
	sstSnapshotStorage := snaprecv.NewSSTSnapshotStorage(eng, rate.NewLimiter(rate.Inf, 0))
	scratch := sstSnapshotStorage.NewScratchSpace(desc.RangeID, snapUUID, nil)
	// Create two below-dormant batches with different keys.
	belowBatch1 := eng.NewWriteBatch()
	key1 := storage.MVCCKey{Key: roachpb.Key("e"), Timestamp: hlc.Timestamp{WallTime: 1}}
	val1, err := storage.EncodeMVCCValue(storage.MVCCValue{Value: roachpb.MakeValueFromString("e1")})
	require.NoError(t, err)
	require.NoError(t, belowBatch1.PutRawMVCC(key1, val1))
	repr1 := append([]byte(nil), belowBatch1.Repr()...)
	belowBatch1.Close()
	belowBatch2 := eng.NewWriteBatch()
	key2 := storage.MVCCKey{Key: roachpb.Key("f"), Timestamp: hlc.Timestamp{WallTime: 1}}
	val2, err := storage.EncodeMVCCValue(storage.MVCCValue{Value: roachpb.MakeValueFromString("f1")})
	require.NoError(t, err)
	require.NoError(t, belowBatch2.PutRawMVCC(key2, val2))
	repr2 := append([]byte(nil), belowBatch2.Repr()...)
	belowBatch2.Close()
	// Also include a normal KVBatch.
	normalBatch := eng.NewWriteBatch()
	normalKey := storage.MVCCKey{Key: roachpb.Key("e"), Timestamp: hlc.Timestamp{WallTime: 2}}
	normalVal, err := storage.EncodeMVCCValue(storage.MVCCValue{Value: roachpb.MakeValueFromString("e2")})
	require.NoError(t, err)
	require.NoError(t, normalBatch.PutRawMVCC(normalKey, normalVal))
	normalRepr := append([]byte(nil), normalBatch.Repr()...)
	normalBatch.Close()
	stream := &mockIncomingSnapshotStream{
		requests: []*kvserverpb.SnapshotRequest{
			{
				KVBatch:             normalRepr,
				BelowDormantKVBatch: repr1,
				HaveDormantRangeDel: true,
			},
			{
				BelowDormantKVBatch: repr2,
				HaveDormantRangeDel: true,
			},
			{Final: true},
		},
	}
	header := kvserverpb.SnapshotRequest_Header{
		State:              kvserverpb.ReplicaState{Desc: desc},
		ExpectInternalKeys: true,
		RaftMessageRequest: kvserverpb.RaftMessageRequest{
			Message: raftpb.Message{
				Snapshot: &raftpb.Snapshot{Data: snapUUID.GetBytes()},
			},
		},
	}
	kvSS := &kvBatchSnapshotStrategy{
		st:      cfg.Settings,
		scratch: scratch,
	}
	inSnap, err := kvSS.Receive(ctx, store, stream, header, func(int64) {})
	require.NoError(t, err)
	require.Equal(t, snapUUID, inSnap.SnapUUID)
}

// TestKVBatchSnapshotStrategyReceiveDormantRollover tests that once the
// receiver sees HaveDormantRangeDel=true, DisableSizeBasedRollover is
// permanent — even if subsequent requests revert to false. With
// MaxSnapshotSSTableSize set to 1 byte, without DisableSizeBasedRollover
// every key would cause a new SST. This test verifies only one MVCC SST
// is produced.
func TestKVBatchSnapshotStrategyReceiveDormantRollover(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	manual := timeutil.NewManualTime(timeutil.Unix(0, 123))
	cfg := TestStoreConfig(hlc.NewClockForTesting(manual))
	cfg.KVAdmissionController = &mockKVAdmissionController{}
	// Set MaxSnapshotSSTableSize to 1 byte so rollover would trigger on
	// every key without DisableSizeBasedRollover.
	MaxSnapshotSSTableSize.Override(ctx, &cfg.Settings.SV, 1)
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()
	testIdent := roachpb.StoreIdent{
		ClusterID: uuid.MakeV4(),
		NodeID:    1,
		StoreID:   1,
	}
	store := &Store{
		cfg:   cfg,
		Ident: &testIdent,
	}
	desc := &roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKey("d"),
		EndKey:   roachpb.RKeyMax,
	}
	snapUUID := uuid.Must(uuid.FromBytes([]byte("foobar1234567890")))
	sstSnapshotStorage := snaprecv.NewSSTSnapshotStorage(eng, rate.NewLimiter(rate.Inf, 0))
	scratch := sstSnapshotStorage.NewScratchSpace(desc.RangeID, snapUUID, nil)
	// Helper to create a batch repr with a single MVCC point key.
	makeMVCCBatch := func(key string, ts int64, valStr string) []byte {
		b := eng.NewWriteBatch()
		defer b.Close()
		mvccKey := storage.MVCCKey{Key: roachpb.Key(key), Timestamp: hlc.Timestamp{WallTime: ts}}
		encodedVal, err := storage.EncodeMVCCValue(storage.MVCCValue{Value: roachpb.MakeValueFromString(valStr)})
		require.NoError(t, err)
		require.NoError(t, b.PutRawMVCC(mvccKey, encodedVal))
		return append([]byte(nil), b.Repr()...)
	}
	// Create a below-dormant batch.
	belowBatch := eng.NewWriteBatch()
	belowKey := storage.MVCCKey{Key: roachpb.Key("e"), Timestamp: hlc.Timestamp{WallTime: 1}}
	belowVal, err := storage.EncodeMVCCValue(storage.MVCCValue{Value: roachpb.MakeValueFromString("e1")})
	require.NoError(t, err)
	require.NoError(t, belowBatch.PutRawMVCC(belowKey, belowVal))
	belowRepr := append([]byte(nil), belowBatch.Repr()...)
	belowBatch.Close()
	// Stream: [false, true, false]. The receiver should disable rollover
	// on the second request and keep it disabled for the third.
	stream := &mockIncomingSnapshotStream{
		requests: []*kvserverpb.SnapshotRequest{
			{
				KVBatch:             makeMVCCBatch("d", 1, "d1"),
				HaveDormantRangeDel: false,
			},
			{
				KVBatch:             makeMVCCBatch("e", 2, "e2"),
				BelowDormantKVBatch: belowRepr,
				HaveDormantRangeDel: true,
			},
			{
				KVBatch:             makeMVCCBatch("f", 1, "f1"),
				HaveDormantRangeDel: false,
			},
			{Final: true},
		},
	}
	header := kvserverpb.SnapshotRequest_Header{
		State:              kvserverpb.ReplicaState{Desc: desc},
		ExpectInternalKeys: true,
		RaftMessageRequest: kvserverpb.RaftMessageRequest{
			Message: raftpb.Message{
				Snapshot: &raftpb.Snapshot{Data: snapUUID.GetBytes()},
			},
		},
	}
	kvSS := &kvBatchSnapshotStrategy{
		st:      cfg.Settings,
		scratch: scratch,
	}
	inSnap, err := kvSS.Receive(ctx, store, stream, header, func(int64) {})
	require.NoError(t, err)
	require.Equal(t, snapUUID, inSnap.SnapUUID)
	// Count MVCC SSTs (the last key range). Despite MaxSnapshotSSTableSize=1,
	// DisableSizeBasedRollover should prevent rollover, producing a single
	// MVCC SST.
	mvccSSTCount := 0
	hasLower := false
	for _, sst := range inSnap.stackedSSTs {
		if sst.UpperSST.Path != "" {
			data, err := fs.ReadFile(eng.Env(), sst.UpperSST.Path)
			require.NoError(t, err)
			pts, _ := storageutils.KeysFromSST(t, data)
			if len(pts) > 0 {
				mvccSSTCount++
			}
		}
		if sst.LowerSST.Path != "" {
			hasLower = true
		}
	}
	require.Equal(t, 1, mvccSSTCount, "expected exactly one MVCC SST (no rollover)")
	require.True(t, hasLower, "expected a lower SST for BelowDormant keys")
}

// TestKVBatchSnapshotStrategySendReceiveDormantE2E tests the full
// send → receive path with dormant range deletions and BelowDormant keys.
// The sender produces KVBatch (with dormant RANGEDEL) and BelowDormantKVBatch
// with HaveDormantRangeDel=true. The receiver processes both through
// MultiSSTWriter, producing stacked SSTs with lower SSTs. This is tested
// with both large and small batch sizes to exercise the multi-flush path.
func TestKVBatchSnapshotStrategySendReceiveDormantE2E(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	manual := timeutil.NewManualTime(timeutil.Unix(0, 123))
	cfg := TestStoreConfig(hlc.NewClockForTesting(manual))
	cfg.KVAdmissionController = &mockKVAdmissionController{}
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()
	// Write point keys at various user keys.
	initBatch := eng.NewUnindexedBatch()
	for _, k := range []string{"d", "e", "f", "g"} {
		ek := storage.MVCCKey{Key: roachpb.Key(k), Timestamp: hlc.Timestamp{WallTime: 1}}
		val := storage.MVCCValue{Value: roachpb.MakeValueFromString(k + "1")}
		require.NoError(t, initBatch.PutMVCC(ek, val))
	}
	require.NoError(t, initBatch.Commit(true))
	initBatch.Close()
	// Write a dormant range deletion spanning [e, g).
	batch := eng.NewUnindexedBatch()
	require.NoError(t, batch.ClearRawRangeDormant(roachpb.Key("e"), roachpb.Key("g")))
	require.NoError(t, batch.Commit(true))
	batch.Close()
	// Write point keys above the dormant.
	aboveBatch := eng.NewUnindexedBatch()
	for _, k := range []string{"e", "f"} {
		ek := storage.MVCCKey{Key: roachpb.Key(k), Timestamp: hlc.Timestamp{WallTime: 2}}
		val := storage.MVCCValue{Value: roachpb.MakeValueFromString(k + "2")}
		require.NoError(t, aboveBatch.PutMVCC(ek, val))
	}
	require.NoError(t, aboveBatch.Commit(true))
	aboveBatch.Close()
	desc := &roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKey("d"),
		EndKey:   roachpb.RKey("h"),
	}
	// verifyStackedSSTs checks upper/lower point keys and dormant RANGEDEL
	// spans in the stacked SSTs produced by Receive.
	verifyStackedSSTs := func(
		t *testing.T, inSnap IncomingSnapshot,
	) {
		t.Helper()
		var upperPointKeys []storage.MVCCKey
		var lowerPointKeys []storage.MVCCKey
		var dormantRangeDelSpans []roachpb.Span
		for _, sst := range inSnap.stackedSSTs {
			if sst.UpperSST.Path != "" {
				data, err := fs.ReadFile(eng.Env(), sst.UpperSST.Path)
				require.NoError(t, err)
				pts, _ := storageutils.KeysFromSST(t, data)
				upperPointKeys = append(upperPointKeys, pts...)
				r, err := sstable.NewMemReader(data, sstable.ReaderOptions{
					Comparer:   &storage.EngineComparer,
					KeySchemas: sstable.MakeKeySchemas(storage.KeySchemas...),
				})
				require.NoError(t, err)
				rdIter, err := r.NewRawRangeDelIter(
					ctx, blockiter.NoFragmentTransforms, sstable.NoReadEnv,
				)
				require.NoError(t, err)
				if rdIter != nil {
					for s, err := rdIter.First(); s != nil; s, err = rdIter.Next() {
						require.NoError(t, err)
						for _, k := range s.Keys {
							if k.Kind() == pebble.InternalKeyKindRangeDeleteDormant {
								startKey, ok := storage.DecodeEngineKey(s.Start)
								require.True(t, ok)
								endKey, ok := storage.DecodeEngineKey(s.End)
								require.True(t, ok)
								dormantRangeDelSpans = append(dormantRangeDelSpans,
									roachpb.Span{Key: startKey.Key.Clone(), EndKey: endKey.Key.Clone()})
							}
						}
					}
					rdIter.Close()
				}
				require.NoError(t, r.Close())
			}
			if sst.LowerSST.Path != "" {
				data, err := fs.ReadFile(eng.Env(), sst.LowerSST.Path)
				require.NoError(t, err)
				pts, _ := storageutils.KeysFromSST(t, data)
				lowerPointKeys = append(lowerPointKeys, pts...)
			}
		}
		require.Equal(t, []storage.MVCCKey{
			{Key: roachpb.Key("d"), Timestamp: hlc.Timestamp{WallTime: 1}},
			{Key: roachpb.Key("e"), Timestamp: hlc.Timestamp{WallTime: 2}},
			{Key: roachpb.Key("f"), Timestamp: hlc.Timestamp{WallTime: 2}},
			{Key: roachpb.Key("g"), Timestamp: hlc.Timestamp{WallTime: 1}},
		}, upperPointKeys)
		require.Equal(t, []storage.MVCCKey{
			{Key: roachpb.Key("e"), Timestamp: hlc.Timestamp{WallTime: 1}},
			{Key: roachpb.Key("f"), Timestamp: hlc.Timestamp{WallTime: 1}},
		}, lowerPointKeys)
		require.Equal(t, []roachpb.Span{
			{Key: roachpb.Key("e"), EndKey: roachpb.Key("g")},
		}, dormantRangeDelSpans)
		require.True(t, inSnap.DataSize > 0)
		require.True(t, inSnap.SSTSize > 0)
	}
	// sendReceive runs the full send→receive path with the given batchSize
	// and verifies the resulting stacked SSTs.
	sendReceive := func(t *testing.T, batchSize int64) {
		t.Helper()
		snap := eng.NewSnapshot()
		defer snap.Close()
		outSnap := &OutgoingSnapshot{
			State:                  kvserverpb.ReplicaState{Desc: desc},
			StateSnap:              snap,
			CanHaveDormantRangeDel: true,
		}
		sendHeader := kvserverpb.SnapshotRequest_Header{
			State: kvserverpb.ReplicaState{Desc: desc},
		}
		sendStream := &mockOutgoingSnapshotStream{}
		sendKVSS := &kvBatchSnapshotStrategy{
			batchSize:     batchSize,
			limiter:       rate.NewLimiter(rate.Inf, 1),
			st:            cfg.Settings,
			newWriteBatch: eng.NewWriteBatch,
		}
		_, err := sendKVSS.Send(ctx, sendStream, sendHeader, outSnap, func(int64) {})
		require.NoError(t, err)
		require.NotEmpty(t, sendStream.sentRequests)
		// At least the last request must have HaveDormantRangeDel.
		lastReq := sendStream.sentRequests[len(sendStream.sentRequests)-1]
		require.True(t, lastReq.HaveDormantRangeDel)
		// --- Receive ---
		testIdent := roachpb.StoreIdent{
			ClusterID: uuid.MakeV4(),
			NodeID:    1,
			StoreID:   1,
		}
		store := &Store{
			cfg:   cfg,
			Ident: &testIdent,
		}
		snapUUID := uuid.Must(uuid.FromBytes([]byte("foobar1234567890")))
		sstSnapshotStorage := snaprecv.NewSSTSnapshotStorage(eng, rate.NewLimiter(rate.Inf, 0))
		recvScratch := sstSnapshotStorage.NewScratchSpace(desc.RangeID, snapUUID, nil)
		var recvReqs []*kvserverpb.SnapshotRequest
		for _, req := range sendStream.sentRequests {
			recvReqs = append(recvReqs, req)
		}
		recvReqs = append(recvReqs, &kvserverpb.SnapshotRequest{Final: true})
		recvStream := &mockIncomingSnapshotStream{requests: recvReqs}
		recvHeader := kvserverpb.SnapshotRequest_Header{
			State:              kvserverpb.ReplicaState{Desc: desc},
			ExpectInternalKeys: true,
			RaftMessageRequest: kvserverpb.RaftMessageRequest{
				Message: raftpb.Message{
					Snapshot: &raftpb.Snapshot{Data: snapUUID.GetBytes()},
				},
			},
		}
		recvKVSS := &kvBatchSnapshotStrategy{
			st:      cfg.Settings,
			scratch: recvScratch,
		}
		inSnap, err := recvKVSS.Receive(ctx, store, recvStream, recvHeader, func(int64) {})
		require.NoError(t, err)
		require.Equal(t, snapUUID, inSnap.SnapUUID)
		verifyStackedSSTs(t, inSnap)
	}
	t.Run("large-batch", func(t *testing.T) {
		sendReceive(t, 1<<20)
	})
	t.Run("small-batch", func(t *testing.T) {
		sendReceive(t, 1)
	})
	// TODO(basalt): apply the stacked SSTs to a real engine and verify
	// results, both as a batch and as an ingest.
}
