// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
)

// NewCombinedBatch creates a Batch backed by StateEngine, combined with the
// range-shared engine if present. The MakeCombinedBatch call (which sets
// SecondaryLSM) is done before PinEngineStateForIterators so that the pinned
// iterator includes the secondary LSM.
func (r *Replica) NewCombinedBatch(readCategory fs.ReadCategory) (storage.Batch, error) {
	batch := r.store.StateEngine().NewBatch()
	r.rsStateMu.RLock()
	defer r.rsStateMu.RUnlock()
	var result storage.Batch = batch
	if r.rsStateMu.rsEngine != nil {
		result = storage.MakeCombinedBatch(batch, r.rsStateMu.rsEngine.NewSnapshot())
	}
	if err := result.PinEngineStateForIterators(readCategory); err != nil {
		result.Close()
		return nil, err
	}
	return result, nil
}

// NewCombinedReadOnly creates a ReadWriter backed by StateEngine, combined
// with the range-shared engine if present. The MakeCombinedReaderWriter call
// (which sets SecondaryLSM) is done before PinEngineStateForIterators so
// that the pinned iterator includes the secondary LSM.
func (r *Replica) NewCombinedReadOnly(readCategory fs.ReadCategory) (storage.ReadWriter, error) {
	rw := r.store.StateEngine().NewReadOnly(storage.StandardDurability)
	r.rsStateMu.RLock()
	defer r.rsStateMu.RUnlock()
	var result storage.ReadWriter = rw
	if r.rsStateMu.rsEngine != nil {
		result = storage.MakeCombinedReaderWriter(rw, r.rsStateMu.rsEngine.NewSnapshot())
	}
	if err := result.PinEngineStateForIterators(readCategory); err != nil {
		result.Close()
		return nil, err
	}
	return result, nil
}

// NewCombinedSnapshot creates a Reader snapshot backed by StateEngine,
// combined with the range-shared engine if present. Both snapshots are taken
// atomically under rsStateMu since NewSnapshot pins state at creation time.
func (r *Replica) NewCombinedSnapshot(keyRanges ...roachpb.Span) storage.Reader {
	r.rsStateMu.RLock()
	defer r.rsStateMu.RUnlock()
	snap := r.store.StateEngine().NewSnapshot(keyRanges...)
	if r.rsStateMu.rsEngine == nil {
		return snap
	}
	return storage.MakeCombinedReader(snap, r.rsStateMu.rsEngine.NewSnapshot())
}
