// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package snaprecv

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/rangedel"
	"github.com/cockroachdb/pebble/rangekey"
)

// MultiSSTWriter is a wrapper around an SSTWriter and SSTSnapshotStorageScratch
// that handles chunking SSTs and persisting them to disk.
//
// When dormant range deletions are present, MultiSSTWriter produces aligned
// upper+lower SST pairs. The upper SST contains normal and dormant RANGEDEL
// keys; the lower SST contains BelowDormant keys. These are ingested as
// StackedLocalSST pairs via IngestAndExciseStacked.
type MultiSSTWriter struct {
	st      *cluster.Settings
	scratch *SSTSnapshotStorageScratch
	currSST storage.SSTWriter
	// localKeySpans are key spans that are considered unsplittable across sstables, and
	// represent the range's range local key spans. In contrast, mvccKeySpan can be split
	// across multiple sstables if one of them exceeds maxSSTSize. The expectation is
	// that for large ranges, keys in mvccKeySpan will dominate in size compared to keys
	// in localKeySpans.
	localKeySpans []roachpb.Span
	mvccKeySpan   roachpb.Span
	// mvccSSTSpans reflects the actual split of the mvccKeySpan into constituent
	// sstables.
	mvccSSTSpans []storage.EngineKeyRange
	// currSpan is the index of the current span being written to. The first
	// len(localKeySpans) spans are localKeySpans, and the rest are mvccSSTSpans.
	// In a sense, currSpan indexes into a slice composed of
	// append(localKeySpans, mvccSSTSpans).
	currSpan int
	// The approximate size of the SST chunk to buffer in memory on the receiver
	// before flushing to disk. Zero disables explicit flushing.
	sstChunkSize int64
	// The total size of the key and value pairs (not the total size of the SSTs),
	// excluding currSST. Updated on SST finalization.
	dataSize int64
	// The total size of the SSTs, excluding currSST. Updated on SST finalization.
	sstSize int64
	// maxSSTSize is the maximum size to use for SSTs containing MVCC/user keys.
	// Once the sstable writer reaches this size, it will be finalized and a new
	// sstable will be created.
	maxSSTSize int64
	// rangeKeyFrag is used to fragment range keys across the SSTs. For each SST
	// (with the exception of the MVCC SST), it's initialized with a range key del
	// for the entire span, but the incoming stream of data may also contain new
	// range keys. As the start key of these incoming range keys increases, the
	// fragmenter emits fragmented range keys into the produced SST.
	rangeKeyFrag rangekey.Fragmenter
	// rangeDelFrag is like rangeKeyFrag, but for range deletions (i.e. operations
	// that simply clear out all keys in a span).
	rangeDelFrag rangedel.Fragmenter
	// currLowerSST is the SST writer for BelowDormant keys. Lazily opened when
	// the first BelowDormant key arrives. Nil when no BelowDormant keys have
	// been seen for the current upper SST.
	currLowerSST *storage.SSTWriter
	// lowerRangeDelFrag fragments BelowDormant range deletions for the lower SST.
	lowerRangeDelFrag rangedel.Fragmenter
	// lowerDataSize and lowerSSTSize track sizes for lower SSTs (excluding the
	// current one).
	lowerDataSize, lowerSSTSize int64
	// lowerSSTFiles tracks the file index within scratch.ssts for each lower SST
	// that was finalized. Used to build StackedLocalSST pairs in Finish.
	lowerSSTFiles []lowerSSTEntry
	// haveDormantKeys is set to true once any dormant range deletion or
	// BelowDormant key is seen.
	haveDormantKeys bool
}

// lowerSSTEntry records the scratch file index for a finalized lower SST and
// the index of the corresponding upper SST in scratch.ssts.
type lowerSSTEntry struct {
	// upperSSTIdx is the scratch file index of the corresponding upper SST.
	upperSSTIdx int
	// lowerSSTIdx is the scratch file index of this lower SST.
	lowerSSTIdx int
}

type MultiSSTWriterOptions struct {
	// SSTChunkSize is the approximate size of the SST chunk buffer before
	// flushing to disk. If zero, there is no explicit flushing until the
	// SST is finalized.
	SSTChunkSize int64
	// MaxSSTSize is the maximum size of an SST containing MVCC/user keys before
	// we finalize it and start a new SST. If zero, there is no limit.
	//
	// This does not affect other SSTs.
	MaxSSTSize int64
}

// TODO(basalt): make correctness more robust.
//
// The correctness of the size based rollover logic in MultiSSTWriter is very
// subtle, and relies on some properties that are not properly documented.
// Specifically, consider the case of a RANGEDEL [b, d), RANGEKEY [b, d), and
// point key b. There is no promise on what order these will show up in the
// ReadOne stream. They may even be split across different batches and so
// arrive in different ReadOne calls. But we can't afford to have a size-based
// rollover in between any of these keys since we will have overlapping SSTs.
//
// One observation is that size-based rollover only happens on the MVCC span,
// where every point key is of the form b@ts. This will sort after the
// rangedel and rangekey spans that start at b (TODO(sumeer): we also fragment
// rangedels and rangekeys at user key boundaries, so there can be spans like
// [b@4, d) after fragmentation -- so we may also be relying on the fact that
// ScanInternal defragments spans).
//
// So then we only have to concern ourselves with rollover in between a
// RANGEDEL [b, d) and a RANGEKEY [b, d). This doesn't happen because size
// based rollover uses currSST.DataSize, which increases when data is actually
// written to the SSTWriter — i.e., via PutEngineKey, ClearEngineKey (point
// keys), and the fragmenter emit callbacks (emitRangeDel →
// ClearRawEncodedRange, emitRangeKey → PutInternalRangeKey). Range deletions
// and range keys are not written directly to the SST. They go into their
// respective fragmenters, which only emit (and thus increase DataSize) when:
//
// 1. A new span with a different start key is Add'd (triggering fragmentation
// of previously buffered spans)
//
// 2. Truncate is called during finalizeSST
//
// 3. Finish is called at the end
//
// So for rangedel [b, d) followed by rangekey [b, d) (same start key):
// rangeDelFrag.Add(...) buffers the span without emitting (assuming no prior
// pending span with a different start key). DataSize is unchanged. The
// subsequent rolloverSST in putRangeKeyWithEnc sees the same DataSize — no
// rollover between them.
//
// The one scenario where DataSize could change between them is if
// rangeDelFrag.Add([b, d)) triggers emission of a previously buffered span
// (e.g., an earlier [a, c) gets fragmented and [a, b) is emitted). But even
// then, the rollover split would be at key b (the rangekey's start), and
// Truncate(b) would keep the pending rangedel [b, d) in the fragmenter (since
// its start key >= the truncation point). Both the rangedel and rangekey end
// up in the new SST. Correct.
//
// So effectively, rollover is driven by point key writes and fragmenter
// emissions from start-key changes, and the fragmenters ensure range
// deletions/keys at the same position can't be separated across SSTs.

// NewMultiSSTWriter returns an initialized MultiSSTWriter.
func NewMultiSSTWriter(
	ctx context.Context,
	st *cluster.Settings,
	scratch *SSTSnapshotStorageScratch,
	localKeySpans []roachpb.Span,
	mvccKeySpan roachpb.Span,
	opts MultiSSTWriterOptions,
) (*MultiSSTWriter, error) {
	msstw := &MultiSSTWriter{
		st:            st,
		scratch:       scratch,
		localKeySpans: localKeySpans,
		mvccKeySpan:   mvccKeySpan,
		mvccSSTSpans: []storage.EngineKeyRange{{
			Start: storage.EngineKey{Key: mvccKeySpan.Key},
			End:   storage.EngineKey{Key: mvccKeySpan.EndKey},
		}},
		sstChunkSize: opts.SSTChunkSize,
		maxSSTSize:   opts.MaxSSTSize,
	}
	msstw.rangeKeyFrag = rangekey.Fragmenter{
		Cmp:    storage.EngineComparer.Compare,
		Format: storage.EngineComparer.FormatKey,
		Emit:   msstw.emitRangeKey,
	}
	msstw.rangeDelFrag = rangedel.Fragmenter{
		Cmp:    storage.EngineComparer.Compare,
		Format: storage.EngineComparer.FormatKey,
		Emit:   msstw.emitRangeDel,
	}

	if err := msstw.initSST(ctx); err != nil {
		return msstw, err
	}
	return msstw, nil
}

// DisableSizeBasedRollover disables size-based SST splitting. Called by the
// receiver when HaveDormantRangeDel is first seen on the stream. Once dormant
// keys exist, we cannot split the MVCC SST because the lower SST must be
// aligned with its upper SST partner.
//
// This should not result in huge sstables since the size of the range in the
// store-local LSM compared to the range-shared LSM will be very small.
func (msstw *MultiSSTWriter) DisableSizeBasedRollover() {
	msstw.maxSSTSize = 0
}

// initLowerSST lazily creates the lower SST writer for BelowDormant keys.
func (msstw *MultiSSTWriter) initLowerSST(ctx context.Context) error {
	newSSTFile, err := msstw.scratch.NewFile(ctx, msstw.sstChunkSize)
	if err != nil {
		return errors.Wrap(err, "failed to create lower sst file")
	}
	lowerSST := storage.MakeIngestionSSTWriter(ctx, msstw.st, newSSTFile)
	msstw.currLowerSST = &lowerSST
	msstw.lowerRangeDelFrag = rangedel.Fragmenter{
		Cmp:    storage.EngineComparer.Compare,
		Format: storage.EngineComparer.FormatKey,
		Emit:   msstw.emitLowerRangeDel,
	}
	return nil
}

func (msstw *MultiSSTWriter) emitLowerRangeDel(span rangedel.Span) {
	// Lower SST only has normal range deletions (BelowDormant rangedels).
	if err := msstw.currLowerSST.ClearRawEncodedRange(span.Start, span.End); err != nil {
		panic(fmt.Sprintf("failed to put range del in lower sst: %s", err))
	}
}

// finalizeLowerSST finishes the lower SST and records its file index.
// upperSSTIdx is the index of the corresponding upper SST in scratch.ssts.
func (msstw *MultiSSTWriter) finalizeLowerSST(upperSSTIdx int) error {
	if msstw.currLowerSST == nil {
		return nil
	}
	msstw.lowerRangeDelFrag.Finish()
	if err := msstw.currLowerSST.Finish(); err != nil {
		return errors.Wrap(err, "failed to finish lower sst")
	}
	msstw.lowerDataSize += msstw.currLowerSST.DataSize
	lowerMeta := msstw.currLowerSST.Meta
	msstw.lowerSSTSize += int64(lowerMeta.Size)
	// The lower SST file was the last file allocated via scratch.NewFile,
	// so its index is len(scratch.ssts)-1.
	msstw.lowerSSTFiles = append(msstw.lowerSSTFiles, lowerSSTEntry{
		upperSSTIdx: upperSSTIdx,
		lowerSSTIdx: len(msstw.scratch.ssts) - 1,
	})
	msstw.currLowerSST.Close()
	msstw.currLowerSST = nil
	return nil
}

func (msstw *MultiSSTWriter) ReadOne(
	ctx context.Context,
	ek storage.EngineKey,
	expectInternalKeys bool,
	batchReader *storage.BatchReader,
	isBelowDormant bool,
) error {
	if isBelowDormant {
		return msstw.readOneBelowDormant(ctx, ek, batchReader)
	}
	switch batchReader.KeyKind() {
	case pebble.InternalKeyKindSet, pebble.InternalKeyKindSetWithDelete:
		if err := msstw.put(ctx, ek, batchReader.Value()); err != nil {
			return errors.Wrapf(err, "writing sst for raft snapshot")
		}
	case pebble.InternalKeyKindDelete, pebble.InternalKeyKindDeleteSized:
		if !expectInternalKeys {
			return errors.AssertionFailedf("unexpected batch entry key kind %d", batchReader.KeyKind())
		}
		if err := msstw.putInternalPointKey(ctx, batchReader.Key(), batchReader.KeyKind(), nil); err != nil {
			return errors.Wrapf(err, "writing sst for raft snapshot")
		}
	case pebble.InternalKeyKindRangeDelete:
		if !expectInternalKeys {
			return errors.AssertionFailedf("unexpected batch entry key kind %d", batchReader.KeyKind())
		}
		start := batchReader.Key()
		end, err := batchReader.EndKey()
		if err != nil {
			return err
		}
		if err := msstw.putInternalRangeDelete(ctx, start, end); err != nil {
			return errors.Wrapf(err, "writing sst for raft snapshot")
		}
	case pebble.InternalKeyKindRangeDeleteDormant:
		if !expectInternalKeys {
			return errors.AssertionFailedf("unexpected batch entry key kind %d", batchReader.KeyKind())
		}
		start := batchReader.Key()
		end, err := batchReader.EndKey()
		if err != nil {
			return err
		}
		if err := msstw.putInternalRangeDormantDelete(ctx, start, end); err != nil {
			return errors.Wrapf(err, "writing sst for raft snapshot")
		}
	case pebble.InternalKeyKindRangeKeyUnset, pebble.InternalKeyKindRangeKeyDelete:
		if !expectInternalKeys {
			return errors.AssertionFailedf("unexpected batch entry key kind %d", batchReader.KeyKind())
		}
		start := batchReader.Key()
		end, err := batchReader.EndKey()
		if err != nil {
			return err
		}
		rangeKeys, err := batchReader.RawRangeKeys()
		if err != nil {
			return err
		}
		for _, rkv := range rangeKeys {
			err := msstw.putInternalRangeKey(ctx, start, end, rkv)
			if err != nil {
				return errors.Wrapf(err, "writing sst for raft snapshot")
			}
		}
	case pebble.InternalKeyKindRangeKeySet:
		start := ek
		end, err := batchReader.EngineEndKey()
		if err != nil {
			return err
		}
		rangeKeys, err := batchReader.EngineRangeKeys()
		if err != nil {
			return err
		}
		for _, rkv := range rangeKeys {
			err := msstw.putRangeKey(ctx, start.Key, end.Key, rkv.Version, rkv.Value)
			if err != nil {
				return errors.Wrapf(err, "writing sst for raft snapshot")
			}
		}
	default:
		return errors.AssertionFailedf("unexpected batch entry key kind %d", batchReader.KeyKind())
	}
	return nil
}

// readOneBelowDormant handles a single BelowDormant key, writing it to the
// lower SST. BelowDormant data contains only point keys and range deletions
// (no range keys or dormant rangedels).
func (msstw *MultiSSTWriter) readOneBelowDormant(
	ctx context.Context, ek storage.EngineKey, batchReader *storage.BatchReader,
) error {
	if !msstw.currSpanIsMVCCSpan() {
		return errors.AssertionFailedf("BelowDormant key %s outside MVCC span", ek)
	}
	msstw.haveDormantKeys = true
	if msstw.currLowerSST == nil {
		if err := msstw.initLowerSST(ctx); err != nil {
			return err
		}
	}
	switch batchReader.KeyKind() {
	case pebble.InternalKeyKindSet, pebble.InternalKeyKindSetWithDelete:
		if err := msstw.currLowerSST.PutEngineKey(ek, batchReader.Value()); err != nil {
			return errors.Wrap(err, "writing lower sst for raft snapshot")
		}
	case pebble.InternalKeyKindDelete, pebble.InternalKeyKindDeleteSized:
		if err := msstw.currLowerSST.ClearEngineKey(ek, storage.ClearOptions{}); err != nil {
			return errors.Wrap(err, "writing lower sst for raft snapshot")
		}
	case pebble.InternalKeyKindRangeDelete:
		start := batchReader.Key()
		end, err := batchReader.EndKey()
		if err != nil {
			return err
		}
		msstw.lowerRangeDelFrag.Add(rangedel.Span{
			Start: start, End: end, Keys: []rangedel.Key{{
				Trailer: pebble.MakeInternalKeyTrailer(0, pebble.InternalKeyKindRangeDelete),
			}},
		})
	default:
		return errors.AssertionFailedf(
			"unexpected BelowDormant key kind %d", batchReader.KeyKind())
	}
	return nil
}

// EstimatedDataSize returns the sum of lengths of keys and values passed
// to any past or current SST. This is monotonically increasing.
//
// Note that this is not the same as the size of the SSTs themselves, as
// SST files carry additional data but also use compression.
func (msstw *MultiSSTWriter) EstimatedDataSize() int64 {
	return msstw.dataSize + msstw.currSST.DataSize
}

func (msstw *MultiSSTWriter) emitRangeKey(key rangekey.Span) {
	for i := range key.Keys {
		if err := msstw.currSST.PutInternalRangeKey(key.Start, key.End, key.Keys[i]); err != nil {
			panic(fmt.Sprintf("failed to put range key in sst: %s", err))
		}
	}
}

func (msstw *MultiSSTWriter) emitRangeDel(span rangedel.Span) {
	if err := msstw.currSST.AddRangeDeleteSpan(span); err != nil {
		panic(fmt.Sprintf("failed to put range del in sst: %s", err))
	}
}

// currentSpan returns the current user-provided span that
// is being written to. Note that this does not account for
// mvcc keys being split across multiple sstables.
func (msstw *MultiSSTWriter) currentSpan() roachpb.Span {
	if msstw.currSpanIsMVCCSpan() {
		return msstw.mvccKeySpan
	}
	return msstw.localKeySpans[msstw.currSpan]
}

func (msstw *MultiSSTWriter) currSpanIsMVCCSpan() bool {
	if msstw.currSpan >= len(msstw.localKeySpans)+len(msstw.mvccSSTSpans) {
		panic("current span is out of bounds")
	}
	return msstw.currSpan >= len(msstw.localKeySpans)
}

func (msstw *MultiSSTWriter) initSST(ctx context.Context) error {
	newSSTFile, err := msstw.scratch.NewFile(ctx, msstw.sstChunkSize)
	if err != nil {
		return errors.Wrap(err, "failed to create new sst file")
	}
	newSST := storage.MakeIngestionSSTWriter(ctx, msstw.st, newSSTFile)
	msstw.currSST = newSST

	// Add a RangeKeyDel as well as a range del for the entire bounds of the SST,
	// meaning upon ingestion any range and point keys existing in the span will
	// be deleted.
	// Note that the MVCC span will be excised on ingest, so this step is skipped
	// for it.
	if !msstw.currSpanIsMVCCSpan() {
		sp := msstw.currentSpan()
		startKey := storage.EngineKey{Key: sp.Key}.Encode()
		endKey := storage.EngineKey{Key: sp.EndKey}.Encode()
		msstw.rangeKeyFrag.Add(rangekey.Span{
			Start: startKey, End: endKey, Keys: []rangekey.Key{{
				Trailer: pebble.MakeInternalKeyTrailer(0, pebble.InternalKeyKindRangeKeyDelete),
			}},
		})
		msstw.rangeDelFrag.Add(rangedel.Span{
			Start: startKey, End: endKey, Keys: []rangedel.Key{{
				Trailer: pebble.MakeInternalKeyTrailer(0, pebble.InternalKeyKindRangeDelete),
			}}},
		)
	}
	return nil
}

// NB: when nextKey is non-nil, do not do anything in this function to cause
// nextKey at the caller to escape to the heap.
func (msstw *MultiSSTWriter) finalizeSST(ctx context.Context, nextKey *storage.EngineKey) error {
	var currEngineSpan storage.EngineKeyRange
	if msstw.currSpanIsMVCCSpan() {
		currEngineSpan = msstw.mvccSSTSpans[msstw.currSpan-len(msstw.localKeySpans)]
	} else {
		cur := msstw.currentSpan()
		currEngineSpan = storage.EngineKeyRange{
			Start: storage.EngineKey{Key: cur.Key},
			End:   storage.EngineKey{Key: cur.EndKey},
		}
	}

	// If we're at the last span, call Finish on the fragmenters. If we're not at the
	// last span, call Truncate.
	if msstw.currSpan == len(msstw.localKeySpans)+len(msstw.mvccSSTSpans)-1 {
		msstw.rangeKeyFrag.Finish()
		msstw.rangeDelFrag.Finish()
	} else {
		msstw.rangeKeyFrag.Truncate(currEngineSpan.End.Encode())
		msstw.rangeDelFrag.Truncate(currEngineSpan.End.Encode())
	}

	// Record the upper SST's index in scratch.ssts before finishing it, since
	// finishing the lower SST will allocate... actually, the upper SST file was
	// already allocated. The index of the upper SST's file is the file index
	// before the lower SST was created. We track this by noting that
	// initSST's NewFile call created the file, and initLowerSST's NewFile call
	// (if any) created the next one. The upper SST's file index in scratch.ssts
	// is: if no lower SST exists, it's len(scratch.ssts)-1. If a lower SST
	// exists, it's len(scratch.ssts)-2 (since initLowerSST added one more file).
	upperSSTIdx := len(msstw.scratch.ssts) - 1
	if msstw.currLowerSST != nil {
		upperSSTIdx = len(msstw.scratch.ssts) - 2
	}
	// Finalize the lower SST (if any) before finishing the upper SST so we
	// have the file index mapping.
	if err := msstw.finalizeLowerSST(upperSSTIdx); err != nil {
		return err
	}
	err := msstw.currSST.Finish()
	if err != nil {
		return errors.Wrap(err, "failed to finish sst")
	}
	if nextKey != nil {
		meta := msstw.currSST.Meta
		encodedNextKey := nextKey.Encode()
		// Use nextKeyCopy for the remainder of this function. Calling
		// errors.Errorf with nextKey caused it to escape to the heap in the
		// caller of finalizeSST (even when finalizeSST was not called), which was
		// costly.
		nextKeyCopy := *nextKey
		if meta.HasPointKeys && storage.EngineComparer.Compare(meta.LargestPoint.UserKey, encodedNextKey) > 0 {
			metaEndKey, ok := storage.DecodeEngineKey(meta.LargestPoint.UserKey)
			if !ok {
				return errors.Errorf("MultiSSTWriter created overlapping ingestion sstables: sstable largest point key %s > next sstable start key %s",
					meta.LargestPoint.UserKey, nextKeyCopy)
			}
			return errors.Errorf("MultiSSTWriter created overlapping ingestion sstables: sstable largest point key %s > next sstable start key %s",
				metaEndKey, nextKeyCopy)
		}
		if meta.HasRangeDelKeys && storage.EngineComparer.Compare(meta.LargestRangeDel.UserKey, encodedNextKey) > 0 {
			metaEndKey, ok := storage.DecodeEngineKey(meta.LargestRangeDel.UserKey)
			if !ok {
				return errors.Errorf("MultiSSTWriter created overlapping ingestion sstables: sstable largest range del %s > next sstable start key %s",
					meta.LargestRangeDel.UserKey, nextKeyCopy)
			}
			return errors.Errorf("MultiSSTWriter created overlapping ingestion sstables: sstable largest range del %s > next sstable start key %s",
				metaEndKey, nextKeyCopy)
		}
		if meta.HasRangeKeys && storage.EngineComparer.Compare(meta.LargestRangeKey.UserKey, encodedNextKey) > 0 {
			metaEndKey, ok := storage.DecodeEngineKey(meta.LargestRangeKey.UserKey)
			if !ok {
				return errors.Errorf("MultiSSTWriter created overlapping ingestion sstables: sstable largest range key %s > next sstable start key %s",
					meta.LargestRangeKey.UserKey, nextKeyCopy)
			}
			return errors.Errorf("MultiSSTWriter created overlapping ingestion sstables: sstable largest range key %s > next sstable start key %s",
				metaEndKey, nextKeyCopy)
		}
	}
	msstw.dataSize += msstw.currSST.DataSize
	msstw.sstSize += int64(msstw.currSST.Meta.Size)
	msstw.currSpan++
	msstw.currSST.Close()
	// Zero the SSTWriter to avoid double-counting in EstimatedDataSize.
	msstw.currSST = storage.SSTWriter{}
	return nil
}

// rolloverSST rolls the underlying SST writer over to the appropriate SST
// writer for writing a point/range key at key. For point keys, endKey and key
// must equal each other.
func (msstw *MultiSSTWriter) rolloverSST(
	ctx context.Context, key storage.EngineKey, endKey storage.EngineKey,
) error {
	for msstw.currentSpan().EndKey.Compare(key.Key) <= 0 {
		// Finish the current SST, write to the file, and move to the next key
		// range.
		if err := msstw.finalizeSST(ctx, &key); err != nil {
			return err
		}
		if err := msstw.initSST(ctx); err != nil {
			return err
		}
	}
	currSpan := msstw.currentSpan()
	if currSpan.Key.Compare(key.Key) > 0 || currSpan.EndKey.Compare(endKey.Key) < 0 {
		if !key.Key.Equal(endKey.Key) {
			return errors.AssertionFailedf("client error: expected %s to fall in one of %s or %s",
				roachpb.Span{Key: key.Key, EndKey: endKey.Key}, msstw.localKeySpans, msstw.mvccKeySpan)
		}
		return errors.AssertionFailedf("client error: expected %s to fall in one of %s or %s", key, msstw.localKeySpans, msstw.mvccKeySpan)
	}
	if msstw.currSpanIsMVCCSpan() && msstw.maxSSTSize > 0 && msstw.currSST.DataSize > msstw.maxSSTSize {
		// We're in an MVCC / user keys span, and the current sstable has exceeded
		// the max size for MVCC sstables that we should be creating. Split this
		// sstable into smaller ones. We do this by splitting the mvccKeySpan
		// from [oldStartKey, oldEndKey) to [oldStartKey, key) and [key, oldEndKey).
		// The split spans are added to msstw.mvccSSTSpans.
		currSpan := &msstw.mvccSSTSpans[msstw.currSpan-len(msstw.localKeySpans)]
		if bytes.Equal(currSpan.Start.Key, key.Key) && bytes.Equal(currSpan.Start.Version, key.Version) {
			panic("unexpectedly reached max sstable size at start of an mvcc sstable span")
		}
		oldEndKey := currSpan.End
		currSpan.End = key.Copy()
		newSpan := storage.EngineKeyRange{Start: currSpan.End, End: oldEndKey}
		msstw.mvccSSTSpans = append(msstw.mvccSSTSpans, newSpan)
		if msstw.currSpan < len(msstw.localKeySpans)+len(msstw.mvccSSTSpans)-2 {
			// This should never happen; we only split sstables when we're at the end
			// of mvccSSTSpans.
			panic("unexpectedly split an earlier mvcc sstable span in MultiSSTWriter")
		}
		if err := msstw.finalizeSST(ctx, &key); err != nil {
			return err
		}
		if err := msstw.initSST(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (msstw *MultiSSTWriter) put(ctx context.Context, key storage.EngineKey, value []byte) error {
	if err := msstw.rolloverSST(ctx, key, key); err != nil {
		return err
	}
	if err := msstw.currSST.PutEngineKey(key, value); err != nil {
		return errors.Wrap(err, "failed to put in sst")
	}
	return nil
}

func (msstw *MultiSSTWriter) putInternalPointKey(
	ctx context.Context, key []byte, kind pebble.InternalKeyKind, val []byte,
) error {
	decodedKey, ok := storage.DecodeEngineKey(key)
	if !ok {
		return errors.New("cannot decode engine key")
	}
	if err := msstw.rolloverSST(ctx, decodedKey, decodedKey); err != nil {
		return err
	}
	var err error
	switch kind {
	case pebble.InternalKeyKindSet, pebble.InternalKeyKindSetWithDelete:
		err = msstw.currSST.PutEngineKey(decodedKey, val)
	case pebble.InternalKeyKindDelete, pebble.InternalKeyKindDeleteSized:
		err = msstw.currSST.ClearEngineKey(decodedKey, storage.ClearOptions{ValueSizeKnown: false})
	default:
		err = errors.New("unexpected key kind")
	}
	if err != nil {
		return errors.Wrap(err, "failed to put in sst")
	}
	return nil
}

func decodeRangeStartEnd(
	start, end []byte,
) (decodedStart, decodedEnd storage.EngineKey, err error) {
	var emptyKey storage.EngineKey
	decodedStart, ok := storage.DecodeEngineKey(start)
	if !ok {
		return emptyKey, emptyKey, errors.New("cannot decode start engine key")
	}
	decodedEnd, ok = storage.DecodeEngineKey(end)
	if !ok {
		return emptyKey, emptyKey, errors.New("cannot decode end engine key")
	}
	if decodedStart.Key.Compare(decodedEnd.Key) >= 0 {
		return emptyKey, emptyKey, errors.AssertionFailedf("start key %s must be before end key %s", end, start)
	}
	return decodedStart, decodedEnd, nil
}

func (msstw *MultiSSTWriter) putInternalRangeDelete(ctx context.Context, start, end []byte) error {
	decodedStart, decodedEnd, err := decodeRangeStartEnd(start, end)
	if err != nil {
		return err
	}
	if err := msstw.rolloverSST(ctx, decodedStart, decodedEnd); err != nil {
		return err
	}
	msstw.rangeDelFrag.Add(rangedel.Span{
		Start: start, End: end, Keys: []rangedel.Key{{
			Trailer: pebble.MakeInternalKeyTrailer(0, pebble.InternalKeyKindRangeDelete),
		}},
	})
	return nil
}

// putInternalRangeDormantDelete adds a dormant range deletion to the upper
// SST's range deletion fragmenter. The fragmenter produces spans containing
// mixed normal/dormant RANGEDEL keys that are written via AddRangeDeleteSpan.
func (msstw *MultiSSTWriter) putInternalRangeDormantDelete(
	ctx context.Context, start, end []byte,
) error {
	decodedStart, decodedEnd, err := decodeRangeStartEnd(start, end)
	if err != nil {
		return err
	}
	if err := msstw.rolloverSST(ctx, decodedStart, decodedEnd); err != nil {
		return err
	}
	msstw.haveDormantKeys = true
	msstw.rangeDelFrag.Add(rangedel.Span{
		Start: start, End: end, Keys: []rangedel.Key{{
			Trailer: pebble.MakeInternalKeyTrailer(0, pebble.InternalKeyKindRangeDeleteDormant),
		}},
	})
	return nil
}

func (msstw *MultiSSTWriter) putInternalRangeKey(
	ctx context.Context, start, end []byte, key rangekey.Key,
) error {
	return msstw.putRangeKeyWithEnc(ctx, storage.EngineKeyRange{}, [2][]byte{start, end}, key)
}

// putRangeKeyWithEnc is the internal implementation of putInternalRangeKey and
// putRangeKey. We need both the encoded and decoded forms of the key range
// here. The caller must supply at least one of `dec` or `enc` depending on what
// they have available.
func (msstw *MultiSSTWriter) putRangeKeyWithEnc(
	ctx context.Context, dec storage.EngineKeyRange, enc [2][]byte, key rangekey.Key,
) error {
	haveDec, haveEnc := len(dec.End.Key) != 0, len(enc[1]) != 0
	switch {
	case !haveDec && !haveEnc:
		return errors.AssertionFailedf("key range must be specified either in encoded or decoded form")
	case !haveDec:
		ds, de, err := decodeRangeStartEnd(enc[0], enc[1])
		if err != nil {
			return err
		}
		dec = storage.EngineKeyRange{
			Start: ds,
			End:   de,
		}
	case !haveEnc:
		enc[0] = dec.Start.Encode()
		enc[1] = dec.End.Encode()
	}

	if k, ek := dec.Start.Key, dec.End.Key; k.Compare(ek) >= 0 {
		return errors.AssertionFailedf("start key %s must be before end key %s", k, ek)
	}

	if err := msstw.rolloverSST(ctx, dec.Start, dec.End); err != nil {
		return err
	}

	msstw.rangeKeyFrag.Add(rangekey.Span{
		Start: enc[0],
		End:   enc[1],
		Keys:  []rangekey.Key{key},
	})
	return nil
}

func (msstw *MultiSSTWriter) putRangeKey(
	ctx context.Context, start, end roachpb.Key, suffix []byte, value []byte,
) error {
	return msstw.putRangeKeyWithEnc(
		ctx,
		storage.EngineKeyRange{
			Start: storage.EngineKey{Key: start},
			End:   storage.EngineKey{Key: end},
		},
		[2][]byte{}, // enc
		rangekey.Key{
			Trailer: pebble.MakeInternalKeyTrailer(0, pebble.InternalKeyKindRangeKeySet),
			Suffix:  suffix,
			Value:   value,
		},
	)
}

func (msstw *MultiSSTWriter) Finish(ctx context.Context) (dataSize, sstSize int64, _ error) {
	if msstw.currSpan < (len(msstw.localKeySpans) + len(msstw.mvccSSTSpans)) {
		for {
			if err := msstw.finalizeSST(ctx, nil /* nextKey */); err != nil {
				return 0, 0, err
			}
			if msstw.currSpan >= (len(msstw.localKeySpans) + len(msstw.mvccSSTSpans)) {
				break
			}
			if err := msstw.initSST(ctx); err != nil {
				return 0, 0, err
			}
		}
	}
	return msstw.dataSize + msstw.lowerDataSize,
		msstw.sstSize + msstw.lowerSSTSize, nil
}

// StackedSSTs returns the list of StackedLocalSST pairs built from the upper
// and lower SST files. Must be called after Finish. For SSTs without a paired
// lower SST, LowerSST.Path is empty.
func (msstw *MultiSSTWriter) StackedSSTs() []pebble.StackedLocalSST {
	allSSTs := msstw.scratch.SSTs()
	// Build a set of lower SST file indices for quick lookup.
	lowerByUpper := make(map[int]int, len(msstw.lowerSSTFiles))
	lowerIdxSet := make(map[int]struct{}, len(msstw.lowerSSTFiles))
	for _, entry := range msstw.lowerSSTFiles {
		lowerByUpper[entry.upperSSTIdx] = entry.lowerSSTIdx
		lowerIdxSet[entry.lowerSSTIdx] = struct{}{}
	}
	var result []pebble.StackedLocalSST
	for i, path := range allSSTs {
		if _, isLower := lowerIdxSet[i]; isLower {
			continue // skip lower SSTs; they are paired with their upper SST
		}
		entry := pebble.StackedLocalSST{
			UpperSST: pebble.LocalSST{Path: path},
		}
		if lowerIdx, ok := lowerByUpper[i]; ok {
			entry.LowerSST = pebble.LocalSST{Path: allSSTs[lowerIdx]}
		}
		result = append(result, entry)
	}
	return result
}

// HaveDormantKeys returns true if any dormant range deletion or BelowDormant
// key was written through this writer.
func (msstw *MultiSSTWriter) HaveDormantKeys() bool {
	return msstw.haveDormantKeys
}

func (msstw *MultiSSTWriter) Close() {
	msstw.currSST.Close()
	if msstw.currLowerSST != nil {
		msstw.currLowerSST.Close()
	}
}
