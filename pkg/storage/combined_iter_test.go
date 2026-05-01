// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// TestMultiLSMBasic is a sanity check that Pebble multi-LSM iteration works
// with CRDB's key format before using it through the storage layer.
func TestMultiLSMBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	_ = ctx
	mkOpts := func() *pebble.Options {
		return &pebble.Options{
			Comparer:           &EngineComparer,
			Merger:             pebble.DefaultMerger,
			FS:                 vfs.NewMem(),
			FormatMajorVersion: pebble.FormatNewest,
			KeySchema:          DefaultKeySchema,
			KeySchemas:         sstable.MakeKeySchemas(KeySchemas...),
		}
	}
	// Top (store-local) DB.
	topDB, err := pebble.Open("", mkOpts())
	require.NoError(t, err)
	defer topDB.Close()
	// Secondary (range-shared) DB.
	secDB, err := pebble.Open("", mkOpts())
	require.NoError(t, err)
	defer secDB.Close()
	// Write to top DB.
	topKey := EncodeMVCCKey(MVCCKey{Key: roachpb.Key("a"), Timestamp: hlc.Timestamp{WallTime: 10}})
	topVal, err := EncodeMVCCValue(MVCCValue{Value: roachpb.MakeValueFromString("a10")})
	require.NoError(t, err)
	require.NoError(t, topDB.Set(topKey, topVal, pebble.NoSync))
	// Write to secondary DB and flush to create SSTs.
	secKey := EncodeMVCCKey(MVCCKey{Key: roachpb.Key("b"), Timestamp: hlc.Timestamp{WallTime: 5}})
	secVal, err := EncodeMVCCValue(MVCCValue{Value: roachpb.MakeValueFromString("b5")})
	require.NoError(t, err)
	require.NoError(t, secDB.Set(secKey, secVal, pebble.NoSync))
	require.NoError(t, secDB.Flush())
	// Get version handle from secondary.
	vh := secDB.NewLSMVersionHandle()
	require.True(t, vh.IsSet())
	defer vh.Close()
	// Create iterator with SecondaryLSM on the top DB.
	iterOpts := &pebble.IterOptions{
		LowerBound:   EncodeMVCCKeyPrefix(roachpb.Key("a")),
		UpperBound:   EncodeMVCCKeyPrefix(roachpb.Key("z")),
		SecondaryLSM: vh.Clone(),
	}
	iter, err := topDB.NewIter(iterOpts)
	require.NoError(t, err)
	defer iter.Close()
	// Should see both keys.
	iter.First()
	require.True(t, iter.Valid())
	k1, _ := DecodeMVCCKey(iter.Key())
	t.Logf("first key: %s/%d", string(k1.Key), k1.Timestamp.WallTime)
	require.Equal(t, "a", string(k1.Key))
	iter.Next()
	require.True(t, iter.Valid())
	k2, _ := DecodeMVCCKey(iter.Key())
	t.Logf("second key: %s/%d", string(k2.Key), k2.Timestamp.WallTime)
	require.Equal(t, "b", string(k2.Key))
	iter.Next()
	require.False(t, iter.Valid())
}

// TestCombinedIteration tests combined iteration over a store-local engine
// and a range-shared secondary LSM using datadriven tests.
func TestCombinedIteration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	datadriven.Walk(t, "testdata/combined_iter", func(t *testing.T, path string) {
		s := newCombinedIterTestState(t)
		defer s.close()
		datadriven.RunTest(t, path, s.run)
	})
}

type combinedIterTestState struct {
	t         *testing.T
	ctx       context.Context
	primary   Engine
	secondary *pebble.DB
	// Active reader/iterator state.
	reader  Reader
	closeFn func() // closes the reader and any combined wrapper
	iter    MVCCIterator
}

func newCombinedIterTestState(t *testing.T) *combinedIterTestState {
	primary := NewDefaultInMemForTesting()
	secondaryOpts := &pebble.Options{
		Comparer:           &EngineComparer,
		Merger:             pebble.DefaultMerger,
		FS:                 vfs.NewMem(),
		FormatMajorVersion: pebble.FormatNewest,
		KeySchema:          DefaultKeySchema,
		KeySchemas:         sstable.MakeKeySchemas(KeySchemas...),
	}
	db, err := pebble.Open("", secondaryOpts)
	require.NoError(t, err)
	return &combinedIterTestState{
		t:         t,
		ctx:       context.Background(),
		primary:   primary,
		secondary: db,
	}
}

func (s *combinedIterTestState) close() {
	if s.iter != nil {
		s.iter.Close()
	}
	if s.closeFn != nil {
		s.closeFn()
	}
	s.secondary.Close()
	s.primary.Close()
}

func (s *combinedIterTestState) run(t *testing.T, d *datadriven.TestData) string {
	switch d.Cmd {
	case "put":
		return s.cmdPut(t, d)
	case "put-shared":
		return s.cmdPutShared(t, d)
	case "del":
		return s.cmdDel(t, d)
	case "del-range":
		return s.cmdDelRange(t, d)
	case "combined-iter":
		return s.cmdCombinedIter(t, d)
	case "iter":
		return s.cmdIter(t, d)
	case "seek-ge":
		return s.cmdSeekGE(t, d)
	case "next":
		return s.cmdNext(t, d)
	case "prev":
		return s.cmdPrev(t, d)
	case "seek-lt":
		return s.cmdSeekLT(t, d)
	case "close-iter":
		return s.cmdCloseIter(t, d)
	case "close-reader":
		return s.cmdCloseReader(t, d)
	default:
		return fmt.Sprintf("unknown command: %s", d.Cmd)
	}
}

func (s *combinedIterTestState) scanKey(d *datadriven.TestData) roachpb.Key {
	var k string
	d.ScanArgs(s.t, "key", &k)
	return roachpb.Key(k)
}

func (s *combinedIterTestState) scanTS(d *datadriven.TestData) hlc.Timestamp {
	var ts int
	d.ScanArgs(s.t, "ts", &ts)
	return hlc.Timestamp{WallTime: int64(ts)}
}

func (s *combinedIterTestState) maybeScanTS(d *datadriven.TestData) hlc.Timestamp {
	for _, arg := range d.CmdArgs {
		if arg.Key == "ts" {
			var ts int
			d.ScanArgs(s.t, "ts", &ts)
			return hlc.Timestamp{WallTime: int64(ts)}
		}
	}
	return hlc.Timestamp{}
}

func (s *combinedIterTestState) scanValue(d *datadriven.TestData) roachpb.Value {
	var v string
	d.ScanArgs(s.t, "value", &v)
	return roachpb.MakeValueFromString(v)
}

// put key=<k> ts=<walltime> value=<v>
func (s *combinedIterTestState) cmdPut(t *testing.T, d *datadriven.TestData) string {
	key := s.scanKey(d)
	ts := s.scanTS(d)
	val := s.scanValue(d)
	_, err := MVCCPut(s.ctx, s.primary, key, ts, val, MVCCWriteOptions{})
	require.NoError(t, err)
	return "ok"
}

// put-shared key=<k> ts=<walltime> value=<v>
func (s *combinedIterTestState) cmdPutShared(t *testing.T, d *datadriven.TestData) string {
	key := s.scanKey(d)
	ts := s.scanTS(d)
	val := s.scanValue(d)
	mvccKey := MVCCKey{Key: key, Timestamp: ts}
	encodedKey := EncodeMVCCKey(mvccKey)
	mvccVal := MVCCValue{Value: val}
	encodedVal, err := EncodeMVCCValue(mvccVal)
	require.NoError(t, err)
	require.NoError(t, s.secondary.Set(encodedKey, encodedVal, pebble.NoSync))
	require.NoError(t, s.secondary.Flush())
	return "ok"
}

// del key=<k> ts=<walltime>
func (s *combinedIterTestState) cmdDel(t *testing.T, d *datadriven.TestData) string {
	key := s.scanKey(d)
	ts := s.scanTS(d)
	_, _, err := MVCCDelete(s.ctx, s.primary, key, ts, MVCCWriteOptions{})
	require.NoError(t, err)
	return "ok"
}

// del-range start=<k> end=<k>
func (s *combinedIterTestState) cmdDelRange(t *testing.T, d *datadriven.TestData) string {
	var start, end string
	d.ScanArgs(t, "start", &start)
	d.ScanArgs(t, "end", &end)
	// Write a raw range deletion over the MVCC key space. This shadows all
	// keys in the secondary LSM within this range.
	startKey := EncodeMVCCKeyPrefix(roachpb.Key(start))
	endKey := EncodeMVCCKeyPrefix(roachpb.Key(end))
	b := s.primary.NewBatch()
	require.NoError(t, b.ClearRawEncodedRange(startKey, endKey))
	require.NoError(t, b.Commit(false))
	b.Close()
	return "ok"
}

// combined-iter type=(readonly|batch|snapshot) [lower=<k>] [upper=<k>] [prefix]
func (s *combinedIterTestState) cmdCombinedIter(t *testing.T, d *datadriven.TestData) string {
	s.closeIterAndReader()
	var readerType string
	d.ScanArgs(t, "type", &readerType)
	vh := s.secondary.NewLSMVersionHandle()
	require.True(t, vh.IsSet(), "NewLSMVersionHandle returned unset handle")
	switch readerType {
	case "readonly":
		rw := s.primary.NewReadOnly(StandardDurability)
		rw.SetSecondaryLSM(vh)
		require.NoError(t, rw.PinEngineStateForIterators(0))
		s.reader = rw
		s.closeFn = func() { rw.Close() }
	case "batch":
		b := s.primary.NewBatch()
		b.SetSecondaryLSM(vh)
		require.NoError(t, b.PinEngineStateForIterators(0))
		s.reader = b
		s.closeFn = func() { b.Close() }
	case "snapshot":
		snap := s.primary.NewSnapshot()
		snap.SetSecondaryLSM(vh)
		s.reader = snap
		s.closeFn = func() { snap.Close() }
	default:
		t.Fatalf("unknown reader type: %s", readerType)
	}
	return s.createIter(t, d)
}

// iter [lower=<k>] [upper=<k>]
func (s *combinedIterTestState) cmdIter(t *testing.T, d *datadriven.TestData) string {
	s.closeIterAndReader()
	ro := s.primary.NewReadOnly(StandardDurability)
	require.NoError(t, ro.PinEngineStateForIterators(0))
	s.reader = ro
	s.closeFn = func() { ro.Close() }
	return s.createIter(t, d)
}

func (s *combinedIterTestState) createIter(t *testing.T, d *datadriven.TestData) string {
	if s.iter != nil {
		s.iter.Close()
		s.iter = nil
	}
	opts := IterOptions{}
	for _, arg := range d.CmdArgs {
		switch arg.Key {
		case "lower":
			opts.LowerBound = roachpb.Key(arg.Vals[0])
		case "upper":
			opts.UpperBound = roachpb.Key(arg.Vals[0])
		case "prefix":
			opts.Prefix = true
		}
	}
	if !opts.Prefix && opts.LowerBound == nil && opts.UpperBound == nil {
		opts.LowerBound = roachpb.KeyMin
		opts.UpperBound = roachpb.KeyMax
	}
	var err error
	s.iter, err = s.reader.NewMVCCIterator(s.ctx, MVCCKeyIterKind, opts)
	require.NoError(t, err)
	return "ok"
}

func (s *combinedIterTestState) cmdSeekGE(t *testing.T, d *datadriven.TestData) string {
	key := s.scanKey(d)
	ts := s.maybeScanTS(d)
	s.iter.SeekGE(MVCCKey{Key: key, Timestamp: ts})
	return s.iterStatus()
}

func (s *combinedIterTestState) cmdNext(t *testing.T, d *datadriven.TestData) string {
	s.iter.Next()
	return s.iterStatus()
}

func (s *combinedIterTestState) cmdPrev(t *testing.T, d *datadriven.TestData) string {
	s.iter.Prev()
	return s.iterStatus()
}

func (s *combinedIterTestState) cmdSeekLT(t *testing.T, d *datadriven.TestData) string {
	key := s.scanKey(d)
	ts := s.maybeScanTS(d)
	s.iter.SeekLT(MVCCKey{Key: key, Timestamp: ts})
	return s.iterStatus()
}

func (s *combinedIterTestState) cmdCloseIter(t *testing.T, d *datadriven.TestData) string {
	if s.iter != nil {
		s.iter.Close()
		s.iter = nil
	}
	return "ok"
}

func (s *combinedIterTestState) cmdCloseReader(t *testing.T, d *datadriven.TestData) string {
	s.closeIterAndReader()
	return "ok"
}

func (s *combinedIterTestState) closeIterAndReader() {
	if s.iter != nil {
		s.iter.Close()
		s.iter = nil
	}
	if s.closeFn != nil {
		s.closeFn()
		s.closeFn = nil
		s.reader = nil
	}
}

func (s *combinedIterTestState) iterStatus() string {
	valid, err := s.iter.Valid()
	if err != nil {
		return fmt.Sprintf("err: %s", err)
	}
	if !valid {
		return "."
	}
	key := s.iter.UnsafeKey()
	val, err := s.iter.UnsafeValue()
	if err != nil {
		return fmt.Sprintf("err: %s", err)
	}
	var valStr string
	if len(val) > 0 {
		mvccVal, err := DecodeMVCCValue(val)
		if err != nil {
			valStr = fmt.Sprintf("<decode-err: %s>", err)
		} else if mvccVal.IsTombstone() {
			valStr = "<tombstone>"
		} else {
			v, err := mvccVal.Value.GetBytes()
			if err == nil {
				valStr = string(v)
			} else {
				valStr = fmt.Sprintf("<bytes-err: %s>", err)
			}
		}
	}
	var buf strings.Builder
	fmt.Fprintf(&buf, "%s/%d", string(key.Key), key.Timestamp.WallTime)
	if valStr != "" {
		fmt.Fprintf(&buf, ": %s", valStr)
	}
	return buf.String()
}
