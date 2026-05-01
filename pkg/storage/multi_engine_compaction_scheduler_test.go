// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"container/heap"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

// testDB implements pebble.DBForCompaction for testing. It tracks the number
// of running compactions, allows configuring the per-engine limit, and records
// Schedule calls.
type testDB struct {
	name                   string
	allowedWithoutPerm     int
	waitingCompaction      *pebble.WaitingCompaction
	scheduleAccept         bool
	scheduledCount         int
	lastScheduledHandle    pebble.CompactionGrantHandle
	pendingHandles         []pebble.CompactionGrantHandle // handles from Schedule calls
	getWaitingCompactionCh chan struct{}                  // if non-nil, blocks GetWaitingCompaction
}

func newTestDB(name string) *testDB {
	return &testDB{
		name:               name,
		allowedWithoutPerm: 1,
		scheduleAccept:     true,
	}
}

func (d *testDB) GetAllowedWithoutPermission() int {
	return d.allowedWithoutPerm
}

func (d *testDB) GetWaitingCompaction() (bool, pebble.WaitingCompaction) {
	if d.getWaitingCompactionCh != nil {
		<-d.getWaitingCompactionCh
	}
	if d.waitingCompaction == nil {
		return false, pebble.WaitingCompaction{}
	}
	return true, *d.waitingCompaction
}

func (d *testDB) Schedule(handle pebble.CompactionGrantHandle) bool {
	d.lastScheduledHandle = handle
	if d.scheduleAccept {
		d.scheduledCount++
		d.pendingHandles = append(d.pendingHandles, handle)
		// Clear waiting compaction after accepting a schedule, like pebble
		// would (the picked compaction is consumed).
		d.waitingCompaction = nil
		return true
	}
	return false
}

func TestMultiEngineCompactionScheduler(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	datadriven.RunTest(t, datapathutils.TestDataPath(t, "multi_engine_compaction_scheduler"),
		func(t *testing.T, d *datadriven.TestData) string {
			return runMultiEngineSchedulerTest(t, d)
		})
}

// testSchedulerEnv holds the state for a datadriven test.
type testSchedulerEnv struct {
	scheduler      *MultiEngineCompactionScheduler
	maxConcurrency int
	depriRatio     float64
	// engines maps names to their engineState + testDB.
	engineCSPlus map[string]CompactionSchedulerPlus
	engineDBs    map[string]*testDB
	// handles tracks grant handles from TrySchedule, keyed by engine name.
	// Multiple handles per engine are stored as name, name.1, name.2, etc.
	handles map[string]pebble.CompactionGrantHandle
}

var testEnvs = map[*testing.T]*testSchedulerEnv{}

func getOrCreateEnv(t *testing.T) *testSchedulerEnv {
	env, ok := testEnvs[t]
	if !ok {
		env = &testSchedulerEnv{
			maxConcurrency: 4,
			depriRatio:     1.0,
			engineCSPlus:   make(map[string]CompactionSchedulerPlus),
			engineDBs:      make(map[string]*testDB),
			handles:        make(map[string]pebble.CompactionGrantHandle),
		}
		env.scheduler = NewMultiEngineCompactionScheduler(SchedulerOptions{
			GetMaxConcurrency:               func() int { return env.maxConcurrency },
			RSEngineDeprioritizationRatio:   func() float64 { return env.depriRatio },
			testingDisableBackgroundGranter: true,
		})
		testEnvs[t] = env
		t.Cleanup(func() {
			env.scheduler.Close()
			delete(testEnvs, t)
		})
	}
	return env
}

func runMultiEngineSchedulerTest(t *testing.T, d *datadriven.TestData) string {
	env := getOrCreateEnv(t)
	var buf strings.Builder
	switch d.Cmd {
	case "init":
		// init max-concurrency=4 depri-ratio=1.0
		for _, arg := range d.CmdArgs {
			switch arg.Key {
			case "max-concurrency":
				v, _ := strconv.Atoi(arg.Vals[0])
				env.maxConcurrency = v
			case "depri-ratio":
				v, _ := strconv.ParseFloat(arg.Vals[0], 64)
				env.depriRatio = v
			}
		}
		return "ok"

	case "open-engine":
		// open-engine name=store-local type=store-local allowed=4
		// open-engine name=rf type=range-flusher allowed=2
		// open-engine name=rs1 type=range-shared allowed=1
		var name string
		var typ EngineType
		allowed := 1
		for _, arg := range d.CmdArgs {
			switch arg.Key {
			case "name":
				name = arg.Vals[0]
			case "type":
				switch arg.Vals[0] {
				case "store-local":
					typ = EngineTypeStoreLocal
				case "range-flusher":
					typ = EngineTypeRangeFlusher
				case "range-shared":
					typ = EngineTypeRangeShared
				}
			case "allowed":
				allowed, _ = strconv.Atoi(arg.Vals[0])
			}
		}
		cs := env.scheduler.OpeningEngine(typ)
		db := newTestDB(name)
		db.allowedWithoutPerm = allowed
		cs.Register(1, db)
		env.engineCSPlus[name] = cs
		env.engineDBs[name] = db
		return "ok"

	case "close-engine":
		// close-engine name=rs1
		name := getCmdArgVal(d, "name")
		cs := env.engineCSPlus[name]
		cs.Unregister()
		delete(env.engineCSPlus, name)
		delete(env.engineDBs, name)
		return "ok"

	case "set-waiting":
		// set-waiting name=store-local optional=false priority=80 score=2.5
		// set-waiting name=rs1 (clears waiting compaction)
		name := getCmdArgVal(d, "name")
		db := env.engineDBs[name]
		optional := getCmdArgBool(d, "optional")
		priority := getCmdArgInt(d, "priority")
		score := getCmdArgFloat(d, "score")
		if priority == 0 && score == 0.0 && !optional {
			// Check if any of these were actually specified.
			hasPriority := hasCmdArg(d, "priority")
			hasScore := hasCmdArg(d, "score")
			if !hasPriority && !hasScore {
				db.waitingCompaction = nil
				return "ok"
			}
		}
		db.waitingCompaction = &pebble.WaitingCompaction{
			Optional: optional,
			Priority: priority,
			Score:    score,
		}
		return "ok"

	case "set-allowed":
		// set-allowed name=store-local allowed=2
		name := getCmdArgVal(d, "name")
		allowed := getCmdArgInt(d, "allowed")
		env.engineDBs[name].allowedWithoutPerm = allowed
		return "ok"

	case "set-schedule-accept":
		// set-schedule-accept name=store-local accept=false
		name := getCmdArgVal(d, "name")
		accept := getCmdArgBool(d, "accept")
		env.engineDBs[name].scheduleAccept = accept
		return "ok"

	case "try-schedule":
		// try-schedule name=store-local
		name := getCmdArgVal(d, "name")
		cs := env.engineCSPlus[name]
		granted, handle := cs.TrySchedule()
		if granted {
			handleKey := nextHandleKey(env, name)
			env.handles[handleKey] = handle
			fmt.Fprintf(&buf, "granted handle=%s", handleKey)
		} else {
			fmt.Fprintf(&buf, "denied")
		}
		return buf.String()

	case "done":
		// done handle=store-local
		handleKey := getCmdArgVal(d, "handle")
		handle, ok := env.handles[handleKey]
		if ok {
			handle.Done()
			delete(env.handles, handleKey)
		} else {
			// Look up in testDB's pending handles from Schedule calls.
			db := env.engineDBs[handleKey]
			if db == nil || len(db.pendingHandles) == 0 {
				return fmt.Sprintf("no handle found for %s", handleKey)
			}
			h := db.pendingHandles[0]
			db.pendingHandles = db.pendingHandles[1:]
			h.Done()
		}
		return "ok"

	case "poke":
		// poke
		env.scheduler.tryGrantForTesting()
		return "ok"

	case "invalidate":
		// invalidate name=rs1
		name := getCmdArgVal(d, "name")
		cs := env.engineCSPlus[name]
		cs.InvalidateRememberedWaitingState()
		return "ok"

	case "set-depri-ratio":
		// set-depri-ratio ratio=2.0
		v := getCmdArgFloat(d, "ratio")
		env.depriRatio = v
		return "ok"

	case "set-max-concurrency":
		// set-max-concurrency max=8
		v := getCmdArgInt(d, "max")
		env.maxConcurrency = v
		return "ok"

	case "status":
		// status
		env.scheduler.mu.Lock()
		fmt.Fprintf(&buf, "global-running: %d", env.scheduler.mu.globalRunning)
		fmt.Fprintf(&buf, "\nrs-heap-size: %d", env.scheduler.mu.rsHeap.Len())
		fmt.Fprintf(&buf, "\nunprobed-size: %d", len(env.scheduler.mu.unprobed))
		// Print per-engine status sorted by name.
		var names []string
		for name := range env.engineCSPlus {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			cs := env.engineCSPlus[name]
			es := cs.(*engineState)
			fmt.Fprintf(&buf, "\n  %s: running=%d waiting=%v", name, es.running, es.waiting)
			if es.hasRemembered {
				fmt.Fprintf(&buf, " remembered={optional=%v,priority=%d,score=%.1f}",
					es.remembered.Optional, es.remembered.Priority, es.remembered.Score)
			}
			if es.inUnprobed {
				fmt.Fprintf(&buf, " in-unprobed")
			}
		}
		fmt.Fprintf(&buf, "\nscheduled-counts:")
		for _, name := range names {
			db := env.engineDBs[name]
			fmt.Fprintf(&buf, " %s=%d", name, db.scheduledCount)
		}
		env.scheduler.mu.Unlock()
		return buf.String()

	default:
		return fmt.Sprintf("unknown command: %s", d.Cmd)
	}
}

func getCmdArgVal(d *datadriven.TestData, key string) string {
	for _, arg := range d.CmdArgs {
		if arg.Key == key {
			return arg.Vals[0]
		}
	}
	return ""
}

func hasCmdArg(d *datadriven.TestData, key string) bool {
	for _, arg := range d.CmdArgs {
		if arg.Key == key {
			return true
		}
	}
	return false
}

func getCmdArgInt(d *datadriven.TestData, key string) int {
	v := getCmdArgVal(d, key)
	if v == "" {
		return 0
	}
	n, _ := strconv.Atoi(v)
	return n
}

func getCmdArgFloat(d *datadriven.TestData, key string) float64 {
	v := getCmdArgVal(d, key)
	if v == "" {
		return 0
	}
	f, _ := strconv.ParseFloat(v, 64)
	return f
}

func getCmdArgBool(d *datadriven.TestData, key string) bool {
	v := getCmdArgVal(d, key)
	return v == "true"
}

func nextHandleKey(env *testSchedulerEnv, name string) string {
	if _, ok := env.handles[name]; !ok {
		return name
	}
	for i := 1; ; i++ {
		key := fmt.Sprintf("%s.%d", name, i)
		if _, ok := env.handles[key]; !ok {
			return key
		}
	}
}

// TestCompareCompactions tests the comparison function directly.
func TestCompareCompactions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		name       string
		a          pebble.WaitingCompaction
		aType      EngineType
		b          pebble.WaitingCompaction
		bType      EngineType
		depriRatio float64
		wantSign   int // -1, 0, or 1
	}{{
		name:       "non-optional beats optional",
		a:          pebble.WaitingCompaction{Optional: false, Priority: 60, Score: 1.0},
		aType:      EngineTypeStoreLocal,
		b:          pebble.WaitingCompaction{Optional: true, Priority: 80, Score: 2.0},
		bType:      EngineTypeStoreLocal,
		depriRatio: 1.0,
		wantSign:   -1,
	}, {
		name:       "higher priority wins",
		a:          pebble.WaitingCompaction{Optional: false, Priority: 80, Score: 1.0},
		aType:      EngineTypeStoreLocal,
		b:          pebble.WaitingCompaction{Optional: false, Priority: 60, Score: 2.0},
		bType:      EngineTypeRangeFlusher,
		depriRatio: 1.0,
		wantSign:   -1,
	}, {
		name:       "higher score wins at equal priority",
		a:          pebble.WaitingCompaction{Optional: false, Priority: 60, Score: 3.0},
		aType:      EngineTypeStoreLocal,
		b:          pebble.WaitingCompaction{Optional: false, Priority: 60, Score: 2.0},
		bType:      EngineTypeStoreLocal,
		depriRatio: 1.0,
		wantSign:   -1,
	}, {
		name:       "deprioritization lowers RS score",
		a:          pebble.WaitingCompaction{Optional: false, Priority: 60, Score: 4.0},
		aType:      EngineTypeRangeShared,
		b:          pebble.WaitingCompaction{Optional: false, Priority: 60, Score: 3.0},
		bType:      EngineTypeStoreLocal,
		depriRatio: 2.0,
		// RS score becomes 4.0/2.0 = 2.0, which is less than store-local's 3.0.
		wantSign: 1,
	}, {
		name:       "deprioritization no effect on non-RS",
		a:          pebble.WaitingCompaction{Optional: false, Priority: 60, Score: 4.0},
		aType:      EngineTypeStoreLocal,
		b:          pebble.WaitingCompaction{Optional: false, Priority: 60, Score: 3.0},
		bType:      EngineTypeRangeFlusher,
		depriRatio: 2.0,
		wantSign:   -1,
	}, {
		name:       "equal compactions",
		a:          pebble.WaitingCompaction{Optional: false, Priority: 60, Score: 1.0},
		aType:      EngineTypeStoreLocal,
		b:          pebble.WaitingCompaction{Optional: false, Priority: 60, Score: 1.0},
		bType:      EngineTypeStoreLocal,
		depriRatio: 1.0,
		wantSign:   0,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := compareCompactions(tt.a, tt.aType, tt.b, tt.bType, tt.depriRatio)
			switch {
			case tt.wantSign < 0:
				require.Less(t, got, 0, "expected a < b (a wins)")
			case tt.wantSign > 0:
				require.Greater(t, got, 0, "expected a > b (b wins)")
			default:
				require.Equal(t, 0, got, "expected equal")
			}
		})
	}
}

// TestMultiEngineCompactionSchedulerConcurrent exercises concurrent
// TrySchedule/Done across multiple engine types and verifies that the
// global concurrency invariant is maintained.
func TestMultiEngineCompactionSchedulerConcurrent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const (
		maxGlobal    = 8
		numRS        = 20
		numOpsPerEng = 50
	)
	scheduler := NewMultiEngineCompactionScheduler(SchedulerOptions{
		GetMaxConcurrency:               func() int { return maxGlobal },
		RSEngineDeprioritizationRatio:   func() float64 { return 1.0 },
		testingDisableBackgroundGranter: true,
	})
	defer scheduler.Close()

	// Track peak global concurrency.
	var peakConcurrency atomic.Int32
	// Create engines.
	type engineAndDB struct {
		cs CompactionSchedulerPlus
		db *concurrentTestDB
	}
	var engines []engineAndDB
	// Store-local.
	{
		cs := scheduler.OpeningEngine(EngineTypeStoreLocal)
		db := newConcurrentTestDB("store-local", 4)
		cs.Register(1, db)
		engines = append(engines, engineAndDB{cs: cs, db: db})
	}
	// Range flusher.
	{
		cs := scheduler.OpeningEngine(EngineTypeRangeFlusher)
		db := newConcurrentTestDB("range-flusher", 4)
		cs.Register(1, db)
		engines = append(engines, engineAndDB{cs: cs, db: db})
	}
	// RS engines.
	for i := 0; i < numRS; i++ {
		cs := scheduler.OpeningEngine(EngineTypeRangeShared)
		db := newConcurrentTestDB(fmt.Sprintf("rs%d", i), 1)
		cs.Register(1, db)
		engines = append(engines, engineAndDB{cs: cs, db: db})
	}
	var wg sync.WaitGroup
	for _, eng := range engines {
		eng := eng
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < numOpsPerEng; i++ {
				granted, handle := eng.cs.TrySchedule()
				if granted {
					// Track concurrency.
					cur := eng.db.running.Add(1)
					for {
						peak := peakConcurrency.Load()
						if cur <= peak || peakConcurrency.CompareAndSwap(peak, cur) {
							break
						}
					}
					// "Do work" then finish.
					eng.db.running.Add(-1)
					handle.Done()
				}
			}
		}()
	}
	wg.Wait()
	// Verify invariants.
	require.LessOrEqual(t, int(peakConcurrency.Load()), maxGlobal,
		"peak concurrency should not exceed global limit")
	// Unregister all.
	for _, eng := range engines {
		eng.cs.Unregister()
	}
	// Verify scheduler is clean.
	scheduler.mu.Lock()
	require.Equal(t, 0, scheduler.mu.globalRunning)
	scheduler.mu.Unlock()
}

// concurrentTestDB implements pebble.DBForCompaction for concurrent tests.
type concurrentTestDB struct {
	name    string
	allowed int
	running atomic.Int32
}

func newConcurrentTestDB(name string, allowed int) *concurrentTestDB {
	return &concurrentTestDB{name: name, allowed: allowed}
}

func (d *concurrentTestDB) GetAllowedWithoutPermission() int {
	return d.allowed
}

func (d *concurrentTestDB) GetWaitingCompaction() (bool, pebble.WaitingCompaction) {
	return true, pebble.WaitingCompaction{Optional: false, Priority: 80, Score: 1.0}
}

func (d *concurrentTestDB) Schedule(handle pebble.CompactionGrantHandle) bool {
	return true
}

// TestRSEngineHeapOrdering verifies the heap orders RS engines correctly.
func TestRSEngineHeapOrdering(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	h := &rsEngineHeap{}
	heap.Init(h)

	entries := []struct {
		id uint64
		wc pebble.WaitingCompaction
	}{
		{1, pebble.WaitingCompaction{Optional: true, Priority: 20, Score: 1.0}},
		{2, pebble.WaitingCompaction{Optional: false, Priority: 80, Score: 3.0}},
		{3, pebble.WaitingCompaction{Optional: false, Priority: 80, Score: 5.0}},
		{4, pebble.WaitingCompaction{Optional: false, Priority: 60, Score: 2.0}},
		{5, pebble.WaitingCompaction{Optional: true, Priority: 40, Score: 10.0}},
	}
	for _, e := range entries {
		es := &engineState{
			id:            e.id,
			heapIndex:     -1,
			hasRemembered: true,
			remembered:    e.wc,
		}
		heap.Push(h, es)
	}

	// Expected pop order: highest priority first.
	// id=3 (non-opt, pri=80, score=5), id=2 (non-opt, pri=80, score=3),
	// id=4 (non-opt, pri=60, score=2), id=5 (opt, pri=40, score=10),
	// id=1 (opt, pri=20, score=1).
	expectedOrder := []uint64{3, 2, 4, 5, 1}
	for i, expectedID := range expectedOrder {
		es := heap.Pop(h).(*engineState)
		require.Equal(t, expectedID, es.id, "pop %d: expected id %d, got %d", i, expectedID, es.id)
	}
	require.Equal(t, 0, h.Len())
}
