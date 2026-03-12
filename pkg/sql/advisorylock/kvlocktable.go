package advisorylock

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// LockData tracks the advisory lock state for a single lock ID within a
// session. Exclusive and Shared references are counted independently,
// matching PostgreSQL semantics where pg_advisory_lock and
// pg_advisory_lock_shared maintain separate refcounts. Each individual
// acquisition is recorded as a lockEntry so that session-scoped and
// transaction-scoped references can be released independently.
type LockData struct {
	id int64
	// exclCount is the number of Exclusive references held on this lock.
	exclCount int
	// exclSeqNum is the sequence number at which the first Exclusive lock
	// was acquired in Pebble. Subsequent Exclusive re-acquisitions are
	// no-ops at the storage layer (MVCCAcquireLock short-circuits), so
	// only the first seq matters. This seq is used for demotion: marking
	// it as ignored causes the storage layer to treat the Exclusive lock
	// as rolled back.
	exclSeqNum enginepb.TxnSeq
	// sharedCount is the number of Shared references held on this lock.
	sharedCount int
	// entries tracks the scope (session vs transaction) of each acquisition.
	entries []lockEntry
}

// isExclusive returns true if any Exclusive references are held.
func (d *LockData) isExclusive() bool {
	return d.exclCount > 0
}

// isHeld returns true if any references (Exclusive or Shared) are held.
func (d *LockData) isHeld() bool {
	return d.exclCount > 0 || d.sharedCount > 0
}

// addRef records a new acquisition with the given mode and scope.
func (d *LockData) addRef(mode LockMode, scope LockScope) {
	d.entries = append(d.entries, lockEntry{mode: mode, scope: scope})
	if mode == LockExclusive {
		d.exclCount++
	} else {
		d.sharedCount++
	}
}

// removeRef removes the last matching entry by scope and mode (searching
// from the end). Returns true if a matching entry was found and removed.
func (d *LockData) removeRef(scope LockScope, mode LockMode) bool {
	for i := len(d.entries) - 1; i >= 0; i-- {
		if d.entries[i].scope == scope && d.entries[i].mode == mode {
			d.entries = append(d.entries[:i], d.entries[i+1:]...)
			if mode == LockExclusive {
				d.exclCount--
			} else {
				d.sharedCount--
			}
			return true
		}
	}
	return false
}

type kvLockTableManager struct {
	lockTxn   *kv.Txn
	locks     map[int64]*LockData
	db        *kv.DB
	codec     keys.SQLCodec
	sessionID string
	// txnStack tracks transaction-scoped lock acquisitions across savepoint
	// levels. Each element maps lock IDs to the modes acquired at that level.
	txnStack []map[int64][]LockMode
}

// ReleaseAllForSession releases only session-scoped locks, leaving
// transaction-scoped locks intact. This is called by pg_advisory_unlock_all().
func (m *kvLockTableManager) ReleaseAllForSession(ctx context.Context) error {
	type lockReq struct {
		id   int64
		mode LockMode
	}
	var locks []lockReq
	for id, data := range m.locks {
		for _, e := range data.entries {
			if e.scope == LockScopeSession {
				locks = append(locks, lockReq{id: id, mode: e.mode})
			}
		}
	}
	for i := len(locks) - 1; i >= 0; i-- {
		if err := m.releaseLockWithScope(
			locks[i].id, locks[i].mode, LockScopeSession,
		); err != nil {
			return err
		}
	}
	return nil
}

// GetAllLocks scans advisory lock metadata keys and returns the current
// tracking state of all locks.
func (m *kvLockTableManager) GetAllLocks() (map[int64]*descpb.AdvisoryLockTracking, error) {
	var locks map[int64]*descpb.AdvisoryLockTracking
	err := m.db.Txn(context.Background(), func(
		ctx context.Context, txn *kv.Txn,
	) error {
		rows, err := txn.Scan(
			ctx, m.codec.AdvisoryLockBase(),
			m.codec.AdvisoryLockBase().PrefixEnd(), 0,
		)
		if err != nil {
			return err
		}
		locks = make(map[int64]*descpb.AdvisoryLockTracking, len(rows))
		for _, row := range rows {
			var t descpb.AdvisoryLockTracking
			if err := row.Value.GetProto(&t); err != nil {
				return err
			}
			locks[t.Lock] = &t
		}
		return nil
	})
	return locks, err
}

func NewManager(db *kv.DB, codec keys.SQLCodec, sessionID string) Manager {
	lockTxn := db.NewTxn(context.Background(), "advisory-lock-txn")
	// Disable pipelining so that GetForUpdate/GetForShare are not tracked as
	// in-flight writes. Without this, AddIgnoredSeqNumRange during demotion
	// causes QueryIntent to fail because the pipeliner still holds the old
	// (now-ignored) seq as an in-flight write.
	_ = lockTxn.DisablePipelining()
	m := &kvLockTableManager{
		locks:     make(map[int64]*LockData),
		db:        db,
		codec:     codec,
		sessionID: sessionID,
		lockTxn:   lockTxn,
	}
	return m
}

func (m *kvLockTableManager) updateTrackingInfo(
	id int64, retryabe func(tracking *descpb.AdvisoryLockTracking),
) error {
	key := m.codec.AdvisoryLockMetaPrefix(id)
	return m.db.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
		var tracking descpb.AdvisoryLockTracking
		kv, err := txn.Get(ctx, key)
		if err != nil {
			return err
		}
		var oldValue []byte
		if kv.Exists() {
			err := kv.Value.GetProto(&tracking)
			if err != nil {
				return err
			}
			oldValue = kv.Value.TagAndDataBytes()
		}
		retryabe(&tracking)
		var newValue roachpb.Value
		err = newValue.SetProto(&tracking)
		if err != nil {
			return err
		}
		return txn.CPut(ctx, key, &newValue, oldValue)
	})
}

// ensureKeyExists creates the advisory lock key if it doesn't already exist.
func (m *kvLockTableManager) ensureKeyExists(ctx context.Context, key roachpb.Key) error {
	return m.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		txn.SetSessionTxn(m.lockTxn.ID(), func() roachpb.Key { return m.lockTxn.Key() })
		v, err := txn.Get(ctx, key)
		if err != nil || v.Exists() {
			return err
		}
		b := txn.NewBatch()
		b.CPut(key, []byte{1}, nil)
		return txn.Run(ctx, b)
	})
}

// acquireKVLock acquires a KV lock on the given key at the specified mode
// and returns the sequence number used for the acquisition.
func (m *kvLockTableManager) acquireKVLock(
	ctx context.Context, key roachpb.Key, mode LockMode,
) (enginepb.TxnSeq, error) {
	_ = m.lockTxn.Sender().ClearRetryableErr(ctx)
	if _, err := m.lockTxn.CreateSavepoint(ctx); err != nil {
		return 0, err
	}
	var err error
	switch mode {
	case LockExclusive:
		_, err = m.lockTxn.GetForUpdate(ctx, key, kvpb.GuaranteedDurability)
	case LockShared:
		_, err = m.lockTxn.GetForShare(ctx, key, kvpb.GuaranteedDurability)
	}
	if err != nil {
		return 0, err
	}
	return m.lockTxn.GetReadSeqNum(), nil
}

func (m *kvLockTableManager) AcquireLock(
	ctx context.Context, id int64, mode LockMode, wait bool, txnScoped bool,
) error {
	scope := LockScopeSession
	if txnScoped {
		scope = LockScopeTransaction
		if len(m.txnStack) == 0 {
			m.txnStack = append(m.txnStack, make(map[int64][]LockMode))
		}
	}

	// Helper to record the acquisition in the txn stack.
	recordTxnLock := func() {
		if txnScoped {
			top := m.txnStack[len(m.txnStack)-1]
			top[id] = append(top[id], mode)
		}
	}

	entry, exists := m.locks[id]
	if exists {
		switch mode {
		case LockExclusive:
			if entry.isExclusive() {
				// Already hold Exclusive. MVCCAcquireLock short-circuits repeated
				// GetForUpdate at same or stronger strength, so just bump count.
				entry.addRef(LockExclusive, scope)
				recordTxnLock()
				return nil
			}
			// Currently hold only Shared. Need to upgrade to Exclusive.
			// The KV layer handles promotion natively (Shared → Exclusive).
			key := m.codec.AdvisoryLockPrefix(id)
			seq, err := m.acquireKVLock(ctx, key, LockExclusive)
			if err != nil {
				return err
			}
			entry.exclSeqNum = seq
			entry.addRef(LockExclusive, scope)
			recordTxnLock()
			return m.updateTrackingInfo(id, func(tracking *descpb.AdvisoryLockTracking) {
				tracking.Lock = id
				tracking.LockState = descpb.AdvisoryLockTracking_EXCLUSIVE
			})

		case LockShared:
			// Whether we hold Exclusive or Shared, just bump Shared count.
			// If Exclusive is held, it subsumes Shared (no KV op needed).
			// If only Shared is held, MVCCAcquireLock short-circuits.
			entry.addRef(LockShared, scope)
			recordTxnLock()
			return nil
		}
	}

	// New lock — no existing entry.
	entry = &LockData{id: id}
	key := m.codec.AdvisoryLockPrefix(id)

	if err := m.ensureKeyExists(ctx, key); err != nil {
		return err
	}

	seq, err := m.acquireKVLock(ctx, key, mode)
	if err != nil {
		return err
	}

	switch mode {
	case LockExclusive:
		entry.exclSeqNum = seq
	}
	entry.addRef(mode, scope)

	m.locks[id] = entry
	recordTxnLock()
	return m.updateTrackingInfo(id, func(tracking *descpb.AdvisoryLockTracking) {
		tracking.Lock = id
		if mode == LockShared {
			tracking.LockState = descpb.AdvisoryLockTracking_SHARED
		} else {
			tracking.LockState = descpb.AdvisoryLockTracking_EXCLUSIVE
		}
		tracking.HolderSessionId = append(tracking.HolderSessionId, m.sessionID)
	})
}

func (m *kvLockTableManager) Savepoint() {
	m.txnStack = append(m.txnStack, make(map[int64][]LockMode))
}

func (m *kvLockTableManager) ReleaseSavepoint() {
	if len(m.txnStack) == 0 {
		return
	}
	top := m.txnStack[len(m.txnStack)-1]
	m.txnStack = m.txnStack[:len(m.txnStack)-1]
	if len(m.txnStack) > 0 {
		parent := m.txnStack[len(m.txnStack)-1]
		for id, modes := range top {
			parent[id] = append(parent[id], modes...)
		}
	}
}

func (m *kvLockTableManager) RollbackToSavepoint() {
	if len(m.txnStack) == 0 {
		return
	}
	top := m.txnStack[len(m.txnStack)-1]
	m.txnStack = m.txnStack[:len(m.txnStack)-1]
	for id, modes := range top {
		for _, mode := range modes {
			_ = m.releaseLockWithScope(id, mode, LockScopeTransaction)
		}
	}
}

func (m *kvLockTableManager) FinishTransaction() {
	for i := len(m.txnStack) - 1; i >= 0; i-- {
		for id, modes := range m.txnStack[i] {
			for _, mode := range modes {
				_ = m.releaseLockWithScope(id, mode, LockScopeTransaction)
			}
		}
	}
	m.txnStack = nil
}

func (m *kvLockTableManager) ReleaseLock(id int64, mode LockMode) error {
	return m.releaseLockWithScope(id, mode, LockScopeSession)
}

// releaseLockWithScope releases a single acquisition of the given mode and
// scope from the lock identified by id. When scope is LockScopeTransaction
// and no matching entry exists, the call is a no-op (the lock may have been
// explicitly released already).
func (m *kvLockTableManager) releaseLockWithScope(id int64, mode LockMode, scope LockScope) error {
	entry, ok := m.locks[id]
	if !ok {
		if scope == LockScopeTransaction {
			return nil
		}
		return ErrLockNotHeld
	}

	wasExclusive := entry.isExclusive()

	found := entry.removeRef(scope, mode)
	if !found {
		if scope == LockScopeTransaction {
			return nil
		}
		return ErrLockNotHeld
	}

	// Case 1: Still hold Exclusive refs. Nothing changes at KV layer.
	if entry.isExclusive() {
		return nil
	}

	// Case 2: Just dropped the last Exclusive ref, but Shared refs remain.
	// Demote the KV lock from Exclusive to Shared.
	if entry.sharedCount > 0 && wasExclusive {
		return m.demoteLock(id, entry)
	}

	// Case 3: Released a Shared ref, Shared refs still remain. No KV change.
	if entry.sharedCount > 0 {
		return nil
	}

	// Case 4: All refs gone. Full release.
	delete(m.locks, id)
	key := m.codec.AdvisoryLockPrefix(id)
	err := m.lockTxn.ClearAdvisoryLock(context.Background(), key)
	if err != nil {
		return err
	}
	return m.updateTrackingInfo(id, func(tracking *descpb.AdvisoryLockTracking) {
		tracking.Lock = id
		for idx, sessionId := range tracking.HolderSessionId {
			if sessionId == m.sessionID {
				tracking.HolderSessionId = append(
					tracking.HolderSessionId[:idx],
					tracking.HolderSessionId[idx+1:]...,
				)
				break
			}
		}
	})
}

// demoteLock transitions a lock from Exclusive to Shared at the KV layer
// using the IgnoredSeqNums mechanism:
//  1. Mark the Exclusive acquisition's seq as ignored.
//  2. Acquire a Shared lock (storage sees Exclusive as rolled back, writes Shared).
//  3. Clean up the rolled-back Exclusive from Pebble via ResolveIntentRequest.
func (m *kvLockTableManager) demoteLock(id int64, entry *LockData) error {
	ctx := context.Background()
	key := m.codec.AdvisoryLockPrefix(id)

	// Step 1: Mark the Exclusive acquisition seq as ignored. This also
	// steps writeSeq so subsequent operations use a non-ignored seq.
	if err := m.lockTxn.AddIgnoredSeqNumRange(ctx, enginepb.IgnoredSeqNumRange{
		Start: entry.exclSeqNum,
		End:   entry.exclSeqNum,
	}); err != nil {
		return err
	}

	// Step 2: Acquire a Shared lock. MVCCAcquireLock sees the old Exclusive
	// as rolled back (its seq is now ignored) and writes a new Shared entry.
	if _, err := m.acquireKVLock(ctx, key, LockShared); err != nil {
		return err
	}

	// Step 3: Send ResolveIntentRequest with IgnoredSeqNums to clean up the
	// rolled-back Exclusive from Pebble. The Shared lock we just wrote is at
	// a non-ignored seq, so it is preserved.
	if err := m.lockTxn.CleanupDemotedAdvisoryLock(ctx, key); err != nil {
		return err
	}

	entry.exclSeqNum = 0

	return m.updateTrackingInfo(id, func(tracking *descpb.AdvisoryLockTracking) {
		tracking.Lock = id
		tracking.LockState = descpb.AdvisoryLockTracking_SHARED
	})
}

func (m *kvLockTableManager) ReleaseAllLocks() error {
	if err := m.lockTxn.Rollback(context.Background()); err != nil {
		return err
	}
	m.lockTxn = m.db.NewTxn(context.Background(), "advisory-lock-txn")
	_ = m.lockTxn.DisablePipelining()
	m.locks = make(map[int64]*LockData)
	return nil
}

func (m *kvLockTableManager) OnNewTxn(txn *kv.Txn) {
	if txn != nil {
		txn.SetSessionTxn(m.lockTxn.ID(), func() roachpb.Key { return m.lockTxn.Key() })
		m.lockTxn.SetSessionTxn(txn.ID(), func() roachpb.Key { return txn.Key() })
	} else {
		m.lockTxn.SetSessionTxn(uuid.UUID{}, nil)
	}
}

var _ Manager = &kvLockTableManager{}
