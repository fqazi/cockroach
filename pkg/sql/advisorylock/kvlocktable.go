package advisorylock

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/errors"
)

type LockData struct {
	id       int
	mode     LockMode
	txn      *kv.Txn
	refCount int
}

type kvLockTableManager struct {
	locks     map[int]*LockData
	db        *kv.DB
	codec     keys.SQLCodec
	sessionID string
}

func (m *kvLockTableManager) ReleaseAllForSession(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (m *kvLockTableManager) GetAllLocks() (map[int]*descpb.AdvisoryLockTracking, error) {
	return make(map[int]*descpb.AdvisoryLockTracking), nil
}

func NewManager(db *kv.DB, codec keys.SQLCodec, sessionID string) Manager {
	return &kvLockTableManager{
		locks:     make(map[int]*LockData),
		db:        db,
		codec:     codec,
		sessionID: sessionID,
	}
}

func (m *kvLockTableManager) updateTrackingInfo(
	id int, retryabe func(tracking *descpb.AdvisoryLockTracking),
) error {
	key := m.codec.AdvisoryLockMetaPrefix(uint32(id))
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
			oldValue = kv.Value.RawBytes
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

func (m *kvLockTableManager) AcquireLock(
	ctx context.Context, id int, mode LockMode, wait bool, txnScoped bool,
) error {
	if txnScoped {
		return errors.New("txn scoped locks not implemented for kvLockTableManager")
	}
	entry, exists := m.locks[id]
	// Acquire an existing lock.
	if exists {
		if entry.mode != mode {
			panic("unimplemented cannot switch modes yet")
		}
		entry.refCount++
		m.locks[id] = entry
		return nil
	}
	// Otherwise, lets setup a new entry.
	entry = &LockData{id: id, mode: mode, refCount: 1}

	// Create the key if doesn't exist.
	key := m.codec.AdvisoryLockPrefix(uint32(id))
	err := m.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		v, err := txn.Get(ctx, key)
		if err != nil || v.Exists() {
			return err
		}
		b := txn.NewBatch()
		b.CPut(key, []byte{1}, nil)
		err = txn.Run(ctx, b)
		return err
	})
	if err != nil {
		return err
	}
	txn := m.db.NewTxn(ctx, "accquire-advisory-lock")
	// FIXME: Double accquire to update waiting state..
	if mode == LockExclusive {
		_, err = txn.GetForUpdate(ctx, key, kvpb.GuaranteedDurability)
	} else if mode == LockShared {
		_, err = txn.GetForShare(ctx, key, kvpb.GuaranteedDurability)
	}
	if err != nil {
		return err
	}
	entry.txn = txn
	m.locks[id] = entry
	return m.updateTrackingInfo(id, func(tracking *descpb.AdvisoryLockTracking) {
		tracking.Lock = int32(id)
		if mode == LockShared {
			tracking.LockState = descpb.AdvisoryLockTracking_EXCLUSIVE

		} else {
			tracking.LockState = descpb.AdvisoryLockTracking_EXCLUSIVE
		}
		tracking.HolderSessionId = append(tracking.HolderSessionId, m.sessionID)

	})
}

func (m *kvLockTableManager) Savepoint()           {}
func (m *kvLockTableManager) ReleaseSavepoint()    {}
func (m *kvLockTableManager) RollbackToSavepoint() {}
func (m *kvLockTableManager) FinishTransaction()   {}

func (m *kvLockTableManager) ReleaseLock(id int, mode LockMode) error {
	entry := m.locks[id]
	entry.refCount--
	if entry.refCount == 0 {
		delete(m.locks, id)
		err := entry.txn.Commit(context.Background())
		if err != nil {
			return err
		}
		return m.updateTrackingInfo(id, func(tracking *descpb.AdvisoryLockTracking) {
			tracking.Lock = int32(id)
			for idx, sessionId := range tracking.HolderSessionId {
				if sessionId == m.sessionID {
					tracking.HolderSessionId = append(tracking.HolderSessionId[:idx], tracking.HolderSessionId[idx+1:]...)
					break
				}
			}
			// FIXME: Set to not held.
		})
	}
	return nil
}

func (m *kvLockTableManager) ReleaseAllLocks() error {
	for _, entry := range m.locks {
		if err := entry.txn.Commit(context.Background()); err != nil {
			return err
		}
	}
	m.locks = make(map[int]*LockData)
	return nil
}

var _ Manager = &kvLockTableManager{}
