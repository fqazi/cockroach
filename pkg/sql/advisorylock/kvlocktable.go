package advisorylock

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/errors"
)

type LockData struct {
	id       int64
	mode     LockMode
	refCount int
}

type kvLockTableManager struct {
	lockTxn   *kv.Txn
	locks     map[int64]*LockData
	db        *kv.DB
	codec     keys.SQLCodec
	sessionID string
}

func (m *kvLockTableManager) ReleaseAllForSession(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (m *kvLockTableManager) GetAllLocks() (map[int64]*descpb.AdvisoryLockTracking, error) {
	return make(map[int64]*descpb.AdvisoryLockTracking), nil
}

func NewManager(db *kv.DB, codec keys.SQLCodec, sessionID string) Manager {
	return &kvLockTableManager{
		locks:     make(map[int64]*LockData),
		db:        db,
		codec:     codec,
		sessionID: sessionID,
		lockTxn:   db.NewTxn(context.Background(), "advisory-lock-txn"),
	}
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

func (m *kvLockTableManager) AcquireLock(
	ctx context.Context, id int64, mode LockMode, wait bool, txnScoped bool,
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
	key := m.codec.AdvisoryLockPrefix(id)
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
	// FIXME: We need to bump up our sequence number of each
	// acquistion.
	// FIMXE: Why are we in a bad state?
	fmt.Printf("retryable: %s\n", m.lockTxn.Sender().GetRetryableErr(ctx))
	_ = m.lockTxn.Sender().ClearRetryableErr(ctx)
	if _, err := m.lockTxn.CreateSavepoint(ctx); err != nil {
		return err
	}
	switch mode {
	case LockExclusive:
		_, err = m.lockTxn.GetForUpdate(ctx, key, kvpb.GuaranteedDurability)
	case LockShared:
		_, err = m.lockTxn.GetForShare(ctx, key, kvpb.GuaranteedDurability)
	}
	if err != nil {
		return err
	}
	m.locks[id] = entry
	return m.updateTrackingInfo(id, func(tracking *descpb.AdvisoryLockTracking) {
		tracking.Lock = id
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

func (m *kvLockTableManager) ReleaseLock(id int64, mode LockMode) error {
	entry := m.locks[id]
	entry.refCount--
	if entry.refCount == 0 {
		delete(m.locks, id)
		// Release our lock and leave this txn in a pending state.
		key := m.codec.AdvisoryLockPrefix(id)
		err := m.lockTxn.ClearAdvisoryLock(context.Background(), key)
		if err != nil {
			return err
		}
		return m.updateTrackingInfo(id, func(tracking *descpb.AdvisoryLockTracking) {
			tracking.Lock = id
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
	if err := m.lockTxn.Rollback(context.Background()); err != nil {
		return err
	}
	m.lockTxn = m.db.NewTxn(context.Background(), "advisory-lock-txn")
	m.locks = make(map[int64]*LockData)
	return nil
}

var _ Manager = &kvLockTableManager{}
