package advisorylock

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

var deadlockError = errors.New("deadlock detected")

type SessionMapProvider func() (map[string]struct{}, error)

type sqlTableManager struct {
	db           *kv.DB
	codec        keys.SQLCodec
	sessionID    string
	sessionMapFn SessionMapProvider
	heldLocks    map[int]int
}

func (s *sqlTableManager) AcquireLock(ctx context.Context, id int, mode LockMode, wait bool) error {
	r := retry.StartWithCtx(ctx, retry.Options{})
	if s.heldLocks[id] > 0 {
		s.heldLocks[id] = s.heldLocks[id] + 1
		return nil
	}
	var userError error
	for r.Next() {
		retryAgain := false
		err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			var err error
			retryAgain, err = s.doAdvisoryLockAcquire(ctx, txn, id, mode, wait)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			if errors.Is(err, deadlockError) {
				userError = err
				break
			}
			return err
		}
		if !retryAgain {
			break
		}
	}
	if userError != nil {
		return userError
	}
	s.heldLocks[id] = s.heldLocks[id] + 1
	return nil
}

func (s *sqlTableManager) ReleaseLock(id int) error {
	if _, exists := s.heldLocks[id]; !exists {
		return errors.New("lock not held")
	}
	s.heldLocks[id] = s.heldLocks[id] - 1
	if s.heldLocks[id] > 0 {
		return nil
	}
	err := s.db.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
		// Construct the key needed for this lock.
		lockPrefix := s.codec.AdvisoryLockKeyPrefix(uint32(id))

		currentLock := descpb.AdvisoryLockTracking{Lock: int32(id)}
		// Fetch the existing value that is stored for the lock.
		currentValue, err := txn.Get(ctx, lockPrefix)
		if err != nil {
			return err
		}
		if !currentValue.Exists() {
			return errors.New("lock not held")
		}
		if err := currentValue.Value.GetProto(&currentLock); err != nil {
			return err
		}
		// Find our holder and remove it
		var newValue roachpb.Value
		for idx, holder := range currentLock.HolderSessionId {
			if holder == s.sessionID {
				currentLock.HolderSessionId = append(currentLock.HolderSessionId[:idx], currentLock.HolderSessionId[idx+1:]...)
				break
			}
		}
		err = newValue.SetProto(&currentLock)
		if err != nil {
			return err
		}
		return txn.CPut(ctx, lockPrefix, &newValue, currentValue.Value.TagAndDataBytes())
	})
	if err != nil {
		return err
	}
	delete(s.heldLocks, id)
	return nil
}

func (s *sqlTableManager) ReleaseAllLocks() error {
	heldLocks := s.heldLocks
	s.heldLocks = make(map[int]int)
	for id := range heldLocks {
		if err := s.ReleaseLock(id); err != nil {
			return err
		}
	}
	return nil
}

func NewSQLManager(
	db *kv.DB, codec keys.SQLCodec, sessionID string, provider SessionMapProvider,
) Manager {
	return &sqlTableManager{
		db:           db,
		codec:        codec,
		sessionID:    sessionID,
		sessionMapFn: provider,
		heldLocks:    make(map[int]int),
	}
}

// checkForDeadlocks checks if sessionID is involved in a deadlock, this
// function can be expensive since it needs to acquire all locks.
func (s *sqlTableManager) checkForDeadlocks(
	ctx context.Context, sessionID string,
) (deadlock bool, err error) {
	err = s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		lockPrefix := s.codec.AdvisoryLockBase()
		// Fetch all the locks that are currently active.
		allLocks, err := txn.Scan(ctx, lockPrefix, lockPrefix.PrefixEnd(), 0)
		if err != nil {
			return err
		}
		// First, build a list of all locks that are currently held.
		lockHolders := make(map[int][]string)
		sessionsWaitingForLocks := make(map[string][]int)
		for _, kv := range allLocks {
			lock := descpb.AdvisoryLockTracking{}
			if err := kv.Value.GetProto(&lock); err != nil {
				return err
			}
			lockID := int(lock.Lock)
			for _, holder := range lock.HolderSessionId {
				lockHolders[lockID] = append(lockHolders[lockID], holder)
			}
			for _, waiter := range lock.Waiters {
				sessionsWaitingForLocks[*waiter.SessionId] = append(sessionsWaitingForLocks[*waiter.SessionId], lockID)
			}
		}

		// Now we check for deadlocks by traversing the wait-for graph.
		visited := make(map[string]struct{})
		onStack := make(map[string]struct{})
		var hasCycle func(string) bool
		hasCycle = func(curr string) bool {
			if _, ok := onStack[curr]; ok {
				return true
			}
			if _, ok := visited[curr]; ok {
				return false
			}
			visited[curr] = struct{}{}
			onStack[curr] = struct{}{}
			defer delete(onStack, curr)

			for _, lockID := range sessionsWaitingForLocks[curr] {
				for _, holder := range lockHolders[lockID] {
					if hasCycle(holder) {
						return true
					}
				}
			}
			return false
		}
		// Check if this session is involved in a deadlock.
		// TODO: For this prototype all members of the cycle are terminated.
		// FIXME: We need to remove this as a lock waiter next.
		if hasCycle(sessionID) {
			deadlock = true
			return nil
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return deadlock, nil
}

// doAdvisoryLockAcquire will run with a transaction and attempt to acquire
// the advisory lock. When acquistion is complete retry will be false; otherwise,
// another attempt should be made with a fresh transaction.
func (s *sqlTableManager) doAdvisoryLockAcquire(
	ctx context.Context, txn *kv.Txn, id int, mode LockMode, wait bool,
) (retry bool, err error) {
	isShared := mode == LockShared
	// Construct the key needed for this lock.
	lockPrefix := s.codec.AdvisoryLockKeyPrefix(uint32(id))

	currentLock := descpb.AdvisoryLockTracking{Lock: int32(id)}

	// Fetch the existing value that is stored for the lock.
	currentValue, err := txn.Get(ctx, lockPrefix)
	if err != nil {
		return
	}
	if currentValue.Exists() {
		if err := currentValue.Value.GetProto(&currentLock); err != nil {
			return false, err
		}
	}

	var newValue roachpb.Value
	lockValue := descpb.AdvisoryLockTracking_EXCLUSIVE
	if isShared {
		lockValue = descpb.AdvisoryLockTracking_SHARED
	}
	uncontendedAccquire := func() bool {
		// Simple case: If the lock is free then just mark it as acquired.
		if len(currentLock.HolderSessionId) == 0 && len(currentLock.Waiters) == 0 {
			currentLock.LockState = lockValue
			return true
		}
		// If it's shared and there are no waiters then we can get it.
		if isShared && currentLock.LockState == lockValue && len(currentLock.Waiters) == 0 {
			return true
		}

		// If its not held, and we are the first waiter then we can get it.
		if len(currentLock.HolderSessionId) == 0 && len(currentLock.Waiters) > 0 &&
			*currentLock.Waiters[0].SessionId == s.sessionID {
			currentLock.LockState = lockValue
			currentLock.Waiters = currentLock.Waiters[1:]
			return true
		}

		return false
	}

	writeLockState := func() error {
		if err := newValue.SetProto(&currentLock); err != nil {
			return err
		}
		var oldValue []byte
		if currentValue.Exists() {
			oldValue = currentValue.Value.TagAndDataBytes()
		}
		err := txn.CPut(ctx, lockPrefix, &newValue, oldValue)
		if err != nil {
			return err
		}
		return nil
	}
	acquired := uncontendedAccquire()

	// If the contended acquired succeeded write the new value
	if acquired {
		currentLock.HolderSessionId = append(currentLock.HolderSessionId, s.sessionID)
		return false, writeLockState()
	}

	// We need to wait for this lock next.
	if !wait {
		return false, errors.New("cannot wait for lock")
	}

	// Check we are on the wait list already.
	needToAdd := true
	for _, waiter := range currentLock.Waiters {
		if *waiter.SessionId == s.sessionID {
			needToAdd = false
		}
	}
	// Add into the wait list if needed.
	if needToAdd {
		currentLock.Waiters = append(currentLock.Waiters, &descpb.AdvisoryLockTracking_LockWaiter{
			SessionId:    &s.sessionID,
			RequiredType: &lockValue,
		})
		return true, writeLockState()
	}
	// Scan if we are at the head due to dead sessions.
	sessionList, err := s.sessionMapFn()
	if err != nil {
		return true, err
	}
	numActive := 0
	offset := 0
	for _, waiter := range currentLock.Waiters {
		if _, found := sessionList[*waiter.SessionId]; !found {
			numActive++
			break
		} else {
			offset++
		}
		// We are at the head of the queue.
		if *waiter.SessionId == s.sessionID {
			break
		}
	}

	checkForDeadlocksBeforeRetry := func() (bool, error) {
		hasDeadlock, err := s.checkForDeadlocks(ctx, s.sessionID)
		if err != nil {
			return true, err
		}
		if hasDeadlock {
			// Remove ourselves from the wait queue.
			for idx, waiter := range currentLock.Waiters {
				if *waiter.SessionId == s.sessionID {
					currentLock.Waiters = append(currentLock.Waiters[:idx], currentLock.Waiters[idx+1:]...)
					break
				}
			}
			if err := writeLockState(); err != nil {
				return true, err
			}
			return false, deadlockError
		}
		return true, nil
	}

	// Retry, we are not at the head of the queue,
	// if we ignore dead sessions.
	if numActive > 0 {
		return checkForDeadlocksBeforeRetry()
	}
	// Validate we are not holding it in a compatible mode already,
	// and there are no active holders. Or if its not shared make sure
	// there are no other holders.
	if (isShared && currentLock.LockState != lockValue) || !isShared {
		// Next, validate if there are no holders or all of them are dead.
		for _, holder := range currentLock.HolderSessionId {
			// Holder is still alive.
			if _, found := sessionList[holder]; found {
				return checkForDeadlocksBeforeRetry()
			}
		}
	}
	// Otherwise, truncate the list to our session.
	currentLock.Waiters = currentLock.Waiters[offset:]
	// Mark the lock as held finally.
	currentLock.LockState = lockValue
	currentLock.HolderSessionId = append(currentLock.HolderSessionId, s.sessionID)
	return false, writeLockState()
}
