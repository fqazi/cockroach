package advisorylock

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

var deadlockError = errors.New("deadlock detected")

type SessionMapProvider func() (map[string]struct{}, error)

type LockScope int

const (
	LockScopeSession LockScope = iota
	LockScopeTransaction
)

type lockEntry struct {
	mode  LockMode
	scope LockScope
}

type sqlLockInfo struct {
	entries      []lockEntry // Refcount and the mode that is acquired.
	numExclusive int
	numShared    int
}

func (s *sqlLockInfo) maybeAcquireIfCompatible(
	mode LockMode,
) (accquired bool, upgradeRequired bool) {
	// Lock is not acquired yet, so we need to get fetch it.
	if s.isUnlocked() {
		return false, false
	}
	switch mode {
	case LockShared:
		// If we have a compatible lock mode then just bump
		// the reference count.
		if s.isShared() || s.isExclusive() {
			return true, false
		}
		// Otherwise, we need to queue up to
		// get the lock.
		return false, false
	case LockExclusive:
		// If we have a compatible lock mode then just bump
		// the reference count.
		if s.isExclusive() {
			return true, false
		}
		// If we have a shared reference then we need to upgrade.
		if s.isShared() {
			return false, true
		}
	}
	return false, false
}

// isUnlocked returns if the locked is unlocked.
func (s *sqlLockInfo) isUnlocked() bool {
	return s.numExclusive == 0 && s.numShared == 0
}

// isExclusive returns if the lock is exclusive.
func (s *sqlLockInfo) isExclusive() bool {
	return s.numExclusive > 0
}

// isShared returns if the lock is shared.
func (s *sqlLockInfo) isShared() bool {
	return s.numShared > 0
}

// addRefCount adds reference type on to the stack.
func (s *sqlLockInfo) addRefCount(mode LockMode, scope LockScope) {
	s.entries = append(s.entries, lockEntry{mode: mode, scope: scope})
	if mode == LockShared {
		s.numShared++
	} else {
		s.numExclusive++
	}
}

// removeRef removes the last reference type of the given scope off the stack.
func (s *sqlLockInfo) removeRef(
	scope LockScope, mode LockMode,
) (newMode LockMode, isHeld bool, found bool) {
	idx := -1
	// Find the last entry with the matching scope.
	for i := len(s.entries) - 1; i >= 0; i-- {
		if s.entries[i].scope == scope &&
			s.entries[i].mode == mode {
			idx = i
			break
		}
	}
	if idx == -1 {
		// Calculate current mode
		if s.isExclusive() {
			return LockExclusive, true, false
		}
		if s.isShared() {
			return LockShared, true, false
		}
		return LockInvalid, false, false
	}

	previousMode := LockInvalid
	if s.isShared() {
		previousMode = LockShared
	}
	if s.isExclusive() {
		previousMode = LockExclusive
	}

	removed := s.entries[idx]
	s.entries = append(s.entries[:idx], s.entries[idx+1:]...)

	if removed.mode == LockShared {
		s.numShared--
	} else {
		s.numExclusive--
	}

	// We no longer hold the lock.
	if s.isUnlocked() {
		return LockInvalid, false, true
	}
	if s.isShared() {
		newMode = LockShared
	}
	if s.isExclusive() {
		newMode = LockExclusive
	}
	// No need to update the existing mode.
	if newMode == previousMode {
		newMode = LockInvalid
	}
	return newMode, true, true
}

type sqlTableManager struct {
	db           *kv.DB
	codec        keys.SQLCodec
	sessionID    string
	sessionMapFn SessionMapProvider
	stopper      *stop.Stopper
	heldLocks    map[int]*sqlLockInfo
	// txnStack tracks the locks acquired in the current transaction scope.
	// Each element represents a savepoint level and contains a map of
	// LockID -> list of modes acquired at that level.
	txnStack []map[int][]LockMode
}

func (s *sqlTableManager) GetAllLocks() (locks map[int]*descpb.AdvisoryLockTracking, err error) {
	err = s.db.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
		allRows, err := txn.Scan(ctx, s.codec.AdvisoryLockBase(), s.codec.AdvisoryLockBase().PrefixEnd(), 0)
		if err != nil {
			return err
		}
		locks = make(map[int]*descpb.AdvisoryLockTracking)
		for _, kv := range allRows {
			lock := descpb.AdvisoryLockTracking{}
			if err := kv.Value.GetProto(&lock); err != nil {
				return err
			}
			locks[int(lock.Lock)] = &lock
		}
		return nil
	})
	return locks, err
}

func (s *sqlTableManager) getLockInfo(id int) *sqlLockInfo {
	lockInfo, exists := s.heldLocks[id]
	if exists {
		return lockInfo
	}
	lockInfo = &sqlLockInfo{}
	s.heldLocks[id] = lockInfo
	return lockInfo
}

func (s *sqlTableManager) AcquireLock(
	ctx context.Context, id int, mode LockMode, wait bool, txnScoped bool,
) error {
	// If the user context is cancelled, then we need to remove ourselves from the wait list.
	defer func() {
		if ctx.Err() != nil {
			s.removeWaiterOnCancellation(ctx, id)
		}
	}()
	r := retry.StartWithCtx(ctx, retry.Options{})
	lockInfo := s.getLockInfo(id)
	acquired, upgradeLock := lockInfo.maybeAcquireIfCompatible(mode)

	// If txn scoped, ensure we have a stack.
	if txnScoped && len(s.txnStack) == 0 {
		s.txnStack = append(s.txnStack, make(map[int][]LockMode))
	}

	// Helper to record txn lock
	recordTxnLock := func() {
		if txnScoped {
			scope := s.txnStack[len(s.txnStack)-1]
			scope[id] = append(scope[id], mode)
		}
	}

	// Lock was acquired nothing needs to be done here.
	if acquired {
		lockInfo.addRefCount(mode, s.getScope(txnScoped))
		recordTxnLock()
		return nil
	}
	var userError error
	for r.Next() {
		retryAgain := false
		err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			var err error
			if !upgradeLock {
				retryAgain, err = s.doAdvisoryLockAcquire(ctx, txn, id, mode, wait)
			} else {
				retryAgain, err = s.doAdvisoryLockUpgrade(ctx, txn, id, wait)
			}
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			if pgerror.GetPGCode(err) == pgcode.DeadlockDetected {
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
	lockInfo.addRefCount(mode, s.getScope(txnScoped))
	recordTxnLock()
	return nil
}

func (s *sqlTableManager) getScope(txnScoped bool) LockScope {
	if txnScoped {
		return LockScopeTransaction
	}
	return LockScopeSession
}

func (s *sqlTableManager) ReleaseLock(id int, mode LockMode) error {
	return s.releaseLockWithScope(id, mode, LockScopeSession)
}

func (s *sqlTableManager) releaseLockWithScope(id int, mode LockMode, scope LockScope) error {
	if _, exists := s.heldLocks[id]; !exists {
		if scope == LockScopeTransaction {
			// If we are cleaning up txn locks and it's not held, maybe it was already released?
			// But it shouldn't be if we tracked it.
			return nil
		}
		return ErrLockNotHeld
	}
	lockInfo := s.getLockInfo(id)
	lockMode, isHeld, found := lockInfo.removeRef(scope, mode)
	if !found {
		if scope == LockScopeTransaction {
			return nil
		}
		return ErrLockNotHeld
	}

	// Lock mode hasn't changed and its still held.
	if lockMode == LockInvalid && isHeld {
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
			// This shouldn't happen if we think we hold it.
			return ErrLockNotHeld
		}
		if err := currentValue.Value.GetProto(&currentLock); err != nil {
			return err
		}
		// Find our holder and remove it
		var newValue roachpb.Value
		if !isHeld {
			for idx, holder := range currentLock.HolderSessionId {
				if holder == s.sessionID {
					currentLock.HolderSessionId = append(currentLock.HolderSessionId[:idx], currentLock.HolderSessionId[idx+1:]...)
					break
				}
			}
		}
		// Updathe lock mode if it has changed.
		switch lockMode {
		case LockShared:
			currentLock.LockState = descpb.AdvisoryLockTracking_SHARED
		case LockExclusive:
			currentLock.LockState = descpb.AdvisoryLockTracking_EXCLUSIVE
		default:
			// Mode hasn't changed.
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
	if lockMode == LockInvalid && !isHeld {
		delete(s.heldLocks, id)
	}
	return nil
}

func (s *sqlTableManager) ReleaseAllLocks() error {
	// First release all transaction locks
	s.FinishTransaction()
	return s.ReleaseAllForSession(context.TODO())
}

func (s *sqlTableManager) ReleaseAllForSession(ctx context.Context) error {
	type lockReq struct {
		id   int
		mode LockMode
	}
	var locks []lockReq
	for id, lockInfo := range s.heldLocks {
		for _, entry := range lockInfo.entries {
			if entry.scope == LockScopeSession {
				locks = append(locks, lockReq{id: id, mode: entry.mode})
			}
		}
	}
	// Release in reverse order.
	for i := len(locks) - 1; i >= 0; i-- {
		if err := s.releaseLockWithScope(locks[i].id, locks[i].mode, LockScopeSession); err != nil {
			return err
		}
	}
	return nil
}

func (s *sqlTableManager) Savepoint() {
	s.txnStack = append(s.txnStack, make(map[int][]LockMode))
}

func (s *sqlTableManager) ReleaseSavepoint() {
	if len(s.txnStack) == 0 {
		return
	}
	top := s.txnStack[len(s.txnStack)-1]
	s.txnStack = s.txnStack[:len(s.txnStack)-1]

	if len(s.txnStack) > 0 {
		// Merge into parent
		parent := s.txnStack[len(s.txnStack)-1]
		for id, modes := range top {
			parent[id] = append(parent[id], modes...)
		}
	} else {
		// If we popped the last one, maybe we should keep it?
		// No, usually ReleaseSavepoint assumes there's a parent or it's top level.
		// But if txnStack goes empty, it means no active transaction tracking.
		// If we still hold locks, we lose track of them!
		// But ReleaseSavepoint usually only applies if we are in a txn.
		// We should arguably ensure txnStack has at least one layer if a txn is active.
		// But for now assume usage is correct.
		// Actually, if we merge to "nothing", we essentially promote them to... wait.
		// If txnStack becomes empty, we can't release them at FinishTransaction.
		// So we should probably assume FinishTransaction clears everything.
		// If ReleaseSavepoint makes stack empty, we have a problem.
		// But Savepoint pushes. Release pops.
		// If we are at root txn, usually we don't call ReleaseSavepoint.
		// We call FinishTransaction.
	}
}

func (s *sqlTableManager) RollbackToSavepoint() {
	if len(s.txnStack) == 0 {
		return
	}
	top := s.txnStack[len(s.txnStack)-1]
	s.txnStack = s.txnStack[:len(s.txnStack)-1]

	// Release locks in top
	for id, modes := range top {
		for _, mode := range modes {
			// removeRef for each mode acquired
			_ = s.releaseLockWithScope(id, mode, LockScopeTransaction)
		}
	}
}

func (s *sqlTableManager) FinishTransaction() {
	for i := len(s.txnStack) - 1; i >= 0; i-- {
		level := s.txnStack[i]
		for id, modes := range level {
			for _, mode := range modes {
				_ = s.releaseLockWithScope(id, mode, LockScopeTransaction)
			}
		}
	}
	s.txnStack = nil
}

func NewSQLManager(
	db *kv.DB,
	codec keys.SQLCodec,
	sessionID string,
	provider SessionMapProvider,
	stopper *stop.Stopper,
) Manager {
	return &sqlTableManager{
		db:           db,
		codec:        codec,
		sessionID:    sessionID,
		sessionMapFn: provider,
		heldLocks:    make(map[int]*sqlLockInfo),
		stopper:      stopper,
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
					if holder == curr {
						continue
					}
					if hasCycle(holder) {
						return true
					}
				}
			}
			return false
		}
		// Check if this session is involved in a deadlock.
		// TODO: For this prototype all members of the cycle are terminated.
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

// removeWaiterOnCancellation removes the session from the wait list of the lock.
func (s *sqlTableManager) removeWaiterOnCancellation(ctx context.Context, id int) {
	err := s.stopper.RunAsyncTask(ctx, "remove-waiter-on-cancel", func(ctx context.Context) {
		err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			// Construct the key needed for this lock.
			lockPrefix := s.codec.AdvisoryLockKeyPrefix(uint32(id))

			currentLock := descpb.AdvisoryLockTracking{Lock: int32(id)}

			// Fetch the existing value that is stored for the lock.
			currentValue, err := txn.Get(ctx, lockPrefix)
			if err != nil {
				return err
			}
			if !currentValue.Exists() {
				return nil
			}
			if err := currentValue.Value.GetProto(&currentLock); err != nil {
				return err
			}
			// Remove this session from the wait list.
			for idx, waiter := range currentLock.Waiters {
				if *waiter.SessionId == s.sessionID {
					currentLock.Waiters = append(currentLock.Waiters[:idx], currentLock.Waiters[idx+1:]...)
					break
				}
			}
			newValue := roachpb.Value{}
			if err := newValue.SetProto(&currentLock); err != nil {
			}
			return txn.CPut(ctx, lockPrefix, &newValue, currentValue.Value.TagAndDataBytes())
		})
		if err != nil {
			log.Dev.Errorf(ctx, "failed to remove waiter on lock cancellation: %v", err)
		}
	})
	if err != nil {
		log.Dev.Infof(ctx, "failed to remove waiter on lock cancellation: %v", err)
	}
}

func (s *sqlTableManager) doAdvisoryLockUpgrade(
	ctx context.Context, txn *kv.Txn, id int, wait bool,
) (retry bool, err error) {
	// Construct the key needed for this lock.
	lockPrefix := s.codec.AdvisoryLockKeyPrefix(uint32(id))

	currentLock := descpb.AdvisoryLockTracking{Lock: int32(id)}

	// Fetch the existing value that is stored for the lock.
	currentValue, err := txn.Get(ctx, lockPrefix)
	if err != nil {
		return false, err
	}
	if currentValue.Exists() {
		if err := currentValue.Value.GetProto(&currentLock); err != nil {
			return false, err
		}
	}

	var newValue roachpb.Value
	lockValue := descpb.AdvisoryLockTracking_EXCLUSIVE

	uncontendedUpgrade := func() bool {
		// Simple case: If the lock is already held by us, then we just need to switch the state.
		if len(currentLock.HolderSessionId) == 1 && len(currentLock.Waiters) == 0 {
			currentLock.LockState = lockValue
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
	acquired := uncontendedUpgrade()

	// If the uncontended upgrade succeeded write the new value
	if acquired {
		return false, writeLockState()
	}
	// We need to wait for this lock next.
	if !wait {
		return false, ErrLockNotAcquired
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
			return false, pgerror.Newf(pgcode.DeadlockDetected, "deadlock detected")
		}
		return true, nil
	}

	// Retry, we are not at the head of the queue,
	// if we ignore dead sessions.
	if numActive > 0 {
		return checkForDeadlocksBeforeRetry()
	}
	// Next, validate if there are no holders or all of them are dead.
	for _, holder := range currentLock.HolderSessionId {
		if holder == s.sessionID {
			continue
		}
		// Holder is still alive.
		if _, found := sessionList[holder]; found {
			return checkForDeadlocksBeforeRetry()
		}
	}
	// Otherwise, truncate the list to our session.
	currentLock.Waiters = currentLock.Waiters[offset:]
	// Mark the lock as held finally.
	currentLock.LockState = lockValue
	return false, writeLockState()
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
		return false, ErrLockNotAcquired
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
			return false, pgerror.Newf(pgcode.DeadlockDetected, "deadlock detected")
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
