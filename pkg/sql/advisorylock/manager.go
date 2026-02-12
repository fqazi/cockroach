package advisorylock

import (
	"context"

	"github.com/cockroachdb/errors"
)

type LockMode int

const (
	LockInvalid LockMode = iota
	LockShared
	LockExclusive
)

var ErrLockNotAcquired = errors.New("lock not acquired")
var ErrLockNotHeld = errors.New("lock not held")

type Manager interface {
	AcquireLock(ctx context.Context, id int, mode LockMode, wait bool, txnScoped bool) error
	ReleaseLock(id int, mode LockMode) error
	ReleaseAllLocks() error

	// Savepoint creates a new savepoint on the transaction stack.
	Savepoint()
	// ReleaseSavepoint releases the last savepoint on the transaction stack,
	// merging the locks into the previous savepoint.
	ReleaseSavepoint()
	// RollbackToSavepoint rolls back the last savepoint on the transaction stack,
	// releasing the locks acquired in that savepoint.
	RollbackToSavepoint()
	// FinishTransaction releases all locks acquired in the transaction.
	FinishTransaction()

	ReleaseAllForSession(ctx context.Context) error
}
