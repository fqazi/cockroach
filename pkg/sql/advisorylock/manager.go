package advisorylock

import "context"

type LockMode int

const (
	LockInvalid LockMode = iota
	LockShared
	LockExclusive
)

type Manager interface {
	AcquireLock(ctx context.Context, id int, mode LockMode, wait bool) error
	ReleaseLock(id int) error
	ReleaseAllLocks() error
}
