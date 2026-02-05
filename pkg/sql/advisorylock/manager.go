package advisorylock

import "context"

type LockMode int

const (
	LockShared LockMode = iota
	LockExclusive
)

type Manager interface {
	AcquireLock(ctx context.Context, id int, mode LockMode, wait bool) error
	ReleaseLock(id int) error
	ReleaseAllLocks() error
}
