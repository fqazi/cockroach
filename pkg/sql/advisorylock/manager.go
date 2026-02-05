package advisorylock

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
)

type LockMode int

const (
	LockShared LockMode = iota
	LockExclusive
)

type LockData struct {
	id       int
	mode     LockMode
	txn      *kv.Txn
	refCount int
}

type Manager interface {
	AcquireLock(ctx context.Context, id int, mode LockMode, wait bool) error
	ReleaseLock(id int) error
	ReleaseAllLocks() error
}
