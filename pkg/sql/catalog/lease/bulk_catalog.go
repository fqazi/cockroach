package lease

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/catkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

type BulkCatalog interface {
	// FIXME: Read timestamp can be used for the validity interval?
	// FIXME: no-op / fallback implementation is the lease manager.
	AcquireByName(
		ctx context.Context,
		timestamp ReadTimestamp,
		parentDatabaseID descpb.ID,
		parentSchemaID descpb.ID,
		name string,
	) (LeasedDescriptor, error)

	Acquire(
		ctx context.Context, timestamp ReadTimestamp, id descpb.ID,
	) (LeasedDescriptor, error)

	IncrRef()
	Release(ctx context.Context)
	GetNamespaces() nstree.Catalog
}

type bulkCatalogWithPrefetch struct {
	mu struct {
		syncutil.Mutex
		leases nstree.NameMap
	}
	refCount       atomic.Int32
	databaseID     descpb.ID
	leaseTimestamp hlc.Timestamp
	lm             *Manager
	namespaces     nstree.Catalog
	cancelCh       chan struct{}
}

func newBulkCatalog(
	ctx context.Context, databaseID descpb.ID, leaseTimestamp hlc.Timestamp, lm *Manager,
) (*bulkCatalogWithPrefetch, error) {
	bc := &bulkCatalogWithPrefetch{
		databaseID:     databaseID,
		leaseTimestamp: leaseTimestamp,
		lm:             lm,
		cancelCh:       make(chan struct{}),
	}
	bc.refCount.Add(1)
	err := bc.startPrefetch(ctx)
	if err != nil {
		return nil, err
	}
	return bc, nil
}

func (b *bulkCatalogWithPrefetch) IncrRef() {
	log.Dev.Infof(context.Background(), "getting ref at: %p %d\n %s\n", b, b.refCount.Load()+1, debug.Stack())
	b.refCount.Add(1)
}
func (b *bulkCatalogWithPrefetch) Release(ctx context.Context) {
	log.Dev.Infof(context.Background(), "releasing ref at: %p %d\n %s\n", b, b.refCount.Load()-1, debug.Stack())
	if b.refCount.Add(-1) != 0 {
		return
	}
	log.Dev.Infof(context.Background(), "releasing descriptors %p\n", b)
	b.releaseLeases(ctx)
}

func (b *bulkCatalogWithPrefetch) releaseLeases(ctx context.Context) {
	// Stop any prefetch activity at this point.
	close(b.cancelCh)
	// Acquire the lock and release all leases.
	b.mu.Lock()
	defer b.mu.Unlock()
	_ = b.mu.leases.IterateByID(func(entry catalog.NameEntry) error {
		entry.(LeasedDescriptor).Release(ctx)
		return nil
	})
	b.mu.leases.Clear()
}

func (b *bulkCatalogWithPrefetch) startPrefetch(ctx context.Context) error {
	reader := catkv.NewUncachedCatalogReader(b.lm.storage.codec)
	db, err := b.lm.Acquire(ctx, TimestampToReadTimestamp(b.leaseTimestamp), b.databaseID)
	if err != nil {
		return err
	}
	defer db.Release(ctx)
	if err := b.lm.storage.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		if err := txn.KV().SetFixedTimestamp(ctx, b.leaseTimestamp); err != nil {
			return err
		}
		catalog, err := reader.ScanNamespaceForDatabaseSchemasAndObjects(ctx, txn.KV(), db.Underlying().(catalog.DatabaseDescriptor))
		if err != nil {
			return err
		}
		// Add all of the namespaces to allow us to prefetch children under this
		// database.
		b.namespaces = catalog
		return nil
	}); err != nil {
		return err
	}
	return b.lm.stopper.RunAsyncTask(ctx, fmt.Sprintf("prefetch-for-%s", db.GetName()), b.runPrefetch)
	// FIXME: Start the prefetcher next.
}

func (b *bulkCatalogWithPrefetch) runPrefetch(ctx context.Context) {
	if err := b.namespaces.ForEachSchemaNamespaceEntryInDatabase(b.databaseID, func(e nstree.NamespaceEntry) error {
		select {
		case <-b.cancelCh:
			return context.Canceled
		case <-ctx.Done():
			return context.Canceled
		default:
		}
		ld, err := b.Acquire(ctx, TimestampToReadTimestamp(b.leaseTimestamp), e.GetID())
		if err != nil {
			if errors.Is(err, catalog.ErrDescriptorNotFound) {
				return nil
			}
			return err
		}
		defer ld.Release(ctx)
		return nil
	}); err != nil {
		log.Dev.Infof(ctx, "failed to prefetch schemas: %v", err)
	}

	if err := b.namespaces.ForEachNamespaceEntry(func(e nstree.NamespaceEntry) error {
		select {
		case <-b.cancelCh:
			return context.Canceled
		case <-ctx.Done():
			return context.Canceled
		default:
		} // Skip schemas and databases.
		if e.GetParentSchemaID() == descpb.InvalidID {
			return nil
		}
		ld, err := b.Acquire(ctx, TimestampToReadTimestamp(b.leaseTimestamp), e.GetID())
		if err != nil {
			return err
		}
		defer ld.Release(ctx)
		return nil
	}); err != nil {
		log.Dev.Infof(ctx, "failed to prefetch objects: %v", err)
	}
	return
}

func (b *bulkCatalogWithPrefetch) GetNamespaces() nstree.Catalog {
	return b.namespaces
}

// FIXME: Support regenerating for a new TS.
// FIXME: Ref counts?

func (b *bulkCatalogWithPrefetch) AcquireByName(
	ctx context.Context,
	timestamp ReadTimestamp,
	parentDatabaseID descpb.ID,
	parentSchemaID descpb.ID,
	name string,
) (LeasedDescriptor, error) {
	getFromCache := func() LeasedDescriptor {
		b.mu.Lock()
		defer b.mu.Unlock()

		// First do a fast look up in the cache.
		entry := b.mu.leases.GetByName(parentSchemaID, parentDatabaseID, name)
		if entry != nil {
			return entry.(LeasedDescriptor)
		}
		return nil
	}

	// Fast path where the cache already had the entry.
	if ld := getFromCache(); ld != nil {
		ld.(*descriptorVersionState).incRefCount(ctx, false)
		return ld, nil
	}

	// Otherwise request the entry from the lease manager.
	// FIXME: We only want one adder for this part once its
	// done.
	ld, err := b.lm.AcquireByName(ctx, timestamp, parentDatabaseID, parentSchemaID, name)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.mu.leases.GetByID(ld.GetID()) == nil {
		b.mu.leases.Upsert(ld, false)
		ld.(*descriptorVersionState).incRefCount(ctx, false)
	}
	return ld, nil
}

func (b *bulkCatalogWithPrefetch) Acquire(
	ctx context.Context, timestamp ReadTimestamp, id descpb.ID,
) (LeasedDescriptor, error) {
	getFromCache := func() LeasedDescriptor {
		b.mu.Lock()
		defer b.mu.Unlock()

		// First do a fast look up in the cache.
		entry := b.mu.leases.GetByID(id)
		if entry != nil {
			return entry.(LeasedDescriptor)
		}
		return nil
	}

	// Fast path where the cache already had the entry.
	if ld := getFromCache(); ld != nil {
		ld.(*descriptorVersionState).incRefCount(ctx, false)
		return ld, nil
	}

	// Otherwise request the entry from the lease manager.
	// FIXME: We only want one adder for this part once its
	// done.
	ld, err := b.lm.Acquire(ctx, timestamp, id)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.mu.leases.GetByID(id) == nil {
		b.mu.leases.Upsert(ld, false)
		ld.(*descriptorVersionState).incRefCount(ctx, false)
	}
	return ld, nil
}

var _ BulkCatalog = &bulkCatalogWithPrefetch{}
