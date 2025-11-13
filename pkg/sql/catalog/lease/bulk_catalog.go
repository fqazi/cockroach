package lease

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/catkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil/singleflight"
	"github.com/cockroachdb/errors"
)

type BulkCatalogKey interface {
	GetDatabaseID() descpb.ID
}

type bulkCatalogByDatabaseID descpb.ID

func (b bulkCatalogByDatabaseID) GetDatabaseID() descpb.ID {
	return descpb.ID(b)
}

type BulkCatalog interface {
	BulkCatalogKey
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

type bulkCatalogEntry struct {
	catalog.NameEntry
	fetchErr error
}

type bulkCatalogWithPrefetch struct {
	mu struct {
		syncutil.Mutex
		leases nstree.NameMap // FIXME: Entry type needs to be something else..
	}
	refCount       atomic.Int32
	databaseID     descpb.ID
	leaseTimestamp hlc.Timestamp
	lm             *Manager
	namespaces     nstree.Catalog

	// Tracks if the prefetch is running.
	prefetchCancel   chan struct{}
	prefetchComplete chan struct{}

	fetcher *singleflight.Group
}

func newBulkCatalog(
	ctx context.Context, databaseID descpb.ID, leaseTimestamp hlc.Timestamp, lm *Manager,
) (*bulkCatalogWithPrefetch, error) {
	bc := &bulkCatalogWithPrefetch{
		databaseID:       databaseID,
		leaseTimestamp:   leaseTimestamp,
		lm:               lm,
		prefetchCancel:   make(chan struct{}),
		prefetchComplete: make(chan struct{}),
		fetcher:          singleflight.NewGroup("prefetch-bulk=descriptor", singleflight.NoTags),
	}
	bc.refCount.Add(1)
	err := bc.startPrefetch(ctx)
	if err != nil {
		return nil, err
	}
	return bc, nil
}

func (b *bulkCatalogWithPrefetch) IncrRef() {
	b.refCount.Add(1)
}
func (b *bulkCatalogWithPrefetch) Release(ctx context.Context) {
	if b.refCount.Add(-1) != 0 {
		return
	}
	b.releaseLeases(ctx)
}

func (b *bulkCatalogWithPrefetch) releaseLeases(ctx context.Context) {
	// Stop any prefetch activity at this point.
	close(b.prefetchCancel)
	<-b.prefetchComplete
	// Acquire the lock and release all leases.
	b.mu.Lock()
	defer b.mu.Unlock()
	_ = b.mu.leases.IterateByID(func(entry catalog.NameEntry) error {
		be := entry.(bulkCatalogEntry)
		if be.fetchErr != nil {
			return nil
		}
		be.NameEntry.(LeasedDescriptor).Release(ctx)
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
	defer close(b.prefetchComplete)
	if err := b.namespaces.ForEachSchemaNamespaceEntryInDatabase(b.databaseID, func(e nstree.NamespaceEntry) error {
		select {
		case <-b.prefetchCancel:
			return context.Canceled
		case <-ctx.Done():
			return context.Canceled
		default:
		}
		ld, err := b.Acquire(ctx, TimestampToReadTimestamp(b.leaseTimestamp), e.GetID())
		if err != nil {
			if errors.Is(err, catalog.ErrDescriptorNotFound) || catalog.HasInactiveDescriptorError(err) {
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
		case <-b.prefetchCancel:
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
			if errors.Is(err, catalog.ErrDescriptorNotFound) || catalog.HasInactiveDescriptorError(err) {
				return nil
			}
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
type nameID descpb.ID

func (n nameID) GetID() descpb.ID {
	return descpb.ID(n)
}

func (n nameID) GetParentID() descpb.ID {
	return descpb.InvalidID
}

func (n nameID) GetName() string {
	return ""
}

func (n nameID) GetParentSchemaID() descpb.ID {
	return descpb.InvalidID
}

func (b *bulkCatalogWithPrefetch) getCachedByName(
	parentDatabaseID descpb.ID, parentSchemaID descpb.ID, name string,
) (exists bool, ld LeasedDescriptor, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	entry := b.mu.leases.GetByName(parentDatabaseID, parentSchemaID, name)
	if entry != nil {
		be := entry.(bulkCatalogEntry)
		if be.fetchErr != nil {
			return false, nil, be.fetchErr
		}
		return true, be.NameEntry.(LeasedDescriptor), nil
	}
	return false, nil, nil
}

func (b *bulkCatalogWithPrefetch) getCachedByID(
	id descpb.ID,
) (exists bool, ld LeasedDescriptor, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	entry := b.mu.leases.GetByID(id)
	if entry != nil {
		be := entry.(bulkCatalogEntry)
		if be.fetchErr != nil {
			return false, nil, be.fetchErr
		}
		return true, be.NameEntry.(LeasedDescriptor), nil
	}
	return false, nil, nil
}

func (b *bulkCatalogWithPrefetch) AcquireByName(
	ctx context.Context,
	timestamp ReadTimestamp,
	parentDatabaseID descpb.ID,
	parentSchemaID descpb.ID,
	name string,
) (LeasedDescriptor, error) {
	// Fast path where the cache already had the entry.
	if exists, ld, err := b.getCachedByName(parentDatabaseID, parentSchemaID, name); exists {
		if err != nil {
			return nil, err
		}
		return ld, nil
	}

	// Otherwise request the entry from the lease manager.
	ne := b.namespaces.LookupNamespaceEntry(descpb.NameInfo{Name: name, ParentID: parentDatabaseID, ParentSchemaID: parentSchemaID})
	skipRefCount := false
	ld, _, err := b.fetcher.Do(ctx, fmt.Sprintf("%d", ne.GetID()), func(ctx context.Context) (interface{}, error) {
		// We could have cached the value right before the single flight, so check again.
		if exists, ld, err := b.getCachedByName(parentDatabaseID, parentSchemaID, name); exists {
			if err != nil {
				return nil, err
			}
			return ld, nil
		}
		ld, err := b.lm.AcquireByName(ctx, timestamp, parentDatabaseID, parentSchemaID, name)
		// If we hit a known error, we will cache the results to help future lookups.
		if err != nil && !errors.Is(err, catalog.ErrDescriptorNotFound) && !catalog.HasInactiveDescriptorError(err) {
			return nil, err
		}
		b.mu.Lock()
		defer b.mu.Unlock()
		skipRefCount = true
		if b.mu.leases.GetByID(ne.GetID()) == nil {
			entry := bulkCatalogEntry{NameEntry: ld, fetchErr: err}
			if err != nil {
				entry.NameEntry = ne
			}
			b.mu.leases.Upsert(entry, false)
			if err == nil {
				ld.(*descriptorVersionState).incRefCount(ctx, false)
			}
		}
		return ld, err
	})
	if err != nil {
		return nil, err
	}
	if !skipRefCount {
		ld.(*descriptorVersionState).incRefCount(ctx, false)
	}
	return ld.(LeasedDescriptor), nil
}

func (b *bulkCatalogWithPrefetch) Acquire(
	ctx context.Context, timestamp ReadTimestamp, id descpb.ID,
) (LeasedDescriptor, error) {
	// Fast path where the cache already had the entry.
	if exists, ld, err := b.getCachedByID(id); exists {
		if err != nil {
			return nil, err
		}
		ld.(*descriptorVersionState).incRefCount(ctx, false)
		return ld, nil
	}

	// Otherwise request the entry from the lease manager.
	fetched := false
	ld, _, err := b.fetcher.Do(ctx, fmt.Sprintf("%d", id), func(ctx context.Context) (interface{}, error) {
		// We could have cached the value right before the single flight, so check again.
		if exists, ld, err := b.getCachedByID(id); exists {
			if err != nil {
				return nil, err
			}
			return ld, nil
		}
		ld, err := b.lm.Acquire(ctx, timestamp, id)
		// If we hit a known error, we will cache the results to help future lookups.
		if err != nil && !errors.Is(err, catalog.ErrDescriptorNotFound) && !catalog.HasInactiveDescriptorError(err) {
			return nil, err
		}
		b.mu.Lock()
		defer b.mu.Unlock()
		fetched = true
		if b.mu.leases.GetByID(id) == nil {
			entry := bulkCatalogEntry{NameEntry: ld, fetchErr: err}
			skipNameMap := false
			if ld == nil {
				entry.NameEntry = nameID(id)
				skipNameMap = true
			}
			b.mu.leases.Upsert(entry, skipNameMap)
			if err == nil {
				ld.(*descriptorVersionState).incRefCount(ctx, false)
			}
		}
		return ld, err
	})
	if err != nil {
		return nil, err
	}
	if !fetched {
		ld.(*descriptorVersionState).incRefCount(ctx, false)
	}

	return ld.(LeasedDescriptor), nil
}

func (b *bulkCatalogWithPrefetch) GetDatabaseID() descpb.ID {
	return b.databaseID
}

var _ BulkCatalog = &bulkCatalogWithPrefetch{}
