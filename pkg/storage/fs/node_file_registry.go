// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fs

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/vfs"
	"golang.org/x/sync/singleflight"
)

// sharedStorageNumOldRegistryFiles is the number of old registry files
// to keep in each node's directory on shared storage. This is set much
// higher than defaultNumOldFileRegistryFiles because remote nodes may
// see stale directory listings and follow markers pointing to files
// that the writing node has already cycled past.
const sharedStorageNumOldRegistryFiles = 16

// NodeFileRegistry is a FileRegistrar implementation for multi-node
// encryption-at-rest on shared storage. Each node writes its file
// registry entries to its own directory (e.g.
// "<registryDir>/n<nodeID>/"), while reads merge entries from all
// per-node directories. This eliminates multi-writer coordination:
// each node directory contains a plain FileRegistry.
//
// Local write operations delegate to the local node's FileRegistry and
// immediately update the merged in-memory map. Read operations return
// data from the merged map, which is populated at startup by loading
// all per-node registries.
type NodeFileRegistry struct {
	fs          vfs.FS
	registryDir string
	nodeID      roachpb.NodeID
	localReg    *FileRegistry
	sfGroup     singleflight.Group
	mu          struct {
		syncutil.RWMutex
		// entries is the merged in-memory map across all node directories.
		// All values are non-nil.
		entries map[string]*enginepb.FileEntry
	}
}

var _ FileRegistrar = (*NodeFileRegistry)(nil)

// NewNodeFileRegistry creates a new NodeFileRegistry. It creates the
// local node's directory under registryDir for nodeID, loads the local
// registry, then scans for other node directories and merges their
// entries into the in-memory map.
//
// registryDir is the parent directory containing all per-node
// directories (e.g. "<cluster-id>/encryption/registry/"). Each node
// directory is a subdirectory named "n<nodeID>".
func NewNodeFileRegistry(
	ctx context.Context, fs vfs.FS, registryDir string, nodeID roachpb.NodeID, readOnly bool,
) (*NodeFileRegistry, error) {
	localDir := fs.PathJoin(registryDir, fmt.Sprintf("n%d", nodeID))

	if !readOnly {
		if err := fs.MkdirAll(registryDir, 0755); err != nil {
			return nil, errors.Wrap(err, "creating registry dir")
		}
		if err := fs.MkdirAll(localDir, 0755); err != nil {
			return nil, errors.Wrap(err, "creating local node dir")
		}
	}

	localReg := &FileRegistry{
		FS:                      fs,
		DBDir:                   localDir,
		ReadOnly:                readOnly,
		NumOldRegistryFiles:     sharedStorageNumOldRegistryFiles,
		CanElideEntry:           elidePlaintext,
		SkipFileDeletionElision: true,
		SyncDir:                 true,
		SealAfterWrite:          true,
		BatchSealWrites:         true,
	}
	if err := localReg.Load(ctx); err != nil {
		return nil, errors.Wrap(err, "loading local registry")
	}

	s := &NodeFileRegistry{
		fs:          fs,
		registryDir: registryDir,
		nodeID:      nodeID,
		localReg:    localReg,
	}
	s.mu.entries = make(map[string]*enginepb.FileEntry)

	// Seed the merged map from the local registry.
	localEntries := localReg.List()
	for filename, entry := range localEntries {
		s.mu.entries[filename] = entry
	}

	// Scan registryDir for other n<id>/ node directories and merge
	// their entries.
	if err := s.refreshRemoteRegistries(ctx); err != nil {
		_ = localReg.Close()
		return nil, errors.Wrap(err, "initial remote registry scan")
	}

	return s, nil
}

// refreshRemoteRegistries re-reads all remote node directories (skipping
// our own nodeID), loads each as a read-only FileRegistry, and merges
// entries into the in-memory map. Overwrites are safe because file
// entries are immutable: an SSTable's encryption settings are fixed at
// write time and never change.
//
// This is a merge-only operation — it does not delete entries that were
// previously in the map but are now absent from a remote node's
// registry. Entries deleted via MaybeDeleteEntry may be resurrected by
// a subsequent refresh. This is acceptable for the current prototype
// but will require tombstone tracking before production use to prevent
// unbounded accumulation of orphaned entries.
// TODO(basalt): Add tombstone tracking to filter deleted entries during
// refresh.
func (s *NodeFileRegistry) refreshRemoteRegistries(ctx context.Context) error {
	dirEntries, err := s.fs.List(s.registryDir)
	if err != nil {
		if oserror.IsNotExist(err) {
			return nil
		}
		return errors.Wrap(err, "listing registry dir")
	}
	for _, name := range dirEntries {
		name = s.fs.PathBase(name)
		remoteID, ok := parseNodeID(name)
		if !ok || remoteID == s.nodeID {
			continue
		}
		remoteDir := s.fs.PathJoin(s.registryDir, name)

		entries := s.loadRemoteRegistry(ctx, name, remoteDir)
		if entries == nil {
			continue
		}
		func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			for filename, entry := range entries {
				s.mu.entries[filename] = entry
			}
		}()
	}
	return nil
}

// loadRemoteRegistry loads entries from a remote node's registry
// directory. It reads the highest-numbered registry file directly,
// which is the most reliable method on shared storage (basaltfs) where
// marker file visibility may lag behind data file visibility or
// deletion of old files.
//
// On basaltfs, only sealed (immutable) files are visible, so the
// highest-numbered file in the directory listing is always a complete
// snapshot. If reading that file fails (e.g. on vfs.NewMem where
// partially-written files are visible), we fall back to the
// marker-based approach which only reads marker-confirmed files.
func (s *NodeFileRegistry) loadRemoteRegistry(
	ctx context.Context, name, remoteDir string,
) map[string]*enginepb.FileEntry {
	// Try reading the highest-numbered file directly. On basaltfs
	// this is the most reliable approach because:
	// 1. Marker visibility may lag behind, pointing to deleted files
	// 2. Sealed files are always complete and immutable
	// 3. The RPC cost is the same as marker-based (List+Open+Read
	//    vs Open(marker)+Open(file)+Read)
	directEntries := s.readHighestRegistryFile(ctx, name, remoteDir)
	if directEntries != nil {
		return directEntries
	}

	// Fall back to marker-based load. This handles filesystems like
	// vfs.NewMem where partially-written files are visible in
	// directory listings and the highest-numbered file might be
	// incomplete.
	remoteReg := &FileRegistry{
		FS:                      s.fs,
		DBDir:                   remoteDir,
		ReadOnly:                true,
		NumOldRegistryFiles:     defaultNumOldFileRegistryFiles,
		CanElideEntry:           elidePlaintext,
		SkipFileDeletionElision: true,
	}
	if err := remoteReg.Load(ctx); err != nil {
		_ = remoteReg.Close()
		return nil
	}
	entries := remoteReg.List()
	if err := remoteReg.Close(); err != nil {
		log.Dev.Warningf(ctx, "closing remote registry %s: %v", name, err)
	}
	return entries
}

// readHighestRegistryFile reads the highest-numbered
// COCKROACHDB_REGISTRY_* file in the remote directory. Returns nil if
// no registry files are found or if reading the file fails.
func (s *NodeFileRegistry) readHighestRegistryFile(
	ctx context.Context, name, remoteDir string,
) map[string]*enginepb.FileEntry {
	files, err := s.fs.List(remoteDir)
	if err != nil {
		return nil
	}

	// Find the highest-numbered registry file.
	var bestFile string
	var bestNum uint64
	for _, f := range files {
		f = s.fs.PathBase(f)
		if !strings.HasPrefix(f, registryFilenameBase+"_") {
			continue
		}
		numStr := strings.TrimPrefix(f, registryFilenameBase+"_")
		num, err := strconv.ParseUint(numStr, 10, 64)
		if err != nil {
			continue
		}
		if num > bestNum {
			bestNum = num
			bestFile = f
		}
	}
	if bestFile == "" {
		return nil
	}

	path := s.fs.PathJoin(remoteDir, bestFile)
	f, err := s.fs.Open(path)
	if err != nil {
		log.Dev.Warningf(ctx,
			"node-file-registry n%d: failed to open remote %s/%s: %v",
			s.nodeID, name, bestFile, err)
		return nil
	}
	entries, err := readRegistryEntries(f)
	if closeErr := f.Close(); closeErr != nil {
		log.Dev.Warningf(ctx,
			"node-file-registry n%d: closing remote %s/%s: %v",
			s.nodeID, name, bestFile, closeErr)
	}
	if err != nil {
		log.Dev.Warningf(ctx,
			"node-file-registry n%d: failed to read entries from remote %s/%s: %v",
			s.nodeID, name, bestFile, err)
		return nil
	}
	return entries
}

// readRegistryEntries reads all entries from a registry file,
// replaying all batches to build the final entry map.
func readRegistryEntries(f vfs.File) (map[string]*enginepb.FileEntry, error) {
	rr := record.NewReader(f, 0)
	// Read header.
	rdr, err := rr.Next()
	if err != nil {
		return nil, errors.Wrap(err, "reading header")
	}
	headerBytes, err := io.ReadAll(rdr)
	if err != nil {
		return nil, errors.Wrap(err, "reading header bytes")
	}
	header := &enginepb.RegistryHeader{}
	if err := protoutil.Unmarshal(headerBytes, header); err != nil {
		return nil, errors.Wrap(err, "unmarshaling header")
	}

	entries := make(map[string]*enginepb.FileEntry)
	for {
		rdr, err := rr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, errors.Wrap(err, "reading record")
		}
		b, err := io.ReadAll(rdr)
		if err != nil {
			return nil, errors.Wrap(err, "reading record bytes")
		}
		batch := &enginepb.RegistryUpdateBatch{}
		if err := protoutil.Unmarshal(b, batch); err != nil {
			return nil, errors.Wrap(err, "unmarshaling batch")
		}
		for _, update := range batch.Updates {
			if update.Entry == nil {
				delete(entries, update.Filename)
			} else {
				entries[update.Filename] = update.Entry
			}
		}
	}
	return entries, nil
}

// parseNodeID parses a node directory name of the form "n<id>"
// and returns the node ID. It returns false if name does not match.
func parseNodeID(name string) (roachpb.NodeID, bool) {
	if !strings.HasPrefix(name, "n") {
		return 0, false
	}
	id, err := strconv.ParseInt(name[1:], 10, 32)
	if err != nil || id <= 0 {
		return 0, false
	}
	return roachpb.NodeID(id), true
}

// GetFileEntry returns the file entry for filename from the merged map.
// On cache miss, it re-scans all remote node registries (coalesced via
// singleflight) and re-checks the map.
func (s *NodeFileRegistry) GetFileEntry(filename string) *enginepb.FileEntry {
	filename = s.localReg.NormalizeFilename(filename)
	s.mu.RLock()
	entry := s.mu.entries[filename]
	s.mu.RUnlock()
	if entry != nil {
		return entry
	}

	// Cache miss: refresh remote registries and re-check.
	ctx := context.Background()
	_, _, _ = s.sfGroup.Do("refresh", func() (interface{}, error) {
		if err := s.refreshRemoteRegistries(ctx); err != nil {
			log.Dev.Warningf(ctx, "refreshing remote registries: %v", err)
		}
		return nil, nil
	})

	s.mu.RLock()
	entry = s.mu.entries[filename]
	s.mu.RUnlock()
	return entry
}

// List returns a shallow copy of all entries from the merged map.
func (s *NodeFileRegistry) List() map[string]*enginepb.FileEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := make(map[string]*enginepb.FileEntry, len(s.mu.entries))
	for k, v := range s.mu.entries {
		m[k] = v
	}
	return m
}

// GetRegistrySnapshot returns an enginepb.FileRegistry snapshot of
// the merged entries.
func (s *NodeFileRegistry) GetRegistrySnapshot() *enginepb.FileRegistry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rv := &enginepb.FileRegistry{
		Version: enginepb.RegistryVersion_Records,
		Files:   make(map[string]*enginepb.FileEntry, len(s.mu.entries)),
	}
	for filename, entry := range s.mu.entries {
		ev := &enginepb.FileEntry{}
		*ev = *entry
		rv.Files[filename] = ev
	}
	return rv
}

// SetFileEntry writes the entry to the local node's registry and
// updates the merged map. When this method returns, the entry is
// durable on the underlying filesystem. Callers may safely proceed
// with Raft proposals or MANIFEST updates.
func (s *NodeFileRegistry) SetFileEntry(filename string, entry *enginepb.FileEntry) error {
	filename = s.localReg.NormalizeFilename(filename)
	if entry == nil {
		return s.MaybeDeleteEntry(filename)
	}
	// Write to the local registry first (does its own locking and I/O).
	if err := s.localReg.SetFileEntry(filename, entry); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.entries[filename] = entry
	return nil
}

// MaybeDeleteEntry deletes the entry from the local node's registry and
// removes it from the merged map.
func (s *NodeFileRegistry) MaybeDeleteEntry(filename string) error {
	filename = s.localReg.NormalizeFilename(filename)
	if err := s.localReg.MaybeDeleteEntry(filename); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.mu.entries, filename)
	return nil
}

// MaybeCopyEntry copies the entry for src to dst. The source entry is
// looked up from the merged map (it may belong to a remote node),
// while the destination is written to the local node's registry.
func (s *NodeFileRegistry) MaybeCopyEntry(src, dst string) error {
	src = s.localReg.NormalizeFilename(src)
	dst = s.localReg.NormalizeFilename(dst)
	// Look up the source entry from the merged map.
	s.mu.RLock()
	srcEntry := s.mu.entries[src]
	s.mu.RUnlock()

	if srcEntry == nil {
		// Source has no registry entry. Don't modify the destination's
		// entry. When encryption-at-rest is enabled all data files have
		// entries, so a nil srcEntry during a Rename retry indicates
		// crash recovery: the previous attempt's MaybeDeleteEntry was
		// persisted but the FS rename was not, leaving the source file
		// on disk without a registry entry. Preserving the destination's
		// entry (copied from the source in the prior attempt) ensures
		// the file can still be decrypted after the FS rename completes.
		return nil
	}

	// Write dst to the local registry.
	if err := s.localReg.SetFileEntry(dst, srcEntry); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.entries[dst] = srcEntry
	return nil
}

// MaybeLinkEntry delegates to MaybeCopyEntry.
func (s *NodeFileRegistry) MaybeLinkEntry(src, dst string) error {
	return s.MaybeCopyEntry(src, dst)
}

// SealPending seals the local node's registry if entries are pending.
// See FileRegistry.SealPending for details.
func (s *NodeFileRegistry) SealPending() {
	s.localReg.SealPending()
}

// Close closes the local node's FileRegistry.
func (s *NodeFileRegistry) Close() error {
	return s.localReg.Close()
}
