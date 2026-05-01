// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fs

import (
	"context"
	"fmt"
	"io"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storageconfig"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
)

// High-level code structure.
//
// A pebble instance can operate in two modes:
// - With a single FS, used for all files.
// - With three FSs, when encryption-at-rest is on. We refer to these FSs as the base-FS,
//   store-FS and data-FS. The base-FS contains unencrypted files, the store-FS contains
//   files encrypted using the user-specified store keys, and the data-FS contains files
//   encrypted using generated keys.
//
// Both the data-FS and store-FS are wrappers around the base-FS, i.e., they encrypt and
// decrypt data being written to and read from files in the base-FS. The environment in which
// these exist knows which file to open in which FS. Specifically,
// - The file registry uses the base-FS to store the FileRegistry proto. This registry provides
//   information about files in the store-FS and data-FS (the EnvType::Store and EnvType::Data
//   respectively). This information includes which FS the file belongs to (for consistency
//   checking that a file created using one FS is not being read in another) and information
//   about encryption settings used for the file, including the key id.
// - The StoreKeyManager uses the base-FS to read the user-specified store keys at startup.
//   These are in two key files: the active key file and the old key file, which contain the
//   key id and the key.
// - The store-FS is used only for storing the key file for the generated keys. It is used by
//   the DataKeyManager. These keys are rotated periodically in a simple manner -- a new
//   active key is generated for future file writes. Existing files are not affected.
//
// The data-FS and store-FS both use a common implementation. They consume:
// - the FS they are wrapping: it is always the base-FS in our case, but it does not matter.
// - The single file registry they share to record information about their files.
// - A KeyManager interface wrapped in a FileStreamCreator, that provides a simple
//   interface for encrypting and decrypting data in a file at arbitrary byte offsets.
//
// Both data-FS and store-FS can be operating in plaintext mode if the user-specified store
// keys are "plain".
//
// For query execution spilling to disk: we want to use encryption, but the registries do not need
// to be on disk since on restart the query path will wipe all the existing files it has written.
// The setup would include a memFS and there would logically be four FSs: base-FS (unencrypted
// disk based FS), mem-FS (unencrypted memory based FS), store-FS (encrypted memory based FS),
// data-FS (encrypted disk based FS).
// - The file registry uses mem-FS to store the FileRegistry proto.
// - The StoreKeyManager uses the base-FS to read the user-specified store keys.
// - The store-FS wraps a mem-FS for reading/writing data keys.
// - The DataKeyManager uses the store-FS.
// - The data-FS wraps a base-FS for reading/writing data.

// NewEncryptedEnv creates an encrypted environment and returns the vfs.FS to
// use for reading and writing data. It's a variable because a few tests mock
// it.
var NewEncryptedEnv = func(
	unencryptedFS vfs.FS,
	fr FileRegistrar,
	dbDir string,
	readOnly bool,
	options *storageconfig.EncryptionOptions,
) (*EncryptionEnv, error) {
	if options.KeySource != storageconfig.EncryptionKeyFromFiles {
		return nil, fmt.Errorf("unknown encryption key source: %d", options.KeySource)
	}
	storeKeyManager := &StoreKeyManager{
		fs:                unencryptedFS,
		activeKeyFilename: options.KeyFiles.CurrentKey,
		oldKeyFilename:    options.KeyFiles.OldKey,
	}
	if err := storeKeyManager.Load(context.TODO()); err != nil {
		return nil, err
	}
	storeFS := &encryptedFS{
		FS:           unencryptedFS,
		fileRegistry: fr,
		streamCreator: &FileCipherStreamCreator{
			envType:    enginepb.EnvType_Store,
			keyManager: storeKeyManager,
		},
	}
	dataKeyManager, err := NewDataKeyManager(
		context.TODO(), storeFS, dbDir, options.RotationPeriod, readOnly,
	)
	if err != nil {
		return nil, err
	}
	dataFS := &encryptedFS{
		FS:           unencryptedFS,
		fileRegistry: fr,
		streamCreator: &FileCipherStreamCreator{
			envType:    enginepb.EnvType_Data,
			keyManager: dataKeyManager,
		},
	}

	if !readOnly {
		key, err := storeKeyManager.ActiveKeyForWriter(context.TODO())
		if err != nil {
			return nil, err
		}
		if err := dataKeyManager.SetActiveStoreKeyInfo(context.TODO(), key.Info); err != nil {
			return nil, err
		}
	}

	return &EncryptionEnv{
		Closer: dataKeyManager,
		FS:     dataFS,
		StatsHandler: &encryptionStatsHandler{
			storeKM: storeKeyManager,
			dataKM:  dataKeyManager,
		},
	}, nil
}

// multiCloser is an io.Closer that closes multiple closers in order.
// If any closer returns an error, the first error is returned after
// all closers have been called.
type multiCloser []io.Closer

func (mc multiCloser) Close() error {
	var firstErr error
	for _, c := range mc {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// NewClusterEncryptedEnv creates a multi-node EncryptionEnv for
// cluster-scoped encryption on shared storage (e.g. basaltfs). It
// wires together a per-node NodeDataKeyManager (on the shared FS), a
// sharded NodeFileRegistry (on the shared FS), and a per-node
// StoreKeyManager (from local key files).
//
// This is used for RSEngine encryption, where files may be hardlinked
// across nodes and require cross-node key discovery. Store-local Pebble
// files use independent per-store encryption via NewBasaltStoreEncryptedEnv
// instead.
//
// sharedFS is the shared filesystem (e.g. basaltfs) used for the file
// registry and data key storage. localFS is used to read store key files
// from local disk (typically vfs.Default). registryDir is the parent
// directory for sharded registry entries (e.g.
// "encryption/registry/"). dataKeysDir is the parent directory for
// per-node data key directories (e.g. "encryption/datakeys/").
//
// Each node manages its own data keys independently in a subdirectory
// named "n<nodeID>" under dataKeysDir. Cross-node key discovery
// happens on demand via cache-miss reads, mirroring the
// NodeFileRegistry pattern.
//
// All nodes must use the same store key. Each node's data keys are
// encrypted by the store key via the storeFS layer. When a node reads
// a file written by another node, it decrypts the remote node's data
// keys using its own store key — a mismatch causes silent decryption
// failure. TODO(annie): add store key fingerprint validation.
//
// The returned FileRegistrar is the NodeFileRegistry; callers that
// need to inspect the registry (e.g. for stats) can use it directly.
func NewClusterEncryptedEnv(
	ctx context.Context,
	sharedFS vfs.FS,
	localFS vfs.FS,
	registryDir string,
	dataKeysDir string,
	nodeID roachpb.NodeID,
	readOnly bool,
	storeKeyOptions *storageconfig.EncryptionOptions,
) (*EncryptionEnv, FileRegistrar, error) {
	if storeKeyOptions.KeySource != storageconfig.EncryptionKeyFromFiles {
		return nil, nil, fmt.Errorf("unknown encryption key source: %d", storeKeyOptions.KeySource)
	}

	// Create the node file registry on the shared FS.
	nodeRegistry, err := NewNodeFileRegistry(
		ctx, sharedFS, registryDir, nodeID, readOnly,
	)
	if err != nil {
		return nil, nil, errors.Wrap(err, "creating node file registry")
	}

	// Create the StoreKeyManager from local key files. The store key files
	// live on local disk (localFS), not on the shared FS.
	storeKeyManager := &StoreKeyManager{
		fs:                localFS,
		activeKeyFilename: storeKeyOptions.KeyFiles.CurrentKey,
		oldKeyFilename:    storeKeyOptions.KeyFiles.OldKey,
	}
	if err := storeKeyManager.Load(ctx); err != nil {
		_ = nodeRegistry.Close()
		return nil, nil, errors.Wrap(err, "loading store key manager")
	}

	// Create the store-encrypted FS for reading/writing data keys.
	storeFS := &encryptedFS{
		FS:           sharedFS,
		fileRegistry: nodeRegistry,
		streamCreator: &FileCipherStreamCreator{
			envType:    enginepb.EnvType_Store,
			keyManager: storeKeyManager,
		},
	}

	// Create the NodeDataKeyManager on the store-encrypted FS.
	nodeDataKeyManager, err := NewNodeDataKeyManager(
		ctx, storeFS, dataKeysDir, nodeID,
		storeKeyOptions.RotationPeriod, readOnly,
	)
	if err != nil {
		_ = nodeRegistry.Close()
		return nil, nil, errors.Wrap(err, "creating node data key manager")
	}

	// Create the data-encrypted FS for reading/writing data files.
	dataFS := &encryptedFS{
		FS:           sharedFS,
		fileRegistry: nodeRegistry,
		streamCreator: &FileCipherStreamCreator{
			envType:    enginepb.EnvType_Data,
			keyManager: nodeDataKeyManager,
		},
	}

	if !readOnly {
		key, err := storeKeyManager.ActiveKeyForWriter(ctx)
		if err != nil {
			_ = nodeDataKeyManager.Close()
			_ = nodeRegistry.Close()
			return nil, nil, errors.Wrap(err, "getting active store key")
		}
		if err := nodeDataKeyManager.SetActiveStoreKeyInfo(ctx, key.Info); err != nil {
			_ = nodeDataKeyManager.Close()
			_ = nodeRegistry.Close()
			return nil, nil, errors.Wrap(err, "setting active store key info")
		}
	}

	env := &EncryptionEnv{
		Closer: multiCloser{nodeDataKeyManager, nodeRegistry},
		FS:     dataFS,
		StatsHandler: &encryptionStatsHandler{
			storeKM: storeKeyManager,
			dataKM:  nodeDataKeyManager.localKM,
		},
	}
	return env, nodeRegistry, nil
}

// NewBasaltStoreEncryptedEnv creates a per-store EncryptionEnv for basalt
// stores on shared storage. Unlike NewClusterEncryptedEnv, this uses a plain
// DataKeyManager + FileRegistry (no cross-node key discovery), because
// store-local Pebble files are only ever read/written by the owning node.
//
// sharedFS is the shared filesystem (e.g. basaltfs) used for the file registry,
// data key storage, and data encryption. localFS is used to read store key
// files from local disk (typically vfs.Default). dbDir is the store's directory
// within the shared FS (e.g. "store-1").
func NewBasaltStoreEncryptedEnv(
	ctx context.Context,
	sharedFS vfs.FS,
	localFS vfs.FS,
	dbDir string,
	readOnly bool,
	storeKeyOptions *storageconfig.EncryptionOptions,
) (*EncryptionEnv, FileRegistrar, error) {
	if storeKeyOptions.KeySource != storageconfig.EncryptionKeyFromFiles {
		return nil, nil, fmt.Errorf("unknown encryption key source: %d", storeKeyOptions.KeySource)
	}

	// Create the file registry on the shared FS within the store directory.
	// SealAfterWrite and SyncDir are required on basaltfs: writes are
	// batched in memory and flushed to a sealed snapshot file via
	// SealPending(), and directory syncs ensure marker visibility.
	fileRegistry := &FileRegistry{
		FS:                  sharedFS,
		DBDir:               dbDir,
		ReadOnly:            readOnly,
		NumOldRegistryFiles: defaultNumOldFileRegistryFiles,
		CanElideEntry:       elidePlaintext,
		SyncDir:             true,
		SealAfterWrite:      true,
	}
	if err := fileRegistry.Load(ctx); err != nil {
		return nil, nil, errors.Wrap(err, "creating store file registry")
	}

	// Create the StoreKeyManager from local key files on local disk.
	storeKeyManager := &StoreKeyManager{
		fs:                localFS,
		activeKeyFilename: storeKeyOptions.KeyFiles.CurrentKey,
		oldKeyFilename:    storeKeyOptions.KeyFiles.OldKey,
	}
	if err := storeKeyManager.Load(ctx); err != nil {
		_ = fileRegistry.Close()
		return nil, nil, errors.Wrap(err, "loading store key manager")
	}

	// Create the store-encrypted FS for reading/writing data keys.
	storeFS := &encryptedFS{
		FS:           sharedFS,
		fileRegistry: fileRegistry,
		streamCreator: &FileCipherStreamCreator{
			envType:    enginepb.EnvType_Store,
			keyManager: storeKeyManager,
		},
	}

	// Create the DataKeyManager on the store-encrypted FS.
	dataKeyManager, err := NewDataKeyManager(
		ctx, storeFS, dbDir, storeKeyOptions.RotationPeriod, readOnly,
	)
	if err != nil {
		_ = fileRegistry.Close()
		return nil, nil, errors.Wrap(err, "creating data key manager")
	}

	// Create the data-encrypted FS for reading/writing data files.
	dataFS := &encryptedFS{
		FS:           sharedFS,
		fileRegistry: fileRegistry,
		streamCreator: &FileCipherStreamCreator{
			envType:    enginepb.EnvType_Data,
			keyManager: dataKeyManager,
		},
	}

	if !readOnly {
		key, err := storeKeyManager.ActiveKeyForWriter(ctx)
		if err != nil {
			_ = dataKeyManager.Close()
			_ = fileRegistry.Close()
			return nil, nil, errors.Wrap(err, "getting active store key")
		}
		if err := dataKeyManager.SetActiveStoreKeyInfo(ctx, key.Info); err != nil {
			_ = dataKeyManager.Close()
			_ = fileRegistry.Close()
			return nil, nil, errors.Wrap(err, "setting active store key info")
		}
	}

	// Only close the DataKeyManager here; the FileRegistry is returned
	// separately and closed by Env.Registry.Close() (see Env.Close).
	// This mirrors the pattern used by NewEncryptedEnv.
	env := &EncryptionEnv{
		Closer: dataKeyManager,
		FS:     dataFS,
		StatsHandler: &encryptionStatsHandler{
			storeKM: storeKeyManager,
			dataKM:  dataKeyManager,
		},
	}
	return env, fileRegistry, nil
}

// resolveEncryptedEnvOptions creates the EncryptionEnv and associated file
// registry if this store has encryption-at-rest enabled; otherwise returns a
// nil EncryptionEnv.
func resolveEncryptedEnvOptions(
	ctx context.Context,
	unencryptedFS vfs.FS,
	dir string,
	encryptionOpts *storageconfig.EncryptionOptions,
	rw RWMode,
) (FileRegistrar, *EncryptionEnv, error) {
	if encryptionOpts == nil {
		// There's no encryption config. This is valid if the user doesn't
		// intend to use encryption-at-rest, and the store has never had
		// encryption-at-rest enabled. Validate that there's no file registry.
		// If there is, the caller is required to specify an
		// --enterprise-encryption flag for this store.
		if err := checkNoRegistryFile(unencryptedFS, dir); err != nil {
			return nil, nil, fmt.Errorf("encryption was used on this store before, but no encryption flags " +
				"specified. You must fully specify the --enterprise-encryption flag")
		}
		return nil, nil, nil
	}

	fileRegistry, err := NewFileRegistry(ctx, unencryptedFS, dir, rw == ReadOnly)
	if err != nil {
		return nil, nil, err
	}
	env, err := NewEncryptedEnv(unencryptedFS, fileRegistry, dir, rw == ReadOnly, encryptionOpts)
	if err != nil {
		return nil, nil, errors.WithSecondaryError(err, fileRegistry.Close())
	}
	return fileRegistry, env, nil
}

// EncryptionEnv describes the encryption-at-rest environment, providing
// access to a filesystem with on-the-fly encryption.
type EncryptionEnv struct {
	// Closer closes the encryption-at-rest environment. Once the
	// environment is closed, the environment's VFS may no longer be
	// used.
	Closer io.Closer
	// FS provides the encrypted virtual filesystem. New files are
	// transparently encrypted.
	FS vfs.FS
	// StatsHandler exposes encryption-at-rest state for observability.
	StatsHandler EncryptionStatsHandler
}

// EncryptionRegistries contains the encryption-related registries:
// Both are serialized protobufs.
type EncryptionRegistries struct {
	// FileRegistry is the list of files with encryption status.
	// serialized storage/engine/enginepb/file_registry.proto::FileRegistry
	FileRegistry []byte
	// KeyRegistry is the list of keys, scrubbed of actual key data.
	// serialized storage/enginepb/key_registry.proto::DataKeysRegistry
	KeyRegistry []byte
}

// EncryptionStatsHandler provides encryption related stats.
type EncryptionStatsHandler interface {
	// Returns a serialized enginepb.EncryptionStatus.
	GetEncryptionStatus() ([]byte, error)
	// Returns a serialized enginepb.DataKeysRegistry, scrubbed of key contents.
	GetDataKeysRegistry() ([]byte, error)
	// Returns the ID of the active data key, or "plain" if none.
	GetActiveDataKeyID() (string, error)
	// Returns the enum value of the encryption type.
	GetActiveStoreKeyType() int32
	// Returns the KeyID embedded in the serialized EncryptionSettings.
	GetKeyIDFromSettings(settings []byte) (string, error)
}

// EnvStats is a set of RocksDB env stats, including encryption status.
type EnvStats struct {
	// TotalFiles is the total number of files reported by rocksdb.
	TotalFiles uint64
	// TotalBytes is the total size of files reported by rocksdb.
	TotalBytes uint64
	// ActiveKeyFiles is the number of files using the active data key.
	ActiveKeyFiles uint64
	// ActiveKeyBytes is the size of files using the active data key.
	ActiveKeyBytes uint64
	// EncryptionType is an enum describing the active encryption algorithm.
	// See: storage/engine/enginepb/key_registry.proto
	EncryptionType int32
	// EncryptionStatus is a serialized enginepb/stats.proto::EncryptionStatus protobuf.
	EncryptionStatus []byte
}

// RegistrySealer is implemented by filesystems that buffer registry
// entries and need an explicit seal to make them visible to other
// nodes. On shared filesystems (basaltfs), files are only visible
// after sealing; this interface lets callers flush pending entries at
// the right time (e.g. before a Raft proposal).
type RegistrySealer interface {
	SealPendingRegistryEntries()
}

// FileEntryProvider exposes the file registry's GetFileEntry and
// SetFileEntry operations. This is implemented by encryptedFS to allow
// callers with only a vfs.FS handle to install encryption entries
// received via Raft (e.g. the MANIFEST FileEntry in RSManifestInstall).
type FileEntryProvider interface {
	GetFileEntry(name string) *enginepb.FileEntry
	SetFileEntry(name string, entry *enginepb.FileEntry) error
}

// encryptedFS implements vfs.FS.
type encryptedFS struct {
	vfs.FS
	fileRegistry  FileRegistrar
	streamCreator *FileCipherStreamCreator
}

// SealPendingRegistryEntries seals any pending file registry entries so
// they become visible to other nodes on shared storage. This is a
// no-op if the underlying registry does not use SealAfterWrite.
func (fs *encryptedFS) SealPendingRegistryEntries() {
	type sealer interface {
		SealPending()
	}
	if s, ok := fs.fileRegistry.(sealer); ok {
		s.SealPending()
	}
}

var _ RegistrySealer = (*encryptedFS)(nil)
var _ FileEntryProvider = (*encryptedFS)(nil)

// GetFileEntry returns the file entry for the named file from the
// underlying file registry. Returns nil if no entry exists.
func (fs *encryptedFS) GetFileEntry(name string) *enginepb.FileEntry {
	return fs.fileRegistry.GetFileEntry(name)
}

// SetFileEntry writes the file entry for the named file into the
// underlying file registry.
func (fs *encryptedFS) SetFileEntry(name string, entry *enginepb.FileEntry) error {
	return fs.fileRegistry.SetFileEntry(name, entry)
}

// registerNewEncryption creates new encryption settings and registers them
// in the file registry. Returns the stream for encrypting file content.
func (fs *encryptedFS) registerNewEncryption(name string) (FileStream, error) {
	settings, stream, err := fs.streamCreator.CreateNew(context.TODO())
	if err != nil {
		return nil, err
	}
	// Add an entry for the file to the pebble file registry if it is encrypted.
	// We choose not to store an entry for unencrypted files since the absence of
	// a file in the file registry implies that it is unencrypted.
	if settings.EncryptionType == enginepb.EncryptionType_Plaintext {
		if err := fs.fileRegistry.MaybeDeleteEntry(name); err != nil {
			return nil, err
		}
	} else {
		fproto := &enginepb.FileEntry{}
		fproto.EnvType = fs.streamCreator.envType
		if fproto.EncryptionSettings, err = protoutil.Marshal(settings); err != nil {
			return nil, err
		}
		if err := fs.fileRegistry.SetFileEntry(name, fproto); err != nil {
			return nil, err
		}
	}
	return stream, nil
}

// initEncryptedFile creates new encryption settings for a file and registers
// it in the file registry. On error, f is closed.
func (fs *encryptedFS) initEncryptedFile(f vfs.File, name string) (vfs.File, error) {
	stream, err := fs.registerNewEncryption(name)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &encryptedFile{File: f, stream: stream}, nil
}

// Create implements vfs.FS.Create.
func (fs *encryptedFS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	f, err := fs.FS.Create(name, category)
	if err != nil {
		return f, err
	}
	return fs.initEncryptedFile(f, name)
}

// Link implements vfs.FS.Link.
func (fs *encryptedFS) Link(oldname, newname string) error {
	if err := fs.FS.Link(oldname, newname); err != nil {
		return err
	}
	return fs.fileRegistry.MaybeLinkEntry(oldname, newname)
}

// Open implements vfs.FS.Open.
func (fs *encryptedFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	f, err := fs.FS.Open(name, opts...)
	if err != nil {
		return f, err
	}
	fileEntry := fs.fileRegistry.GetFileEntry(name)
	var settings *enginepb.EncryptionSettings
	if fileEntry != nil {
		if fileEntry.EnvType != fs.streamCreator.envType {
			f.Close()
			return nil, fmt.Errorf("filename: %s has env %d not equal to FS env %d",
				name, fileEntry.EnvType, fs.streamCreator.envType)
		}
		settings = &enginepb.EncryptionSettings{}
		if err := protoutil.Unmarshal(fileEntry.EncryptionSettings, settings); err != nil {
			f.Close()
			return nil, err
		}
	}
	stream, err := fs.streamCreator.CreateExisting(settings)
	if err != nil {
		f.Close()
		return nil, err
	}
	return &encryptedFile{File: f, stream: stream}, nil
}

// Remove implements vfs.FS.Remove.
func (fs *encryptedFS) Remove(name string) error {
	if err := fs.FS.Remove(name); err != nil {
		return err
	}
	return fs.fileRegistry.MaybeDeleteEntry(name)
}

// Rename implements vfs.FS.Rename. A rename operation needs to both
// move the file's file registry entry and move the files on the
// physical filesystem. These operations cannot be done atomically. The
// encryptedFS's Rename operation provides atomicity only if the
// destination path does not exist.
//
// Rename will first copy the old path's file registry entry to the
// new path. If the destination exists, a crash after this copy will
// leave the file at the new path unreadable.
//
// Rename then performs a filesystem rename of the actual file. If a
// crash occurs after the rename, the file at the new path will be
// readable. The file at the old path won't exist, but the file registry
// will contain a dangling entry for the old path. The dangling entry
// will be elided when the file registry is loaded again.
func (fs *encryptedFS) Rename(oldname, newname string) error {
	// First copy the metadata from the old name to the new name. If a
	// file exists at newname, this copy action will make the file at
	// newname unlegible, because the encryption-at-rest metadata will
	// not match the file's encryption. This is what makes Rename
	// non-atomic. If no file exists at newname, the dangling copied
	// entry has no effect.
	if err := fs.fileRegistry.MaybeCopyEntry(oldname, newname); err != nil {
		return err
	}
	// Perform the filesystem rename. After the filesystem rename, the
	// new path is guaranteed to be readable.
	if err := fs.FS.Rename(oldname, newname); err != nil {
		return err
	}
	// Remove the old name's metadata.
	return fs.fileRegistry.MaybeDeleteEntry(oldname)
}

// ReuseForWrite implements vfs.FS.ReuseForWrite. The underlying file is
// reused to avoid filesystem metadata updates, but new encryption settings
// (nonce/counter) are generated to avoid CTR keystream reuse.
func (fs *encryptedFS) ReuseForWrite(
	oldname, newname string, category vfs.DiskWriteCategory,
) (vfs.File, error) {
	// Register metadata for newname BEFORE the filesystem operation, following
	// the same pattern as Rename. This ensures the file has metadata after the
	// filesystem operation succeeds. If we crash between registering metadata
	// and the filesystem operation, the dangling registry entry for newname is
	// cleaned up on registry load.
	stream, err := fs.registerNewEncryption(newname)
	if err != nil {
		return nil, err
	}
	// Reuse the underlying file (avoids filesystem metadata updates).
	f, err := fs.FS.ReuseForWrite(oldname, newname, category)
	if err != nil {
		return nil, err
	}
	// Delete old file's metadata.
	if err := fs.fileRegistry.MaybeDeleteEntry(oldname); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &encryptedFile{File: f, stream: stream}, nil
}

// encryptedFile implements vfs.File.
type encryptedFile struct {
	vfs.File
	mu struct {
		syncutil.Mutex
		rOffset int64
		wOffset int64
	}
	stream FileStream
}

// Write implements io.Writer.
func (f *encryptedFile) Write(p []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stream.Encrypt(f.mu.wOffset, p)
	n, err = f.File.Write(p)
	f.mu.wOffset += int64(n)
	return n, err
}

// Read implements io.Reader.
func (f *encryptedFile) Read(p []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, err = f.ReadAt(p, f.mu.rOffset)
	f.mu.rOffset += int64(n)
	return n, err
}

// ReadAt implements io.ReaderAt
func (f *encryptedFile) ReadAt(p []byte, off int64) (n int, err error) {
	n, err = f.File.ReadAt(p, off)
	if n > 0 {
		f.stream.Decrypt(off, p[:n])
	}
	return n, err
}

type encryptionStatsHandler struct {
	storeKM *StoreKeyManager
	dataKM  *DataKeyManager
}

func (e *encryptionStatsHandler) GetEncryptionStatus() ([]byte, error) {
	var s enginepb.EncryptionStatus
	if e.storeKM.activeKey != nil {
		s.ActiveStoreKey = e.storeKM.activeKey.Info
	}
	ki := e.dataKM.ActiveKeyInfoForStats()
	s.ActiveDataKey = ki
	return protoutil.Marshal(&s)
}

func (e *encryptionStatsHandler) GetDataKeysRegistry() ([]byte, error) {
	r := e.dataKM.getScrubbedRegistry()
	return protoutil.Marshal(r)
}

func (e *encryptionStatsHandler) GetActiveDataKeyID() (string, error) {
	ki := e.dataKM.ActiveKeyInfoForStats()
	if ki != nil {
		return ki.KeyId, nil
	}
	return "plain", nil
}

func (e *encryptionStatsHandler) GetActiveStoreKeyType() int32 {
	if e.storeKM.activeKey != nil {
		return int32(e.storeKM.activeKey.Info.EncryptionType)
	}
	return int32(enginepb.EncryptionType_Plaintext)
}

func (e *encryptionStatsHandler) GetKeyIDFromSettings(settings []byte) (string, error) {
	var s enginepb.EncryptionSettings
	if err := protoutil.Unmarshal(settings, &s); err != nil {
		return "", err
	}
	return s.KeyId, nil
}
