// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fs

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/basaltclient"
	"github.com/cockroachdb/basaltclient/basaltpb"
	"github.com/cockroachdb/basaltfs"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/storageconfig"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
)

// BasaltEnvConfig holds shared basalt resources used by all basalt stores on a
// node. A single ControllerClient and BlobDataClientPool are shared across
// stores for efficiency. The caller (CreateEngines) creates these shared
// resources once and registers cleanup via Env.OnClose on the last basalt Env.
//
// Encryption is split into two independent domains:
//   - Store-local: each store creates its own per-store DataKeyManager +
//     FileRegistry via NewBasaltStoreEncryptedEnv. No cross-node key discovery
//     is needed since store-local Pebble files are only read/written by the
//     owning node.
//   - RSEngine: a single cluster-scoped encryption env using
//     NodeDataKeyManager + NodeFileRegistry for cross-node key discovery.
//     RSEngine files may be hardlinked across nodes.
type BasaltEnvConfig struct {
	// ControllerClient is the shared gRPC client to the basalt controller.
	ControllerClient *basaltclient.ControllerClient
	// DataPool is the shared connection pool for blob server data operations.
	DataPool *basaltclient.BlobDataClientPool
	// Aliases maps alias names to controller addresses, parsed from
	// --basalt=<alias>@<addr1>,<addr2> flags.
	Aliases map[string][]string
	// ClusterID is the UUID identifying this CockroachDB cluster in basalt's
	// namespace hierarchy. Provided via --cluster-id.
	ClusterID string
	// LocalZone is this node's zone from the locality configuration, used to
	// prefer local replicas for reads.
	LocalZone string
	// StoreKeyOptions holds the encryption key file paths and rotation period.
	// Non-nil when --basalt-store-key is provided.
	StoreKeyOptions *storageconfig.EncryptionOptions
	// NodeID is the unique node identifier for per-node encryption directories.
	NodeID roachpb.NodeID

	// ClusterFS is the single basaltfs.FS rooted at the cluster directory,
	// shared across all stores. Created once in CreateEngines. Nil when basalt
	// is not configured.
	ClusterFS vfs.FS

	// LocalFS is the local filesystem for reading store key files.
	// Typically vfs.Default. Stored here for testability.
	LocalFS vfs.FS

	// RSEngineEncryptionEnv is the cluster-scoped encryption env for RSEngine
	// only. Uses NodeDataKeyManager + NodeFileRegistry for cross-node key
	// discovery of hardlinked files. Nil when encryption is not configured.
	RSEngineEncryptionEnv *EncryptionEnv
	// ClusterEncryptedFS is the encrypted FS for RSEngine operations.
	// Passed to StoreConfig.BasaltFS. Nil when encryption is not configured.
	ClusterEncryptedFS vfs.FS
}

// Close releases the shared basalt resources (RSEngine encryption env, cluster
// FS, controller client, and data pool). This should be called after all
// basalt-backed engines have been closed. Per-store encryption envs are closed
// individually by each Env.Close().
func (c *BasaltEnvConfig) Close() {
	if c.RSEngineEncryptionEnv != nil {
		_ = c.RSEngineEncryptionEnv.Closer.Close()
	}
	if c.ClusterFS != nil {
		if closer, ok := c.ClusterFS.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
	}
	if c.DataPool != nil {
		_ = c.DataPool.Close()
	}
	if c.ControllerClient != nil {
		_ = c.ControllerClient.Close()
	}
}

// AliasResolver implements basaltclient.AliasResolver using an alias map
// that maps alias names to controller addresses.
type AliasResolver struct {
	Aliases map[string][]string
}

// Resolve returns the controller addresses for the given alias name.
func (r *AliasResolver) Resolve(name string) ([]string, error) {
	addrs, ok := r.Aliases[name]
	if !ok {
		return nil, fmt.Errorf("unknown basalt alias %q", name)
	}
	return addrs, nil
}

// initBasaltEnv creates an Env backed by a per-store basaltfs.FS for a basalt
// store spec.
//
// The store spec path has the format "basalt://<alias-or-addr>/<store-path>"
// which is parsed by basaltclient.ParsePath. The store path (e.g. "store-1")
// identifies a subdirectory within the cluster directory.
//
// Each store gets its own basaltfs.FS rooted at the store's directory. This
// prevents Lock conflicts: when Pebble calls Lock(), each store's FS locks
// its own directory rather than all stores contending on the shared cluster
// directory. The ControllerClient and DataPool from BasaltEnvConfig are shared
// across all per-store FS instances.
func initBasaltEnv(ctx context.Context, spec base.StoreSpec, cfg EnvConfig) (*Env, error) {
	if cfg.Basalt == nil {
		return nil, errors.New("basalt store configured but BasaltEnvConfig is nil")
	}
	bc := cfg.Basalt

	// Parse the basalt:// path to extract the store path.
	resolver := &AliasResolver{Aliases: bc.Aliases}
	parsed, err := basaltclient.ParsePath(spec.Path, bc.LocalZone, resolver)
	if err != nil {
		return nil, errors.Wrapf(err, "parsing basalt path %q", spec.Path)
	}
	if parsed == nil {
		return nil, errors.Newf("basalt store path %q is not a valid basalt path", spec.Path)
	}

	storePath := strings.TrimPrefix(parsed.Path, "/")
	if storePath == "" {
		return nil, errors.Newf(
			"basalt store path %q has no store identifier after the controller address",
			spec.Path,
		)
	}

	// Resolve the namespace hierarchy: root → cluster-id → store-path.
	// The cluster ID directory groups all stores belonging to this CRDB
	// cluster. The store path identifies the specific store within the
	// cluster.
	ctrl := bc.ControllerClient

	// Ensure the cluster directory exists. The root directory ID is the
	// zero UUID for basalt controllers.
	var rootID basaltpb.UUID
	clusterDirID, err := ctrl.Mkdir(ctx, rootID, bc.ClusterID, nil)
	if err != nil {
		// Directory may already exist; try to look it up.
		resp, lookupErr := ctrl.StatByPath(ctx, rootID, bc.ClusterID)
		if lookupErr != nil {
			return nil, errors.Wrapf(err, "creating cluster directory %q", bc.ClusterID)
		}
		clusterDirID = resp.Meta.Id
	}

	// Walk the store path components, creating directories as needed.
	storeDirID := clusterDirID
	for _, component := range strings.Split(storePath, "/") {
		if component == "" {
			continue
		}
		dirID, mkErr := ctrl.Mkdir(ctx, storeDirID, component, nil)
		if mkErr != nil {
			resp, lookupErr := ctrl.StatByPath(ctx, storeDirID, component)
			if lookupErr != nil {
				return nil, errors.Wrapf(mkErr, "creating store directory %q", component)
			}
			dirID = resp.Meta.Id
		}
		storeDirID = dirID
	}

	// Create a per-store basaltfs.FS rooted at the store directory. This
	// prevents Lock conflicts: when Pebble calls Lock(), each store's FS
	// locks its own directory rather than all stores contending on the
	// shared cluster directory. The ControllerClient and DataPool are shared
	// (caller-owned), so only the FS itself needs to be closed.
	bfs, err := basaltfs.NewFS(basaltfs.Options{
		ControllerClient: ctrl,
		DataPool:         bc.DataPool,
		DirectoryID:      storeDirID,
		LocalZone:        bc.LocalZone,
		NegativeDirCache: true,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "creating per-store basaltfs for %q", storePath)
	}

	env := NewBasicEnv(bfs, ".")
	env.OnClose(func() { _ = bfs.Close() })

	// If encryption is configured, create a per-store encryption env with
	// its own DataKeyManager and FileRegistry. Each store's encryption is
	// independent — no cross-node key discovery is needed since store-local
	// Pebble files are only read/written by the owning node.
	if bc.StoreKeyOptions != nil {
		encEnv, registry, err := NewBasaltStoreEncryptedEnv(
			ctx, bfs, bc.LocalFS, ".",
			false, bc.StoreKeyOptions,
		)
		if err != nil {
			return nil, errors.Wrapf(err, "initializing per-store encryption for %q", storePath)
		}
		env.Encryption = encEnv
		env.Registry = registry
		env.defaultFS = encEnv.FS
	}

	// Validate the min-version file so that Pebble knows whether this is a
	// new store or an existing one. Without this, StoreClusterVersion remains
	// empty and Pebble sets ErrorIfNotPristine=true, which causes restart
	// failures when WAL files from the previous run are present.
	env.StoreClusterVersion, err = ValidateMinVersionFile(bfs, ".", cfg.Version)
	if err != nil {
		return nil, err
	}

	return env, nil
}

// ParseBasaltAliases parses --basalt flag values into an alias map. Each value
// has the format "<alias>@<addr1>,<addr2>,...".
func ParseBasaltAliases(specs []string) (map[string][]string, error) {
	aliases := make(map[string][]string, len(specs))
	for _, spec := range specs {
		idx := strings.Index(spec, "@")
		if idx < 0 {
			return nil, errors.Newf(
				"invalid --basalt value %q: expected format <alias>@<addr1>,<addr2>,...", spec,
			)
		}
		alias := spec[:idx]
		if alias == "" {
			return nil, errors.Newf("invalid --basalt value %q: empty alias name", spec)
		}
		addrs := strings.Split(spec[idx+1:], ",")
		for i, addr := range addrs {
			addrs[i] = strings.TrimSpace(addr)
			if addrs[i] == "" {
				return nil, errors.Newf(
					"invalid --basalt value %q: empty controller address", spec,
				)
			}
		}
		if _, exists := aliases[alias]; exists {
			return nil, errors.Newf("duplicate basalt alias %q", alias)
		}
		aliases[alias] = addrs
	}
	return aliases, nil
}
