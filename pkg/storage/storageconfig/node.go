// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storageconfig

// Node contains all the node-level storage related configuration.
type Node struct {
	// WALFailover enables and configures automatic WAL failover when latency to
	// a store's primary WAL increases.
	WALFailover WALFailover
	// SharedStorage is specified to enable disaggregated shared storage. It is
	// enabled if the uri is set.
	SharedStorage SharedStorage
	// BasaltConfig holds basalt-related configuration from CLI flags. Basalt
	// provides disaggregated object storage for Pebble SSTables.
	BasaltConfig BasaltConfig
}

// BasaltConfig holds the basalt-related configuration from CLI flags. The
// raw string values are stored here to avoid importing basalt-specific
// packages in this low-level config package. The actual basalt client setup
// happens in the pkg/storage/basalt integration package.
type BasaltConfig struct {
	// AliasSpecs contains the raw --basalt flag values. Each entry has the
	// format "<alias>@<controller1>,<controller2>,...".
	AliasSpecs []string
	// ClusterID is the UUID string identifying the CockroachDB cluster in
	// basalt's namespace, provided via --cluster-id.
	ClusterID string
	// StoreKey is the path to the active store key file for basalt
	// encryption-at-rest, or "plain" for no encryption.
	StoreKey string
	// StoreKeyOld is the path to the previous store key file for basalt
	// encryption-at-rest, or "plain". Used during key rotation.
	StoreKeyOld string
	// NodeID is the unique node identifier for per-node encryption
	// directories within basalt.
	NodeID int32
}

// SharedStorage specifies the properties of the shared storage.
type SharedStorage struct {
	// URI is the base location to read and write shared storage files.
	URI string
	// Cache is the size of the secondary cache used to store blocks from
	// disaggregated shared storage.
	Cache Size
}
