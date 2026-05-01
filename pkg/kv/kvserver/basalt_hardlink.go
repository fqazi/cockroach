// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/vfs"
)

// linkManifestAndFiles hardlinks the manifest and SSTable files from srcDir to
// dstDir. If a destination file already exists, the error is ignored since
// file numbers are unique (same filenum always refers to the same content).
func linkManifestAndFiles(basaltFS vfs.FS, srcDir, dstDir string, info storage.ManifestInfo) error {
	link := func(name string) error {
		srcPath := basaltFS.PathJoin(srcDir, name)
		dstPath := basaltFS.PathJoin(dstDir, name)
		if err := basaltFS.Link(srcPath, dstPath); err != nil && !oserror.IsExist(err) {
			return errors.Wrapf(err, "linking %s", name)
		}
		return nil
	}
	for _, file := range info.Files {
		if err := link(file.Name); err != nil {
			return err
		}
	}
	return link(info.Manifest.Name)
}

// createSnapshotHardlinks creates hardlinks for the RS manifest and SSTable
// files from the sender's replica directory to the recipient's directory.
// This is called during snapshot sending to ensure the recipient can open
// the RSEngine and read range-shared data.
func createSnapshotHardlinks(
	basaltFS vfs.FS,
	srcStoreID roachpb.StoreID,
	srcRangeID roachpb.RangeID,
	srcReplicaID roachpb.ReplicaID,
	dstStoreID roachpb.StoreID,
	dstRangeID roachpb.RangeID,
	dstReplicaID roachpb.ReplicaID,
	manifestInfo storage.ManifestInfo,
) error {
	srcDir := BasaltDir(basaltFS, srcStoreID, srcRangeID, srcReplicaID)
	dstDir := BasaltDir(basaltFS, dstStoreID, dstRangeID, dstReplicaID)
	if err := basaltFS.MkdirAll(dstDir, 0755); err != nil {
		return errors.Wrap(err, "creating destination directory")
	}
	return linkManifestAndFiles(basaltFS, srcDir, dstDir, manifestInfo)
}
