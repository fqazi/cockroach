// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fs

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestNodeFileRegistry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	mem := vfs.NewMem()
	ctx := context.Background()
	const registryDir = "/registry"

	// Track open registries keyed by node ID.
	regs := make(map[roachpb.NodeID]*NodeFileRegistry)
	var currentReg *NodeFileRegistry
	var currentNode roachpb.NodeID

	datadriven.RunTest(t, datapathutils.TestDataPath(t, "node_file_registry"),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "open":
				var node int
				d.ScanArgs(t, "node", &node)
				nodeID := roachpb.NodeID(node)
				// Close any previous registry for this node.
				if prev, ok := regs[nodeID]; ok {
					require.NoError(t, prev.Close())
					delete(regs, nodeID)
				}
				reg, err := NewNodeFileRegistry(ctx, mem, registryDir, nodeID, false /* readOnly */)
				require.NoError(t, err)
				regs[nodeID] = reg
				currentReg = reg
				currentNode = nodeID
				return "ok\n"

			case "close":
				require.NotNil(t, currentReg)
				require.NoError(t, currentReg.Close())
				delete(regs, currentNode)
				currentReg = nil
				currentNode = 0
				return "ok\n"

			case "set":
				var file, settings string
				d.ScanArgs(t, "file", &file)
				d.ScanArgs(t, "settings", &settings)
				entry := &enginepb.FileEntry{
					EnvType:            enginepb.EnvType_Data,
					EncryptionSettings: []byte(settings),
				}
				require.NoError(t, currentReg.SetFileEntry(file, entry))
				return "ok\n"

			case "get":
				var file string
				d.ScanArgs(t, "file", &file)
				entry := currentReg.GetFileEntry(file)
				if entry == nil {
					return "<nil>\n"
				}
				return string(entry.EncryptionSettings) + "\n"

			case "delete":
				var file string
				d.ScanArgs(t, "file", &file)
				require.NoError(t, currentReg.MaybeDeleteEntry(file))
				return "ok\n"

			case "copy":
				var src, dst string
				d.ScanArgs(t, "src", &src)
				d.ScanArgs(t, "dst", &dst)
				require.NoError(t, currentReg.MaybeCopyEntry(src, dst))
				return "ok\n"

			case "list":
				type fe struct {
					name  string
					entry *enginepb.FileEntry
				}
				var entries []fe
				for name, entry := range currentReg.List() {
					entries = append(entries, fe{name: name, entry: entry})
				}
				slices.SortFunc(entries, func(a, b fe) int {
					return cmp.Compare(a.name, b.name)
				})
				var buf strings.Builder
				for _, e := range entries {
					fmt.Fprintf(&buf, "file=%s settings=%s\n", e.name, string(e.entry.EncryptionSettings))
				}
				return buf.String()

			case "parse-node-id":
				var name string
				d.ScanArgs(t, "name", &name)
				id, ok := parseNodeID(name)
				if !ok {
					return "err\n"
				}
				return fmt.Sprintf("id=%d\n", id)

			case "mkdir":
				for _, line := range strings.Split(d.Input, "\n") {
					line = strings.TrimSpace(line)
					if line == "" {
						continue
					}
					require.NoError(t, mem.MkdirAll(line, 0755))
				}
				return "ok\n"

			case "remote-set":
				// Write directly to a node's underlying FileRegistry,
				// bypassing the open NodeFileRegistry. This simulates a
				// remote node writing an entry after our registry is open.
				var node int
				var file, settings string
				d.ScanArgs(t, "node", &node)
				d.ScanArgs(t, "file", &file)
				d.ScanArgs(t, "settings", &settings)
				nodeDir := mem.PathJoin(registryDir, fmt.Sprintf("n%d", node))
				require.NoError(t, mem.MkdirAll(nodeDir, 0755))
				reg := &FileRegistry{
					FS:                      mem,
					DBDir:                   nodeDir,
					ReadOnly:                false,
					NumOldRegistryFiles:     defaultNumOldFileRegistryFiles,
					CanElideEntry:           elidePlaintext,
					SkipFileDeletionElision: true,
				}
				require.NoError(t, reg.Load(ctx))
				entry := &enginepb.FileEntry{
					EnvType:            enginepb.EnvType_Data,
					EncryptionSettings: []byte(settings),
				}
				require.NoError(t, reg.SetFileEntry(file, entry))
				require.NoError(t, reg.Close())
				return "ok\n"

			case "mkfile":
				for _, line := range strings.Split(d.Input, "\n") {
					line = strings.TrimSpace(line)
					if line == "" {
						continue
					}
					f, err := mem.Create(line, UnspecifiedWriteCategory)
					require.NoError(t, err)
					require.NoError(t, f.Close())
				}
				return "ok\n"

			default:
				t.Fatalf("unrecognized command: %s", d.Cmd)
				return ""
			}
		})
}
