#!/usr/bin/env bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# This script bumps the basaltclient and basaltfs dependencies in cockroach's
# go.mod to the latest commits on their main branches. It mirrors the
# bump-pebble.sh workflow.
#
# Usage:
#
#   ./scripts/bump-basalt.sh [--client-sha <sha>] [--fs-sha <sha>]
#
# If SHAs are not provided, the latest commit on each repo's main branch is
# used. The script must be run from the cockroach repo root.

set -euo pipefail

BASALTCLIENT_BRANCH=main
BASALTFS_BRANCH=main
BASALTCLIENT_SHA=""
BASALTFS_SHA=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --client-sha)
      BASALTCLIENT_SHA="$2"
      shift 2
      ;;
    --fs-sha)
      BASALTFS_SHA="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1" >&2
      echo "Usage: $0 [--client-sha <sha>] [--fs-sha <sha>]" >&2
      exit 1
      ;;
  esac
done

BASALTCLIENT_MOD="github.com/cockroachdb/basaltclient"
BASALTFS_MOD="github.com/cockroachdb/basaltfs"

# These are private repos, so bypass the Go checksum database and module
# proxy. Without this, `go mod tidy` fails trying to verify checksums
# via sum.golang.org which can't access the repos.
export GONOSUMCHECK="${BASALTCLIENT_MOD},${BASALTFS_MOD}"
export GONOSUMDB="${BASALTCLIENT_MOD},${BASALTFS_MOD}"
export GOPRIVATE="${BASALTCLIENT_MOD},${BASALTFS_MOD}"

# Grab current SHAs from go.mod.
OLD_CLIENT_SHA=$(grep "$BASALTCLIENT_MOD" go.mod | grep -o -E '[a-f0-9]{12}$' || echo "none")
OLD_FS_SHA=$(grep "$BASALTFS_MOD" go.mod | grep -o -E '[a-f0-9]{12}$' || echo "none")
echo "Current basaltclient SHA: $OLD_CLIENT_SHA"
echo "Current basaltfs SHA:     $OLD_FS_SHA"

# Resolve basaltclient SHA.
if [ -z "$BASALTCLIENT_SHA" ]; then
  BASALTCLIENT_SHA=$(git ls-remote "https://github.com/cockroachdb/basaltclient.git" "refs/heads/$BASALTCLIENT_BRANCH" | cut -f1)
  echo "Using latest basaltclient $BASALTCLIENT_BRANCH SHA: ${BASALTCLIENT_SHA:0:12}"
else
  echo "Using provided basaltclient SHA: ${BASALTCLIENT_SHA:0:12}"
fi

# Resolve basaltfs SHA.
if [ -z "$BASALTFS_SHA" ]; then
  BASALTFS_SHA=$(git ls-remote "https://github.com/cockroachdb/basaltfs.git" "refs/heads/$BASALTFS_BRANCH" | cut -f1)
  echo "Using latest basaltfs $BASALTFS_BRANCH SHA: ${BASALTFS_SHA:0:12}"
else
  echo "Using provided basaltfs SHA: ${BASALTFS_SHA:0:12}"
fi

# Fetch commit logs between old and new SHAs for the commit message.
TMPDIR_CLIENT=$(mktemp -d)
TMPDIR_FS=$(mktemp -d)
trap "rm -rf $TMPDIR_CLIENT $TMPDIR_FS" EXIT

echo "Fetching basaltclient commit log..."
git clone --no-checkout "https://github.com/cockroachdb/basaltclient.git" "$TMPDIR_CLIENT" 2>/dev/null
CLIENT_COMMITS=$(git -C "$TMPDIR_CLIENT" log --no-merges \
  --pretty='format: * [`%h`](https://github.com/cockroachdb/basaltclient/commit/%h) %s' \
  "${OLD_CLIENT_SHA}..${BASALTCLIENT_SHA}" 2>/dev/null || echo " (no prior SHA to diff)")

echo "Fetching basaltfs commit log..."
git clone --no-checkout "https://github.com/cockroachdb/basaltfs.git" "$TMPDIR_FS" 2>/dev/null
FS_COMMITS=$(git -C "$TMPDIR_FS" log --no-merges \
  --pretty='format: * [`%h`](https://github.com/cockroachdb/basaltfs/commit/%h) %s' \
  "${OLD_FS_SHA}..${BASALTFS_SHA}" 2>/dev/null || echo " (no prior SHA to diff)")

echo
echo "basaltclient changes:"
echo "$CLIENT_COMMITS"
echo
echo "basaltfs changes:"
echo "$FS_COMMITS"
echo

# Update go.mod. Use GOFLAGS=-mod=mod so `go list -m` is allowed to update
# go.mod when resolving pseudo-versions.
echo "Updating basaltclient..."
NEW_CLIENT_VER=$(GOFLAGS=-mod=mod go list -m -f '{{.Version}}' "${BASALTCLIENT_MOD}@${BASALTCLIENT_SHA}")
go mod edit -require "${BASALTCLIENT_MOD}@${NEW_CLIENT_VER}"

echo "Updating basaltfs..."
NEW_FS_VER=$(GOFLAGS=-mod=mod go list -m -f '{{.Version}}' "${BASALTFS_MOD}@${BASALTFS_SHA}")
go mod edit -require "${BASALTFS_MOD}@${NEW_FS_VER}"

echo "Generating protobuf files (needed for go mod tidy)..."
./dev generate protobuf

echo "Running go mod tidy..."
go mod tidy

echo "Regenerating bazel deps..."
./dev generate bazel --mirror

echo
echo "Staging and committing..."
git add go.mod go.sum DEPS.bzl build/bazelutil/distdir_files.bzl
git commit -m "go.mod: bump basaltclient to ${BASALTCLIENT_SHA:0:12}, basaltfs to ${BASALTFS_SHA:0:12}

basaltclient changes:

$CLIENT_COMMITS

basaltfs changes:

$FS_COMMITS

Release note: none.
Epic: none.
"

echo "Done. basaltclient: $OLD_CLIENT_SHA -> ${BASALTCLIENT_SHA:0:12}, basaltfs: $OLD_FS_SHA -> ${BASALTFS_SHA:0:12}"
