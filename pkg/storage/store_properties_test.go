// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/pebble"
)

func TestComputeStoreProperties(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	t.Run("local", func(t *testing.T) {
		dir := t.TempDir()
		env := &fs.Env{Dir: dir}
		cfg := engineConfig{env: env, opts: &pebble.Options{}}
		props := computeStoreProperties(ctx, cfg)

		if props.Dir != dir {
			t.Fatalf("expected Dir=%q, got %q", dir, props.Dir)
		}
		if props.FileStoreProperties == nil {
			t.Fatal("expected non-nil FileStoreProperties for local store")
		}
	})

	t.Run("in-memory", func(t *testing.T) {
		env := &fs.Env{Dir: ""}
		cfg := engineConfig{env: env, opts: &pebble.Options{}}
		props := computeStoreProperties(ctx, cfg)

		if props.Dir != "" {
			t.Fatalf("expected empty Dir, got %q", props.Dir)
		}
		if props.FileStoreProperties != nil {
			t.Fatal("expected nil FileStoreProperties for in-memory store")
		}
	})

	t.Run("basalt", func(t *testing.T) {
		basaltURI := "basalt://prod/store-1"
		env := &fs.Env{Dir: "."}
		cfg := engineConfig{env: env, opts: &pebble.Options{}, basaltPath: basaltURI}
		props := computeStoreProperties(ctx, cfg)

		if props.Dir != basaltURI {
			t.Fatalf("expected Dir=%q, got %q", basaltURI, props.Dir)
		}
		if props.FileStoreProperties == nil {
			t.Fatal("expected non-nil FileStoreProperties for basalt store")
		}
		if props.FileStoreProperties.Path != basaltURI {
			t.Fatalf("expected Path=%q, got %q", basaltURI, props.FileStoreProperties.Path)
		}
		if props.FileStoreProperties.FsType != "basalt" {
			t.Fatalf("expected FsType=%q, got %q", "basalt", props.FileStoreProperties.FsType)
		}
		if props.FileStoreProperties.BlockDevice != "" {
			t.Fatalf("expected empty BlockDevice, got %q", props.FileStoreProperties.BlockDevice)
		}
	})
}

func TestPathIsInside(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		base, target string
		expected     bool
	}{
		{
			base:     "/",
			target:   "/cockroach/cockroach-data",
			expected: true,
		},
		{
			base:     "/cockroach",
			target:   "/cockroach/cockroach-data",
			expected: true,
		},
		{
			base:     "/cockroach/cockroach-data",
			target:   "/cockroach/cockroach-data",
			expected: true,
		},
		{
			base:     "/cockroach/cockroach-data/foo",
			target:   "/cockroach/cockroach-data",
			expected: false,
		},
		{
			base:     "/cockroach/cockroach-data1",
			target:   "/cockroach/cockroach-data",
			expected: false,
		},
		{
			base:     "/run/user/1001",
			target:   "/cockroach/cockroach-data",
			expected: false,
		},
		{
			base:     "/..foo",
			target:   "/..foo/data",
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			result := pathIsInside(filepath.FromSlash(tc.base), filepath.FromSlash(tc.target))
			if result != tc.expected {
				t.Fatalf("%q, %q: expected %t, got %t", tc.base, tc.target, tc.expected, result)
			}
		})
	}
}
