// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cliflagcfg

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

// TestStringArrayFlagPreservesCommas verifies that StringArrayFlag does not
// split values on commas, unlike StringSliceFlag. This is critical for the
// --basalt flag where values contain comma-separated controller addresses
// (e.g., "prod@ctrl1:5000,ctrl2:5000").
func TestStringArrayFlagPreservesCommas(t *testing.T) {
	var vals []string
	f := pflag.NewFlagSet("test", pflag.ContinueOnError)
	StringArrayFlag(f, &vals, cliflags.Basalt)

	// Simulate: --basalt=prod@ctrl1:5000,ctrl2:5000 --basalt=dev@ctrl3:5000
	err := f.Parse([]string{
		"--basalt=prod@ctrl1:5000,ctrl2:5000",
		"--basalt=dev@ctrl3:5000",
	})
	require.NoError(t, err)
	// Each --basalt value should be preserved as a single string, with
	// commas intact.
	require.Equal(t, []string{
		"prod@ctrl1:5000,ctrl2:5000",
		"dev@ctrl3:5000",
	}, vals)
}

// TestStringSliceFlagSplitsCommas verifies that StringSliceFlag splits values
// on commas, which is the behavior we moved away from for --basalt.
func TestStringSliceFlagSplitsCommas(t *testing.T) {
	var vals []string
	f := pflag.NewFlagSet("test", pflag.ContinueOnError)
	StringSliceFlag(f, &vals, cliflags.Basalt)

	err := f.Parse([]string{
		"--basalt=prod@ctrl1:5000,ctrl2:5000",
	})
	require.NoError(t, err)
	// StringSliceFlag splits on commas — this is the broken behavior for
	// basalt aliases with multiple controllers.
	require.Equal(t, []string{
		"prod@ctrl1:5000",
		"ctrl2:5000",
	}, vals)
}
