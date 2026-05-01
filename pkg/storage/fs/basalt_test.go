// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fs

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cli/cliflagcfg"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

// TestBasaltFlagParsing verifies that the --basalt flag, registered as a
// StringArrayFlag, correctly preserves commas within each flag value. With the
// old StringSliceFlag, pflag would split "prod@ctrl1:5000,ctrl2:5000" into
// separate slice entries, breaking multi-controller aliases.
func TestBasaltFlagParsing(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var basaltSpecs []string
	f := pflag.NewFlagSet("test", pflag.ContinueOnError)
	cliflagcfg.StringArrayFlag(f, &basaltSpecs, cliflags.Basalt)

	// Simulate: --basalt=prod@ctrl1:5000,ctrl2:5000 --basalt=dev@ctrl3:5000
	err := f.Parse([]string{
		"--basalt=prod@ctrl1:5000,ctrl2:5000",
		"--basalt=dev@ctrl3:5000",
	})
	require.NoError(t, err)
	require.Equal(t, []string{
		"prod@ctrl1:5000,ctrl2:5000",
		"dev@ctrl3:5000",
	}, basaltSpecs)

	// Verify the parsed flag values flow correctly through ParseBasaltAliases.
	aliases, err := ParseBasaltAliases(basaltSpecs)
	require.NoError(t, err)
	require.Equal(t, map[string][]string{
		"prod": {"ctrl1:5000", "ctrl2:5000"},
		"dev":  {"ctrl3:5000"},
	}, aliases)
}

func TestParseBasaltAliases(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		specs    []string
		expected map[string][]string
		errMsg   string
	}{
		{
			name:     "single alias single controller",
			specs:    []string{"prod@ctrl1:5000"},
			expected: map[string][]string{"prod": {"ctrl1:5000"}},
		},
		{
			name:  "single alias multiple controllers",
			specs: []string{"prod@ctrl1:5000,ctrl2:5000,ctrl3:5000"},
			expected: map[string][]string{
				"prod": {"ctrl1:5000", "ctrl2:5000", "ctrl3:5000"},
			},
		},
		{
			name: "multiple aliases",
			specs: []string{
				"prod@ctrl1:5000,ctrl2:5000",
				"dev@ctrl3:5000",
			},
			expected: map[string][]string{
				"prod": {"ctrl1:5000", "ctrl2:5000"},
				"dev":  {"ctrl3:5000"},
			},
		},
		{
			name:   "missing @ separator",
			specs:  []string{"prod-ctrl1:5000"},
			errMsg: `invalid --basalt value "prod-ctrl1:5000": expected format`,
		},
		{
			name:   "empty alias name",
			specs:  []string{"@ctrl1:5000"},
			errMsg: `invalid --basalt value "@ctrl1:5000": empty alias name`,
		},
		{
			name:   "empty controller address",
			specs:  []string{"prod@ctrl1:5000,,ctrl2:5000"},
			errMsg: `invalid --basalt value "prod@ctrl1:5000,,ctrl2:5000": empty controller address`,
		},
		{
			name:   "duplicate alias",
			specs:  []string{"prod@ctrl1:5000", "prod@ctrl2:5000"},
			errMsg: `duplicate basalt alias "prod"`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			aliases, err := ParseBasaltAliases(tc.specs)
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, aliases)
		})
	}
}
