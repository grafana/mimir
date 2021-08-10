// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/chunk/opts_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package chunk

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Refer to https://github.com/prometheus/prometheus/issues/2651.
func TestFindSetMatches(t *testing.T) {
	cases := []struct {
		pattern string
		exp     []string
	}{
		// Simple sets.
		{
			pattern: "foo|bar|baz",
			exp: []string{
				"foo",
				"bar",
				"baz",
			},
		},
		// Simple sets containing escaped characters.
		{
			pattern: "fo\\.o|bar\\?|\\^baz",
			exp: []string{
				"fo.o",
				"bar?",
				"^baz",
			},
		},
		// Simple sets containing special characters without escaping.
		{
			pattern: "fo.o|bar?|^baz",
			exp:     nil,
		},
		{
			pattern: "foo\\|bar\\|baz",
			exp: []string{
				"foo|bar|baz",
			},
		},
	}

	for _, c := range cases {
		matches := FindSetMatches(c.pattern)
		require.Equal(t, c.exp, matches)
	}
}
