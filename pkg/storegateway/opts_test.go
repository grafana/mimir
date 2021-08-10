// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/opts_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package storegateway

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
		matches := findSetMatches(c.pattern)
		assert.Equal(t, c.exp, matches)
	}
}
