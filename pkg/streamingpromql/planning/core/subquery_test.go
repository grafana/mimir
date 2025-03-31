// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSubquery_Describe(t *testing.T) {
	testCases := map[string]struct {
		node     *Subquery
		expected string
	}{
		"no timestamp and no offset": {
			node: &Subquery{
				SubqueryDetails: &SubqueryDetails{
					Range: time.Minute,
					Step:  20 * time.Second,
				},
			},
			expected: "[1m0s:20s]",
		},
		"no timestamp, has offset": {
			node: &Subquery{
				SubqueryDetails: &SubqueryDetails{
					Range:  time.Minute,
					Step:   20 * time.Second,
					Offset: time.Hour,
				},
			},
			expected: "[1m0s:20s] offset 1h0m0s",
		},
		"has timestamp and no offset": {
			node: &Subquery{
				SubqueryDetails: &SubqueryDetails{
					Range:     time.Minute,
					Step:      20 * time.Second,
					Timestamp: &Timestamp{Timestamp: 123456},
				},
			},
			expected: "[1m0s:20s] @ 123456 (1970-01-01T00:02:03.456Z)",
		},
		"has timestamp and offset": {
			node: &Subquery{
				SubqueryDetails: &SubqueryDetails{
					Range:     time.Minute,
					Step:      20 * time.Second,
					Offset:    time.Hour,
					Timestamp: &Timestamp{Timestamp: 123456},
				},
			},
			expected: "[1m0s:20s] @ 123456 (1970-01-01T00:02:03.456Z) offset 1h0m0s",
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := testCase.node.Describe()
			require.Equal(t, testCase.expected, actual)
		})
	}
}
