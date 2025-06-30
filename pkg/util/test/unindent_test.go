// SPDX-License-Identifier: AGPL-3.0-only

package test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnindent(t *testing.T) {
	testCases := map[string]struct {
		input    string
		expected string
	}{
		"tabs": {
			input: Unindent(t, `
				overrides:
					"1234":
						ha_cluster_label: "cluster"
						ha_replica_label: "replica"
			`),
			expected: `
overrides:
	"1234":
		ha_cluster_label: "cluster"
		ha_replica_label: "replica"
`,
		},
		"spaces": {
			input: Unindent(t, `
              overrides:
                "1234":
                  ha_cluster_label: "cluster"
                  ha_replica_label: "replica"
            `),
			expected: `
overrides:
  "1234":
    ha_cluster_label: "cluster"
    ha_replica_label: "replica"
`,
		},
		"mixed": {
			input: Unindent(t, `
				overrides:
				  "1234":
				    ha_cluster_label: "cluster"
				    ha_replica_label: "replica"
			`),
			expected: `
overrides:
  "1234":
    ha_cluster_label: "cluster"
    ha_replica_label: "replica"
`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			require.Equal(t, tc.expected, tc.input)
		})
	}
}
