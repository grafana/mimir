// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/upstreamtestdata"
)

func TestThreeWayMerge(t *testing.T) {
	testCases := map[string]struct {
		ours              string
		theirs            string
		expected          string
		expectedConflicts bool
	}{
		"upstream unchanged: our disabling is kept": {
			ours:     "eval instant at 0m A\n  A 1\n\n# Unsupported by streaming engine.\n# eval instant at 0m B\n#   B 7\n",
			theirs:   "eval instant at 0m A\n  A 1\n\neval instant at 0m B\n  B 7\n",
			expected: "eval instant at 0m A\n  A 1\n\n# Unsupported by streaming engine.\n# eval instant at 0m B\n#   B 7\n",
		},
		"upstream changed an enabled case: change applied, disabling kept": {
			ours:     "eval instant at 0m A\n  A 1\n\n# Unsupported by streaming engine.\n# eval instant at 0m B\n#   B 7\n",
			theirs:   "eval instant at 0m A\n  A 2\n\neval instant at 0m B\n  B 7\n",
			expected: "eval instant at 0m A\n  A 2\n\n# Unsupported by streaming engine.\n# eval instant at 0m B\n#   B 7\n",
		},
		"upstream changed a disabled case: only that block is taken from upstream": {
			ours:              "# Unsupported by streaming engine.\n# eval instant at 0m B\n#   B 7\n\n# Unsupported by streaming engine.\n# eval instant at 0m C\n#   C 1\n",
			theirs:            "eval instant at 0m B\n  B 6\n\neval instant at 0m C\n  C 1\n",
			expected:          "eval instant at 0m B\n  B 6\n\n# Unsupported by streaming engine.\n# eval instant at 0m C\n#   C 1\n",
			expectedConflicts: true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			// The tool reconstructs the merge base from our copy in the same way.
			base := upstreamtestdata.RestoreUnsupportedTestCases(tc.ours)

			merged, conflicts, err := threeWayMerge(base, tc.ours, tc.theirs)
			require.NoError(t, err)
			require.Equal(t, tc.expected, merged)
			require.Equal(t, tc.expectedConflicts, conflicts)
		})
	}
}
