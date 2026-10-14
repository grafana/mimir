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
			ours: `eval instant at 0m A
  A 1

# Unsupported by streaming engine.
# eval instant at 0m B
#   B 7
`,
			theirs: `eval instant at 0m A
  A 1

eval instant at 0m B
  B 7
`,
			expected: `eval instant at 0m A
  A 1

# Unsupported by streaming engine.
# eval instant at 0m B
#   B 7
`,
		},
		"upstream changed an enabled case: change applied, disabling kept": {
			ours: `eval instant at 0m A
  A 1

# Unsupported by streaming engine.
# eval instant at 0m B
#   B 7
`,
			theirs: `eval instant at 0m A
  A 2

eval instant at 0m B
  B 7
`,
			expected: `eval instant at 0m A
  A 2

# Unsupported by streaming engine.
# eval instant at 0m B
#   B 7
`,
		},
		"upstream changed a disabled case: only that block is taken from upstream": {
			ours: `# Unsupported by streaming engine.
# eval instant at 0m B
#   B 7

# Unsupported by streaming engine.
# eval instant at 0m C
#   C 1
`,
			theirs: `eval instant at 0m B
  B 6

eval instant at 0m C
  C 1
`,
			expected: `eval instant at 0m B
  B 6

# Unsupported by streaming engine.
# eval instant at 0m C
#   C 1
`,
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
