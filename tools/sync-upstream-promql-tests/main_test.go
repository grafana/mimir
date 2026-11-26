// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"os"
	"path/filepath"
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

func TestRunFailsWithoutUpstreamFiles(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "a", "b")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	t.Chdir(dir)

	// Resolved like run() does, so the expected path is the same on every OS, whatever the temp dir's symlinks.
	upstreamDir, err := filepath.Abs(filepath.Join("..", "..", "vendor", "github.com", "prometheus", "prometheus", "promql", "promqltest", "testdata"))
	require.NoError(t, err)
	require.EqualError(t, run(), "no upstream test files found in "+upstreamDir+": run this tool from its own directory")
}
