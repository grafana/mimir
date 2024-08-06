// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// This test confirms that the test cases in testdata/upstream are in sync with the test cases
// in github.com/prometheus/prometheus.
//
// We can't just use the upstream test cases as-is, as we need to disable those test cases we
// don't yet support.
//
// See testdata/upstream/README.md for more information.
func TestOurUpstreamTestCasesAreInSyncWithUpstream(t *testing.T) {
	sourceTestCaseDir, err := filepath.Abs(filepath.Join("..", "..", "vendor", "github.com", "prometheus", "prometheus", "promql", "promqltest", "testdata"))
	require.NoError(t, err)
	sourceTestCases, err := filepath.Glob(filepath.Join(sourceTestCaseDir, "*.test"))
	require.NoError(t, err)

	ourUpstreamTestCaseDir, err := filepath.Abs(filepath.Join("testdata", "upstream"))
	require.NoError(t, err)
	ourUpstreamTestCases, err := filepath.Glob(filepath.Join(ourUpstreamTestCaseDir, "*.test*"))
	require.NoError(t, err)

	// For each file in sourceTestCases: check there is a corresponding file in our test cases
	for _, sourceTestCase := range sourceTestCases {
		name := filepath.Base(sourceTestCase)

		hasEnabledTestFile := slices.ContainsFunc(ourUpstreamTestCases, func(s string) bool {
			return filepath.Base(s) == name
		})

		hasDisabledTestFile := slices.ContainsFunc(ourUpstreamTestCases, func(s string) bool {
			return filepath.Base(s) == name+".disabled"
		})

		require.Truef(t, hasEnabledTestFile || hasDisabledTestFile, "upstream test case %v has no corresponding test file in our test cases (in %v)", sourceTestCase, ourUpstreamTestCaseDir)
	}

	// For each file in ourUpstreamTestCases: check there is a corresponding file in upstream
	for _, ourUpstreamTestCase := range ourUpstreamTestCases {
		name := strings.TrimSuffix(filepath.Base(ourUpstreamTestCase), ".disabled") // Remove the .disabled suffix, if it's there.

		hasUpstreamTestFile := slices.ContainsFunc(sourceTestCases, func(s string) bool {
			return filepath.Base(s) == name
		})

		require.Truef(t, hasUpstreamTestFile, "our test case %v has no corresponding test file upstream (in %v)", ourUpstreamTestCase, sourceTestCaseDir)
	}

	for _, ourUpstreamTestCase := range ourUpstreamTestCases {
		name := strings.TrimSuffix(filepath.Base(ourUpstreamTestCase), ".disabled") // Remove the .disabled suffix, if it's there.

		t.Run(name, func(t *testing.T) {
			licenseHeader := strings.Join([]string{
				"# SPDX-License-Identifier: AGPL-3.0-only",
				"# Provenance-includes-location: https://github.com/prometheus/prometheus/tree/main/promql/testdata/" + name,
				"# Provenance-includes-license: Apache-2.0",
				"# Provenance-includes-copyright: The Prometheus Authors",
				"",
				"",
			}, "\n")

			isDisabled := strings.HasSuffix(ourUpstreamTestCase, ".disabled")

			sourcePath := filepath.Join(sourceTestCaseDir, name)
			sourceBytes, err := os.ReadFile(sourcePath)
			require.NoError(t, err)
			sourceString := string(sourceBytes)

			ourBytes, err := os.ReadFile(ourUpstreamTestCase)
			require.NoError(t, err)
			ourString := string(ourBytes)

			// Some of the upstream test cases have trailing whitespace which we don't care about.
			sourceString = stripLineTrailingWhitespace(sourceString)
			ourString = stripLineTrailingWhitespace(ourString)

			require.Truef(t, strings.HasPrefix(ourString, licenseHeader), "%v does not have the expected license header:\n%v", ourUpstreamTestCase, licenseHeader)
			ourString = strings.TrimPrefix(ourString, licenseHeader) // Ignore the license header in later checks.

			if isDisabled {
				// If the test case is disabled, then it should be identical to the upstream test case (apart from the license header).
				require.Equalf(t, sourceString, ourString, "our test case %v is not in sync with the corresponding upstream test case %v", ourUpstreamTestCase, sourcePath)
			} else {
				// Check test cases are equivalent, after re-enabling disabled test cases.
				ourString = restoreUnsupportedTestCases(ourString)
				require.Equalf(t, sourceString, ourString, "our test case %v is not in sync with the corresponding upstream test case %v after unignoring unsupported test cases", ourUpstreamTestCase, sourcePath)
			}
		})
	}
}

func stripLineTrailingWhitespace(s string) string {
	lines := strings.Split(s, "\n")

	for i := range lines {
		lines[i] = strings.TrimRight(lines[i], " \t")
	}

	return strings.Join(lines, "\n")
}

func restoreUnsupportedTestCases(s string) string {
	lines := strings.Split(s, "\n")
	inUnsupportedTestCase := false

	for i := 0; i < len(lines); i++ {
		line := lines[i]

		if line == "# Unsupported by streaming engine." {
			lines = slices.Delete(lines, i, i+1)
			inUnsupportedTestCase = true
			i--
		} else if inUnsupportedTestCase && strings.HasPrefix(line, "# ") {
			lines[i] = strings.TrimPrefix(line, "# ")
		} else if inUnsupportedTestCase && strings.HasPrefix(line, "#\t") {
			lines[i] = strings.TrimPrefix(line, "#")
		} else {
			inUnsupportedTestCase = false
		}
	}

	return strings.Join(lines, "\n")
}
