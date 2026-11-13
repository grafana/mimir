// SPDX-License-Identifier: AGPL-3.0-only

// Package fileformat holds the format of our copies of the upstream PromQL test files: their license
// header, how a case is commented out as unsupported, and how that is reversed.
//
// It must only import the standard library. The mimir-prometheus vendoring workflow runs the re-sync tool,
// which uses this package, in a job that holds a GitHub App token, right after updating the vendored
// dependencies. Any other import would run code from those dependencies before anyone has reviewed them.
// The re-sync tool's tests and the vendoring workflow check this.
package fileformat

import (
	"slices"
	"strings"
)

// UnsupportedMarker precedes a test case we have commented out because Mimir's engine does not run it.
const UnsupportedMarker = "# Unsupported by streaming engine."

// LicenseHeader returns the header every copied upstream test file must start with. name is the test
// file name (without any ".disabled" suffix).
func LicenseHeader(name string) string {
	return strings.Join([]string{
		"# SPDX-License-Identifier: AGPL-3.0-only",
		"# Provenance-includes-location: https://github.com/prometheus/prometheus/tree/main/promql/testdata/" + name,
		"# Provenance-includes-license: Apache-2.0",
		"# Provenance-includes-copyright: The Prometheus Authors",
		"",
		"",
	}, "\n")
}

// RestoreUnsupportedTestCases re-enables the cases we disabled with UnsupportedMarker, so the result
// should equal the corresponding upstream file (apart from the license header).
func RestoreUnsupportedTestCases(s string) string {
	lines := strings.Split(s, "\n")
	inUnsupportedTestCase := false

	for i := 0; i < len(lines); i++ {
		line := lines[i]

		if line == UnsupportedMarker {
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

// CommentOutLine comments out one line of a test case after UnsupportedMarker. It is the exact inverse of
// the un-commenting in RestoreUnsupportedTestCases: a tab-indented line is prefixed with just "#" (so it
// round-trips via the "#\t" case), any other line with "# ".
func CommentOutLine(l string) string {
	if strings.HasPrefix(l, "\t") {
		return "#" + l
	}
	return "# " + l
}

// StripLineTrailingWhitespace removes trailing spaces and tabs from each line. Upstream and our copies
// are compared after this normalization, as trailing whitespace is not significant.
func StripLineTrailingWhitespace(s string) string {
	lines := strings.Split(s, "\n")
	for i := range lines {
		lines[i] = strings.TrimRight(lines[i], " \t")
	}
	return strings.Join(lines, "\n")
}
