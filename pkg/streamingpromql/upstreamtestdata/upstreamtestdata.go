// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/tree/main/promql/promqltest/test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

// Package upstreamtestdata holds helpers shared by the tooling that keeps
// pkg/streamingpromql/testdata/upstream in sync with the upstream PromQL test cases: the in-sync
// test, the re-sync tool, the disabling sync test, and the disabled-but-supported checker. Keeping
// these in one place ensures, in particular, that the re-sync tool disables cases in exactly the form
// the in-sync test expects to reverse.
package upstreamtestdata

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/grafana/regexp"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/streamingpromql/compat"
)

// UnsupportedMarker precedes a test case we have commented out because Mimir's engine does not run it.
const UnsupportedMarker = "# Unsupported by streaming engine."

// These are the same expressions promqltest uses to parse eval commands.
var (
	patEvalInstant = regexp.MustCompile(`^eval(?:_(fail|warn|ordered|info))?\s+instant\s+(?:at\s+(.+?))?\s+(.+)$`)
	patEvalRange   = regexp.MustCompile(`^eval(?:_(fail|warn|info))?\s+range\s+from\s+(.+)\s+to\s+(.+)\s+step\s+(.+?)\s+(.+)$`)
)

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

// StripLineTrailingWhitespace removes trailing spaces and tabs from each line. Upstream and our copies
// are compared after this normalization, as trailing whitespace is not significant.
func StripLineTrailingWhitespace(s string) string {
	lines := strings.Split(s, "\n")
	for i := range lines {
		lines[i] = strings.TrimRight(lines[i], " \t")
	}
	return strings.Join(lines, "\n")
}

// ParseEval extracts the query from an eval command line, reporting whether it is an instant query and
// whether the line was a recognizable eval command at all.
func ParseEval(evalLine string) (expr string, isInstant, ok bool) {
	if m := patEvalInstant.FindStringSubmatch(evalLine); m != nil {
		return m[3], true, true
	}
	if m := patEvalRange.FindStringSubmatch(evalLine); m != nil {
		return m[5], false, true
	}
	return "", false, false
}

// IsEval reports whether evalLine is a recognizable eval command.
func IsEval(evalLine string) bool {
	_, _, ok := ParseEval(evalLine)
	return ok
}

// BuildResult is the outcome of trying to build an eval command's query against Mimir's engine.
type BuildResult int

const (
	// BuildNotEval means the line was not a recognizable eval command.
	BuildNotEval BuildResult = iota
	// BuildOK means the query built successfully.
	BuildOK
	// BuildUnsupported means the query failed to build because the feature is not implemented.
	BuildUnsupported
	// BuildError means the query failed to build for some other reason.
	BuildError
)

// ClassifyEval parses evalLine and tries to build its query against engine, reporting whether Mimir's
// engine supports it. The returned error is the build error, for BuildUnsupported and BuildError.
func ClassifyEval(ctx context.Context, engine promql.QueryEngine, evalLine string) (BuildResult, error) {
	expr, isInstant, ok := ParseEval(evalLine)
	if !ok {
		return BuildNotEval, nil
	}

	var (
		q   promql.Query
		err error
	)
	if isInstant {
		q, err = engine.NewInstantQuery(ctx, nil, nil, expr, timestamp.Time(0))
	} else {
		q, err = engine.NewRangeQuery(ctx, nil, nil, expr, timestamp.Time(0), timestamp.Time(1000), time.Millisecond)
	}

	switch {
	case err == nil:
		q.Close()
		return BuildOK, nil
	case errors.Is(err, compat.NotSupportedError{}):
		return BuildUnsupported, err
	default:
		return BuildError, err
	}
}
