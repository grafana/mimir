// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The tool relies on promqltest naming each case's subtest "line <N>/<expr>" under the file's subtest;
// these lines follow what `go test -json` emits for TestUpstreamTestCases.
func TestParseFailures(t *testing.T) {
	out := []byte(`{"Action":"run","Package":"github.com/grafana/mimir/pkg/streamingpromql/comparisons","Test":"TestUpstreamTestCases"}
{"Action":"output","Test":"TestUpstreamTestCases/upstream/info.test/line_433/info(dataa_metric,_{__name__=\"info_metric\"})","Output":"    test.go:1710: \n"}
{"Action":"fail","Test":"TestUpstreamTestCases/upstream/info.test/line_433/info(dataa_metric,_{__name__=\"info_metric\"})"}
{"Action":"pass","Test":"TestUpstreamTestCases/upstream/functions.test/line_12/rate(x[5m])/2"}
{"Action":"fail","Test":"TestUpstreamTestCases/upstream/functions.test/line_20/rate(x[5m])/2"}
{"Action":"fail","Test":"TestUpstreamTestCases/upstream/functions.test"}
{"Action":"fail","Test":"TestUpstreamTestCases"}
{"Action":"fail","Package":"github.com/grafana/mimir/pkg/streamingpromql/comparisons"}
FAIL	github.com/grafana/mimir/pkg/streamingpromql/comparisons	2.1s
`)

	require.Equal(t, map[string]map[int]bool{
		"info.test":      {433: true},
		"functions.test": {20: true},
	}, parseFailures(out))
}

func TestBaselineOrigin(t *testing.T) {
	b := parseBaseline(
		"# a comment\n" +
			"load 1m\n" +
			"  metric 1 2 3\n" +
			"\n" +
			"eval instant at 0m sum(metric)\n" +
			"  {} 6\n" +
			"\n" +
			"# Unsupported by streaming engine.\n" +
			"# eval instant at 0m rate(metric[1m])\n" +
			"#   {} 0\n" +
			"\n" +
			"# Unsupported by streaming engine.\n" +
			"# eval instant at 0m count(metric)\n" +
			"#   {} 3\n" +
			"\n" +
			"eval instant at 0m count(metric)\n" +
			"  {} 3\n")

	testCases := map[string]struct {
		baseline *baseline
		evalLine string
		expected origin
	}{
		"enabled before":                           {baseline: b, evalLine: "eval instant at 0m sum(metric)", expected: originExisting},
		"disabled before":                          {baseline: b, evalLine: "eval instant at 0m rate(metric[1m])", expected: originPreviouslyDisabled},
		"both enabled and disabled before":         {baseline: b, evalLine: "eval instant at 0m count(metric)", expected: originExisting},
		"not in the file before":                   {baseline: b, evalLine: "eval instant at 0m max(metric)", expected: originNew},
		"file new upstream: every case counts new": {baseline: parseBaseline(""), evalLine: "eval instant at 0m sum(metric)", expected: originNew},
		"no baseline":                              {baseline: nil, evalLine: "eval instant at 0m sum(metric)", expected: originUnknown},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expected, tc.baseline.origin(tc.evalLine))
		})
	}
}

func TestRenderDisabledCases(t *testing.T) {
	cases := map[caseGroup][]string{
		{originNew, true}:                 {"- new unsupported"},
		{originExisting, false}:           {"- existing divergent"},
		{originPreviouslyDisabled, false}: {"- previously disabled"},
		{originNew, false}:                {"- new divergent"},
	}

	testCases := map[string]struct {
		cases                     map[caseGroup][]string
		includePreviouslyDisabled bool
		expected                  string
	}{
		"nothing disabled": {},
		"possible regressions come first and empty sections are omitted": {
			cases:                     cases,
			includePreviouslyDisabled: true,
			expected: "**Divergent result or runtime error in EXISTING cases (possible regression, please review carefully):**\n\n- existing divergent\n\n" +
				"**Divergent result or runtime error in NEWLY-SYNCED upstream cases:**\n\n- new divergent\n\n" +
				"**Unsupported by Mimir's engine in NEWLY-SYNCED upstream cases (feature not implemented):**\n\n- new unsupported\n\n" +
				"**Divergent result or runtime error in PREVIOUSLY-DISABLED cases that upstream changes re-enabled (disabled again):**\n\n- previously disabled\n\n",
		},
		"previously disabled cases left out": {
			cases: cases,
			expected: "**Divergent result or runtime error in EXISTING cases (possible regression, please review carefully):**\n\n- existing divergent\n\n" +
				"**Divergent result or runtime error in NEWLY-SYNCED upstream cases:**\n\n- new divergent\n\n" +
				"**Unsupported by Mimir's engine in NEWLY-SYNCED upstream cases (feature not implemented):**\n\n- new unsupported\n\n",
		},
		"only previously disabled cases, left out": {
			cases: map[caseGroup][]string{{originPreviouslyDisabled, true}: {"- previously disabled"}},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expected, renderDisabledCases(tc.cases, tc.includePreviouslyDisabled))
		})
	}
}
