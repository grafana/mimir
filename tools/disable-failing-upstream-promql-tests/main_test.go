// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"os"
	"path/filepath"
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

func TestLoadBaselineEnabledEvals(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "x.test"), []byte(
		"# a comment\n"+
			"load 1m\n"+
			"  metric 1 2 3\n"+
			"\n"+
			"eval instant at 0m sum(metric)\n"+
			"  {} 6\n"+
			"\n"+
			"# Unsupported by streaming engine.\n"+
			"# eval instant at 0m rate(metric[1m])\n"+
			"#   {} 0\n"+
			"\n"+
			"eval range from 0 to 1m step 30s count(metric)\n"+
			"  {} 1 1 1\n"), 0o644))

	t.Run("enabled evals only", func(t *testing.T) {
		require.Equal(t, map[string]bool{
			"eval instant at 0m sum(metric)":                 true,
			"eval range from 0 to 1m step 30s count(metric)": true,
		}, loadBaselineEnabledEvals(dir, "x.test"))
	})

	t.Run("file new upstream: every case counts as new", func(t *testing.T) {
		require.Equal(t, map[string]bool{}, loadBaselineEnabledEvals(dir, "missing.test"))
	})

	t.Run("no baseline: origin unknown", func(t *testing.T) {
		require.Nil(t, loadBaselineEnabledEvals("", "x.test"))
	})
}

func TestRenderDisabledCases(t *testing.T) {
	t.Run("nothing disabled", func(t *testing.T) {
		require.Empty(t, renderDisabledCases(nil, nil, nil, nil))
	})

	t.Run("possible regressions come first and empty sections are omitted", func(t *testing.T) {
		require.Equal(t,
			"**Divergent result or runtime error in EXISTING cases (possible regression, please review carefully):**\n\n- existing\n\n"+
				"**Divergent result or runtime error in NEWLY-SYNCED upstream cases:**\n\n- new\n\n",
			renderDisabledCases(nil, []string{"- existing"}, []string{"- new"}, nil))
	})
}
