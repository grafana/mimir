// SPDX-License-Identifier: AGPL-3.0-only

package analyze

import (
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirtool/rules/rwrulefmt"
	"github.com/grafana/mimir/pkg/mimirtool/util"
)

func TestParseMetricsInRuleGroup_MetricNameMatcher(t *testing.T) {
	group := rwrulefmt.RuleGroup{
		RuleGroup: rulefmt.RuleGroup{
			Name: "group",
			Rules: []rulefmt.Rule{
				{Record: "recorded:metric", Expr: `sum({__name__="by_matcher", job="a"})`},
				{Alert: "Alert", Expr: `by_name{job="a"} > 0 and {__name__="other_by_matcher"} > 0`},
				// A regex matcher doesn't select a single metric name, so it must not be reported.
				{Alert: "Regex", Expr: `{__name__=~"foo.*"} > 0`},
			},
		},
	}

	mir := &MetricsInRuler{OverallMetrics: map[string]struct{}{}}
	require.NoError(t, ParseMetricsInRuleGroup(mir, group, "ns", util.CreatePromQLParser(false), log.NewNopLogger()))

	require.Len(t, mir.RuleGroups, 1)
	require.Equal(t, []string{"by_matcher", "by_name", "other_by_matcher"}, mir.RuleGroups[0].Metrics)
	require.Empty(t, mir.RuleGroups[0].ParseErrors)
}
