// SPDX-License-Identifier: AGPL-3.0-only

package core_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// info()'s first argument qualifies for own mode when it maps one selector's series one to one onto its own, and its
// matchers are that selector's, with the filters of a shared selector on the way. The queries are planned with every
// optimization pass, so that the nodes they add, such as common subexpression elimination's, are on the way.
func TestOwnResourcesMatchers(t *testing.T) {
	planner, err := streamingpromql.NewQueryPlanner(streamingpromql.NewTestEngineOpts(), streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	lookback := streamingpromql.DefaultLookbackDelta
	require.Equal(t, 5*time.Minute, lookback, "the ranges below are on either side of the lookback")

	for name, tc := range map[string]struct {
		query string
		// The selector's matchers, nil when the first argument doesn't qualify.
		matchers []string
		// A node the planner puts on the way, which the case is about.
		via planning.NodeType
	}{
		"a vector selector": {query: `info(up{job="a"})`, matchers: []string{`job="a"`, `__name__="up"`}},
		"with an offset":    {query: `info(up offset 5m)`, matchers: []string{`__name__="up"`}},
		// In a range query, step invariance wraps the info() call, so the selector carries the @ itself.
		"with @":                                  {query: `info(up @ 100)`, matchers: []string{`__name__="up"`}},
		"a range function within the lookback":    {query: `info(rate(requests_total[5m]))`, matchers: []string{`__name__="requests_total"`}},
		"a range function past the lookback":      {query: `info(rate(requests_total[10m]))`},
		"a range function with a scalar argument": {query: `info(quantile_over_time(0.9, latency[5m]))`, matchers: []string{`__name__="latency"`}},
		"absent_over_time":                        {query: `info(absent_over_time(up[5m]))`},
		"an element-wise function":                {query: `info(clamp_min(up, 0))`, matchers: []string{`__name__="up"`}},
		"a binary operation with a scalar":        {query: `info(2 * up)`, matchers: []string{`__name__="up"`}},
		"a binary operation between vectors":      {query: `info(up * up_too)`},
		"an aggregation":                          {query: `info(sum by (job, instance) (up))`},
		"a function that changes labels":          {query: `info(label_replace(up, "a", "b", "c", "d"))`},
		"a subquery":                              {query: `info(max_over_time(up[5m:1m]))`},
		// Common subexpression elimination shares the selector between the two.
		"a shared selector": {query: `up + info(up)`, matchers: []string{`__name__="up"`}, via: planning.NODE_TYPE_DUPLICATE},
		// Subset selector elimination shares the broader selector, and filters it for info()'s argument.
		"a shared selector, filtered": {query: `up + on(job, instance) group_left info(up{pod="p"})`, matchers: []string{`__name__="up"`, `pod="p"`}, via: planning.NODE_TYPE_DUPLICATE_FILTER},
	} {
		t.Run(name, func(t *testing.T) {
			plan, err := planner.NewQueryPlan(context.Background(), tc.query, types.NewInstantQueryTimeRange(time.Now()), lookback, false, nil, streamingpromql.NoopPlanningObserver{})
			require.NoError(t, err)
			info := findInfoCall(plan.Root)
			require.NotNil(t, info)
			if tc.via != 0 {
				require.True(t, hasNodeType(info.Args[0], tc.via), "the planner put %v on the way", tc.via)
			}

			matchers, ok := core.OwnResourcesMatchers(info.Args[0], lookback)
			require.Equal(t, tc.matchers != nil, ok)
			var got []string
			for _, m := range matchers {
				got = append(got, m.String())
			}
			require.ElementsMatch(t, tc.matchers, got)
		})
	}
}

func findInfoCall(node planning.Node) *core.FunctionCall {
	if f, ok := node.(*core.FunctionCall); ok && f.Function == functions.FUNCTION_INFO {
		return f
	}
	for i := range node.ChildCount() {
		if f := findInfoCall(node.Child(i)); f != nil {
			return f
		}
	}
	return nil
}

func hasNodeType(node planning.Node, nodeType planning.NodeType) bool {
	if node.NodeType() == nodeType {
		return true
	}
	for i := range node.ChildCount() {
		if hasNodeType(node.Child(i), nodeType) {
			return true
		}
	}
	return false
}
