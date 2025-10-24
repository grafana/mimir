// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"testing"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
)

func TestContainedExperimentalFunctions(t *testing.T) {
	t.Cleanup(func() { parser.EnableExperimentalFunctions = false })
	parser.EnableExperimentalFunctions = true

	testCases := map[string]struct {
		query  string
		expect []string
	}{
		"sum by": {
			query: `sum(up) by (namespace)`,
		},
		"mad_over_time": {
			query:  `mad_over_time(up[5m])`,
			expect: []string{"mad_over_time"},
		},
		"mad_over_time with sum and by": {
			query:  `sum(mad_over_time(up[5m])) by (namespace)`,
			expect: []string{"mad_over_time"},
		},
		"sort_by_label": {
			query:  `sort_by_label({__name__=~".+"}, "__name__")`,
			expect: []string{"sort_by_label"},
		},
		"sort_by_label_desc": {
			query:  `sort_by_label_desc({__name__=~".+"}, "__name__")`,
			expect: []string{"sort_by_label_desc"},
		},
		"limitk": {
			query:  `limitk by (group) (0, up)`,
			expect: []string{"limitk"},
		},
		"limit_ratio": {
			query:  `limit_ratio(0.5, up)`,
			expect: []string{"limit_ratio"},
		},
		"limit_ratio with mad_over_time": {
			query:  `limit_ratio(0.5, mad_over_time(up[5m]))`,
			expect: []string{"limit_ratio", "mad_over_time"},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			expr, err := parser.ParseExpr(tc.query)
			require.NoError(t, err)
			var enabled []string
			for key := range containedExperimentalFunctions(expr) {
				enabled = append(enabled, key)
			}
			require.ElementsMatch(t, tc.expect, enabled)
		})
	}
}
