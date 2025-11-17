// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"testing"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
)

func TestContainedExperimentalFunctions(t *testing.T) {
	t.Cleanup(func() {
		parser.EnableExperimentalFunctions = false
		parser.EnableExtendedRangeSelectors = false
	})
	parser.EnableExperimentalFunctions = true
	parser.EnableExtendedRangeSelectors = true

	testCases := map[string]struct {
		query  string
		expect []string
		err    string
	}{
		"sum by": {
			query: `sum(up) by (namespace)`,
			err:   "function \"sum by\" is not enabled for tenant",
		},
		"mad_over_time": {
			query:  `mad_over_time(up[5m])`,
			expect: []string{"mad_over_time"},
			err:    "function \"mad_over_time\" is not enabled for tenant",
		},
		"mad_over_time with sum and by": {
			query:  `sum(mad_over_time(up[5m])) by (namespace)`,
			expect: []string{"mad_over_time"},
			err:    "function \"mad_over_time\" is not enabled for tenant",
		},
		"sort_by_label": {
			query:  `sort_by_label({__name__=~".+"}, "__name__")`,
			expect: []string{"sort_by_label"},
			err:    "function \"sort_by_label\" is not enabled for tenant",
		},
		"sort_by_label_desc": {
			query:  `sort_by_label_desc({__name__=~".+"}, "__name__")`,
			expect: []string{"sort_by_label_desc"},
			err:    "function \"sort_by_label_desc\" is not enabled for tenant",
		},
		"limitk": {
			query:  `limitk by (group) (0, up)`,
			expect: []string{"limitk"},
			err:    "aggregation \"limitk\" is not enabled for tenant",
		},
		"limit_ratio": {
			query:  `limit_ratio(0.5, up)`,
			expect: []string{"limit_ratio"},
			err:    "aggregation \"limit_ratio\" is not enabled for tenant",
		},
		"limit_ratio with mad_over_time": {
			query:  `limit_ratio(0.5, mad_over_time(up[5m]))`,
			expect: []string{"limit_ratio", "mad_over_time"},
		},
		"metric smoothed": {
			query:  `metric smoothed`,
			expect: []string{"smoothed"},
			err:    "extended range selector modifier \"smoothed\" is not enabled for tenant",
		},
		"metric[1m] smoothed": {
			query:  `metric[1m] smoothed`,
			expect: []string{"smoothed"},
			err:    "extended range selector modifier \"smoothed\" is not enabled for tenant",
		},
		"metric[1m] anchored": {
			query:  `metric[1m] anchored`,
			expect: []string{"anchored"},
			err:    "extended range selector modifier \"anchored\" is not enabled for tenant",
		},
		"rate(metric[1m] smoothed)": {
			query:  `rate(metric[1m] smoothed)`,
			expect: []string{"smoothed"},
			err:    "extended range selector modifier \"smoothed\" is not enabled for tenant",
		},
		"increase(metric[1m] anchored)": {
			query:  `increase(metric[1m] anchored)`,
			expect: []string{"anchored"},
			err:    "extended range selector modifier \"anchored\" is not enabled for tenant",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			expr, err := parser.ParseExpr(tc.query)
			require.NoError(t, err)
			var enabled []string
			for op, opType := range containedExperimentalOperations(expr) {
				enabled = append(enabled, op)
				if len(tc.err) > 0 {
					// test that if an error was raised for this function/aggregate/modifier that the expected error string is formed
					// an empty tc.err allows for the case to be skipped - such as where we have multiple errors which are validated elsewhere
					err := createExperimentalOperationError(opType, op)
					require.ErrorContains(t, err, tc.err)
				}
			}
			require.ElementsMatch(t, tc.expect, enabled)
		})
	}
}
