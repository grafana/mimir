// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"testing"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
)

func TestContainsExperimentalFunction(t *testing.T) {
	t.Cleanup(func() { parser.EnableExperimentalFunctions = false })
	parser.EnableExperimentalFunctions = true

	testCases := map[string]struct {
		query  string
		expect bool
	}{
		"sum by": {
			query: `sum(up) by (namespace)`,
		},
		"mad_over_time": {
			query:  `mad_over_time(up[5m])`,
			expect: true,
		},
		"mad_over_time with sum and by": {
			query:  `sum(mad_over_time(up[5m])) by (namespace)`,
			expect: true,
		},
		"sort_by_label": {
			query:  `sort_by_label({__name__=~".+"}, "__name__")`,
			expect: true,
		},
		"sort_by_label_desc": {
			query:  `sort_by_label_desc({__name__=~".+"}, "__name__")`,
			expect: true,
		},
		"limit k": {
			query:  `limitk by (group) (0, up)`,
			expect: true,
		},
		"limit ratio": {
			query:  `limit_ratio(0.5, up)`,
			expect: true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			expr, err := parser.ParseExpr(tc.query)
			require.NoError(t, err)
			result, _ := containsExperimentalFunction(expr)
			require.Equal(t, tc.expect, result)
		})
	}
}
