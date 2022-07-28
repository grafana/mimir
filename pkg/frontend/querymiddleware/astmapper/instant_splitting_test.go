// SPDX-License-Identifier: AGPL-3.0-only

package astmapper

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInstantSplitter(t *testing.T) {
	splitInterval := 1 * time.Minute
	splitter := NewInstantQuerySplitter(splitInterval, log.NewNopLogger())

	for _, tt := range []struct {
		in                   string
		out                  string
		expectedSplitQueries int
	}{
		// Range vector aggregators
		{
			in:                   `avg_over_time({app="foo"}[3m])`,
			out:                  `(sum without() (` + concatOffsets(splitInterval, 3, `sum_over_time({app="foo"}[x]y)`) + `)) / (sum without() (` + concatOffsets(splitInterval, 3, `count_over_time({app="foo"}[x]y)`) + `))`,
			expectedSplitQueries: 6,
		},
		{
			in:                   `count_over_time({app="foo"}[3m])`,
			out:                  `sum without() (` + concatOffsets(splitInterval, 3, `count_over_time({app="foo"}[x]y)`) + `)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `max_over_time({app="foo"}[3m])`,
			out:                  `max without() (` + concatOffsets(splitInterval, 3, `max_over_time({app="foo"}[x]y)`) + `)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `min_over_time({app="foo"}[3m])`,
			out:                  `min without() (` + concatOffsets(splitInterval, 3, `min_over_time({app="foo"}[x]y)`) + `)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `rate({app="foo"}[3m])`,
			out:                  `sum without() (` + concatOffsets(splitInterval, 3, `increase({app="foo"}[x]y)`) + `) / 180`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `sum_over_time({app="foo"}[3m])`,
			out:                  `sum without() (` + concatOffsets(splitInterval, 3, `sum_over_time({app="foo"}[x]y)`) + `)`,
			expectedSplitQueries: 3,
		},
		// Vector aggregators
		{
			in:                   `avg(rate({app="foo"}[3m]))`,
			out:                  `avg (sum without() (` + concatOffsets(splitInterval, 3, `increase({app="foo"}[x]y)`) + `) / 180)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `avg by (bar) (rate({app="foo"}[3m]))`,
			out:                  `avg by (bar) (sum without() (` + concatOffsets(splitInterval, 3, `increase({app="foo"}[x]y)`) + `) / 180)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `count(rate({app="foo"}[3m]))`,
			out:                  `count (sum without() (` + concatOffsets(splitInterval, 3, `increase({app="foo"}[x]y)`) + `) / 180)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `count by (bar) (rate({app="foo"}[3m]))`,
			out:                  `count by (bar) (sum without() (` + concatOffsets(splitInterval, 3, `increase({app="foo"}[x]y)`) + `) / 180)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `max(rate({app="foo"}[3m]))`,
			out:                  `max (sum (` + concatOffsets(splitInterval, 3, `max(increase({app="foo"}[x]y))`) + `) / 180)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `max by (bar) (rate({app="foo"}[3m]))`,
			out:                  `max by (bar) (sum by (bar) (` + concatOffsets(splitInterval, 3, `max by (bar) (increase({app="foo"}[x]y))`) + `) / 180)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `min(rate({app="foo"}[3m]))`,
			out:                  `min (sum (` + concatOffsets(splitInterval, 3, `min(increase({app="foo"}[x]y))`) + `) / 180)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `min by (bar) (rate({app="foo"}[3m]))`,
			out:                  `min by (bar) (sum by (bar) (` + concatOffsets(splitInterval, 3, `min by (bar) (increase({app="foo"}[x]y))`) + `) / 180)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `sum(rate({app="foo"}[3m]))`,
			out:                  `sum (sum (` + concatOffsets(splitInterval, 3, `sum(increase({app="foo"}[x]y))`) + `) / 180)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `sum by (bar) (rate({app="foo"}[3m]))`,
			out:                  `sum by (bar) (sum by (bar) (` + concatOffsets(splitInterval, 3, `sum by (bar) (increase({app="foo"}[x]y))`) + `) / 180)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `topk(10, rate({app="foo"}[3m]))`,
			out:                  `topk(10, sum without() (` + concatOffsets(splitInterval, 3, `increase({app="foo"}[x]y)`) + `) / 180)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `topk(10, sum(rate({app="foo"}[3m])))`,
			out:                  `topk(10, sum(sum(` + concatOffsets(splitInterval, 3, `sum(increase({app="foo"}[x]y))`) + `) / 180))`,
			expectedSplitQueries: 3,
		},
		// Binary operations
		{
			in:                   `rate({app="foo"}[3m]) / rate({app="baz"}[6m])`,
			out:                  `(sum without() (` + concatOffsets(splitInterval, 3, `increase({app="foo"}[x]y)`) + `) / 180) / (sum without() (` + concatOffsets(splitInterval, 6, `increase({app="baz"}[x]y)`) + `) / 360)`,
			expectedSplitQueries: 9,
		},
		{
			in:                   `rate({app="foo"}[3m]) / 10`,
			out:                  `(sum without() (` + concatOffsets(splitInterval, 3, `increase({app="foo"}[x]y)`) + `) / 180) / (10)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `10 / rate({app="foo"}[3m])`,
			out:                  `(10) / (sum without() (` + concatOffsets(splitInterval, 3, `increase({app="foo"}[x]y)`) + `) / 180)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `rate({app="foo"}[3m]) / rate({app="foo"}[3m]) > 0.5`,
			out:                  `((sum without() (` + concatOffsets(splitInterval, 3, `increase({app="foo"}[x]y)`) + `) / 180) / (sum without() (` + concatOffsets(splitInterval, 3, `increase({app="foo"}[x]y)`) + `) / 180)) > (0.5)`,
			expectedSplitQueries: 6,
		},
		// Should map inner binary operations
		{
			in:                   `sum(sum_over_time({app="foo"}[3m]) + count_over_time({app="foo"}[3m]))`,
			out:                  `sum ((sum without() (` + concatOffsets(splitInterval, 3, `sum_over_time({app="foo"}[x]y)`) + `)) + (sum without() (` + concatOffsets(splitInterval, 3, `count_over_time({app="foo"}[x]y)`) + `)))`,
			expectedSplitQueries: 6,
		},
		// Should map only left-hand side operand of inner binary operation, if right-hand side range interval is too small
		{
			in:                   `sum(sum_over_time({app="foo"}[3m]) + count_over_time({app="foo"}[1m]))`,
			out:                  `sum ((sum without() (` + concatOffsets(splitInterval, 3, `sum_over_time({app="foo"}[x]y)`) + `)) + ` + concat(`(count_over_time({app="foo"}[1m]))`) + `)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `sum_over_time({app="foo"}[3m]) * count_over_time({app="foo"}[1m])`,
			out:                  `(sum without() (` + concatOffsets(splitInterval, 3, `sum_over_time({app="foo"}[x]y)`) + `)) * ` + concat(`(count_over_time({app="foo"}[1m]))`),
			expectedSplitQueries: 3,
		},
		// Should map only right-hand side operand of inner binary operation, if left-hand side range interval is too small
		{
			in:                   `sum(sum_over_time({app="foo"}[1m]) + count_over_time({app="foo"}[3m]))`,
			out:                  `sum (` + concat(`(sum_over_time({app="foo"}[1m]))`) + ` + (sum without() (` + concatOffsets(splitInterval, 3, `count_over_time({app="foo"}[x]y)`) + `)))`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `sum_over_time({app="foo"}[1m]) * count_over_time({app="foo"}[3m])`,
			out:                  concat(`(sum_over_time({app="foo"}[1m]))`) + ` * (sum without() (` + concatOffsets(splitInterval, 3, `count_over_time({app="foo"}[x]y)`) + `))`,
			expectedSplitQueries: 3,
		},
		// Parenthesis expression
		{
			in:                   `(avg_over_time({app="foo"}[3m]))`,
			out:                  `((sum without() (` + concatOffsets(splitInterval, 3, `sum_over_time({app="foo"}[x]y)`) + `)) / (sum without() (` + concatOffsets(splitInterval, 3, `count_over_time({app="foo"}[x]y)`) + `)))`,
			expectedSplitQueries: 6,
		},
		// Vector aggregator of avg_over_time should not be moved downstream
		{
			in:                   `sum(avg_over_time({app="foo"}[3m]))`,
			out:                  `sum((sum without() (` + concatOffsets(splitInterval, 3, `sum_over_time({app="foo"}[x]y)`) + `)) / (sum without() (` + concatOffsets(splitInterval, 3, `count_over_time({app="foo"}[x]y)`) + `)))`,
			expectedSplitQueries: 6,
		},
		// Should split deeper in the tree if an inner expression is splittable
		{
			in:                   `topk(10, histogram_quantile(0.9, rate({app="foo"}[3m])))`,
			out:                  `topk(10, histogram_quantile(0.9, sum without() (` + concatOffsets(splitInterval, 3, `increase({app="foo"}[x]y)`) + `) / 180))`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `stddev(rate(metric[3m]))`,
			out:                  `stddev(sum without() (` + concatOffsets(splitInterval, 3, `increase(metric[x]y)`) + `) / 180)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `count_values("dst", count_over_time(metric[3m]))`,
			out:                  `count_values("dst", sum without() (` + concatOffsets(splitInterval, 3, `count_over_time(metric[x]y)`) + `))`,
			expectedSplitQueries: 3,
		},
		// Multi-level vector aggregators should be moved downstream
		{
			in:                   `sum(max(rate({app="foo"}[3m])))`,
			out:                  `sum(max(sum (` + concatOffsets(splitInterval, 3, `sum(max(increase({app="foo"}[x]y)))`) + `) / 180))`,
			expectedSplitQueries: 3,
		},
		// Non-aggregative functions should not stop the mapping, cause children could be split anyway.
		{
			in:                   `label_replace(sum(sum_over_time(up[1m]) + count_over_time(up[3m])), "dst", "$1", "src", "(.*)")`,
			out:                  `label_replace(sum(` + concat(`(sum_over_time(up[1m]))`) + ` + (sum without() (` + concatOffsets(splitInterval, 3, `count_over_time(up[x]y)`) + `))), "dst", "$1", "src", "(.*)")`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `ceil(sum(sum_over_time(up[1m]) + count_over_time(up[3m])))`,
			out:                  `ceil(sum(` + concat(`(sum_over_time(up[1m]))`) + ` + (sum without() (` + concatOffsets(splitInterval, 3, `count_over_time(up[x]y)`) + `))))`,
			expectedSplitQueries: 3,
		},
	} {
		tt := tt

		t.Run(tt.in, func(t *testing.T) {
			expr, err := parser.ParseExpr(tt.in)
			require.NoError(t, err)
			out, err := parser.ParseExpr(tt.out)
			require.NoError(t, err)

			stats := NewMapperStats()
			mapped, err := splitter.Map(expr, stats)
			require.NoError(t, err)
			require.Equal(t, out.String(), mapped.String())

			assert.Equal(t, tt.expectedSplitQueries, stats.GetShardedQueries())
		})
	}
}

func TestInstantSplitterUnevenRangeInterval(t *testing.T) {
	splitInterval := 2 * time.Minute
	splitter := NewInstantQuerySplitter(splitInterval, log.NewNopLogger())

	for _, tt := range []struct {
		in                   string
		out                  string
		expectedSplitQueries int
	}{
		{
			in:                   `rate({app="foo"}[5m])`,
			out:                  `sum without() (__embedded_queries__{__queries__="{\"Concat\":[\"increase({app=\\\"foo\\\"}[1m] offset 4m)\",\"increase({app=\\\"foo\\\"}[2m] offset 2m)\",\"increase({app=\\\"foo\\\"}[2m])\"]}"}) / 300`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `avg_over_time({app="foo"}[3m])`,
			out:                  `(sum without() (__embedded_queries__{__queries__="{\"Concat\":[\"sum_over_time({app=\\\"foo\\\"}[1m] offset 2m)\",\"sum_over_time({app=\\\"foo\\\"}[2m])\"]}"})) / (sum without() (__embedded_queries__{__queries__="{\"Concat\":[\"count_over_time({app=\\\"foo\\\"}[1m] offset 2m)\",\"count_over_time({app=\\\"foo\\\"}[2m])\"]}"}))`,
			expectedSplitQueries: 4,
		},
		// Should support expressions with offset operator
		{
			in:                   `sum_over_time({app="foo"}[4m] offset 1m)`,
			out:                  `sum without() (__embedded_queries__{__queries__="{\"Concat\":[\"sum_over_time({app=\\\"foo\\\"}[2m] offset 3m)\",\"sum_over_time({app=\\\"foo\\\"}[2m] offset 1m)\"]}"})`,
			expectedSplitQueries: 2,
		},
		{
			in:                   `count_over_time({app="foo"}[3m] offset 1m)`,
			out:                  `sum without() (__embedded_queries__{__queries__="{\"Concat\":[\"count_over_time({app=\\\"foo\\\"}[1m] offset 3m)\",\"count_over_time({app=\\\"foo\\\"}[2m] offset 1m)\"]}"})`,
			expectedSplitQueries: 2,
		},
	} {
		tt := tt

		t.Run(tt.in, func(t *testing.T) {
			expr, err := parser.ParseExpr(tt.in)
			require.NoError(t, err)
			out, err := parser.ParseExpr(tt.out)
			require.NoError(t, err)

			stats := NewMapperStats()
			mapped, err := splitter.Map(expr, stats)
			require.NoError(t, err)
			require.Equal(t, out.String(), mapped.String())

			assert.Equal(t, tt.expectedSplitQueries, stats.GetShardedQueries())
		})
	}
}

func TestInstantSplitterNoOp(t *testing.T) {
	splitInterval := 1 * time.Minute
	splitter := NewInstantQuerySplitter(splitInterval, log.NewNopLogger())

	for _, tt := range []struct {
		query string
	}{
		// should be noop if expression is not splittable
		{
			query: `quantile_over_time(0.95, foo[3m])`,
		},
		{
			query: `topk(10, histogram_quantile(0.9, irate({app="foo"}[3m])))`,
		},
		// should be noop if range interval is lower or equal to split interval (1m)
		{
			query: `rate({app="foo"}[1m])`,
		},
		// should be noop if expression is a number literal
		{
			query: `5`,
		},
		// should be noop if binary expression's operands are both constant scalars
		{
			query: `20 / 10`,
		},
		{
			query: `(20 / 10)`,
		},
		{
			query: `(20) / (10)`,
		},
		{
			query: `time() != bool 0`,
		},
		// should be noop if binary operation is not mapped
		//   - first operand `rate(metric_counter[1m])` has a smaller range interval than the configured splitting
		//   - second operand `rate(metric_counter[5h:5m])` is a subquery
		{
			query: `rate({app="foo"}[1m]) / rate({app="bar"}[5h:5m]) > 0.5`,
		},
		// should be noop if inner binary operation is not mapped
		{
			query: `sum(rate({app="foo"}[1h:5m]) * 60) by (bar)`,
		},
		// should be noop if subquery
		{
			query: `sum_over_time(metric_counter[1h:5m])`,
		},
		{
			query: `sum(rate(metric_counter[30m:5s]))`,
		},
		{
			// Parenthesis expression between sum_over_time() and the subquery.
			query: `sum_over_time((metric_counter[30m:5s]))`,
		},
		{
			// Multiple parenthesis expressions between sum_over_time() and the subquery.
			query: `sum_over_time((((metric_counter[30m:5s]))))`,
		},
		{
			query: `quantile_over_time(1, metric_counter[10m:1m])`,
		},
		{
			query: `sum(avg_over_time(metric_counter[1h:5m])) by (bar)`,
		},
		{
			query: `min_over_time(sum by(group_1) (rate(metric_counter[5m]))[10m:2m])`,
		},
		{
			query: `max_over_time(stddev_over_time(deriv(rate(metric_counter[10m])[5m:1m])[2m:])[10m:])`,
		},
		{
			query: `rate(sum by(group_1) (rate(metric_counter[5m]))[10m:])`,
		},
		{
			query: `absent_over_time(rate(metric_counter[5m])[10m:])`,
		},
		{
			query: `max_over_time(stddev_over_time(deriv(sort(metric_counter)[5m:1m])[2m:])[10m:])`,
		},
		{
			query: `max_over_time(absent_over_time(deriv(rate(metric_counter[1m])[5m:1m])[2m:])[10m:])`,
		},
	} {
		tt := tt

		t.Run(tt.query, func(t *testing.T) {
			expr, err := parser.ParseExpr(tt.query)
			require.NoError(t, err)

			stats := NewMapperStats()

			// Do not assert if the mapped expression is equal to the input one, because it could actually be slightly
			// transformed (e.g. added parenthesis). The actual way to check if it was split or not is to read it from
			// the statistics.
			_, err = splitter.Map(expr, stats)
			require.NoError(t, err)
			assert.Equal(t, 0, stats.GetShardedQueries())
		})
	}
}

func TestGetRangeIntervals(t *testing.T) {
	tests := []struct {
		query    string
		expected []time.Duration
	}{
		{
			query:    `time()`,
			expected: []time.Duration{},
		}, {
			query:    `sum(rate(metric[1m]))`,
			expected: []time.Duration{time.Minute},
		}, {
			query:    `sum(rate(metric[1m])) + sum(rate(metric[5m]))`,
			expected: []time.Duration{time.Minute, 5 * time.Minute},
		}, {
			query:    `sum_over_time(rate(metric[1m])[1h:5m])`,
			expected: []time.Duration{time.Hour, time.Minute},
		},
	}

	for _, testData := range tests {
		t.Run(testData.query, func(t *testing.T) {
			expr, err := parser.ParseExpr(testData.query)
			require.NoError(t, err)
			assert.Equal(t, testData.expected, getRangeIntervals(expr))
		})
	}
}

func concatOffsets(splitInterval time.Duration, offsets int, queryTemplate string) string {
	queries := make([]string, offsets)
	offsetIndex := offsets
	for offset := range queries {
		offsetIndex--
		offsetQuery := fmt.Sprintf("[%s]%s", splitInterval, getSplitOffset(splitInterval, offsetIndex))
		queries[offset] = strings.ReplaceAll(queryTemplate, "[x]y", offsetQuery)
	}
	return concat(queries...)
}

func getSplitOffset(splitInterval time.Duration, offset int) string {
	if offset == 0 {
		return ""
	}
	return fmt.Sprintf("offset %v", time.Duration(offset)*splitInterval)
}
