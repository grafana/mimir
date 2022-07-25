package astmapper

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
)

func TestRangeMapper(t *testing.T) {
	splitInterval := 1 * time.Minute
	mapper, err := NewRangeMapper(splitInterval, log.NewNopLogger())
	require.NoError(t, err)

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
			expectedSplitQueries: 3,
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
		// Should map inner binary operations
		{
			in:                   `sum(sum_over_time({app="foo"}[3m]) + count_over_time({app="foo"}[3m]))`,
			out:                  `sum ((sum without() (` + concatOffsets(splitInterval, 3, `sum_over_time({app="foo"}[x]y)`) + `)) + (sum without() (` + concatOffsets(splitInterval, 3, `count_over_time({app="foo"}[x]y)`) + `)))`,
			expectedSplitQueries: 6,
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
			expectedSplitQueries: 3,
		},
		// Multi-level vector aggregators should be moved downstream
		{
			in:                   `sum(max(rate({app="foo"}[3m])))`,
			out:                  `sum(max(sum (` + concatOffsets(splitInterval, 3, `sum(max(increase({app="foo"}[x]y)))`) + `) / 180))`,
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
			mapped, err := mapper.Map(expr, stats)
			require.NoError(t, err)
			require.Equal(t, out.String(), mapped.String())

			//assert.Equal(t, tt.expectedSplitQueries, stats.GetShardedQueries())
		})
	}
}

func TestRangeMapperUnevenRangeInterval(t *testing.T) {
	splitInterval := 1 * time.Minute
	mapper, err := NewRangeMapper(splitInterval, log.NewNopLogger())
	require.NoError(t, err)

	for _, tt := range []struct {
		in                   string
		out                  string
		expectedSplitQueries int
	}{
		// TODO: Should support expressions with offset operator
		//{
		//	in:                   `count_over_time({app="foo"}[4m] offset 1m)`,
		//	out:                  `sum without() (__embedded_queries__{__queries__="{\"Concat\":[\"count_over_time({app=\\\"foo\\\"}[1m] offset 3m)\",\"count_over_time({app=\\\"foo\\\"}[2m] offset 2m)\",\"count_over_time({app=\\\"foo\\\"}[2m] offset 1)\"]}"})`,
		//	expectedSplitQueries: 3,
		//},
		//{
		//	// Should add the remainder range interval
		//	in:                   `count_over_time({app="foo"}[3m])`,
		//	out:                  `sum without() (__embedded_queries__{__queries__="{\"Concat\":[\"count_over_time({app=\\\"foo\\\"}[1m] offset 2m)\",\"count_over_time({app=\\\"foo\\\"}[2m])\"]}"})`,
		//	expectedSplitQueries: 3,
		//},
		//{
		//	// Should add the remainder range interval
		//	in:                   `count_over_time({app="foo"}[5m])`,
		//	out:                  `sum without() (__embedded_queries__{__queries__="{\"Concat\":[\"count_over_time({app=\\\"foo\\\"}[1m] offset 4m)\",\"count_over_time({app=\\\"foo\\\"}[2m] offset 2m)\",\"count_over_time({app=\\\"foo\\\"}[2m])\"]}"})`,
		//	expectedSplitQueries: 3,
		//},
	} {
		tt := tt

		t.Run(tt.in, func(t *testing.T) {
			expr, err := parser.ParseExpr(tt.in)
			require.NoError(t, err)
			out, err := parser.ParseExpr(tt.out)
			require.NoError(t, err)

			stats := NewMapperStats()
			mapped, err := mapper.Map(expr, stats)
			require.NoError(t, err)
			require.Equal(t, out.String(), mapped.String())

			//assert.Equal(t, tt.expectedSplitQueries, stats.GetShardedQueries())
		})
	}
}

func TestRangeMapperNoOp(t *testing.T) {
	splitInterval := 1 * time.Minute
	mapper, err := NewRangeMapper(splitInterval, log.NewNopLogger())
	require.NoError(t, err)

	for _, tt := range []struct {
		in                   string
		out                  string
		expectedSplitQueries int
	}{
		// TODO: should not split binary operation if both operands are number literals
	} {
		tt := tt

		t.Run(tt.in, func(t *testing.T) {
			expr, err := parser.ParseExpr(tt.in)
			require.NoError(t, err)
			out, err := parser.ParseExpr(tt.out)
			require.NoError(t, err)

			stats := NewMapperStats()
			mapped, err := mapper.Map(expr, stats)
			require.NoError(t, err)
			require.Equal(t, out.String(), mapped.String())

			//assert.Equal(t, tt.expectedSplitQueries, stats.GetShardedQueries())
		})
	}
}

func concatOffsets(splitInterval time.Duration, offsets int, queryTemplate string) string {
	queries := make([]string, offsets)
	offsetIndex := offsets
	for offset := range queries {
		offsetIndex--
		offsetQuery := fmt.Sprintf("[%s]%s", splitInterval, getOffset(splitInterval, offsetIndex))
		queries[offset] = strings.ReplaceAll(queryTemplate, "[x]y", offsetQuery)
	}
	return concat(queries...)
}

func getOffset(splitInterval time.Duration, offset int) string {
	if offset == 0 {
		return ""
	}
	return fmt.Sprintf("offset %v", time.Duration(offset)*splitInterval)
}
