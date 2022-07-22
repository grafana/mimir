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
			in:                   `count(sum_over_time({app="foo"}[3m]))`,
			out:                  `count (sum without() (` + concatOffsets(splitInterval, 3, `sum_over_time({app="foo"}[x]y)`) + `))`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `count by (bar) (sum_over_time({app="foo"}[3m]))`,
			out:                  `count by (bar) (sum without() (` + concatOffsets(splitInterval, 3, `sum_over_time({app="foo"}[x]y)`) + `))`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `max(sum_over_time({app="foo"}[3m]))`,
			out:                  `max (sum (` + concatOffsets(splitInterval, 3, `max(sum_over_time({app="foo"}[x]y))`) + `))`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `max by (bar) (sum_over_time({app="foo"}[3m]))`,
			out:                  `max by (bar) (sum by (bar) (` + concatOffsets(splitInterval, 3, `max by (bar) (sum_over_time({app="foo"}[x]y))`) + `))`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `min(sum_over_time({app="foo"}[3m]))`,
			out:                  `min (sum (` + concatOffsets(splitInterval, 3, `min(sum_over_time({app="foo"}[x]y))`) + `))`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `min by (bar) (sum_over_time({app="foo"}[3m]))`,
			out:                  `min by (bar) (sum by (bar) (` + concatOffsets(splitInterval, 3, `min by (bar) (sum_over_time({app="foo"}[x]y))`) + `))`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `sum(sum_over_time({app="foo"}[3m]))`,
			out:                  `sum (sum (` + concatOffsets(splitInterval, 3, `sum(sum_over_time({app="foo"}[x]y))`) + `))`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `sum by (bar) (sum_over_time({app="foo"}[3m]))`,
			out:                  `sum by (bar) (sum by (bar) (` + concatOffsets(splitInterval, 3, `sum by (bar) (sum_over_time({app="foo"}[x]y))`) + `))`,
			expectedSplitQueries: 3,
		},
		//{
		//	in:                   `sum(max(sum_over_time({app="foo"}[3m])))`,
		//	out:                  `sum (max (sum(` + concatOffsets(splitInterval, 3, `sum(max(sum_over_time({app="foo"}[x]y)))`) + `)))`,
		//	expectedSplitQueries: 3,
		//},
		// TODO: binary expressions - if 2 number literals do not split, if one number literal split
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
		//{
		//	in: `count_over_time({app="foo"}[4m])`,
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
