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
	splitInterval := 2 * time.Minute
	mapper, err := NewRangeMapper(splitInterval, log.NewNopLogger())
	require.NoError(t, err)

	for _, tt := range []struct {
		in                   string
		out                  string
		expectedSplitQueries int
	}{
		{
			in:                   `count_over_time({app="foo"}[4m])`,
			out:                  `sum without() (` + concatOffsets(splitInterval, 2, `count_over_time({app="foo"}[x]y)`) + `)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `sum_over_time({app="foo"}[4m])`,
			out:                  `sum without() (` + concatOffsets(splitInterval, 2, `sum_over_time({app="foo"}[x]y)`) + `)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `max_over_time({app="foo"}[4m])`,
			out:                  `max without() (` + concatOffsets(splitInterval, 2, `max_over_time({app="foo"}[x]y)`) + `)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `min_over_time({app="foo"}[4m])`,
			out:                  `min without() (` + concatOffsets(splitInterval, 2, `min_over_time({app="foo"}[x]y)`) + `)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `sum(count_over_time({app="foo"}[4m]))`,
			out:                  `sum (sum without () (` + concatOffsets(splitInterval, 2, `sum(count_over_time({app="foo"}[x]y))`) + `))`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `max(count_over_time({app="foo"}[4m]))`,
			out:                  `max (sum without () (` + concatOffsets(splitInterval, 2, `max(count_over_time({app="foo"}[x]y))`) + `))`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `min(count_over_time({app="foo"}[4m]))`,
			out:                  `min (sum without () (` + concatOffsets(splitInterval, 2, `min(count_over_time({app="foo"}[x]y))`) + `))`,
			expectedSplitQueries: 3,
		},
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
		//{
		//	in:                   `sum(count(count_over_time({app="foo"}[4m])))`,
		//	out:                  `sum (sum without () (` + concatOffsets(splitInterval, 2, `sum(count(count_over_time({app="foo"}[x]y)))`) + `))`,
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
