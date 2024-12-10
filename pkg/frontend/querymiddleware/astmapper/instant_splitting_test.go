// SPDX-License-Identifier: AGPL-3.0-only

package astmapper

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInstantSplitter(t *testing.T) {
	splitInterval := 1 * time.Minute

	for _, tt := range []struct {
		in                   string
		out                  string
		expectedSplitQueries int
	}{
		// Splittable range vector aggregators
		{
			in:                   `avg_over_time({app="foo"}[3m])`,
			out:                  `(sum without() (` + concatOffsets(splitInterval, 3, false, `sum_over_time({app="foo"}[x]y)`) + `)) / (sum without() (` + concatOffsets(splitInterval, 3, false, `count_over_time({app="foo"}[x]y)`) + `))`,
			expectedSplitQueries: 6,
		},
		{
			in:                   `count_over_time({app="foo"}[3m])`,
			out:                  `sum without() (` + concatOffsets(splitInterval, 3, false, `count_over_time({app="foo"}[x]y)`) + `)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `increase({app="foo"}[3m])`,
			out:                  `sum without() (` + concatOffsets(splitInterval, 3, true, `increase({app="foo"}[x]y)`) + `)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `max_over_time({app="foo"}[3m])`,
			out:                  `max without() (` + concatOffsets(splitInterval, 3, true, `max_over_time({app="foo"}[x]y)`) + `)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `min_over_time({app="foo"}[3m])`,
			out:                  `min without() (` + concatOffsets(splitInterval, 3, true, `min_over_time({app="foo"}[x]y)`) + `)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `present_over_time({app="foo"}[3m])`,
			out:                  `max without() (` + concatOffsets(splitInterval, 3, true, `present_over_time({app="foo"}[x]y)`) + `)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `rate({app="foo"}[3m])`,
			out:                  `sum without() (` + concatOffsets(splitInterval, 3, true, `increase({app="foo"}[x]y)`) + `) / 180`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `sum_over_time({app="foo"}[3m])`,
			out:                  `sum without() (` + concatOffsets(splitInterval, 3, false, `sum_over_time({app="foo"}[x]y)`) + `)`,
			expectedSplitQueries: 3,
		},
		// Splittable aggregations wrapped by non-aggregative functions.
		{
			in:                   `absent(sum_over_time({app="foo"}[3m]))`,
			out:                  `absent(sum without() (` + concatOffsets(splitInterval, 3, false, `sum_over_time({app="foo"}[x]y)`) + `))`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `ceil(sum_over_time({app="foo"}[3m]))`,
			out:                  `ceil(sum without() (` + concatOffsets(splitInterval, 3, false, `sum_over_time({app="foo"}[x]y)`) + `))`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `clamp(sum_over_time({app="foo"}[3m]), 1, 10)`,
			out:                  `clamp(sum without() (` + concatOffsets(splitInterval, 3, false, `sum_over_time({app="foo"}[x]y)`) + `), 1, 10)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `clamp_max(sum_over_time({app="foo"}[3m]), 10)`,
			out:                  `clamp_max(sum without() (` + concatOffsets(splitInterval, 3, false, `sum_over_time({app="foo"}[x]y)`) + `), 10)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `clamp_min(sum_over_time({app="foo"}[3m]), 1)`,
			out:                  `clamp_min(sum without() (` + concatOffsets(splitInterval, 3, false, `sum_over_time({app="foo"}[x]y)`) + `), 1)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `exp(sum_over_time({app="foo"}[3m]))`,
			out:                  `exp(sum without() (` + concatOffsets(splitInterval, 3, false, `sum_over_time({app="foo"}[x]y)`) + `))`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `floor(sum_over_time({app="foo"}[3m]))`,
			out:                  `floor(sum without() (` + concatOffsets(splitInterval, 3, false, `sum_over_time({app="foo"}[x]y)`) + `))`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `histogram_quantile(0.9, sum_over_time({app="foo"}[3m]))`,
			out:                  `histogram_quantile(0.9, sum without() (` + concatOffsets(splitInterval, 3, false, `sum_over_time({app="foo"}[x]y)`) + `) )`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `label_join(sum_over_time({app="foo"}[3m]), "foo", ",", "group_1", "group_2", "const")`,
			out:                  `label_join(sum without() (` + concatOffsets(splitInterval, 3, false, `sum_over_time({app="foo"}[x]y)`) + `), "foo", ",", "group_1", "group_2", "const")`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `label_replace(sum_over_time({app="foo"}[3m]), "foo", "bar$1", "group_2", "(.*)")`,
			out:                  `label_replace(sum without() (` + concatOffsets(splitInterval, 3, false, `sum_over_time({app="foo"}[x]y)`) + `), "foo", "bar$1", "group_2", "(.*)")`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `ln(sum_over_time({app="foo"}[3m]))`,
			out:                  `ln(sum without() (` + concatOffsets(splitInterval, 3, false, `sum_over_time({app="foo"}[x]y)`) + `))`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `log2(sum_over_time({app="foo"}[3m]))`,
			out:                  `log2(sum without() (` + concatOffsets(splitInterval, 3, false, `sum_over_time({app="foo"}[x]y)`) + `))`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `round(sum_over_time({app="foo"}[3m]))`,
			out:                  `round(sum without() (` + concatOffsets(splitInterval, 3, false, `sum_over_time({app="foo"}[x]y)`) + `))`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `scalar(sum_over_time({app="foo"}[3m]))`,
			out:                  `scalar(sum without() (` + concatOffsets(splitInterval, 3, false, `sum_over_time({app="foo"}[x]y)`) + `))`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `sgn(sum_over_time({app="foo"}[3m]))`,
			out:                  `sgn(sum without() (` + concatOffsets(splitInterval, 3, false, `sum_over_time({app="foo"}[x]y)`) + `))`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `sort(sum_over_time({app="foo"}[3m]))`,
			out:                  `sort(sum without() (` + concatOffsets(splitInterval, 3, false, `sum_over_time({app="foo"}[x]y)`) + `))`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `sort_desc(sum_over_time({app="foo"}[3m]))`,
			out:                  `sort_desc(sum without() (` + concatOffsets(splitInterval, 3, false, `sum_over_time({app="foo"}[x]y)`) + `))`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `sqrt(sum_over_time({app="foo"}[3m]))`,
			out:                  `sqrt(sum without() (` + concatOffsets(splitInterval, 3, false, `sum_over_time({app="foo"}[x]y)`) + `))`,
			expectedSplitQueries: 3,
		},
		// Vector aggregators
		{
			in:                   `avg(rate({app="foo"}[3m]))`,
			out:                  `avg (sum without() (` + concatOffsets(splitInterval, 3, true, `increase({app="foo"}[x]y)`) + `) / 180)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `avg by (bar) (rate({app="foo"}[3m]))`,
			out:                  `avg by (bar) (sum without() (` + concatOffsets(splitInterval, 3, true, `increase({app="foo"}[x]y)`) + `) / 180)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `count(rate({app="foo"}[3m]))`,
			out:                  `count (sum without() (` + concatOffsets(splitInterval, 3, true, `increase({app="foo"}[x]y)`) + `) / 180)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `count by (bar) (rate({app="foo"}[3m]))`,
			out:                  `count by (bar) (sum without() (` + concatOffsets(splitInterval, 3, true, `increase({app="foo"}[x]y)`) + `) / 180)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `max(rate({app="foo"}[3m]))`,
			out:                  `max (sum (` + concatOffsets(splitInterval, 3, true, `max(increase({app="foo"}[x]y))`) + `) / 180)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `max by (bar) (rate({app="foo"}[3m]))`,
			out:                  `max by (bar) (sum by (bar) (` + concatOffsets(splitInterval, 3, true, `max by (bar) (increase({app="foo"}[x]y))`) + `) / 180)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `min(rate({app="foo"}[3m]))`,
			out:                  `min (sum (` + concatOffsets(splitInterval, 3, true, `min(increase({app="foo"}[x]y))`) + `) / 180)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `min by (bar) (rate({app="foo"}[3m]))`,
			out:                  `min by (bar) (sum by (bar) (` + concatOffsets(splitInterval, 3, true, `min by (bar) (increase({app="foo"}[x]y))`) + `) / 180)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `sum(rate({app="foo"}[3m]))`,
			out:                  `sum (sum (` + concatOffsets(splitInterval, 3, true, `sum(increase({app="foo"}[x]y))`) + `) / 180)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `sum by (bar) (rate({app="foo"}[3m]))`,
			out:                  `sum by (bar) (sum by (bar) (` + concatOffsets(splitInterval, 3, true, `sum by (bar) (increase({app="foo"}[x]y))`) + `) / 180)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `topk(10, rate({app="foo"}[3m]))`,
			out:                  `topk(10, sum without() (` + concatOffsets(splitInterval, 3, true, `increase({app="foo"}[x]y)`) + `) / 180)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `topk(10, sum(rate({app="foo"}[3m])))`,
			out:                  `topk(10, sum(sum(` + concatOffsets(splitInterval, 3, true, `sum(increase({app="foo"}[x]y))`) + `) / 180))`,
			expectedSplitQueries: 3,
		},
		// Binary operations
		{
			in:                   `rate({app="foo"}[3m]) / rate({app="baz"}[6m])`,
			out:                  `(sum without() (` + concatOffsets(splitInterval, 3, true, `increase({app="foo"}[x]y)`) + `) / 180) / (sum without() (` + concatOffsets(splitInterval, 6, true, `increase({app="baz"}[x]y)`) + `) / 360)`,
			expectedSplitQueries: 9,
		},
		{
			in:                   `rate({app="foo"}[3m]) / 10`,
			out:                  `(sum without() (` + concatOffsets(splitInterval, 3, true, `increase({app="foo"}[x]y)`) + `) / 180) / (10)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `10 / rate({app="foo"}[3m])`,
			out:                  `(10) / (sum without() (` + concatOffsets(splitInterval, 3, true, `increase({app="foo"}[x]y)`) + `) / 180)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `rate({app="foo"}[3m]) / rate({app="foo"}[3m]) > 0.5`,
			out:                  `((sum without() (` + concatOffsets(splitInterval, 3, true, `increase({app="foo"}[x]y)`) + `) / 180) / (sum without() (` + concatOffsets(splitInterval, 3, true, `increase({app="foo"}[x]y)`) + `) / 180)) > (0.5)`,
			expectedSplitQueries: 6,
		},
		// Should map inner binary operations
		{
			in:                   `sum(sum_over_time({app="foo"}[3m]) + count_over_time({app="foo"}[3m]))`,
			out:                  `sum ((sum without() (` + concatOffsets(splitInterval, 3, false, `sum_over_time({app="foo"}[x]y)`) + `)) + (sum without() (` + concatOffsets(splitInterval, 3, false, `count_over_time({app="foo"}[x]y)`) + `)))`,
			expectedSplitQueries: 6,
		},
		// Should map only left-hand side operand of inner binary operation, if right-hand side range interval is too small
		{
			in:                   `sum(sum_over_time({app="foo"}[3m]) + count_over_time({app="foo"}[1m]))`,
			out:                  `sum ((sum without() (` + concatOffsets(splitInterval, 3, false, `sum_over_time({app="foo"}[x]y)`) + `)) + ` + concat(`(count_over_time({app="foo"}[1m]))`) + `)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `sum_over_time({app="foo"}[3m]) * count_over_time({app="foo"}[1m])`,
			out:                  `(sum without() (` + concatOffsets(splitInterval, 3, false, `sum_over_time({app="foo"}[x]y)`) + `)) * ` + concat(`(count_over_time({app="foo"}[1m]))`),
			expectedSplitQueries: 3,
		},
		// Should map only right-hand side operand of inner binary operation, if left-hand side range interval is too small
		{
			in:                   `sum(sum_over_time({app="foo"}[1m]) + count_over_time({app="foo"}[3m]))`,
			out:                  `sum (` + concat(`(sum_over_time({app="foo"}[1m]))`) + ` + (sum without() (` + concatOffsets(splitInterval, 3, false, `count_over_time({app="foo"}[x]y)`) + `)))`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `sum_over_time({app="foo"}[1m]) * count_over_time({app="foo"}[3m])`,
			out:                  concat(`(sum_over_time({app="foo"}[1m]))`) + ` * (sum without() (` + concatOffsets(splitInterval, 3, false, `count_over_time({app="foo"}[x]y)`) + `))`,
			expectedSplitQueries: 3,
		},
		// Parenthesis expression
		{
			in:                   `(avg_over_time({app="foo"}[3m]))`,
			out:                  `((sum without() (` + concatOffsets(splitInterval, 3, false, `sum_over_time({app="foo"}[x]y)`) + `)) / (sum without() (` + concatOffsets(splitInterval, 3, false, `count_over_time({app="foo"}[x]y)`) + `)))`,
			expectedSplitQueries: 6,
		},
		// Vector aggregator of avg_over_time should not be moved downstream
		{
			in:                   `sum(avg_over_time({app="foo"}[3m]))`,
			out:                  `sum((sum without() (` + concatOffsets(splitInterval, 3, false, `sum_over_time({app="foo"}[x]y)`) + `)) / (sum without() (` + concatOffsets(splitInterval, 3, false, `count_over_time({app="foo"}[x]y)`) + `)))`,
			expectedSplitQueries: 6,
		},
		// Offset operator
		{
			in:                   `rate({app="foo"}[3m] offset 3m)`,
			out:                  `sum without() (__embedded_queries__{__queries__="{\"Concat\":[{\"Expr\":\"increase({app=\\\"foo\\\"}[1m] offset 5m)\"},{\"Expr\":\"increase({app=\\\"foo\\\"}[1m] offset 4m)\"},{\"Expr\":\"increase({app=\\\"foo\\\"}[1m] offset 3m)\"}]}"}) / 180`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `avg_over_time({app="foo"}[3m] offset 5m)`,
			out:                  `(sum without() (__embedded_queries__{__queries__="{\"Concat\":[{\"Expr\":\"sum_over_time({app=\\\"foo\\\"}[1m] offset 7m)\"},{\"Expr\":\"sum_over_time({app=\\\"foo\\\"}[59s999ms] offset 6m)\"},{\"Expr\":\"sum_over_time({app=\\\"foo\\\"}[59s999ms] offset 5m)\"}]}"})) / (sum without() (__embedded_queries__{__queries__="{\"Concat\":[{\"Expr\":\"count_over_time({app=\\\"foo\\\"}[1m] offset 7m)\"},{\"Expr\":\"count_over_time({app=\\\"foo\\\"}[59s999ms] offset 6m)\"},{\"Expr\":\"count_over_time({app=\\\"foo\\\"}[59s999ms] offset 5m)\"}]}"}))`,
			expectedSplitQueries: 6,
		},
		{
			in:                   `rate({app="foo"}[3m] offset 30s)`,
			out:                  `sum without() (__embedded_queries__{__queries__="{\"Concat\":[{\"Expr\":\"increase({app=\\\"foo\\\"}[1m] offset 2m30s)\"},{\"Expr\":\"increase({app=\\\"foo\\\"}[1m] offset 1m30s)\"},{\"Expr\":\"increase({app=\\\"foo\\\"}[1m] offset 30s)\"}]}"}) / 180`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `count_over_time({app="foo"}[3m] offset -3m)`,
			out:                  `sum without() (__embedded_queries__{__queries__="{\"Concat\":[{\"Expr\":\"count_over_time({app=\\\"foo\\\"}[1m] offset -1m)\"},{\"Expr\":\"count_over_time({app=\\\"foo\\\"}[59s999ms] offset -2m)\"},{\"Expr\":\"count_over_time({app=\\\"foo\\\"}[59s999ms] offset -3m)\"}]}"})`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `avg_over_time({app="foo"}[3m] offset -5m)`,
			out:                  `(sum without() (__embedded_queries__{__queries__="{\"Concat\":[{\"Expr\":\"sum_over_time({app=\\\"foo\\\"}[1m] offset -3m)\"},{\"Expr\":\"sum_over_time({app=\\\"foo\\\"}[59s999ms] offset -4m)\"},{\"Expr\":\"sum_over_time({app=\\\"foo\\\"}[59s999ms] offset -5m)\"}]}"})) / (sum without() (__embedded_queries__{__queries__="{\"Concat\":[{\"Expr\":\"count_over_time({app=\\\"foo\\\"}[1m] offset -3m)\"},{\"Expr\":\"count_over_time({app=\\\"foo\\\"}[59s999ms] offset -4m)\"},{\"Expr\":\"count_over_time({app=\\\"foo\\\"}[59s999ms] offset -5m)\"}]}"}))`,
			expectedSplitQueries: 6,
		},
		{
			in:                   `count_over_time({app="foo"}[3m] offset -30s)`,
			out:                  `sum without() (__embedded_queries__{__queries__="{\"Concat\":[{\"Expr\":\"count_over_time({app=\\\"foo\\\"}[1m] offset 1m30s)\"},{\"Expr\":\"count_over_time({app=\\\"foo\\\"}[59s999ms] offset 30s)\"},{\"Expr\":\"count_over_time({app=\\\"foo\\\"}[59s999ms] offset -30s)\"}]}"})`,
			expectedSplitQueries: 3,
		},
		// @ modifier
		{
			in:                   `rate({app="foo"}[3m] @ start())`,
			out:                  `sum without() (__embedded_queries__{__queries__="{\"Concat\":[{\"Expr\":\"increase({app=\\\"foo\\\"}[1m] @ start() offset 2m)\"},{\"Expr\":\"increase({app=\\\"foo\\\"}[1m] @ start() offset 1m)\"},{\"Expr\":\"increase({app=\\\"foo\\\"}[1m] @ start())\"}]}"}) / 180`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `sum(sum_over_time({app="foo"}[3m] @ end()))`,
			out:                  `sum(sum(__embedded_queries__{__queries__="{\"Concat\":[{\"Expr\":\"sum(sum_over_time({app=\\\"foo\\\"}[1m] @ end() offset 2m))\"},{\"Expr\":\"sum(sum_over_time({app=\\\"foo\\\"}[59s999ms] @ end() offset 1m))\"},{\"Expr\":\"sum(sum_over_time({app=\\\"foo\\\"}[59s999ms] @ end()))\"}]}"}))`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `avg(avg_over_time({app="foo"}[3m] @ 1609746000))`,
			out:                  `avg((sum without() (__embedded_queries__{__queries__="{\"Concat\":[{\"Expr\":\"sum_over_time({app=\\\"foo\\\"}[1m] @ 1609746000.000 offset 2m)\"},{\"Expr\":\"sum_over_time({app=\\\"foo\\\"}[59s999ms] @ 1609746000.000 offset 1m)\"},{\"Expr\":\"sum_over_time({app=\\\"foo\\\"}[59s999ms] @ 1609746000.000)\"}]}"})) / (sum without() (__embedded_queries__{__queries__="{\"Concat\":[{\"Expr\":\"count_over_time({app=\\\"foo\\\"}[1m] @ 1609746000.000 offset 2m)\"},{\"Expr\":\"count_over_time({app=\\\"foo\\\"}[59s999ms] @ 1609746000.000 offset 1m)\"},{\"Expr\":\"count_over_time({app=\\\"foo\\\"}[59s999ms] @ 1609746000.000)\"}]}"})))`,
			expectedSplitQueries: 6,
		},
		// Should support both offset and @ operators
		{
			in:                   `max_over_time({app="foo"}[3m] @ 1609746000 offset 1m)`,
			out:                  `max without() (__embedded_queries__{__queries__="{\"Concat\":[{\"Expr\":\"max_over_time({app=\\\"foo\\\"}[1m] @ 1609746000.000 offset 3m)\"},{\"Expr\":\"max_over_time({app=\\\"foo\\\"}[1m] @ 1609746000.000 offset 2m)\"},{\"Expr\":\"max_over_time({app=\\\"foo\\\"}[1m] @ 1609746000.000 offset 1m)\"}]}"})`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `max_over_time({app="foo"}[3m] offset 1m @ 1609746000)`,
			out:                  `max without() (__embedded_queries__{__queries__="{\"Concat\":[{\"Expr\":\"max_over_time({app=\\\"foo\\\"}[1m] @ 1609746000.000 offset 3m)\"},{\"Expr\":\"max_over_time({app=\\\"foo\\\"}[1m] @ 1609746000.000 offset 2m)\"},{\"Expr\":\"max_over_time({app=\\\"foo\\\"}[1m] @ 1609746000.000 offset 1m)\"}]}"})`,
			expectedSplitQueries: 3,
		},
		// Should split deeper in the tree if an inner expression is splittable
		{
			in:                   `topk(10, histogram_quantile(0.9, rate({app="foo"}[3m])))`,
			out:                  `topk(10, histogram_quantile(0.9, sum without() (` + concatOffsets(splitInterval, 3, true, `increase({app="foo"}[x]y)`) + `) / 180))`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `stddev(rate(metric[3m]))`,
			out:                  `stddev(sum without() (` + concatOffsets(splitInterval, 3, true, `increase(metric[x]y)`) + `) / 180)`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `count_values("dst", count_over_time(metric[3m]))`,
			out:                  `count_values("dst", sum without() (` + concatOffsets(splitInterval, 3, false, `count_over_time(metric[x]y)`) + `))`,
			expectedSplitQueries: 3,
		},
		// Multi-level vector aggregators should be moved downstream
		{
			in:                   `sum(max(rate({app="foo"}[3m])))`,
			out:                  `sum(max(sum (` + concatOffsets(splitInterval, 3, true, `sum(max(increase({app="foo"}[x]y)))`) + `) / 180))`,
			expectedSplitQueries: 3,
		},
		// Non-aggregative functions should not stop the mapping, cause children could be split anyway.
		{
			in:                   `label_replace(sum(sum_over_time(up[1m]) + count_over_time(up[3m])), "dst", "$1", "src", "(.*)")`,
			out:                  `label_replace(sum(` + concat(`(sum_over_time(up[1m]))`) + ` + (sum without() (` + concatOffsets(splitInterval, 3, false, `count_over_time(up[x]y)`) + `))), "dst", "$1", "src", "(.*)")`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `ceil(sum(sum_over_time(up[1m]) + count_over_time(up[3m])))`,
			out:                  `ceil(sum(` + concat(`(sum_over_time(up[1m]))`) + ` + (sum without() (` + concatOffsets(splitInterval, 3, false, `count_over_time(up[x]y)`) + `))))`,
			expectedSplitQueries: 3,
		},
	} {
		tt := tt

		t.Run(tt.in, func(t *testing.T) {
			stats := NewInstantSplitterStats()
			mapper := NewInstantQuerySplitter(context.Background(), splitInterval, log.NewNopLogger(), stats)

			expr, err := parser.ParseExpr(tt.in)
			require.NoError(t, err)
			out, err := parser.ParseExpr(tt.out)
			require.NoError(t, err)

			mapped, err := mapper.Map(expr)
			require.NoError(t, err)
			require.Equal(t, out.String(), mapped.String())

			assert.Equal(t, tt.expectedSplitQueries, stats.GetSplitQueries())
			assert.Equal(t, noneSkippedReason, stats.GetSkippedReason())
		})
	}
}

func TestInstantSplitterUnevenRangeInterval(t *testing.T) {
	splitInterval := 2 * time.Minute

	for _, tt := range []struct {
		in                   string
		out                  string
		expectedSplitQueries int
	}{
		{
			in:                   `rate({app="foo"}[5m])`,
			out:                  `sum without() (__embedded_queries__{__queries__="{\"Concat\":[{\"Expr\":\"increase({app=\\\"foo\\\"}[1m] offset 4m)\"},{\"Expr\":\"increase({app=\\\"foo\\\"}[2m] offset 2m)\"},{\"Expr\":\"increase({app=\\\"foo\\\"}[2m])\"}]}"}) / 300`,
			expectedSplitQueries: 3,
		},
		{
			in:                   `avg_over_time({app="foo"}[3m])`,
			out:                  `(sum without() (__embedded_queries__{__queries__="{\"Concat\":[{\"Expr\":\"sum_over_time({app=\\\"foo\\\"}[1m] offset 2m)\"},{\"Expr\":\"sum_over_time({app=\\\"foo\\\"}[1m59s999ms])\"}]}"})) / (sum without() (__embedded_queries__{__queries__="{\"Concat\":[{\"Expr\":\"count_over_time({app=\\\"foo\\\"}[1m] offset 2m)\"},{\"Expr\":\"count_over_time({app=\\\"foo\\\"}[1m59s999ms])\"}]}"}))`,
			expectedSplitQueries: 4,
		},
		// Should support expressions with offset operator
		{
			in:                   `sum_over_time({app="foo"}[4m] offset 1m)`,
			out:                  `sum without() (__embedded_queries__{__queries__="{\"Concat\":[{\"Expr\":\"sum_over_time({app=\\\"foo\\\"}[2m] offset 3m)\"},{\"Expr\":\"sum_over_time({app=\\\"foo\\\"}[1m59s999ms] offset 1m)\"}]}"})`,
			expectedSplitQueries: 2,
		},
		{
			in:                   `count_over_time({app="foo"}[3m] offset 1m)`,
			out:                  `sum without() (__embedded_queries__{__queries__="{\"Concat\":[{\"Expr\":\"count_over_time({app=\\\"foo\\\"}[1m] offset 3m)\"},{\"Expr\":\"count_over_time({app=\\\"foo\\\"}[1m59s999ms] offset 1m)\"}]}"})`,
			expectedSplitQueries: 2,
		},
	} {
		tt := tt

		t.Run(tt.in, func(t *testing.T) {
			stats := NewInstantSplitterStats()
			mapper := NewInstantQuerySplitter(context.Background(), splitInterval, log.NewNopLogger(), stats)

			expr, err := parser.ParseExpr(tt.in)
			require.NoError(t, err)
			out, err := parser.ParseExpr(tt.out)
			require.NoError(t, err)

			mapped, err := mapper.Map(expr)
			require.NoError(t, err)
			require.Equal(t, out.String(), mapped.String())

			assert.Equal(t, tt.expectedSplitQueries, stats.GetSplitQueries())
			assert.Equal(t, noneSkippedReason, stats.GetSkippedReason())
		})
	}
}

func TestInstantSplitterSkippedQueryReason(t *testing.T) {
	splitInterval := 1 * time.Minute

	for _, tt := range []struct {
		query         string
		skippedReason SkippedReason
	}{
		// should be noop if range vector aggregator is not splittable
		{
			query:         `absent_over_time({app="foo"}[3m])`,
			skippedReason: SkippedReasonNonSplittable,
		},
		{
			query:         `changes({app="foo"}[3m])`,
			skippedReason: SkippedReasonNonSplittable,
		},
		{
			query:         `delta({app="foo"}[3m])`,
			skippedReason: SkippedReasonNonSplittable,
		},
		{
			query:         `deriv({app="foo"}[3m])`,
			skippedReason: SkippedReasonNonSplittable,
		},
		// holt_winters is a backwards compatible, non-experimental, alias for double_exponential_smoothing.
		{
			query:         `holt_winters({app="foo"}[3m], 1, 10)`,
			skippedReason: SkippedReasonNonSplittable,
		},
		{
			query:         `idelta({app="foo"}[3m])`,
			skippedReason: SkippedReasonNonSplittable,
		},
		{
			query:         `irate({app="foo"}[3m])`,
			skippedReason: SkippedReasonNonSplittable,
		},
		{
			query:         `last_over_time({app="foo"}[3m])`,
			skippedReason: SkippedReasonNonSplittable,
		},
		{
			query:         `predict_linear({app="foo"}[3m], 1)`,
			skippedReason: SkippedReasonNonSplittable,
		},
		{
			query:         `quantile_over_time(0.95, foo[3m])`,
			skippedReason: SkippedReasonNonSplittable,
		},
		{
			query:         `resets(foo[3m])`,
			skippedReason: SkippedReasonNonSplittable,
		},
		{
			query:         `stddev_over_time(foo[3m])`,
			skippedReason: SkippedReasonNonSplittable,
		},
		{
			query:         `stdvar_over_time(foo[3m])`,
			skippedReason: SkippedReasonNonSplittable,
		},
		{
			query:         `time()`,
			skippedReason: SkippedReasonNonSplittable,
		},
		{
			query:         `vector(10)`,
			skippedReason: SkippedReasonNonSplittable,
		},
		// should be noop if expression is not splittable
		{
			query:         `topk(10, histogram_quantile(0.9, delta({app="foo"}[3m])))`,
			skippedReason: SkippedReasonNonSplittable,
		},
		// should be noop if range interval is lower or equal to split interval (1m)
		{
			query:         `rate({app="foo"}[1m])`,
			skippedReason: SkippedReasonSmallInterval,
		},
		// should be noop if expression is a number literal
		{
			query:         `5`,
			skippedReason: SkippedReasonNonSplittable,
		},
		// should be noop if binary expression's operands are both constant scalars
		{
			query:         `20 / 10`,
			skippedReason: SkippedReasonNonSplittable,
		},
		{
			query:         `(20 / 10)`,
			skippedReason: SkippedReasonNonSplittable,
		},
		{
			query:         `(20) / (10)`,
			skippedReason: SkippedReasonNonSplittable,
		},
		{
			query:         `time() != bool 0`,
			skippedReason: SkippedReasonNonSplittable,
		},
		// should be noop if binary operation is not mapped
		//   - first operand `rate(metric_counter[1m])` has a smaller range interval than the configured splitting
		//   - second operand `rate(metric_counter[5h:5m])` is a subquery
		{
			query:         `rate({app="foo"}[1m]) / rate({app="bar"}[5h:5m]) > 0.5`,
			skippedReason: SkippedReasonSmallInterval,
		},
		// should be noop if inner binary operation is not mapped
		{
			query:         `sum(rate({app="foo"}[1h:5m]) * 60) by (bar)`,
			skippedReason: SkippedReasonSubquery,
		},
		// should be noop if subquery
		{
			query:         `sum_over_time(metric_counter[1h:5m])`,
			skippedReason: SkippedReasonSubquery,
		},
		{
			query:         `sum(rate(metric_counter[30m:5s]))`,
			skippedReason: SkippedReasonSubquery,
		},
		{
			// Parenthesis expression between sum_over_time() and the subquery.
			query:         `sum_over_time((metric_counter[30m:5s]))`,
			skippedReason: SkippedReasonSubquery,
		},
		{
			// Multiple parenthesis expressions between sum_over_time() and the subquery.
			query:         `sum_over_time((((metric_counter[30m:5s]))))`,
			skippedReason: SkippedReasonSubquery,
		},
		{
			query:         `quantile_over_time(1, metric_counter[10m:1m])`,
			skippedReason: SkippedReasonSubquery,
		},
		{
			query:         `sum(avg_over_time(metric_counter[1h:5m])) by (bar)`,
			skippedReason: SkippedReasonSubquery,
		},
		{
			query:         `min_over_time(sum by(group_1) (rate(metric_counter[5m]))[10m:2m])`,
			skippedReason: SkippedReasonSubquery,
		},
		{
			query:         `max_over_time(stddev_over_time(deriv(rate(metric_counter[10m])[5m:1m])[2m:])[10m:])`,
			skippedReason: SkippedReasonSubquery,
		},
		{
			query:         `rate(sum by(group_1) (rate(metric_counter[5m]))[10m:])`,
			skippedReason: SkippedReasonSubquery,
		},
		{
			query:         `absent_over_time(rate(metric_counter[5m])[10m:])`,
			skippedReason: SkippedReasonSubquery,
		},
		{
			query:         `max_over_time(stddev_over_time(deriv(sort(metric_counter)[5m:1m])[2m:])[10m:])`,
			skippedReason: SkippedReasonSubquery,
		},
		{
			query:         `max_over_time(absent_over_time(deriv(rate(metric_counter[1m])[5m:1m])[2m:])[10m:])`,
			skippedReason: SkippedReasonSubquery,
		},
		{
			query:         `sum by(group_1) (sum_over_time(metric_counter[7d:] @ start()))`,
			skippedReason: SkippedReasonSubquery,
		},
	} {
		tt := tt

		t.Run(tt.query, func(t *testing.T) {
			stats := NewInstantSplitterStats()
			mapper := NewInstantQuerySplitter(context.Background(), splitInterval, log.NewNopLogger(), stats)

			expr, err := parser.ParseExpr(tt.query)
			require.NoError(t, err)

			// Do not assert if the mapped expression is equal to the input one, because it could actually be slightly
			// transformed (e.g. added parenthesis). The actual way to check if it was split or not is to read it from
			// the statistics.
			_, err = mapper.Map(expr)
			require.NoError(t, err)

			assert.Equal(t, 0, stats.GetSplitQueries())
			assert.Equal(t, tt.skippedReason, stats.GetSkippedReason())
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

func TestUpdateRangeInterval(t *testing.T) {
	tests := []struct {
		expr         string
		interval     time.Duration
		expectedExpr string
		expectedErr  string
	}{
		{
			expr:        `time()`,
			interval:    time.Hour,
			expectedErr: "no matrix selector has been found",
		}, {
			expr:         `sum(rate(metric[1m]))`,
			interval:     time.Hour,
			expectedExpr: `sum(rate(metric[1h]))`,
		}, {
			expr:         `sum(label_replace(rate(metric[1m]), "dst", "$1", "src", ".*"))`,
			interval:     time.Hour,
			expectedExpr: `sum(label_replace(rate(metric[1h]), "dst", "$1", "src", ".*"))`,
		}, {
			expr:        `sum(rate(metric[1m])) + sum(rate(metric[5m]))`,
			interval:    time.Hour,
			expectedErr: "multiple matrix selectors have been found",
		}, {
			expr:        `sum(rate(metric[1m]))`,
			interval:    -time.Minute,
			expectedErr: "negative interval",
		},
	}

	for _, testData := range tests {
		t.Run(testData.expr, func(t *testing.T) {
			expr, err := parser.ParseExpr(testData.expr)
			require.NoError(t, err)

			actualErr := updateRangeInterval(expr, testData.interval)
			if testData.expectedErr != "" {
				require.Error(t, actualErr)
				assert.Contains(t, actualErr.Error(), testData.expectedErr)
			} else {
				assert.Equal(t, testData.expectedExpr, expr.String())
			}
		})
	}
}

func TestUpdateOffset(t *testing.T) {
	tests := []struct {
		expr         string
		offset       time.Duration
		expectedExpr string
		expectedErr  string
	}{
		{
			expr:        `time()`,
			offset:      time.Hour,
			expectedErr: "no vector selector has been found",
		}, {
			expr:         `sum(rate(metric[1m]))`,
			offset:       time.Hour,
			expectedExpr: `sum(rate(metric[1m] offset 1h))`,
		}, {
			expr:         `sum(rate(metric[1m]))`,
			offset:       -time.Hour,
			expectedExpr: `sum(rate(metric[1m] offset -1h))`,
		}, {
			expr:         `sum(label_replace(rate(metric[1m]), "dst", "$1", "src", ".*"))`,
			offset:       time.Hour,
			expectedExpr: `sum(label_replace(rate(metric[1m] offset 1h), "dst", "$1", "src", ".*"))`,
		}, {
			expr:         `sum(label_replace(rate(metric[1m]), "dst", "$1", "src", ".*"))`,
			offset:       -time.Hour,
			expectedExpr: `sum(label_replace(rate(metric[1m] offset -1h), "dst", "$1", "src", ".*"))`,
		}, {
			expr:        `sum(rate(metric[1m])) + sum(rate(metric[5m]))`,
			offset:      time.Hour,
			expectedErr: "multiple vector selectors have been found",
		},
	}

	for _, testData := range tests {
		t.Run(testData.expr, func(t *testing.T) {
			expr, err := parser.ParseExpr(testData.expr)
			require.NoError(t, err)

			actualErr := updateOffset(expr, testData.offset)
			if testData.expectedErr != "" {
				require.Error(t, actualErr)
				assert.Contains(t, actualErr.Error(), testData.expectedErr)
			} else {
				assert.Equal(t, testData.expectedExpr, expr.String())
			}
		})
	}
}

func TestGetOffsets(t *testing.T) {
	tests := []struct {
		query    string
		expected []time.Duration
	}{
		{
			query:    `time()`,
			expected: []time.Duration{},
		},
		{
			query:    `sum(rate(metric[5m] offset 1m))`,
			expected: []time.Duration{time.Minute},
		},
		{
			query:    `sum(rate(metric[5m] offset 3m)) + sum(rate(metric[5m] offset 5m))`,
			expected: []time.Duration{3 * time.Minute, 5 * time.Minute},
		},
		{
			query:    `rate(metric[5m] offset 5s)`,
			expected: []time.Duration{5 * time.Second},
		},
		{
			query:    `avg_over_time(metric[5m] offset -5m)`,
			expected: []time.Duration{-5 * time.Minute},
		},
		{
			query:    `sum_over_time(rate(metric[5m] offset 3s)[1h:5m] offset 1m)`,
			expected: []time.Duration{time.Minute, 3 * time.Second},
		},
	}

	for _, testData := range tests {
		t.Run(testData.query, func(t *testing.T) {
			expr, err := parser.ParseExpr(testData.query)
			require.NoError(t, err)
			assert.Equal(t, testData.expected, getOffsets(expr))
		})
	}
}

func concatOffsets(splitInterval time.Duration, offsets int, overlapping bool, queryTemplate string) string {
	queries := make([]string, offsets)
	offsetIndex := offsets
	for offset := range queries {
		offsetIndex--
		offsetSplitInterval := splitInterval
		if offset > 0 && !overlapping {
			offsetSplitInterval -= time.Millisecond
		}

		offsetQuery := fmt.Sprintf("[%s]%s", model.Duration(offsetSplitInterval), getSplitOffset(splitInterval, offsetIndex))
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
