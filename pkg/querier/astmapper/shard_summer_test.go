// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/astmapper/shard_summer_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package astmapper

import (
	"fmt"
	"testing"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
)

// orSquasher is a custom squasher which mimics the intuitive but less efficient OR'ing of sharded vectors.
// It's helpful for tests because of its intuitive & human readable output.
func orSquasher(nodes ...parser.Node) (parser.Expr, error) {
	combined := nodes[0]
	for i := 1; i < len(nodes); i++ {
		combined = &parser.BinaryExpr{
			Op:  parser.LOR,
			LHS: combined.(parser.Expr),
			RHS: nodes[i].(parser.Expr),
		}
	}
	return combined.(parser.Expr), nil
}

func TestShardSummer(t *testing.T) {
	testExpr := []struct {
		shards   int
		input    string
		expected string
	}{
		{
			shards: 3,
			input:  `sum(rate(bar1{baz="blip"}[1m]))`,
			expected: `sum without(__query_shard__) (
			  sum by(__query_shard__) (rate(bar1{__query_shard__="0_of_3",baz="blip"}[1m])) or
			  sum by(__query_shard__) (rate(bar1{__query_shard__="1_of_3",baz="blip"}[1m])) or
			  sum by(__query_shard__) (rate(bar1{__query_shard__="2_of_3",baz="blip"}[1m]))
			)`,
		},
		{
			shards: 3,
			input:  `sum by(foo) (rate(bar1{baz="blip"}[1m]))`,
			expected: `sum by(foo) (
			  sum by(foo, __query_shard__) (rate(bar1{__query_shard__="0_of_3",baz="blip"}[1m])) or
			  sum by(foo, __query_shard__) (rate(bar1{__query_shard__="1_of_3",baz="blip"}[1m])) or
			  sum by(foo, __query_shard__) (rate(bar1{__query_shard__="2_of_3",baz="blip"}[1m]))
			)`,
		},
		{
			shards: 2,
			input: `sum(
				sum by (foo) (rate(bar1{baz="blip"}[1m]))
				/
				sum by (foo) (rate(foo{baz="blip"}[1m]))
			)`,
			expected: `sum(
			  sum by(foo) (
				sum by(foo, __query_shard__) (rate(bar1{__query_shard__="0_of_2",baz="blip"}[1m])) or
				sum by(foo, __query_shard__) (rate(bar1{__query_shard__="1_of_2",baz="blip"}[1m]))
			  )
			  /
			  sum by(foo) (
				sum by(foo, __query_shard__) (rate(foo{__query_shard__="0_of_2",baz="blip"}[1m])) or
				sum by(foo, __query_shard__) (rate(foo{__query_shard__="1_of_2",baz="blip"}[1m]))
			  )
			)`,
		},
		// This nested sum example is nonsensical, but should not try to shard nested aggregations.
		// Instead it only maps the subAggregation but not the outer one.
		{
			shards: 2,
			input:  `sum(sum by(foo) (rate(bar1{baz="blip"}[1m])))`,
			expected: `sum(
			  sum by(foo) (
			    sum by(foo, __query_shard__) (rate(bar1{__query_shard__="0_of_2",baz="blip"}[1m])) or
			    sum by(foo, __query_shard__) (rate(bar1{__query_shard__="1_of_2",baz="blip"}[1m]))
			  )
			)`,
		},
		// without
		{
			shards: 2,
			input:  `sum without(foo) (rate(bar1{baz="blip"}[1m]))`,
			expected: `sum without(__query_shard__) (
			  sum without(foo) (rate(bar1{__query_shard__="0_of_2",baz="blip"}[1m])) or
			  sum without(foo) (rate(bar1{__query_shard__="1_of_2",baz="blip"}[1m]))
			)`,
		},
		// multiple dimensions
		{
			shards: 2,
			input:  `sum by(foo, bom) (rate(bar1{baz="blip"}[1m]))`,
			expected: `sum by(foo, bom) (
			  sum by(foo, bom, __query_shard__) (rate(bar1{__query_shard__="0_of_2",baz="blip"}[1m])) or
			  sum by(foo, bom, __query_shard__) (rate(bar1{__query_shard__="1_of_2",baz="blip"}[1m]))
			)`,
		},
		// sharding histogram inputs
		{
			shards: 2,
			input:  `histogram_quantile(0.9, sum(rate(alertmanager_http_request_duration_seconds_bucket[10m])) by (job, le))`,
			expected: `histogram_quantile(
				    0.9,
				    sum by(job, le) (
				      sum by(job, le, __query_shard__) (rate(alertmanager_http_request_duration_seconds_bucket{__query_shard__="0_of_2"}[10m])) or
				      sum by(job, le, __query_shard__) (rate(alertmanager_http_request_duration_seconds_bucket{__query_shard__="1_of_2"}[10m]))
				    )
				  )`,
		},
		{
			// Disallow sharding nested aggregations as they may merge series in a non-associative manner.
			shards:   2,
			input:    `sum(count(foo{}))`,
			expected: `sum(count(foo{}))`,
		},
	}

	for i, c := range testExpr {
		t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
			summer, err := newShardSummer(c.shards, orSquasher, nil)
			require.Nil(t, err)
			expr, err := parser.ParseExpr(c.input)
			require.Nil(t, err)
			res, err := summer.Map(expr)
			require.Nil(t, err)

			expected, err := parser.ParseExpr(c.expected)
			require.Nil(t, err)

			require.Equal(t, expected.String(), res.String())
		})
	}
}

func TestShardSummerWithEncoding(t *testing.T) {
	for i, c := range []struct {
		shards   int
		input    string
		expected string
	}{
		{
			shards:   3,
			input:    `sum(rate(bar1{baz="blip"}[1m]))`,
			expected: `sum without(__query_shard__) (__embedded_queries__{__cortex_queries__="{\"Concat\":[\"sum by(__query_shard__) (rate(bar1{__query_shard__=\\\"0_of_3\\\",baz=\\\"blip\\\"}[1m]))\",\"sum by(__query_shard__) (rate(bar1{__query_shard__=\\\"1_of_3\\\",baz=\\\"blip\\\"}[1m]))\",\"sum by(__query_shard__) (rate(bar1{__query_shard__=\\\"2_of_3\\\",baz=\\\"blip\\\"}[1m]))\"]}"})`,
		},
	} {
		t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
			summer, err := newShardSummer(c.shards, vectorSquasher, nil)
			require.Nil(t, err)
			expr, err := parser.ParseExpr(c.input)
			require.Nil(t, err)
			res, err := summer.Map(expr)
			require.Nil(t, err)

			expected, err := parser.ParseExpr(c.expected)
			require.Nil(t, err)

			require.Equal(t, expected.String(), res.String())
		})
	}
}
