// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/queryable_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queryrange

import (
	"context"
	"sync"
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/astmapper"
)

func TestShardedQuerier_Select(t *testing.T) {
	var testExpr = []struct {
		name    string
		querier *ShardedQuerier
		fn      func(*testing.T, *ShardedQuerier)
	}{
		{
			name: "errors non embedded query",
			querier: mkShardedQuerier(
				nil,
			),
			fn: func(t *testing.T, q *ShardedQuerier) {
				set := q.Select(false, nil)
				require.Equal(t, set.Err(), errNoEmbeddedQueries)
			},
		},
		{
			name: "replaces query",
			querier: mkShardedQuerier(mockHandlerWith(
				&PrometheusResponse{},
				nil,
			)),
			fn: func(t *testing.T, q *ShardedQuerier) {

				expected := &PrometheusResponse{
					Status: "success",
					Data: PrometheusData{
						ResultType: string(parser.ValueTypeVector),
					},
				}

				// override handler func to assert new query has been substituted
				q.handler = HandlerFunc(
					func(ctx context.Context, req Request) (Response, error) {
						require.Equal(t, `http_requests_total{cluster="prod"}`, req.GetQuery())
						return expected, nil
					},
				)

				encoded, err := astmapper.JSONCodec.Encode([]string{`http_requests_total{cluster="prod"}`})
				require.Nil(t, err)
				set := q.Select(
					false,
					nil,
					labels.MustNewMatcher(labels.MatchEqual, "__name__", astmapper.EmbeddedQueriesMetricName),
					labels.MustNewMatcher(labels.MatchEqual, astmapper.QueryLabel, encoded),
				)
				require.Nil(t, set.Err())
			},
		},
		{
			name: "propagates response error",
			querier: mkShardedQuerier(mockHandlerWith(
				&PrometheusResponse{
					Error: "SomeErr",
				},
				nil,
			)),
			fn: func(t *testing.T, q *ShardedQuerier) {
				encoded, err := astmapper.JSONCodec.Encode([]string{`http_requests_total{cluster="prod"}`})
				require.Nil(t, err)
				set := q.Select(
					false,
					nil,
					labels.MustNewMatcher(labels.MatchEqual, "__name__", astmapper.EmbeddedQueriesMetricName),
					labels.MustNewMatcher(labels.MatchEqual, astmapper.QueryLabel, encoded),
				)
				require.EqualError(t, set.Err(), "SomeErr")
			},
		},
		{
			name: "returns SeriesSet",
			querier: mkShardedQuerier(mockHandlerWith(
				&PrometheusResponse{
					Data: PrometheusData{
						ResultType: string(parser.ValueTypeVector),
						Result: []SampleStream{
							{
								Labels: []mimirpb.LabelAdapter{
									{Name: "a", Value: "a1"},
									{Name: "b", Value: "b1"},
								},
								Samples: []mimirpb.Sample{
									{
										Value:       1,
										TimestampMs: 1,
									},
									{
										Value:       2,
										TimestampMs: 2,
									},
								},
							},
							{
								Labels: []mimirpb.LabelAdapter{
									{Name: "a", Value: "a1"},
									{Name: "b", Value: "b1"},
								},
								Samples: []mimirpb.Sample{
									{
										Value:       8,
										TimestampMs: 1,
									},
									{
										Value:       9,
										TimestampMs: 2,
									},
								},
							},
						},
					},
				},
				nil,
			)),
			fn: func(t *testing.T, q *ShardedQuerier) {
				encoded, err := astmapper.JSONCodec.Encode([]string{`http_requests_total{cluster="prod"}`})
				require.Nil(t, err)
				set := q.Select(
					false,
					nil,
					labels.MustNewMatcher(labels.MatchEqual, "__name__", astmapper.EmbeddedQueriesMetricName),
					labels.MustNewMatcher(labels.MatchEqual, astmapper.QueryLabel, encoded),
				)
				require.Nil(t, set.Err())
				require.Equal(
					t,
					NewSeriesSet([]SampleStream{
						{
							Labels: []mimirpb.LabelAdapter{
								{Name: "a", Value: "a1"},
								{Name: "b", Value: "b1"},
							},
							Samples: []mimirpb.Sample{
								{
									Value:       1,
									TimestampMs: 1,
								},
								{
									Value:       2,
									TimestampMs: 2,
								},
							},
						},
						{
							Labels: []mimirpb.LabelAdapter{
								{Name: "a", Value: "a1"},
								{Name: "b", Value: "b1"},
							},
							Samples: []mimirpb.Sample{
								{
									Value:       8,
									TimestampMs: 1,
								},
								{
									Value:       9,
									TimestampMs: 2,
								},
							},
						},
					}),
					set,
				)
			},
		},
	}

	for _, c := range testExpr {
		t.Run(c.name, func(t *testing.T) {
			c.fn(t, c.querier)
		})
	}
}

func TestShardedQuerier_Select_ShouldConcurrentlyRunEmbeddedQueries(t *testing.T) {
	embeddedQueries := []string{
		`sum(rate(metric{__query_shard__="0_of_3"}[1m]))`,
		`sum(rate(metric{__query_shard__="1_of_3"}[1m]))`,
		`sum(rate(metric{__query_shard__="2_of_3"}[1m]))`,
	}

	// Mock the downstream handler to wait until all concurrent queries have been
	// received. If the test succeeds we have the guarantee they were called concurrently
	// otherwise the test times out while hanging in the downstream handler.
	downstreamWg := sync.WaitGroup{}
	downstreamWg.Add(len(embeddedQueries))

	querier := mkShardedQuerier(HandlerFunc(func(ctx context.Context, req Request) (Response, error) {
		// Wait until the downstream handler has been concurrently called for each embedded query.
		downstreamWg.Done()
		downstreamWg.Wait()

		return &PrometheusResponse{
			Data: PrometheusData{
				ResultType: string(parser.ValueTypeVector),
				Result: []SampleStream{{
					Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "1"}},
					Samples: []mimirpb.Sample{{Value: 1, TimestampMs: 1}},
				}},
			},
		}, nil
	}))

	encodedQueries, err := astmapper.JSONCodec.Encode(embeddedQueries)
	require.Nil(t, err)

	seriesSet := querier.Select(
		false,
		nil,
		labels.MustNewMatcher(labels.MatchEqual, "__name__", astmapper.EmbeddedQueriesMetricName),
		labels.MustNewMatcher(labels.MatchEqual, astmapper.QueryLabel, encodedQueries),
	)

	require.NoError(t, seriesSet.Err())

	// We expect 1 resulting series for each embedded query.
	var actualSeries int
	for seriesSet.Next() {
		actualSeries++
	}
	assert.NoError(t, seriesSet.Err())
	require.Equal(t, len(embeddedQueries), actualSeries)
}

func mkShardedQuerier(handler Handler) *ShardedQuerier {
	return &ShardedQuerier{ctx: context.Background(), req: &PrometheusRequest{}, handler: handler, responseHeaders: map[string][]string{}}
}
