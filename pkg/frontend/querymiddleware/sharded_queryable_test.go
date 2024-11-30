// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/queryable_test.go
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/value_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querymiddleware

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestShardedQuerier_Select(t *testing.T) {
	ctx := context.Background()
	var testExpr = []struct {
		name    string
		querier *shardedQuerier
		fn      func(*testing.T, *shardedQuerier)
	}{
		{
			name: "errors non embedded query",
			querier: mkShardedQuerier(
				nil,
			),
			fn: func(t *testing.T, q *shardedQuerier) {
				set := q.Select(ctx, false, nil)
				require.Equal(t, set.Err(), errNoEmbeddedQueries)
			},
		},
		{
			name: "replaces query",
			querier: mkShardedQuerier(mockHandlerWith(
				&PrometheusResponse{},
				nil,
			)),
			fn: func(t *testing.T, q *shardedQuerier) {

				expected := &PrometheusResponse{
					Status: "success",
					Data: &PrometheusData{
						ResultType: string(parser.ValueTypeVector),
					},
				}

				// override handler func to assert new query has been substituted
				q.handler = HandlerFunc(
					func(_ context.Context, req MetricsQueryRequest) (Response, error) {
						require.Equal(t, `http_requests_total{cluster="prod"}`, req.GetQuery())
						return expected, nil
					},
				)

				encoded, err := astmapper.JSONCodec.Encode([]astmapper.EmbeddedQuery{astmapper.NewEmbeddedQuery(`http_requests_total{cluster="prod"}`, nil)})
				require.Nil(t, err)
				set := q.Select(
					ctx,
					false,
					nil,
					labels.MustNewMatcher(labels.MatchEqual, "__name__", astmapper.EmbeddedQueriesMetricName),
					labels.MustNewMatcher(labels.MatchEqual, astmapper.EmbeddedQueriesLabelName, encoded),
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
			fn: func(t *testing.T, q *shardedQuerier) {
				encoded, err := astmapper.JSONCodec.Encode([]astmapper.EmbeddedQuery{astmapper.NewEmbeddedQuery(`http_requests_total{cluster="prod"}`, nil)})
				require.Nil(t, err)
				set := q.Select(
					ctx,
					false,
					nil,
					labels.MustNewMatcher(labels.MatchEqual, "__name__", astmapper.EmbeddedQueriesMetricName),
					labels.MustNewMatcher(labels.MatchEqual, astmapper.EmbeddedQueriesLabelName, encoded),
				)
				require.EqualError(t, set.Err(), "SomeErr")
			},
		},
		{
			name: "returns SeriesSet",
			querier: mkShardedQuerier(mockHandlerWith(
				&PrometheusResponse{
					Data: &PrometheusData{
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
			fn: func(t *testing.T, q *shardedQuerier) {
				encoded, err := astmapper.JSONCodec.Encode([]astmapper.EmbeddedQuery{astmapper.NewEmbeddedQuery(`http_requests_total{cluster="prod"}`, nil)})
				require.Nil(t, err)
				set := q.Select(
					ctx,
					false,
					nil,
					labels.MustNewMatcher(labels.MatchEqual, "__name__", astmapper.EmbeddedQueriesMetricName),
					labels.MustNewMatcher(labels.MatchEqual, astmapper.EmbeddedQueriesLabelName, encoded),
				)
				require.Nil(t, set.Err())

				expected := []SampleStream{
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
				}

				actual, err := seriesSetToSampleStreams(set)
				require.NoError(t, err)
				assertEqualSampleStream(t, expected, actual)
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
	embeddedQueriesRaw := []string{
		`sum(rate(metric{__query_shard__="0_of_3"}[1m]))`,
		`sum(rate(metric{__query_shard__="1_of_3"}[1m]))`,
		`sum(rate(metric{__query_shard__="2_of_3"}[1m]))`,
	}

	embeddedQueries := make([]astmapper.EmbeddedQuery, len(embeddedQueriesRaw))
	for i, query := range embeddedQueriesRaw {
		embeddedQueries[i] = astmapper.NewEmbeddedQuery(query, nil)
	}

	ctx := context.Background()

	// Mock the downstream handler to wait until all concurrent queries have been
	// received. If the test succeeds we have the guarantee they were called concurrently
	// otherwise the test times out while hanging in the downstream handler.
	downstreamWg := sync.WaitGroup{}
	downstreamWg.Add(len(embeddedQueries))

	querier := mkShardedQuerier(HandlerFunc(func(context.Context, MetricsQueryRequest) (Response, error) {
		// Wait until the downstream handler has been concurrently called for each embedded query.
		downstreamWg.Done()
		downstreamWg.Wait()

		return &PrometheusResponse{
			Data: &PrometheusData{
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
		ctx,
		false,
		nil,
		labels.MustNewMatcher(labels.MatchEqual, "__name__", astmapper.EmbeddedQueriesMetricName),
		labels.MustNewMatcher(labels.MatchEqual, astmapper.EmbeddedQueriesLabelName, encodedQueries),
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

func TestShardedQueryable_GetResponseHeaders(t *testing.T) {
	queryable := NewShardedQueryable(&PrometheusRangeQueryRequest{}, nil, nil, nil, nil, nil)
	assert.Empty(t, queryable.getResponseHeaders())

	// Merge some response headers from the 1st querier.
	querier, err := queryable.Querier(math.MinInt64, math.MaxInt64)
	require.NoError(t, err)

	querier.(*shardedQuerier).responseHeaders.mergeHeaders([]*PrometheusHeader{
		{Name: "content-type", Values: []string{"application/json"}},
		{Name: "cache-control", Values: []string{"no-cache"}},
	})
	assert.ElementsMatch(t, []*PrometheusHeader{
		{Name: "content-type", Values: []string{"application/json"}},
		{Name: "cache-control", Values: []string{"no-cache"}},
	}, queryable.getResponseHeaders())

	// Merge some response headers from the 2nd querier.
	querier, err = queryable.Querier(math.MinInt64, math.MaxInt64)
	require.NoError(t, err)

	querier.(*shardedQuerier).responseHeaders.mergeHeaders([]*PrometheusHeader{
		{Name: "content-type", Values: []string{"application/json"}},
		{Name: "cache-control", Values: []string{"no-store"}},
	})
	assert.ElementsMatch(t, []*PrometheusHeader{
		{Name: "content-type", Values: []string{"application/json"}},
		{Name: "cache-control", Values: []string{"no-cache", "no-store"}},
	}, queryable.getResponseHeaders())
}

func mkShardedQuerier(handler MetricsQueryHandler) *shardedQuerier {
	return &shardedQuerier{req: &PrometheusRangeQueryRequest{}, handler: handler, responseHeaders: newResponseHeadersTracker(), handleEmbeddedQuery: defaultHandleEmbeddedQueryFunc}
}

func TestNewSeriesSetFromEmbeddedQueriesResults(t *testing.T) {
	tests := map[string]struct {
		input    []SampleStream
		hints    *storage.SelectHints
		expected []SampleStream
	}{
		"should add a stale marker at the end even if if input samples have no gaps": {
			input: []SampleStream{{
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "1"}},
				Samples: []mimirpb.Sample{{TimestampMs: 10, Value: 1}, {TimestampMs: 20, Value: 2}, {TimestampMs: 30, Value: 3}},
			}, {
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "b"}},
				Samples: []mimirpb.Sample{{TimestampMs: 20, Value: 2}, {TimestampMs: 30, Value: 3}},
			}},
			hints: &storage.SelectHints{Step: 10},
			expected: []SampleStream{{
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "1"}},
				Samples: []mimirpb.Sample{{TimestampMs: 10, Value: 1}, {TimestampMs: 20, Value: 2}, {TimestampMs: 30, Value: 3}, {TimestampMs: 40, Value: math.Float64frombits(value.StaleNaN)}},
			}, {
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "b"}},
				Samples: []mimirpb.Sample{{TimestampMs: 20, Value: 2}, {TimestampMs: 30, Value: 3}, {TimestampMs: 40, Value: math.Float64frombits(value.StaleNaN)}},
			}},
		},
		"should add stale markers at the beginning of each gap and one at the end of the series": {
			input: []SampleStream{{
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "1"}},
				Samples: []mimirpb.Sample{{TimestampMs: 10, Value: 1}, {TimestampMs: 40, Value: 4}, {TimestampMs: 90, Value: 9}},
			}, {
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "b"}},
				Samples: []mimirpb.Sample{{TimestampMs: 20, Value: 2}, {TimestampMs: 30, Value: 3}},
			}},
			hints: &storage.SelectHints{Step: 10},
			expected: []SampleStream{{
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "1"}},
				Samples: []mimirpb.Sample{{TimestampMs: 10, Value: 1}, {TimestampMs: 20, Value: math.Float64frombits(value.StaleNaN)}, {TimestampMs: 40, Value: 4}, {TimestampMs: 50, Value: math.Float64frombits(value.StaleNaN)}, {TimestampMs: 90, Value: 9}, {TimestampMs: 100, Value: math.Float64frombits(value.StaleNaN)}},
			}, {
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "b"}},
				Samples: []mimirpb.Sample{{TimestampMs: 20, Value: 2}, {TimestampMs: 30, Value: 3}, {TimestampMs: 40, Value: math.Float64frombits(value.StaleNaN)}},
			}},
		},
		"should not add stale markers even if points have gaps if hints is not passed": {
			input: []SampleStream{{
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "1"}},
				Samples: []mimirpb.Sample{{TimestampMs: 10, Value: 1}, {TimestampMs: 40, Value: 4}, {TimestampMs: 90, Value: 9}},
			}, {
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "b"}},
				Samples: []mimirpb.Sample{{TimestampMs: 20, Value: 2}, {TimestampMs: 30, Value: 3}},
			}},
			hints: nil,
			expected: []SampleStream{{
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "1"}},
				Samples: []mimirpb.Sample{{TimestampMs: 10, Value: 1}, {TimestampMs: 40, Value: 4}, {TimestampMs: 90, Value: 9}},
			}, {
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "b"}},
				Samples: []mimirpb.Sample{{TimestampMs: 20, Value: 2}, {TimestampMs: 30, Value: 3}},
			}},
		},
		"should not add stale markers even if points have gaps if step == 0": {
			input: []SampleStream{{
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "1"}},
				Samples: []mimirpb.Sample{{TimestampMs: 10, Value: 1}, {TimestampMs: 40, Value: 4}, {TimestampMs: 90, Value: 9}},
			}, {
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "b"}},
				Samples: []mimirpb.Sample{{TimestampMs: 20, Value: 2}, {TimestampMs: 30, Value: 3}},
			}},
			hints: &storage.SelectHints{Step: 0},
			expected: []SampleStream{{
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "1"}},
				Samples: []mimirpb.Sample{{TimestampMs: 10, Value: 1}, {TimestampMs: 40, Value: 4}, {TimestampMs: 90, Value: 9}},
			}, {
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "b"}},
				Samples: []mimirpb.Sample{{TimestampMs: 20, Value: 2}, {TimestampMs: 30, Value: 3}},
			}},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			set := newSeriesSetFromEmbeddedQueriesResults([][]SampleStream{testData.input}, testData.hints)
			actual, err := seriesSetToSampleStreams(set)
			require.NoError(t, err)
			assertEqualSampleStream(t, testData.expected, actual)
		})
	}
}

func TestResponseToSamples(t *testing.T) {
	input := &PrometheusResponse{
		Data: &PrometheusData{
			ResultType: string(parser.ValueTypeMatrix),
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
	}

	streams, err := ResponseToSamples(input)
	require.NoError(t, err)
	assertEqualSampleStream(t, input.Data.Result, streams)
}

func assertEqualSampleStream(t *testing.T, expected, actual []SampleStream) {
	// Expect the same length.
	require.Equal(t, len(expected), len(actual))

	for idx, expectedStream := range expected {
		actualStream := actual[idx]

		// Expect the same labels.
		require.Equal(t, expectedStream.Labels, actualStream.Labels)

		// Expect the same samples (in this comparison, NaN == NaN and StaleNaN == StaleNaN).
		require.Equal(t, len(expectedStream.Samples), len(actualStream.Samples))

		for idx, expectedSample := range expectedStream.Samples {
			actualSample := actualStream.Samples[idx]
			require.Equal(t, expectedSample.TimestampMs, actualSample.TimestampMs)

			if value.IsStaleNaN(expectedSample.Value) {
				assert.True(t, value.IsStaleNaN(actualSample.Value))
			} else if math.IsNaN(expectedSample.Value) {
				assert.True(t, math.IsNaN(actualSample.Value))
			} else {
				assert.Equal(t, expectedSample.Value, actualSample.Value)
			}
		}
	}
}

// seriesSetToSampleStreams iterate through the input storage.SeriesSet and returns it as a []SampleStream.
func seriesSetToSampleStreams(set storage.SeriesSet) ([]SampleStream, error) {
	var out []SampleStream

	var it chunkenc.Iterator
	for set.Next() {
		stream := SampleStream{Labels: mimirpb.FromLabelsToLabelAdapters(set.At().Labels())}

		it = set.At().Iterator(it)
		for valType := it.Next(); valType != chunkenc.ValNone; valType = it.Next() {
			switch valType {
			case chunkenc.ValFloat:
				t, v := it.At()
				stream.Samples = append(stream.Samples, mimirpb.Sample{
					Value:       v,
					TimestampMs: t,
				})
			case chunkenc.ValHistogram:
				t, v := it.AtHistogram(nil)
				stream.Histograms = append(stream.Histograms, mimirpb.FloatHistogramPair{
					Histogram:   mimirpb.FloatHistogramFromPrometheusModel(v.ToFloat(nil)),
					TimestampMs: t,
				})
			case chunkenc.ValFloatHistogram:
				t, v := it.AtFloatHistogram(nil)
				stream.Histograms = append(stream.Histograms, mimirpb.FloatHistogramPair{
					Histogram:   mimirpb.FloatHistogramFromPrometheusModel(v),
					TimestampMs: t,
				})
			default:
				panic(fmt.Errorf("Unexpected value type: %x", valType))
			}
		}

		if it.Err() != nil {
			return nil, it.Err()
		}

		out = append(out, stream)
	}

	if set.Err() != nil {
		return nil, set.Err()
	}

	return out, nil
}
