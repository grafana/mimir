// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/remote_read_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"bytes"
	"context"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	prom_remote "github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/series"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/test"
)

type mockSampleAndChunkQueryable struct {
	queryableFn      func(mint, maxt int64) (storage.Querier, error)
	chunkQueryableFn func(mint, maxt int64) (storage.ChunkQuerier, error)
}

func (m mockSampleAndChunkQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	return m.queryableFn(mint, maxt)
}

func (m mockSampleAndChunkQueryable) ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	return m.chunkQueryableFn(mint, maxt)
}

type mockQuerier struct {
	storage.Querier

	selectFn func(ctx context.Context, sorted bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet
}

func (m mockQuerier) Close() error {
	return nil
}

func (m mockQuerier) Select(ctx context.Context, sorted bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	if m.selectFn != nil {
		return m.selectFn(ctx, sorted, hints, matchers...)
	}

	return storage.ErrSeriesSet(errors.New("the Select() function has not been mocked in the test"))
}

type mockChunkQuerier struct {
	storage.ChunkQuerier

	selectFn func(ctx context.Context, sorted bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.ChunkSeriesSet
}

func (m mockChunkQuerier) Select(ctx context.Context, sorted bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.ChunkSeriesSet {
	if m.selectFn != nil {
		return m.selectFn(ctx, sorted, hints, matchers...)
	}

	return storage.ErrChunkSeriesSet(errors.New("the Select() function has not been mocked in the test"))
}

func (m mockChunkQuerier) Close() error {
	if m.ChunkQuerier != nil {
		return m.ChunkQuerier.Close()
	}
	return nil
}

type partiallyFailingSeriesSet struct {
	ss        storage.SeriesSet
	failAfter int
	err       error
}

func (p *partiallyFailingSeriesSet) Next() bool {
	if p.failAfter == 0 {
		return false
	}
	p.failAfter--
	return p.ss.Next()
}

func (p *partiallyFailingSeriesSet) At() storage.Series {
	return p.ss.At()
}

func (p *partiallyFailingSeriesSet) Err() error {
	if p.failAfter == 0 {
		return p.err
	}
	return p.ss.Err()
}

func (p *partiallyFailingSeriesSet) Warnings() annotations.Annotations {
	return p.ss.Warnings()
}

func TestRemoteReadHandler_Samples(t *testing.T) {
	type expectedResult struct {
		queryStartEnd
		timeseries []*prompb.TimeSeries
	}

	queries := map[string]struct {
		query           []*prompb.Query
		expectedResults []expectedResult
	}{
		"query without hints": {
			query: []*prompb.Query{
				{
					StartTimestampMs: 1,
					EndTimestampMs:   10,
				},
			},
			expectedResults: []expectedResult{
				{
					queryStartEnd: queryStartEnd{
						start: 1,
						end:   10,
					},
					timeseries: []*prompb.TimeSeries{
						{
							Labels: []prompb.Label{
								{Name: "foo", Value: "bar"},
							},
							Samples: []prompb.Sample{
								{Value: 1, Timestamp: 1},
								{Value: 2, Timestamp: 2},
								{Value: 3, Timestamp: 3},
							},
							Histograms: []prompb.Histogram{
								prompb.FromIntHistogram(4, test.GenerateTestHistogram(4)),
							},
						},
					},
				},
			},
		},
		"query with hints": {
			query: []*prompb.Query{
				{
					StartTimestampMs: 1,
					EndTimestampMs:   10,
					Hints: &prompb.ReadHints{
						StartMs: 2,
						EndMs:   3,
					},
				},
			},
			expectedResults: []expectedResult{
				{
					queryStartEnd: queryStartEnd{
						start: 2,
						end:   3,
					},
					timeseries: []*prompb.TimeSeries{
						{
							Labels: []prompb.Label{
								{Name: "foo", Value: "bar"},
							},
							Samples: []prompb.Sample{
								{Value: 2, Timestamp: 2},
								{Value: 3, Timestamp: 3},
							},
						},
					},
				},
			},
		},
		"multiple queries": {
			query: []*prompb.Query{
				{
					StartTimestampMs: 1,
					EndTimestampMs:   5,
					Matchers: []*prompb.LabelMatcher{
						{Type: prompb.LabelMatcher_EQ, Name: model.MetricNameLabel, Value: "metric1"},
					},
				},
				{
					StartTimestampMs: 6,
					EndTimestampMs:   10,
					Matchers: []*prompb.LabelMatcher{
						{Type: prompb.LabelMatcher_EQ, Name: model.MetricNameLabel, Value: "metric2"},
					},
				},
			},
			expectedResults: []expectedResult{
				{
					queryStartEnd: queryStartEnd{
						start: 1,
						end:   5,
					},
					timeseries: []*prompb.TimeSeries{
						{
							Labels: []prompb.Label{
								{Name: model.MetricNameLabel, Value: "metric1"},
								{Name: "foo", Value: "bar"},
							},
							Samples: []prompb.Sample{
								{Value: 1, Timestamp: 1},
								{Value: 2, Timestamp: 2},
							},
						},
					},
				},
				{
					queryStartEnd: queryStartEnd{
						start: 6,
						end:   10,
					},
					timeseries: []*prompb.TimeSeries{
						{
							Labels: []prompb.Label{
								{Name: model.MetricNameLabel, Value: "metric2"},
								{Name: "foo", Value: "bar"},
							},
							Samples: []prompb.Sample{
								{Value: 6, Timestamp: 6},
								{Value: 7, Timestamp: 7},
							},
						},
					},
				},
			},
		},
	}

	for queryType, queryData := range queries {
		t.Run(queryType, func(t *testing.T) {
			// Create a slice of atomics, one per query
			queryCalls := make([]atomic.Int64, len(queryData.query))

			q := &mockSampleAndChunkQueryable{
				queryableFn: func(_, _ int64) (storage.Querier, error) {
					return mockQuerier{
						selectFn: func(_ context.Context, _ bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
							require.NotNil(t, hints, "select hints must be set")

							// Find which query this request corresponds to
							queryIndex := findQueryIndexByMatchers(queryData.query, matchers)
							require.True(t, queryIndex >= 0 && queryIndex < len(queryData.expectedResults), "Failed to find matching query for matchers")

							// Verify the start and end times match the expected query
							expectedResult := queryData.expectedResults[queryIndex]
							require.Equal(t, expectedResult.start, hints.Start, "Start time mismatch for query %d", queryIndex)
							require.Equal(t, expectedResult.end, hints.End, "End time mismatch for query %d", queryIndex)

							// Increment the call count for this specific query
							queryCalls[queryIndex].Inc()

							// Return different data based on matchers
							var metricName string
							for _, matcher := range matchers {
								if matcher.Name == model.MetricNameLabel {
									metricName = matcher.Value
									break
								}
							}

							switch metricName {
							case "metric1":
								return series.NewConcreteSeriesSetFromUnsortedSeries([]storage.Series{
									series.NewConcreteSeries(
										labels.FromStrings("__name__", "metric1", "foo", "bar"),
										[]model.SamplePair{{Timestamp: 1, Value: 1}, {Timestamp: 2, Value: 2}},
										nil,
									),
								})
							case "metric2":
								return series.NewConcreteSeriesSetFromUnsortedSeries([]storage.Series{
									series.NewConcreteSeries(
										labels.FromStrings("__name__", "metric2", "foo", "bar"),
										[]model.SamplePair{{Timestamp: 6, Value: 6}, {Timestamp: 7, Value: 7}},
										nil,
									),
								})
							default:
								// Default for single queries without specific matchers
								return series.NewConcreteSeriesSetFromUnsortedSeries([]storage.Series{
									series.NewConcreteSeries(
										labels.FromStrings("foo", "bar"),
										[]model.SamplePair{{Timestamp: 1, Value: 1}, {Timestamp: 2, Value: 2}, {Timestamp: 3, Value: 3}},
										[]mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(4, test.GenerateTestHistogram(4))},
									),
								})
							}
						},
					}, nil
				},
			}
			handler := RemoteReadHandler(q, log.NewNopLogger(), Config{})

			requestBody, err := proto.Marshal(&prompb.ReadRequest{Queries: queryData.query})
			require.NoError(t, err)
			requestBody = snappy.Encode(nil, requestBody)
			request, err := http.NewRequest(http.MethodPost, "/api/v1/read", bytes.NewReader(requestBody))
			require.NoError(t, err)
			request.Header.Set("X-Prometheus-Remote-Read-Version", "0.1.0")

			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)

			require.Equal(t, 200, recorder.Result().StatusCode)
			require.Equal(t, []string{"application/x-protobuf"}, recorder.Result().Header["Content-Type"])
			responseBody, err := io.ReadAll(recorder.Result().Body)
			require.NoError(t, err)
			responseBody, err = snappy.Decode(nil, responseBody)
			require.NoError(t, err)
			var response prompb.ReadResponse
			err = proto.Unmarshal(responseBody, &response)
			require.NoError(t, err)

			// Build expected response
			var expectedResults []*prompb.QueryResult
			for _, expectedResult := range queryData.expectedResults {
				expectedResults = append(expectedResults, &prompb.QueryResult{
					Timeseries: expectedResult.timeseries,
				})
			}

			expected := prompb.ReadResponse{
				Results: expectedResults,
			}
			require.Equal(t, expected, response)

			// Verify each query was called exactly once
			for i, queryCall := range queryCalls {
				require.Equal(t, int64(1), queryCall.Load(), "Query %d should be called exactly once", i)
			}
		})
	}
}

func TestRemoteReadSamples_SampleCountStats(t *testing.T) {
	equivalentCountForHistogram := func(i int) uint64 {
		h := test.GenerateTestHistogram(i)
		return uint64(types.EquivalentFloatSampleCount(h.ToFloat(nil)))
	}

	tests := map[string]struct {
		queries                       []*prompb.Query
		seriesSets                    func() []storage.SeriesSet
		expectedStatusCode            int
		expectedPhysicalSampleCount   uint64
		expectedEquivalentSampleCount uint64
	}{
		"single query with float samples only": {
			queries: []*prompb.Query{
				{StartTimestampMs: 0, EndTimestampMs: 10},
			},
			seriesSets: func() []storage.SeriesSet {
				return []storage.SeriesSet{
					series.NewConcreteSeriesSetFromUnsortedSeries([]storage.Series{
						series.NewConcreteSeries(
							labels.FromStrings("foo", "bar"),
							[]model.SamplePair{{Timestamp: 1, Value: 1}, {Timestamp: 2, Value: 2}, {Timestamp: 3, Value: 3}},
							nil,
						),
					}),
				}
			},
			expectedPhysicalSampleCount:   3,
			expectedEquivalentSampleCount: 3,
		},
		"single query with histograms": {
			queries: []*prompb.Query{
				{StartTimestampMs: 0, EndTimestampMs: 10},
			},
			seriesSets: func() []storage.SeriesSet {
				return []storage.SeriesSet{
					series.NewConcreteSeriesSetFromUnsortedSeries([]storage.Series{
						series.NewConcreteSeries(
							labels.FromStrings("foo", "bar"),
							nil,
							[]mimirpb.Histogram{
								mimirpb.FromHistogramToHistogramProto(1, test.GenerateTestHistogram(1)),
								mimirpb.FromHistogramToHistogramProto(2, test.GenerateTestHistogram(2)),
							},
						),
					}),
				}
			},
			expectedPhysicalSampleCount:   equivalentCountForHistogram(1) + equivalentCountForHistogram(2),
			expectedEquivalentSampleCount: equivalentCountForHistogram(1) + equivalentCountForHistogram(2),
		},
		"single query with mixed float and histogram samples": {
			queries: []*prompb.Query{
				{StartTimestampMs: 0, EndTimestampMs: 10},
			},
			seriesSets: func() []storage.SeriesSet {
				return []storage.SeriesSet{
					series.NewConcreteSeriesSetFromUnsortedSeries([]storage.Series{
						series.NewConcreteSeries(
							labels.FromStrings("foo", "bar"),
							[]model.SamplePair{{Timestamp: 1, Value: 1}, {Timestamp: 2, Value: 2}},
							[]mimirpb.Histogram{
								mimirpb.FromHistogramToHistogramProto(3, test.GenerateTestHistogram(3)),
							},
						),
					}),
				}
			},
			expectedPhysicalSampleCount:   2 + equivalentCountForHistogram(3),
			expectedEquivalentSampleCount: 2 + equivalentCountForHistogram(3),
		},
		"multiple queries": {
			queries: []*prompb.Query{
				{StartTimestampMs: 0, EndTimestampMs: 10},
				{StartTimestampMs: 0, EndTimestampMs: 10},
			},
			seriesSets: func() []storage.SeriesSet {
				return []storage.SeriesSet{
					series.NewConcreteSeriesSetFromUnsortedSeries([]storage.Series{
						series.NewConcreteSeries(
							labels.FromStrings("foo", "bar"),
							[]model.SamplePair{{Timestamp: 1, Value: 1}, {Timestamp: 2, Value: 2}},
							nil,
						),
					}),
					series.NewConcreteSeriesSetFromUnsortedSeries([]storage.Series{
						series.NewConcreteSeries(
							labels.FromStrings("foo", "baz"),
							[]model.SamplePair{{Timestamp: 3, Value: 3}},
							nil,
						),
					}),
				}
			},
			expectedPhysicalSampleCount:   3,
			expectedEquivalentSampleCount: 3,
		},
		"empty series set": {
			queries: []*prompb.Query{
				{StartTimestampMs: 0, EndTimestampMs: 10},
			},
			seriesSets: func() []storage.SeriesSet {
				return []storage.SeriesSet{
					series.NewConcreteSeriesSetFromUnsortedSeries(nil),
				}
			},
			expectedPhysicalSampleCount:   0,
			expectedEquivalentSampleCount: 0,
		},
		"stale samples not counted": {
			queries: []*prompb.Query{
				{StartTimestampMs: 0, EndTimestampMs: 10},
			},
			seriesSets: func() []storage.SeriesSet {
				staleNaN := model.SampleValue(math.Float64frombits(value.StaleNaN))
				return []storage.SeriesSet{
					series.NewConcreteSeriesSetFromUnsortedSeries([]storage.Series{
						series.NewConcreteSeries(
							labels.FromStrings("foo", "bar"),
							[]model.SamplePair{
								{Timestamp: 1, Value: 1},
								{Timestamp: 2, Value: staleNaN},
								{Timestamp: 3, Value: 3},
								{Timestamp: 4, Value: staleNaN},
								{Timestamp: 5, Value: 5},
							},
							nil,
						),
					}),
				}
			},
			expectedPhysicalSampleCount:   3,
			expectedEquivalentSampleCount: 3,
		},
		"stats reported after partial read error": {
			queries: []*prompb.Query{
				{StartTimestampMs: 0, EndTimestampMs: 10},
			},
			seriesSets: func() []storage.SeriesSet {
				// The SeriesSet yields one series and then fails on Err(); the already-read
				// series must still be counted, matching the streaming path's behaviour.
				return []storage.SeriesSet{
					&partiallyFailingSeriesSet{
						ss: series.NewConcreteSeriesSetFromUnsortedSeries([]storage.Series{
							series.NewConcreteSeries(
								labels.FromStrings("foo", "bar"),
								[]model.SamplePair{{Timestamp: 1, Value: 1}, {Timestamp: 2, Value: 2}},
								nil,
							),
							series.NewConcreteSeries(
								labels.FromStrings("foo", "baz"),
								[]model.SamplePair{{Timestamp: 3, Value: 3}},
								nil,
							),
						}),
						failAfter: 1,
						err:       errors.New("partial series set failure"),
					},
				}
			},
			expectedStatusCode:            http.StatusInternalServerError,
			expectedPhysicalSampleCount:   2,
			expectedEquivalentSampleCount: 2,
		},
	}
	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			seriesSets := tc.seriesSets()
			callCount := atomic.NewInt64(0)

			q := &mockSampleAndChunkQueryable{
				queryableFn: func(int64, int64) (storage.Querier, error) {
					return mockQuerier{
						selectFn: func(_ context.Context, _ bool, _ *storage.SelectHints, _ ...*labels.Matcher) storage.SeriesSet {
							idx := callCount.Inc() - 1
							return seriesSets[idx]
						},
					}, nil
				},
			}

			queryStats, ctx := stats.ContextWithEmptyStats(t.Context())
			w := httptest.NewRecorder()
			req := &prompb.ReadRequest{Queries: tc.queries}

			remoteReadSamples(ctx, q, w, req, 0, log.NewNopLogger())

			expectedCode := tc.expectedStatusCode
			if expectedCode == 0 {
				expectedCode = http.StatusOK
			}
			require.Equal(t, expectedCode, w.Code)
			require.Equal(t, tc.expectedPhysicalSampleCount, queryStats.LoadPhysicalSamplesRead())
			require.Equal(t, tc.expectedEquivalentSampleCount, queryStats.LoadEquivalentSamplesRead())
		})
	}
}

func TestRemoteReadStreamedXORChunks_SampleCountStats(t *testing.T) {
	tests := map[string]struct {
		queries                       []*prompb.Query
		chunkSeriesSets               func() []storage.ChunkSeriesSet
		expectedPhysicalSampleCount   uint64
		expectedEquivalentSampleCount uint64
	}{
		"single query with float samples": {
			queries: []*prompb.Query{
				{StartTimestampMs: 0, EndTimestampMs: 10},
			},
			chunkSeriesSets: func() []storage.ChunkSeriesSet {
				return []storage.ChunkSeriesSet{
					storage.NewSeriesSetToChunkSet(
						series.NewConcreteSeriesSetFromUnsortedSeries([]storage.Series{
							series.NewConcreteSeries(
								labels.FromStrings("foo", "bar"),
								[]model.SamplePair{{Timestamp: 1, Value: 1}, {Timestamp: 2, Value: 2}, {Timestamp: 3, Value: 3}},
								nil,
							),
						}),
					),
				}
			},
			expectedPhysicalSampleCount:   3,
			expectedEquivalentSampleCount: 3,
		},
		"single query with histogram samples": {
			queries: []*prompb.Query{
				{StartTimestampMs: 0, EndTimestampMs: 10},
			},
			chunkSeriesSets: func() []storage.ChunkSeriesSet {
				return []storage.ChunkSeriesSet{
					storage.NewSeriesSetToChunkSet(
						series.NewConcreteSeriesSetFromUnsortedSeries([]storage.Series{
							series.NewConcreteSeries(
								labels.FromStrings("foo", "bar"),
								nil,
								[]mimirpb.Histogram{
									mimirpb.FromHistogramToHistogramProto(1, test.GenerateTestHistogram(1)),
									mimirpb.FromHistogramToHistogramProto(2, test.GenerateTestHistogram(2)),
								},
							),
						}),
					),
				}
			},
			expectedPhysicalSampleCount: 2,
			// Both histograms land in one chunk so the equivalent cost is first sample's weight * NumSamples().
			expectedEquivalentSampleCount: func() uint64 {
				h := test.GenerateTestHistogram(1)
				perSample := types.EquivalentFloatSampleCount(h.ToFloat(nil))
				return uint64(perSample) * 2
			}(),
		},
		"single query with many samples across multiple chunks": {
			queries: []*prompb.Query{
				{StartTimestampMs: 0, EndTimestampMs: 1000},
			},
			chunkSeriesSets: func() []storage.ChunkSeriesSet {
				return []storage.ChunkSeriesSet{
					storage.NewSeriesSetToChunkSet(
						series.NewConcreteSeriesSetFromUnsortedSeries([]storage.Series{
							series.NewConcreteSeries(
								labels.FromStrings("foo", "bar"),
								getNSamples(250),
								nil,
							),
						}),
					),
				}
			},
			expectedPhysicalSampleCount:   250,
			expectedEquivalentSampleCount: 250,
		},
		"multiple queries": {
			queries: []*prompb.Query{
				{StartTimestampMs: 0, EndTimestampMs: 10},
				{StartTimestampMs: 0, EndTimestampMs: 10},
			},
			chunkSeriesSets: func() []storage.ChunkSeriesSet {
				return []storage.ChunkSeriesSet{
					storage.NewSeriesSetToChunkSet(
						series.NewConcreteSeriesSetFromUnsortedSeries([]storage.Series{
							series.NewConcreteSeries(
								labels.FromStrings("foo", "bar"),
								[]model.SamplePair{{Timestamp: 1, Value: 1}, {Timestamp: 2, Value: 2}},
								nil,
							),
						}),
					),
					storage.NewSeriesSetToChunkSet(
						series.NewConcreteSeriesSetFromUnsortedSeries([]storage.Series{
							series.NewConcreteSeries(
								labels.FromStrings("foo", "baz"),
								[]model.SamplePair{{Timestamp: 3, Value: 3}},
								nil,
							),
						}),
					),
				}
			},
			expectedPhysicalSampleCount:   3,
			expectedEquivalentSampleCount: 3,
		},
		"stale samples are not counted": {
			queries: []*prompb.Query{
				{StartTimestampMs: 0, EndTimestampMs: 10},
			},
			chunkSeriesSets: func() []storage.ChunkSeriesSet {
				staleNaN := model.SampleValue(math.Float64frombits(value.StaleNaN))
				return []storage.ChunkSeriesSet{
					storage.NewSeriesSetToChunkSet(
						series.NewConcreteSeriesSetFromUnsortedSeries([]storage.Series{
							series.NewConcreteSeries(
								labels.FromStrings("foo", "bar"),
								[]model.SamplePair{
									{Timestamp: 1, Value: 1},
									{Timestamp: 2, Value: staleNaN},
									{Timestamp: 3, Value: 3},
									{Timestamp: 4, Value: staleNaN},
									{Timestamp: 5, Value: 5},
								},
								nil,
							),
						}),
					),
				}
			},
			expectedPhysicalSampleCount:   3,
			expectedEquivalentSampleCount: 3,
		},
		"over-read samples outside the query range are not counted": {
			queries: []*prompb.Query{
				{StartTimestampMs: 0, EndTimestampMs: 10},
			},
			chunkSeriesSets: func() []storage.ChunkSeriesSet {
				// The querier can over-read whole chunks; the series spans t=1..20 but the
				// query only asks for [0,10], so only 10 samples must be metered.
				samples := make([]model.SamplePair, 0, 20)
				for ts := int64(1); ts <= 20; ts++ {
					samples = append(samples, model.SamplePair{Timestamp: model.Time(ts), Value: model.SampleValue(ts)})
				}
				return []storage.ChunkSeriesSet{
					storage.NewSeriesSetToChunkSet(
						series.NewConcreteSeriesSetFromUnsortedSeries([]storage.Series{
							series.NewConcreteSeries(labels.FromStrings("foo", "bar"), samples, nil),
						}),
					),
				}
			},
			expectedPhysicalSampleCount:   10,
			expectedEquivalentSampleCount: 10,
		},
		"empty series set": {
			queries: []*prompb.Query{
				{StartTimestampMs: 0, EndTimestampMs: 10},
			},
			chunkSeriesSets: func() []storage.ChunkSeriesSet {
				return []storage.ChunkSeriesSet{
					storage.NewSeriesSetToChunkSet(
						series.NewConcreteSeriesSetFromUnsortedSeries(nil),
					),
				}
			},
			expectedPhysicalSampleCount:   0,
			expectedEquivalentSampleCount: 0,
		},
	}
	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			chunkSeriesSets := tc.chunkSeriesSets()
			callCount := atomic.NewInt64(0)

			q := &mockSampleAndChunkQueryable{
				chunkQueryableFn: func(int64, int64) (storage.ChunkQuerier, error) {
					return mockChunkQuerier{
						selectFn: func(_ context.Context, _ bool, _ *storage.SelectHints, _ ...*labels.Matcher) storage.ChunkSeriesSet {
							idx := callCount.Inc() - 1
							return chunkSeriesSets[idx]
						},
					}, nil
				},
			}

			queryStats, ctx := stats.ContextWithEmptyStats(t.Context())
			w := httptest.NewRecorder()
			req := &prompb.ReadRequest{
				Queries:               tc.queries,
				AcceptedResponseTypes: []prompb.ReadRequest_ResponseType{prompb.ReadRequest_STREAMED_XOR_CHUNKS},
			}

			remoteReadStreamedXORChunks(ctx, q, w, req, maxRemoteReadFrameBytes, 0, log.NewNopLogger())

			require.Equal(t, http.StatusOK, w.Code)
			require.Equal(t, tc.expectedPhysicalSampleCount, queryStats.LoadPhysicalSamplesRead())
			require.Equal(t, tc.expectedEquivalentSampleCount, queryStats.LoadEquivalentSamplesRead())
		})
	}
}

type queryStartEnd struct {
	start int64
	end   int64
}

// findQueryIndexByMatchers finds the index of the query that matches the given matchers
func findQueryIndexByMatchers(queries []*prompb.Query, matchers []*labels.Matcher) int {
	// For multiple queries, match by comparing matchers
	for i, query := range queries {
		if matchersEqual(query.Matchers, matchers) {
			return i
		}
	}

	return -1 // Not found
}

// matchersEqual compares prompb matchers with labels matchers
func matchersEqual(prompbMatchers []*prompb.LabelMatcher, labelsMatchers []*labels.Matcher) bool {
	if len(prompbMatchers) != len(labelsMatchers) {
		return false
	}

	// Convert prompb matchers to a map for easier comparison
	prompbMap := make(map[string]*prompb.LabelMatcher)
	for _, m := range prompbMatchers {
		key := m.Name + ":" + m.Value + ":" + m.Type.String()
		prompbMap[key] = m
	}

	// Check if all labels matchers have corresponding prompb matchers
	for _, m := range labelsMatchers {
		key := m.Name + ":" + m.Value + ":" + matcherTypeToPrompbType(m.Type).String()
		if _, exists := prompbMap[key]; !exists {
			return false
		}
	}

	return true
}

// matcherTypeToPrompbType converts labels.MatchType to prompb.LabelMatcher_Type
func matcherTypeToPrompbType(t labels.MatchType) prompb.LabelMatcher_Type {
	switch t {
	case labels.MatchEqual:
		return prompb.LabelMatcher_EQ
	case labels.MatchNotEqual:
		return prompb.LabelMatcher_NEQ
	case labels.MatchRegexp:
		return prompb.LabelMatcher_RE
	case labels.MatchNotRegexp:
		return prompb.LabelMatcher_NRE
	default:
		return prompb.LabelMatcher_EQ
	}
}

func TestRemoteReadHandler_StreamedXORChunks(t *testing.T) {
	type expectedResult struct {
		queryStartEnd
		responses []*prompb.ChunkedReadResponse
	}

	tests := map[string]struct {
		query           []*prompb.Query
		samples         []model.SamplePair
		histograms      []mimirpb.Histogram
		expectedResults []expectedResult
	}{
		"single query without hints - 120 samples, we expect 1 frame with 1 chunk": {
			query: []*prompb.Query{
				{
					StartTimestampMs: 1,
					EndTimestampMs:   10,
				},
			},
			samples: getNSamples(120),
			expectedResults: []expectedResult{
				{
					queryStartEnd: queryStartEnd{
						start: 1,
						end:   10,
					},
					responses: []*prompb.ChunkedReadResponse{
						{
							ChunkedSeries: []*prompb.ChunkedSeries{
								{
									Labels: []prompb.Label{{Name: "foo", Value: "bar"}},
									Chunks: []prompb.Chunk{
										{
											MinTimeMs: 0,
											MaxTimeMs: 119,
											Type:      prompb.Chunk_XOR,
											Data:      getIndexedChunk(0, 120, chunkenc.EncXOR),
										},
									},
								},
							},
							QueryIndex: 0,
						},
					},
				},
			},
		},
		"single query with hints - 120 samples, we expect 1 frame with 1 chunk": {
			query: []*prompb.Query{
				{
					StartTimestampMs: 1,
					EndTimestampMs:   10,
					Hints: &prompb.ReadHints{
						StartMs: 2,
						EndMs:   9,
					},
				},
			},
			samples: getNSamples(120),
			expectedResults: []expectedResult{
				{
					queryStartEnd: queryStartEnd{
						start: 2,
						end:   9,
					},
					responses: []*prompb.ChunkedReadResponse{
						{
							ChunkedSeries: []*prompb.ChunkedSeries{
								{
									Labels: []prompb.Label{{Name: "foo", Value: "bar"}},
									Chunks: []prompb.Chunk{
										{
											MinTimeMs: 0,
											MaxTimeMs: 119,
											Type:      prompb.Chunk_XOR,
											Data:      getIndexedChunk(0, 120, chunkenc.EncXOR),
										},
									},
								},
							},
							QueryIndex: 0,
						},
					},
				},
			},
		},
		"single query - 121 samples, we expect 1 frame with 2 chunks": {
			query: []*prompb.Query{
				{
					StartTimestampMs: 1,
					EndTimestampMs:   10,
				},
			},
			samples: getNSamples(121),
			expectedResults: []expectedResult{
				{
					queryStartEnd: queryStartEnd{
						start: 1,
						end:   10,
					},
					responses: []*prompb.ChunkedReadResponse{
						{
							ChunkedSeries: []*prompb.ChunkedSeries{
								{
									Labels: []prompb.Label{{Name: "foo", Value: "bar"}},
									Chunks: []prompb.Chunk{
										{
											MinTimeMs: 0,
											MaxTimeMs: 119,
											Type:      prompb.Chunk_XOR,
											Data:      getIndexedChunk(0, 121, chunkenc.EncXOR),
										},
										{
											MinTimeMs: 120,
											MaxTimeMs: 120,
											Type:      prompb.Chunk_XOR,
											Data:      getIndexedChunk(1, 121, chunkenc.EncXOR),
										},
									},
								},
							},
							QueryIndex: 0,
						},
					},
				},
			},
		},
		"single query - 481 samples, we expect 2 frames with 2 chunks, and 1 frame with 1 chunk due to frame limit": {
			query: []*prompb.Query{
				{
					StartTimestampMs: 1,
					EndTimestampMs:   10,
				},
			},
			samples: getNSamples(481),
			expectedResults: []expectedResult{
				{
					queryStartEnd: queryStartEnd{
						start: 1,
						end:   10,
					},
					responses: []*prompb.ChunkedReadResponse{
						{
							ChunkedSeries: []*prompb.ChunkedSeries{
								{
									Labels: []prompb.Label{{Name: "foo", Value: "bar"}},
									Chunks: []prompb.Chunk{
										{
											MinTimeMs: 0,
											MaxTimeMs: 119,
											Type:      prompb.Chunk_XOR,
											Data:      getIndexedChunk(0, 481, chunkenc.EncXOR),
										},
										{
											MinTimeMs: 120,
											MaxTimeMs: 239,
											Type:      prompb.Chunk_XOR,
											Data:      getIndexedChunk(1, 481, chunkenc.EncXOR),
										},
									},
								},
							},
							QueryIndex: 0,
						},
						{
							ChunkedSeries: []*prompb.ChunkedSeries{
								{
									Labels: []prompb.Label{{Name: "foo", Value: "bar"}},
									Chunks: []prompb.Chunk{
										{
											MinTimeMs: 240,
											MaxTimeMs: 359,
											Type:      prompb.Chunk_XOR,
											Data:      getIndexedChunk(2, 481, chunkenc.EncXOR),
										},
										{
											MinTimeMs: 360,
											MaxTimeMs: 479,
											Type:      prompb.Chunk_XOR,
											Data:      getIndexedChunk(3, 481, chunkenc.EncXOR),
										},
									},
								},
							},
							QueryIndex: 0,
						},
						{
							ChunkedSeries: []*prompb.ChunkedSeries{
								{
									Labels: []prompb.Label{{Name: "foo", Value: "bar"}},
									Chunks: []prompb.Chunk{
										{
											MinTimeMs: 480,
											MaxTimeMs: 480,
											Type:      prompb.Chunk_XOR,
											Data:      getIndexedChunk(4, 481, chunkenc.EncXOR),
										},
									},
								},
							},
							QueryIndex: 0,
						},
					},
				},
			},
		},
		"single query - 120 native histograms": {
			query: []*prompb.Query{
				{
					StartTimestampMs: 1,
					EndTimestampMs:   10,
				},
			},
			histograms: getNHistogramSamples(120),
			expectedResults: []expectedResult{
				{
					queryStartEnd: queryStartEnd{
						start: 1,
						end:   10,
					},
					responses: []*prompb.ChunkedReadResponse{
						{
							ChunkedSeries: []*prompb.ChunkedSeries{
								{
									Labels: []prompb.Label{{Name: "foo", Value: "bar"}},
									Chunks: []prompb.Chunk{
										{
											MinTimeMs: 0,
											MaxTimeMs: 119,
											Type:      prompb.Chunk_HISTOGRAM,
											Data:      getIndexedChunk(0, 120, chunkenc.EncHistogram),
										},
									},
								},
							},
							QueryIndex: 0,
						},
					},
				},
			},
		},
		"single query - 120 native float histograms": {
			query: []*prompb.Query{
				{
					StartTimestampMs: 1,
					EndTimestampMs:   10,
				},
			},
			histograms: getNFloatHistogramSamples(120),
			expectedResults: []expectedResult{
				{
					queryStartEnd: queryStartEnd{
						start: 1,
						end:   10,
					},
					responses: []*prompb.ChunkedReadResponse{
						{
							ChunkedSeries: []*prompb.ChunkedSeries{
								{
									Labels: []prompb.Label{{Name: "foo", Value: "bar"}},
									Chunks: []prompb.Chunk{
										{
											MinTimeMs: 0,
											MaxTimeMs: 119,
											Type:      prompb.Chunk_FLOAT_HISTOGRAM,
											Data:      getIndexedChunk(0, 120, chunkenc.EncFloatHistogram),
										},
									},
								},
							},
							QueryIndex: 0,
						},
					},
				},
			},
		},
		"multiple queries": {
			query: []*prompb.Query{
				{
					StartTimestampMs: 1,
					EndTimestampMs:   5,
					Matchers: []*prompb.LabelMatcher{
						{Type: prompb.LabelMatcher_EQ, Name: model.MetricNameLabel, Value: "metric1"},
					},
				},
				{
					StartTimestampMs: 6,
					EndTimestampMs:   10,
					Matchers: []*prompb.LabelMatcher{
						{Type: prompb.LabelMatcher_EQ, Name: model.MetricNameLabel, Value: "metric2"},
					},
				},
			},
			samples: getNSamples(120),
			expectedResults: []expectedResult{
				{
					queryStartEnd: queryStartEnd{
						start: 1,
						end:   5,
					},
					responses: []*prompb.ChunkedReadResponse{
						{
							ChunkedSeries: []*prompb.ChunkedSeries{
								{
									Labels: []prompb.Label{
										{Name: model.MetricNameLabel, Value: "metric1"},
										{Name: "foo", Value: "bar"},
									},
									Chunks: []prompb.Chunk{
										{
											MinTimeMs: 0,
											MaxTimeMs: 119,
											Type:      prompb.Chunk_XOR,
											Data:      getIndexedChunk(0, 120, chunkenc.EncXOR),
										},
									},
								},
							},
							QueryIndex: 0,
						},
					},
				},
				{
					queryStartEnd: queryStartEnd{
						start: 6,
						end:   10,
					},
					responses: []*prompb.ChunkedReadResponse{
						{
							ChunkedSeries: []*prompb.ChunkedSeries{
								{
									Labels: []prompb.Label{
										{Name: model.MetricNameLabel, Value: "metric2"},
										{Name: "foo", Value: "bar"},
									},
									Chunks: []prompb.Chunk{
										{
											MinTimeMs: 0,
											MaxTimeMs: 119,
											Type:      prompb.Chunk_XOR,
											Data:      getIndexedChunk(0, 120, chunkenc.EncXOR),
										},
									},
								},
							},
							QueryIndex: 1,
						},
					},
				},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {

			// Create a slice of atomics, one per query
			queryCalls := make([]atomic.Int64, len(testData.query))

			q := &mockSampleAndChunkQueryable{
				chunkQueryableFn: func(int64, int64) (storage.ChunkQuerier, error) {
					return mockChunkQuerier{
						selectFn: func(_ context.Context, _ bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.ChunkSeriesSet {
							require.NotNil(t, hints, "select hints must be set")

							// Find which query this request corresponds to
							queryIndex := findQueryIndexByMatchers(testData.query, matchers)
							require.True(t, queryIndex >= 0 && queryIndex < len(testData.expectedResults), "Failed to find matching query for matchers")

							// Verify the start and end times match the expected query
							expectedResult := testData.expectedResults[queryIndex]
							require.Equal(t, expectedResult.start, hints.Start, "Start time mismatch for query %d", queryIndex)
							require.Equal(t, expectedResult.end, hints.End, "End time mismatch for query %d", queryIndex)

							// Increment the call count for this specific query
							queryCalls[queryIndex].Inc()

							// Return different data based on matchers for multiple queries
							var metricName string
							for _, matcher := range matchers {
								if matcher.Name == model.MetricNameLabel {
									metricName = matcher.Value
									break
								}
							}

							switch metricName {
							case "metric1":
								return storage.NewSeriesSetToChunkSet(
									series.NewConcreteSeriesSetFromUnsortedSeries([]storage.Series{
										series.NewConcreteSeries(
											labels.FromStrings("__name__", "metric1", "foo", "bar"),
											testData.samples,
											testData.histograms,
										),
									}),
								)
							case "metric2":
								return storage.NewSeriesSetToChunkSet(
									series.NewConcreteSeriesSetFromUnsortedSeries([]storage.Series{
										series.NewConcreteSeries(
											labels.FromStrings("__name__", "metric2", "foo", "bar"),
											testData.samples,
											testData.histograms,
										),
									}),
								)
							default:
								// Default for single queries without specific matchers
								return storage.NewSeriesSetToChunkSet(
									series.NewConcreteSeriesSetFromUnsortedSeries([]storage.Series{
										series.NewConcreteSeries(
											labels.FromStrings("foo", "bar"),
											testData.samples,
											testData.histograms,
										),
									}),
								)
							}
						},
					}, nil
				},
			}
			// The labelset for this test has 10 bytes and a full chunk is roughly 165 bytes; for this test we want a
			// frame to contain at most 2 chunks.
			maxBytesInFrame := 10 + 165*2

			handler := remoteReadHandler(q, maxBytesInFrame, 0, log.NewNopLogger())

			requestBody, err := proto.Marshal(&prompb.ReadRequest{
				Queries:               testData.query,
				AcceptedResponseTypes: []prompb.ReadRequest_ResponseType{prompb.ReadRequest_STREAMED_XOR_CHUNKS},
			})
			require.NoError(t, err)
			requestBody = snappy.Encode(nil, requestBody)
			request, err := http.NewRequest(http.MethodPost, "/api/v1/read", bytes.NewReader(requestBody))
			require.NoError(t, err)
			request.Header.Set("X-Prometheus-Remote-Read-Version", "0.1.0")

			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)

			require.Equal(t, 200, recorder.Result().StatusCode)
			require.Equal(t, []string{api.ContentTypeRemoteReadStreamedChunks}, recorder.Result().Header["Content-Type"])

			stream := prom_remote.NewChunkedReader(recorder.Result().Body, config.DefaultChunkedReadLimit, nil)

			var responses []*prompb.ChunkedReadResponse
			for {
				var res prompb.ChunkedReadResponse
				err := stream.NextProto(&res)
				if errors.Is(err, io.EOF) {
					break
				}
				require.NoError(t, err)
				responses = append(responses, &res)
			}

			actualResponsesByQuery := map[int64][]*prompb.ChunkedReadResponse{}
			expectedResponsesByQuery := map[int64][]*prompb.ChunkedReadResponse{}
			for _, expectedResponse := range testData.expectedResults {
				for _, response := range expectedResponse.responses {
					expectedResponsesByQuery[response.QueryIndex] = append(expectedResponsesByQuery[response.QueryIndex], response)
				}
			}
			for _, actualResponse := range responses {
				actualResponsesByQuery[actualResponse.QueryIndex] = append(actualResponsesByQuery[actualResponse.QueryIndex], actualResponse)
			}

			require.Len(t, expectedResponsesByQuery, len(actualResponsesByQuery))
			for queryID, expectedResponses := range expectedResponsesByQuery {
				require.Equal(t, expectedResponses, actualResponsesByQuery[queryID])
			}

			// Verify each query was called exactly once
			for i, queryCall := range queryCalls {
				require.Equal(t, int64(1), queryCall.Load(), "Query %d should be called exactly once", i)
			}
		})
	}
}

func getNSamples(n int) []model.SamplePair {
	var ret []model.SamplePair
	for i := 0; i < n; i++ {
		ret = append(ret, model.SamplePair{
			Timestamp: model.Time(i),
			Value:     model.SampleValue(i),
		})
	}
	return ret
}

func getNHistogramSamples(n int) []mimirpb.Histogram {
	var ret []mimirpb.Histogram
	for i := 0; i < n; i++ {
		h := test.GenerateTestHistogram(i)
		ret = append(ret, mimirpb.FromHistogramToHistogramProto(int64(i), h))
	}
	return ret
}

func getNFloatHistogramSamples(n int) []mimirpb.Histogram {
	var ret []mimirpb.Histogram
	for i := 0; i < n; i++ {
		h := test.GenerateTestFloatHistogram(i)
		ret = append(ret, mimirpb.FromFloatHistogramToHistogramProto(int64(i), h))
	}
	return ret
}

func getIndexedChunk(idx, samplesCount int, encoding chunkenc.Encoding) []byte {
	const samplesPerChunk = 120

	var enc chunkenc.Chunk
	switch encoding {
	case chunkenc.EncXOR:
		enc = chunkenc.NewXORChunk()
	case chunkenc.EncHistogram:
		enc = chunkenc.NewHistogramChunk()
	case chunkenc.EncFloatHistogram:
		enc = chunkenc.NewFloatHistogramChunk()
	}
	ap, _ := enc.Appender()

	baseIdx := idx * samplesPerChunk
	for i := 0; i < samplesPerChunk; i++ {
		j := baseIdx + i
		if j >= samplesCount {
			break
		}

		switch encoding {
		case chunkenc.EncXOR:
			ap.Append(0, int64(j), float64(j))
		case chunkenc.EncHistogram:
			_, _, _, err := ap.AppendHistogram(nil, 0, int64(j), test.GenerateTestHistogram(j), true)
			if err != nil {
				panic(err)
			}
		case chunkenc.EncFloatHistogram:
			_, _, _, err := ap.AppendFloatHistogram(nil, 0, int64(j), test.GenerateTestFloatHistogram(j), true)
			if err != nil {
				panic(err)
			}
		}
	}
	return enc.Bytes()
}

// equivalentSampleCountDecodeAll iterates every sample in a chunk and sums
// the per-sample equivalent float sample count. For float chunks it falls back
// to NumSamples(). This is the "most accurate" baseline for benchmarking.
func equivalentSampleCountDecodeAll(chk chunkenc.Chunk) uint64 {
	enc := chk.Encoding()
	if enc == chunkenc.EncXOR || enc == chunkenc.EncXOR2 {
		return uint64(chk.NumSamples())
	}

	var count uint64
	it := chk.Iterator(nil)
	for valType := it.Next(); valType != chunkenc.ValNone; valType = it.Next() {
		var fh histogram.FloatHistogram
		_, h := it.AtFloatHistogram(&fh)
		count += uint64(types.EquivalentFloatSampleCount(h))
	}
	return count
}

// makeChunk creates a chunkenc.Chunk with n samples of the given encoding.
func makeChunk(tb testing.TB, encoding chunkenc.Encoding, n int) chunkenc.Chunk {
	tb.Helper()

	var chk chunkenc.Chunk
	switch encoding {
	case chunkenc.EncXOR:
		chk = chunkenc.NewXORChunk()
	case chunkenc.EncHistogram:
		chk = chunkenc.NewHistogramChunk()
	case chunkenc.EncFloatHistogram:
		chk = chunkenc.NewFloatHistogramChunk()
	default:
		tb.Fatalf("unsupported encoding: %v", encoding)
	}

	ap, err := chk.Appender()
	require.NoError(tb, err)

	for i := range n {
		switch encoding {
		case chunkenc.EncXOR:
			ap.Append(0, int64(i), float64(i))
		case chunkenc.EncHistogram:
			_, _, _, err = ap.AppendHistogram(nil, 0, int64(i), test.GenerateTestHistogram(i), true)
			require.NoError(tb, err)
		case chunkenc.EncFloatHistogram:
			_, _, _, err = ap.AppendFloatHistogram(nil, 0, int64(i), test.GenerateTestFloatHistogram(i), true)
			require.NoError(tb, err)
		}
	}

	return chk
}

func TestSampleCountsForChunk(t *testing.T) {
	staleSum := math.Float64frombits(value.StaleNaN)
	// Wide bounds that include every sample, for cases not exercising the time filter.
	const allTimes, allTimesEnd = int64(math.MinInt64), int64(math.MaxInt64)

	t.Run("float samples within range are counted", func(t *testing.T) {
		chk := chunkenc.NewXORChunk()
		ap, err := chk.Appender()
		require.NoError(t, err)
		for i := range 5 {
			ap.Append(0, int64(i), float64(i))
		}

		physical, equivalent, err := sampleCountsForChunk(chk, allTimes, allTimesEnd)
		require.NoError(t, err)
		require.Equal(t, uint64(5), physical)
		require.Equal(t, uint64(5), equivalent)
	})

	t.Run("float samples outside the range are excluded", func(t *testing.T) {
		chk := chunkenc.NewXORChunk()
		ap, err := chk.Appender()
		require.NoError(t, err)
		for i := range 10 {
			ap.Append(0, int64(i), float64(i)) // timestamps 0..9
		}

		// Only timestamps 3,4,5,6 are within [3,6].
		physical, equivalent, err := sampleCountsForChunk(chk, 3, 6)
		require.NoError(t, err)
		require.Equal(t, uint64(4), physical)
		require.Equal(t, uint64(4), equivalent)
	})

	t.Run("stale float samples are skipped including mid-chunk", func(t *testing.T) {
		chk := chunkenc.NewXORChunk()
		ap, err := chk.Appender()
		require.NoError(t, err)
		ap.Append(0, 0, 1)
		ap.Append(0, 1, staleSum)
		ap.Append(0, 2, 3)
		ap.Append(0, 3, staleSum)
		ap.Append(0, 4, 5)

		physical, equivalent, err := sampleCountsForChunk(chk, allTimes, allTimesEnd)
		require.NoError(t, err)
		require.Equal(t, uint64(3), physical)
		require.Equal(t, uint64(3), equivalent)
	})

	t.Run("all stale float histogram samples return zero", func(t *testing.T) {
		chk := chunkenc.NewFloatHistogramChunk()
		ap, err := chk.Appender()
		require.NoError(t, err)
		for i := range 3 {
			_, _, _, err = ap.AppendFloatHistogram(nil, 0, int64(i), &histogram.FloatHistogram{Sum: staleSum}, true)
			require.NoError(t, err)
		}
		require.Equal(t, 3, chk.NumSamples())

		physical, equivalent, err := sampleCountsForChunk(chk, allTimes, allTimesEnd)
		require.NoError(t, err)
		require.Equal(t, uint64(0), physical)
		require.Equal(t, uint64(0), equivalent)
	})

	t.Run("all stale integer histogram samples return zero", func(t *testing.T) {
		chk := chunkenc.NewHistogramChunk()
		ap, err := chk.Appender()
		require.NoError(t, err)
		for i := range 3 {
			_, _, _, err = ap.AppendHistogram(nil, 0, int64(i), &histogram.Histogram{Sum: staleSum}, true)
			require.NoError(t, err)
		}
		require.Equal(t, 3, chk.NumSamples())

		physical, equivalent, err := sampleCountsForChunk(chk, allTimes, allTimesEnd)
		require.NoError(t, err)
		require.Equal(t, uint64(0), physical)
		require.Equal(t, uint64(0), equivalent)
	})

	t.Run("histogram samples are counted and weighted within range", func(t *testing.T) {
		chk := chunkenc.NewFloatHistogramChunk()
		ap, err := chk.Appender()
		require.NoError(t, err)
		for i := range 5 {
			_, _, _, err = ap.AppendFloatHistogram(nil, 0, int64(i), test.GenerateTestFloatHistogram(i), true)
			require.NoError(t, err)
		}

		// All samples in the chunk share one bucket layout, so each has the same weight.
		perSample := uint64(types.EquivalentFloatSampleCount(test.GenerateTestFloatHistogram(0)))
		require.Greater(t, perSample, uint64(0))

		// Only timestamps 2,3,4 are within [2,10].
		physical, equivalent, err := sampleCountsForChunk(chk, 2, 10)
		require.NoError(t, err)
		require.Equal(t, uint64(3), physical)
		require.Equal(t, perSample*3, equivalent)
	})

	t.Run("cached histogram weight matches per-sample decode", func(t *testing.T) {
		// sampleCountsForChunk reuses the first non-stale sample's weight for the whole chunk;
		// that must equal summing every sample's weight, since all samples in a histogram chunk
		// share one bucket layout. Guards the optimization against regressions.
		for _, enc := range []chunkenc.Encoding{chunkenc.EncHistogram, chunkenc.EncFloatHistogram} {
			for _, n := range []int{120, 500} {
				chk := makeChunk(t, enc, n)
				_, equivalent, err := sampleCountsForChunk(chk, allTimes, allTimesEnd)
				require.NoError(t, err)
				require.Equal(t, equivalentSampleCountDecodeAll(chk), equivalent)
			}
		}
	})
}

func BenchmarkSampleCountsForChunk(b *testing.B) {
	floatChunk120 := makeChunk(b, chunkenc.EncXOR, 120)
	histChunk120 := makeChunk(b, chunkenc.EncHistogram, 120)
	histChunk500 := makeChunk(b, chunkenc.EncHistogram, 500)

	// Verify chunks were built correctly.
	require.Equal(b, 120, floatChunk120.NumSamples())
	require.Equal(b, 120, histChunk120.NumSamples())
	require.Equal(b, 500, histChunk500.NumSamples())

	const allTimes, allTimesEnd = int64(math.MinInt64), int64(math.MaxInt64)

	// --- Float 120 ---

	b.Run("float_120/NumSamples", func(b *testing.B) {
		for b.Loop() {
			_ = floatChunk120.NumSamples()
		}
	})

	b.Run("float_120/SampleCounts", func(b *testing.B) {
		for b.Loop() {
			_, _, _ = sampleCountsForChunk(floatChunk120, allTimes, allTimesEnd)
		}
	})

	b.Run("float_120/DecodeAll", func(b *testing.B) {
		for b.Loop() {
			_ = equivalentSampleCountDecodeAll(floatChunk120)
		}
	})

	// --- Histogram 120 ---

	b.Run("hist_120/NumSamples", func(b *testing.B) {
		for b.Loop() {
			_ = histChunk120.NumSamples()
		}
	})

	b.Run("hist_120/SampleCounts", func(b *testing.B) {
		for b.Loop() {
			_, _, _ = sampleCountsForChunk(histChunk120, allTimes, allTimesEnd)
		}
	})

	b.Run("hist_120/DecodeAll", func(b *testing.B) {
		for b.Loop() {
			_ = equivalentSampleCountDecodeAll(histChunk120)
		}
	})

	// --- Histogram 500 ---

	b.Run("hist_500/NumSamples", func(b *testing.B) {
		for b.Loop() {
			_ = histChunk500.NumSamples()
		}
	})

	b.Run("hist_500/SampleCounts", func(b *testing.B) {
		for b.Loop() {
			_, _, _ = sampleCountsForChunk(histChunk500, allTimes, allTimesEnd)
		}
	})

	b.Run("hist_500/DecodeAll", func(b *testing.B) {
		for b.Loop() {
			_ = equivalentSampleCountDecodeAll(histChunk500)
		}
	})
}

func TestRemoteReadErrorParsing(t *testing.T) {
	someSeries := series.NewConcreteSeriesSetFromSortedSeries([]storage.Series{
		series.NewConcreteSeries(
			labels.FromStrings("foo", "bar"),
			[]model.SamplePair{{Timestamp: 0, Value: 0}, {Timestamp: 1, Value: 1}, {Timestamp: 2, Value: 2}, {Timestamp: 3, Value: 3}},
			[]mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(4, test.GenerateTestHistogram(4))},
		),
		series.NewConcreteSeries(
			labels.FromStrings("foo", "baz"),
			[]model.SamplePair{{Timestamp: 0, Value: 0}, {Timestamp: 1, Value: 1}, {Timestamp: 2, Value: 2}, {Timestamp: 3, Value: 3}},
			[]mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(4, test.GenerateTestHistogram(4))},
		),
	})

	someMoreSeries := series.NewConcreteSeriesSetFromSortedSeries([]storage.Series{
		series.NewConcreteSeries(
			labels.FromStrings("foo", "qux"),
			[]model.SamplePair{{Timestamp: 0, Value: 0}, {Timestamp: 1, Value: 1}, {Timestamp: 2, Value: 2}, {Timestamp: 3, Value: 3}},
			[]mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(4, test.GenerateTestHistogram(4))},
		),
		series.NewConcreteSeries(
			labels.FromStrings("foo", "quux"),
			[]model.SamplePair{{Timestamp: 0, Value: 0}, {Timestamp: 1, Value: 1}, {Timestamp: 2, Value: 2}, {Timestamp: 3, Value: 3}},
			[]mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(4, test.GenerateTestHistogram(4))},
		),
	})

	testCases := map[string]struct {
		getQuerierErr []error
		seriesSet     []storage.SeriesSet

		expectedStatusCode  int
		expectedContentType string
	}{
		"single query - no error": {
			getQuerierErr: []error{nil},
			seriesSet:     []storage.SeriesSet{someSeries},

			expectedStatusCode: 200,
		},
		"single query - empty series set": {
			getQuerierErr: []error{nil},
			seriesSet:     []storage.SeriesSet{storage.ErrSeriesSet(nil)},

			expectedStatusCode: 200,
		},
		"single query - validation error": {
			getQuerierErr: []error{NewMaxQueryLengthError(time.Hour, time.Minute)},
			seriesSet:     []storage.SeriesSet{someSeries},

			expectedStatusCode:  400,
			expectedContentType: "text/plain; charset=utf-8",
		},
		"single query - validation error while iterating samples": {
			getQuerierErr: []error{nil},
			seriesSet:     []storage.SeriesSet{&partiallyFailingSeriesSet{ss: someSeries, failAfter: 1, err: NewMaxQueryLengthError(time.Hour, time.Minute)}},

			expectedStatusCode:  400,
			expectedContentType: "text/plain; charset=utf-8",
		},
		"single query - promQL storage error": {
			getQuerierErr: []error{promql.ErrStorage{Err: errors.New("cannot reach ingesters")}},
			seriesSet:     []storage.SeriesSet{nil},

			expectedStatusCode:  500,
			expectedContentType: "text/plain; charset=utf-8",
		},
		"single query - promQL storage error while iterating samples": {
			getQuerierErr: []error{nil},
			seriesSet:     []storage.SeriesSet{&partiallyFailingSeriesSet{ss: someSeries, failAfter: 1, err: errors.New("cannot reach ingesters")}},

			expectedStatusCode:  500,
			expectedContentType: "text/plain; charset=utf-8",
		},
		"multiple queries - first query fails": {
			getQuerierErr: []error{promql.ErrStorage{Err: errors.New("cannot reach ingesters")}, nil},
			seriesSet:     []storage.SeriesSet{nil, someSeries},

			expectedStatusCode:  500,
			expectedContentType: "text/plain; charset=utf-8",
		},
		"multiple queries - second query fails": {
			getQuerierErr: []error{nil, promql.ErrStorage{Err: errors.New("cannot reach ingesters")}},
			seriesSet:     []storage.SeriesSet{someSeries, nil},

			expectedStatusCode:  500,
			expectedContentType: "text/plain; charset=utf-8",
		},
		"multiple queries - both succeed": {
			getQuerierErr: []error{nil, nil},
			seriesSet:     []storage.SeriesSet{someSeries, someMoreSeries},

			expectedStatusCode: 200,
		},
	}

	t.Run("samples", func(t *testing.T) {
		for tn, tc := range testCases {
			t.Run(tn, func(t *testing.T) {
				callCount := atomic.NewInt64(0)
				q := &mockSampleAndChunkQueryable{
					queryableFn: func(int64, int64) (storage.Querier, error) {
						currentCall := callCount.Inc() - 1
						if currentCall >= int64(len(tc.getQuerierErr)) {
							return nil, errors.New("unexpected extra query call")
						}

						err := tc.getQuerierErr[currentCall]
						seriesSet := tc.seriesSet[currentCall]

						return mockQuerier{
							selectFn: func(_ context.Context, _ bool, hints *storage.SelectHints, _ ...*labels.Matcher) storage.SeriesSet {
								require.NotNil(t, hints, "select hints must be set")
								return seriesSet
							},
						}, err
					},
				}
				handler := remoteReadHandler(q, 1024*1024, 0, log.NewNopLogger())

				// Create queries based on the number of expected errors/series sets
				var queries []*prompb.Query
				for i := 0; i < len(tc.getQuerierErr); i++ {
					queries = append(queries, &prompb.Query{
						StartTimestampMs: int64(i * 10),
						EndTimestampMs:   int64((i + 1) * 10),
					})
				}

				requestBody, err := proto.Marshal(&prompb.ReadRequest{
					Queries:               queries,
					AcceptedResponseTypes: []prompb.ReadRequest_ResponseType{prompb.ReadRequest_SAMPLES},
				})
				require.NoError(t, err)
				requestBody = snappy.Encode(nil, requestBody)
				request, err := http.NewRequest(http.MethodPost, "/api/v1/read", bytes.NewReader(requestBody))
				require.NoError(t, err)
				request.Header.Set("X-Prometheus-Remote-Read-Version", "0.1.0")

				recorder := httptest.NewRecorder()
				handler.ServeHTTP(recorder, request)

				require.Equal(t, tc.expectedStatusCode, recorder.Result().StatusCode)
				if tc.expectedContentType == "" {
					tc.expectedContentType = "application/x-protobuf"
				}
				require.Equal(t, tc.expectedContentType, recorder.Result().Header.Get("Content-Type"))
			})
		}
	})

	t.Run("streaming_chunks", func(t *testing.T) {
		for tn, tc := range testCases {
			t.Run(tn, func(t *testing.T) {
				callCount := atomic.NewInt64(0)
				q := &mockSampleAndChunkQueryable{
					chunkQueryableFn: func(int64, int64) (storage.ChunkQuerier, error) {
						currentCall := callCount.Inc() - 1
						if currentCall >= int64(len(tc.getQuerierErr)) {
							return nil, errors.New("unexpected extra query call")
						}

						err := tc.getQuerierErr[currentCall]
						seriesSet := tc.seriesSet[currentCall]

						return mockChunkQuerier{
							selectFn: func(_ context.Context, _ bool, _ *storage.SelectHints, _ ...*labels.Matcher) storage.ChunkSeriesSet {
								return storage.NewSeriesSetToChunkSet(seriesSet)
							},
						}, err
					},
				}
				handler := remoteReadHandler(q, 1024*1024, 0, log.NewNopLogger())

				// Create queries based on the number of expected errors/series sets
				var queries []*prompb.Query
				for i := 0; i < len(tc.getQuerierErr); i++ {
					queries = append(queries, &prompb.Query{
						StartTimestampMs: int64(i * 10),
						EndTimestampMs:   int64((i + 1) * 10),
					})
				}

				requestBody, err := proto.Marshal(&prompb.ReadRequest{
					Queries:               queries,
					AcceptedResponseTypes: []prompb.ReadRequest_ResponseType{prompb.ReadRequest_STREAMED_XOR_CHUNKS},
				})
				require.NoError(t, err)
				requestBody = snappy.Encode(nil, requestBody)
				request, err := http.NewRequest(http.MethodPost, "/api/v1/read", bytes.NewReader(requestBody))
				require.NoError(t, err)
				request.Header.Set("X-Prometheus-Remote-Read-Version", "0.1.0")

				recorder := httptest.NewRecorder()
				handler.ServeHTTP(recorder, request)

				require.Equal(t, tc.expectedStatusCode, recorder.Result().StatusCode)
				if tc.expectedContentType == "" {
					tc.expectedContentType = api.ContentTypeRemoteReadStreamedChunks
				}
				require.Equal(t, tc.expectedContentType, recorder.Result().Header.Get("Content-Type"))
			})
		}
	})
}

func TestQueryFromRemoteReadQuery(t *testing.T) {
	tests := map[string]struct {
		query            *prompb.Query
		expectedStart    model.Time
		expectedEnd      model.Time
		expectedMinT     model.Time
		expectedMaxT     model.Time
		expectedMatchers []*labels.Matcher
		expectedHints    *storage.SelectHints
	}{
		"remote read request query without hints": {
			query: &prompb.Query{
				StartTimestampMs: 1000,
				EndTimestampMs:   2000,
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_EQ, Name: model.MetricNameLabel, Value: "metric"},
				},
			},
			expectedStart:    1000,
			expectedEnd:      2000,
			expectedMinT:     1000,
			expectedMaxT:     2000,
			expectedMatchers: []*labels.Matcher{{Type: labels.MatchEqual, Name: model.MetricNameLabel, Value: "metric"}},
			expectedHints: &storage.SelectHints{
				Start: 1000,
				End:   2000,
			},
		},
		"remote read request query with hints": {
			query: &prompb.Query{
				StartTimestampMs: 1000,
				EndTimestampMs:   2000,
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_EQ, Name: model.MetricNameLabel, Value: "metric"},
				},
				Hints: &prompb.ReadHints{
					StartMs: 500,
					EndMs:   1500,
				},
			},
			expectedStart:    1000,
			expectedEnd:      2000,
			expectedMinT:     500,
			expectedMaxT:     1500,
			expectedMatchers: []*labels.Matcher{{Type: labels.MatchEqual, Name: model.MetricNameLabel, Value: "metric"}},
			expectedHints: &storage.SelectHints{
				Start: 500,
				End:   1500,
			},
		},
		"remote read request query with zero-value hints": {
			query: &prompb.Query{
				StartTimestampMs: 1000,
				EndTimestampMs:   2000,
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_EQ, Name: model.MetricNameLabel, Value: "metric"},
				},
				Hints: &prompb.ReadHints{},
			},
			expectedStart:    1000,
			expectedEnd:      2000,
			expectedMinT:     1000,
			expectedMaxT:     2000,
			expectedMatchers: []*labels.Matcher{{Type: labels.MatchEqual, Name: model.MetricNameLabel, Value: "metric"}},
			expectedHints: &storage.SelectHints{
				// Fallback to start/end time range given the read hints are zero values.
				Start: 1000,
				End:   2000,
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			actualStart, actualEnd, actualMinT, actualMaxT, actualMatchers, actualHints, err := queryFromRemoteReadQuery(testData.query)
			require.NoError(t, err)
			require.Equal(t, testData.expectedStart, actualStart)
			require.Equal(t, testData.expectedEnd, actualEnd)
			require.Equal(t, testData.expectedMinT, actualMinT)
			require.Equal(t, testData.expectedMaxT, actualMaxT)
			require.Equal(t, testData.expectedMatchers, actualMatchers)
			require.Equal(t, testData.expectedHints, actualHints)
		})
	}
}

func TestRemoteReadHandler_ConcurrencyLimit(t *testing.T) {
	concurrentQueries := atomic.NewInt32(0)
	controlChan := make(chan struct{})

	// Mock queryable that waits for control signal
	q := mockSampleAndChunkQueryable{
		queryableFn: func(mint, maxt int64) (storage.Querier, error) {
			return &mockQuerier{
				selectFn: func(ctx context.Context, sorted bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
					concurrentQueries.Inc()
					defer concurrentQueries.Dec()
					<-controlChan

					// Return a simple series set
					return series.NewConcreteSeriesSetFromUnsortedSeries(
						[]storage.Series{
							series.NewConcreteSeries(labels.FromStrings("foo", "bar"), []model.SamplePair{{Timestamp: 1, Value: 1.0}}, nil),
						},
					)
				},
			}, nil
		},
	}

	tests := []struct {
		name                  string
		queries               int
		maxConcurrency        int
		expectedMaxConcurrent int32
	}{
		{
			name:                  "unlimited concurrency (0) allows all queries to run concurrently",
			queries:               5,
			maxConcurrency:        0,
			expectedMaxConcurrent: 5,
		},
		{
			name:                  "concurrency limit of 2 restricts to 2 concurrent queries",
			queries:               5,
			maxConcurrency:        2,
			expectedMaxConcurrent: 2,
		},
		{
			name:                  "concurrency limit of 1 serializes all queries",
			queries:               3,
			maxConcurrency:        1,
			expectedMaxConcurrent: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset counters and ensure control channel is empty
			concurrentQueries.Store(0)
			controlChan = make(chan struct{})

			// Create handler with configurable concurrency
			handler := RemoteReadHandler(q, log.NewNopLogger(), Config{MaxConcurrentRemoteReadQueries: tt.maxConcurrency})

			// Create multiple queries
			queries := make([]*prompb.Query, tt.queries)
			for i := 0; i < tt.queries; i++ {
				queries[i] = &prompb.Query{
					StartTimestampMs: 1,
					EndTimestampMs:   10,
					Matchers: []*prompb.LabelMatcher{
						{Type: prompb.LabelMatcher_EQ, Name: model.MetricNameLabel, Value: "test_metric"},
					},
				}
			}

			requestBody, err := proto.Marshal(&prompb.ReadRequest{Queries: queries})
			require.NoError(t, err)
			requestBody = snappy.Encode(nil, requestBody)

			request, err := http.NewRequest(http.MethodPost, "/api/v1/read", bytes.NewReader(requestBody))
			require.NoError(t, err)
			request.Header.Add("Content-Encoding", "snappy")
			request.Header.Set("Content-Type", "application/x-protobuf")

			response := httptest.NewRecorder()

			// Start request in goroutine
			done := make(chan struct{})
			go func() {
				defer close(done)
				handler.ServeHTTP(response, request)
			}()

			// Wait for expected concurrency to be reached and verify limits
			maxObserved := int32(0)
			require.Eventually(t, func() bool {
				current := concurrentQueries.Load()
				maxObserved = max(current, maxObserved)

				// Check that we never exceed the expected limit
				require.LessOrEqualf(t, current, tt.expectedMaxConcurrent,
					"concurrent queries (%d) exceeded limit (%d)", current, tt.expectedMaxConcurrent)

				// Return true when we've reached the expected concurrency
				return current == tt.expectedMaxConcurrent
			}, 10*time.Second, 100*time.Millisecond, "failed to reach expected concurrency level")

			// Release all queries by sending signals to control channel
			for i := 0; i < tt.queries; i++ {
				controlChan <- struct{}{}
			}

			// Wait for request to complete
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for request to complete")
			}

			// Verify response is successful
			require.Equal(t, http.StatusOK, response.Code)

			// Verify we observed the expected maximum concurrency
			require.Equal(t, tt.expectedMaxConcurrent, maxObserved,
				"expected max concurrent queries: %d, observed: %d", tt.expectedMaxConcurrent, maxObserved)

			// Verify control channel is empty (all signals consumed)
			require.Equal(t, 0, len(controlChan), "control channel should be empty")
		})
	}
}
