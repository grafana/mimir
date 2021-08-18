// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/querysharding_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queryrange

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util"
)

func TestQueryshardingMiddleware(t *testing.T) {
	testExpr := []struct {
		name     string
		next     Handler
		input    Request
		ctx      context.Context
		expected *PrometheusResponse
		err      bool
		override func(*testing.T, Handler)
	}{
		{
			name: "invalid query error",
			// if the query parses correctly force it to succeed
			next: mockHandlerWith(&PrometheusResponse{
				Status: "",
				Data: PrometheusData{
					ResultType: string(parser.ValueTypeVector),
					Result:     []SampleStream{},
				},
				ErrorType: "",
				Error:     "",
			}, nil),
			input:    &PrometheusRequest{Query: "^GARBAGE"},
			ctx:      context.Background(),
			expected: nil,
			err:      true,
		},
		{
			name:     "downstream err",
			next:     mockHandlerWith(nil, errors.Errorf("some err")),
			input:    defaultReq(),
			ctx:      context.Background(),
			expected: nil,
			err:      true,
		},
		{
			name: "successful trip",
			next: mockHandlerWith(sampleMatrixResponse(), nil),
			override: func(t *testing.T, handler Handler) {
				// pre-encode the query so it doesn't try to re-split. We're just testing if it passes through correctly
				qry := defaultReq().WithQuery(
					`__embedded_queries__{__cortex_queries__="{\"Concat\":[\"http_requests_total{cluster=\\\"prod\\\"}\"]}"}`,
				)
				out, err := handler.Do(context.Background(), qry)
				require.Nil(t, err)
				require.Equal(t, string(parser.ValueTypeMatrix), out.(*PrometheusResponse).Data.ResultType)
				require.Equal(t, sampleMatrixResponse(), out)
			},
		},
	}

	for _, c := range testExpr {
		t.Run(c.name, func(t *testing.T) {
			engine := promql.NewEngine(promql.EngineOpts{
				Logger:     log.NewNopLogger(),
				Reg:        nil,
				MaxSamples: 1000,
				Timeout:    time.Minute,
			})

			handler := NewQueryShardingMiddleware(
				log.NewNopLogger(),
				engine,
				3,
				nil,
			).Wrap(c.next)

			// escape hatch for custom tests
			if c.override != nil {
				c.override(t, handler)
				return
			}

			out, err := handler.Do(c.ctx, c.input)

			if c.err {
				require.NotNil(t, err)
			} else {
				require.Nil(t, err)
				require.Equal(t, c.expected, out)
			}
		})
	}
}

func sampleMatrixResponse() *PrometheusResponse {
	return &PrometheusResponse{
		Status: StatusSuccess,
		Data: PrometheusData{
			ResultType: string(parser.ValueTypeMatrix),
			Result: []SampleStream{
				{
					Labels: []mimirpb.LabelAdapter{
						{Name: "a", Value: "a1"},
						{Name: "b", Value: "b1"},
					},
					Samples: []mimirpb.Sample{
						{
							TimestampMs: 5,
							Value:       1,
						},
						{
							TimestampMs: 10,
							Value:       2,
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
							TimestampMs: 5,
							Value:       8,
						},
						{
							TimestampMs: 10,
							Value:       9,
						},
					},
				},
			},
		},
	}
}

func mockHandlerWith(resp *PrometheusResponse, err error) Handler {
	return HandlerFunc(func(ctx context.Context, req Request) (Response, error) {
		if expired := ctx.Err(); expired != nil {
			return nil, expired
		}

		return resp, err
	})
}

func defaultReq() *PrometheusRequest {
	return &PrometheusRequest{
		Path:    "/query_range",
		Start:   00,
		End:     10,
		Step:    5,
		Timeout: time.Minute,
		Query:   `sum(rate(http_requests_total{}[5m]))`,
	}
}

// approximatelyEquals ensures two responses are approximately equal, up to 6 decimals precision per sample
func approximatelyEquals(t *testing.T, a, b *PrometheusResponse) {
	// Ensure both queries succeeded.
	require.Equal(t, StatusSuccess, a.Status)
	require.Equal(t, StatusSuccess, b.Status)

	as, err := ResponseToSamples(a)
	require.Nil(t, err)
	bs, err := ResponseToSamples(b)
	require.Nil(t, err)

	require.Equal(t, len(as), len(bs), "expected same number of series")

	for i := 0; i < len(as); i++ {
		a := as[i]
		b := bs[i]
		require.Equal(t, a.Labels, b.Labels)
		require.Equal(t, len(a.Samples), len(b.Samples), "expected same number of samples")

		for j := 0; j < len(a.Samples); j++ {
			aSample := &a.Samples[j]
			aSample.Value = math.Round(aSample.Value*1e6) / 1e6
			bSample := &b.Samples[j]
			bSample.Value = math.Round(bSample.Value*1e6) / 1e6
		}
		require.Equal(t, a, b)
	}
}

func TestQueryShardingCorrectness(t *testing.T) {
	var (
		numSeries        = 1000
		numHistograms    = 100
		histogramBuckets = []float64{1.0, 2.0, 4.0, 10.0, 100.0, math.Inf(1)}
	)

	tests := map[string]struct {
		query string
	}{
		"sum() no grouping": {
			query: `sum(metric_counter)`,
		},
		"sum() grouping 'by'": {
			query: `sum by(group_1) (metric_counter)`,
		},
		"sum() grouping 'without'": {
			query: `sum without(unique) (metric_counter)`,
		},
		"sum(rate()) no grouping": {
			query: `sum(rate(metric_counter[1m]))`,
		},
		"sum(rate()) grouping 'by'": {
			query: `sum by(group_1) (rate(metric_counter[1m]))`,
		},
		"sum(rate()) grouping 'without'": {
			query: `sum without(unique) (rate(metric_counter[1m]))`,
		},
		"histogram_quantile() no grouping": {
			query: `histogram_quantile(0.5, sum by(le) (rate(metric_histogram_bucket[1m])))`,
		},
		"histogram_quantile() grouping 'by'": {
			query: `histogram_quantile(0.5, sum by(group_1, le) (rate(metric_histogram_bucket[1m])))`,
		},
		"histogram_quantile() grouping 'without'": {
			query: `histogram_quantile(0.5, sum without(group_1, group_2, unique) (rate(metric_histogram_bucket[1m])))`,
		},
		"min() no grouping": {
			query: `min(metric_counter{group_1="0"})`,
		},
		"min() grouping 'by'": {
			query: `min by(group_2) (metric_counter{group_1="0"})`,
		},
		"min() grouping 'without'": {
			query: `min without(unique) (metric_counter{group_1="0"})`,
		},
		"max() no grouping": {
			query: `max(metric_counter{group_1="0"})`,
		},
		"max() grouping 'by'": {
			query: `max by(group_2) (metric_counter{group_1="0"})`,
		},
		"max() grouping 'without'": {
			query: `max without(unique) (metric_counter{group_1="0"})`,
		},
		"count() no grouping": {
			query: `count(metric_counter)`,
		},
		"count() grouping 'by'": {
			query: `count by(group_2) (metric_counter)`,
		},
		"count() grouping 'without'": {
			query: `count without(unique) (metric_counter)`,
		},
		"sum(count())": {
			query: `sum(count by(group_1) (metric_counter))`,
		},
		"avg() no grouping": {
			query: `avg(metric_counter)`,
		},
		"avg() grouping 'by'": {
			query: `avg by(group_2) (metric_counter)`,
		},
		"avg() grouping 'without'": {
			query: `avg without(unique) (metric_counter)`,
		},
		"sum(min_over_time())": {
			query: `sum by (group_1, group_2) (min_over_time(metric_counter{const="fixed"}[2m]))`,
		},
		"sum(max_over_time())": {
			query: `sum by (group_1, group_2) (max_over_time(metric_counter{const="fixed"}[2m]))`,
		},
		"sum(avg_over_time())": {
			query: `sum by (group_1, group_2) (avg_over_time(metric_counter{const="fixed"}[2m]))`,
		},
		"or": {
			query: `sum(rate(metric_counter{group_1="0"}[1m])) or sum(rate(metric_counter{group_1="1"}[1m]))`,
		},
		"and": {
			query: `
				sum without(unique) (rate(metric_counter{group_1="0"}[1m]))
				and
				max without(unique) (metric_counter) > 0`,
		},
		"sum(rate()) > avg(rate())": {
			query: `
				sum(rate(metric_counter[1m]))
				>
				avg(rate(metric_counter[1m]))`,
		},
		"nested count()": {
			query: `sum(
				  count(
				    count(metric_counter) by (group_1, group_2)
				  ) by (group_1)
				)`,
		},

		//
		// The following queries are not expected to be shardable.
		//
		"stddev()": {
			query: `stddev(metric_counter{const="fixed"})`,
		},
		"stdvar()": {
			query: `stdvar(metric_counter{const="fixed"})`,
		},
	}

	// Generate fixtures (series).
	series := make([]*promql.StorageSeries, 0, numSeries+(numHistograms*len(histogramBuckets)))

	for i := 0; i < numSeries; i++ {
		series = append(series, newSeries(
			labels.Labels{
				{Name: "__name__", Value: "metric_counter"},    // Same metric name for all series.
				{Name: "const", Value: "fixed"},                // A constant label.
				{Name: "unique", Value: strconv.Itoa(i)},       // A unique label.
				{Name: "group_1", Value: strconv.Itoa(i % 10)}, // A first grouping label.
				{Name: "group_2", Value: strconv.Itoa(i % 5)},  // A second grouping label.
			},
			factor(float64(i)*0.1)))
	}

	for i := numSeries; i < numSeries+numHistograms; i++ {
		for bucketIdx, bucketLe := range histogramBuckets {
			series = append(series, newSeries(
				labels.Labels{
					{Name: "__name__", Value: "metric_histogram_bucket"}, // Same metric name for all series.
					{Name: "le", Value: fmt.Sprintf("%f", bucketLe)},
					{Name: "const", Value: "fixed"},                // A constant label.
					{Name: "unique", Value: strconv.Itoa(i)},       // A unique label.
					{Name: "group_1", Value: strconv.Itoa(i % 10)}, // A first grouping label.
					{Name: "group_2", Value: strconv.Itoa(i % 5)},  // A second grouping label.
				},
				// We expect each bucket to have a value higher than the previous one.
				factor(float64(i)*float64(bucketIdx)*0.1)))
		}
	}

	// Create a queryable on the fixtures.
	queryable := storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
		return &querierMock{
			series: series,
		}, nil
	})

	for testName, testData := range tests {
		for _, numShards := range []int{2, 4, 8, 16} {
			t.Run(fmt.Sprintf("%s (shards: %d)", testName, numShards), func(t *testing.T) {
				req := &PrometheusRequest{
					Path:  "/query_range",
					Start: util.TimeToMillis(start),
					End:   util.TimeToMillis(end),
					Step:  step.Milliseconds(),
				}

				shardingware := NewQueryShardingMiddleware(
					log.NewNopLogger(),
					engine,
					numShards,
					nil,
				)
				downstream := &downstreamHandler{
					engine:    engine,
					queryable: queryable,
				}
				r := req.WithQuery(testData.query)

				// Run the query without sharding.
				expectedRes, err := downstream.Do(context.Background(), r)
				require.Nil(t, err)

				// Ensure the query produces some results.
				require.NotEmpty(t, expectedRes.(*PrometheusResponse).Data.Result)

				// Run the query with sharding.
				shardedRes, err := shardingware.Wrap(downstream).Do(context.Background(), r)
				require.Nil(t, err)

				// Ensure the two results matches (float precision can slightly differ, there's no guarantee in PromQL engine too
				// if you rerun the same query twice).
				approximatelyEquals(t, expectedRes.(*PrometheusResponse), shardedRes.(*PrometheusResponse))
			})
		}
	}
}

func BenchmarkQuerySharding(b *testing.B) {
	var shards []int

	// max out at half available cpu cores in order to minimize noisy neighbor issues while benchmarking
	for shard := 1; shard <= runtime.NumCPU()/2; shard = shard * 2 {
		shards = append(shards, shard)
	}

	for _, tc := range []struct {
		labelBuckets     int
		labels           []string
		samplesPerSeries int
		query            string
		desc             string
	}{
		// Ensure you have enough cores to run these tests without blocking.
		// We want to simulate parallel computations and waiting in queue doesn't help

		// no-group
		{
			labelBuckets:     16,
			labels:           []string{"a", "b", "c"},
			samplesPerSeries: 100,
			query:            `sum(rate(http_requests_total[5m]))`,
			desc:             "sum nogroup",
		},
		// sum by
		{
			labelBuckets:     16,
			labels:           []string{"a", "b", "c"},
			samplesPerSeries: 100,
			query:            `sum by(a) (rate(http_requests_total[5m]))`,
			desc:             "sum by",
		},
		// sum without
		{
			labelBuckets:     16,
			labels:           []string{"a", "b", "c"},
			samplesPerSeries: 100,
			query:            `sum without (a) (rate(http_requests_total[5m]))`,
			desc:             "sum without",
		},
	} {
		for _, delayPerSeries := range []time.Duration{
			0,
			time.Millisecond / 10,
		} {
			engine := promql.NewEngine(promql.EngineOpts{
				Logger:     log.NewNopLogger(),
				Reg:        nil,
				MaxSamples: 100000000,
				Timeout:    time.Minute,
			})

			queryable := NewMockShardedQueryable(
				tc.samplesPerSeries,
				tc.labels,
				tc.labelBuckets,
				delayPerSeries,
			)
			downstream := &downstreamHandler{
				engine:    engine,
				queryable: queryable,
			}

			var (
				start int64 = 0
				end         = int64(1000 * tc.samplesPerSeries)
				step        = (end - start) / 1000
			)

			req := &PrometheusRequest{
				Path:    "/query_range",
				Start:   start,
				End:     end,
				Step:    step,
				Timeout: time.Minute,
				Query:   tc.query,
			}

			for _, shardFactor := range shards {
				shardingware := NewQueryShardingMiddleware(
					log.NewNopLogger(),
					engine,
					shardFactor,
					nil,
				).Wrap(downstream)

				b.Run(
					fmt.Sprintf(
						"desc:[%s]---shards:[%d]---series:[%.0f]---delayPerSeries:[%s]---samplesPerSeries:[%d]",
						tc.desc,
						shardFactor,
						math.Pow(float64(tc.labelBuckets), float64(len(tc.labels))),
						delayPerSeries,
						tc.samplesPerSeries,
					),
					func(b *testing.B) {
						for n := 0; n < b.N; n++ {
							_, err := shardingware.Do(
								context.Background(),
								req,
							)
							if err != nil {
								b.Fatal(err.Error())
							}
						}
					},
				)
			}
			fmt.Println()
		}

		fmt.Print("--------------------------------\n\n")
	}
}

type downstreamHandler struct {
	engine    *promql.Engine
	queryable storage.Queryable
}

func (h *downstreamHandler) Do(ctx context.Context, r Request) (Response, error) {
	qry, err := h.engine.NewRangeQuery(
		h.queryable,
		r.GetQuery(),
		util.TimeFromMillis(r.GetStart()),
		util.TimeFromMillis(r.GetEnd()),
		time.Duration(r.GetStep())*time.Millisecond,
	)
	if err != nil {
		return nil, err
	}

	res := qry.Exec(ctx)
	extracted, err := FromResult(res)
	if err != nil {
		return nil, err
	}

	return &PrometheusResponse{
		Status: StatusSuccess,
		Data: PrometheusData{
			ResultType: string(res.Value.Type()),
			Result:     extracted,
		},
	}, nil
}
