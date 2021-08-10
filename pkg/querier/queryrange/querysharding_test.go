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
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/astmapper"
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
	require.Equal(t, a.Status, b.Status)
	if a.Status != StatusSuccess {
		return
	}
	as, err := ResponseToSamples(a)
	require.Nil(t, err)
	bs, err := ResponseToSamples(b)
	require.Nil(t, err)

	require.Equal(t, len(as), len(bs))

	for i := 0; i < len(as); i++ {
		a := as[i]
		b := bs[i]
		require.Equal(t, a.Labels, b.Labels)
		require.Equal(t, len(a.Samples), len(b.Samples))

		for j := 0; j < len(a.Samples); j++ {
			aSample := &a.Samples[j]
			aSample.Value = math.Round(aSample.Value*1e6) / 1e6
			bSample := &b.Samples[j]
			bSample.Value = math.Round(bSample.Value*1e6) / 1e6
		}
		require.Equal(t, a, b)
	}
}

func TestQueryshardingCorrectness(t *testing.T) {
	req := &PrometheusRequest{
		Path:  "/query_range",
		Start: util.TimeToMillis(start),
		End:   util.TimeToMillis(end),
		Step:  int64(step) / int64(time.Second),
	}
	for _, tc := range []struct {
		desc   string
		query  string
		mapped string
	}{
		{
			desc:   "fully encoded histogram_quantile",
			query:  `histogram_quantile(0.5, rate(bar1{baz="blip"}[30s]))`,
			mapped: `__embedded_queries__{__cortex_queries__="{\"Concat\":[\"histogram_quantile(0.5, rate(bar1{baz=\\\"blip\\\"}[30s]))\"]}"}`,
		},
		{
			desc:   "entire query with shard summer",
			query:  `sum by (foo,bar) (min_over_time(bar1{baz="blip"}[1m]))`,
			mapped: `sum by(foo, bar) (__embedded_queries__{__cortex_queries__="{\"Concat\":[\"sum by(foo, bar, __cortex_shard__) (min_over_time(bar1{__cortex_shard__=\\\"0_of_2\\\",baz=\\\"blip\\\"}[1m]))\",\"sum by(foo, bar, __cortex_shard__) (min_over_time(bar1{__cortex_shard__=\\\"1_of_2\\\",baz=\\\"blip\\\"}[1m]))\"]}"})`,
		},
		{
			desc:   "shard one leg encode the other",
			query:  "sum(rate(bar1[1m])) or rate(bar1[1m])",
			mapped: `sum without(__cortex_shard__) (__embedded_queries__{__cortex_queries__="{\"Concat\":[\"sum by(__cortex_shard__) (rate(bar1{__cortex_shard__=\\\"0_of_2\\\"}[1m]))\",\"sum by(__cortex_shard__) (rate(bar1{__cortex_shard__=\\\"1_of_2\\\"}[1m]))\"]}"}) or __embedded_queries__{__cortex_queries__="{\"Concat\":[\"rate(bar1[1m])\"]}"}`,
		},
		{
			desc:   "should skip encoding leaf scalar/strings",
			query:  `histogram_quantile(0.5, sum(rate(cortex_cache_value_size_bytes_bucket[5m])) by (le))`,
			mapped: `histogram_quantile(0.5, sum by(le) (__embedded_queries__{__cortex_queries__="{\"Concat\":[\"sum by(le, __cortex_shard__) (rate(cortex_cache_value_size_bytes_bucket{__cortex_shard__=\\\"0_of_2\\\"}[5m]))\",\"sum by(le, __cortex_shard__) (rate(cortex_cache_value_size_bytes_bucket{__cortex_shard__=\\\"1_of_2\\\"}[5m]))\"]}"}))`,
		},
		{
			desc: "ensure sharding sub aggregations are skipped to avoid non-associative series merging across shards",
			query: `sum(
				  count(
				    count(
				      bar1
				    )  by (drive,instance)
				  )  by (instance)
				)`,
			mapped: `__embedded_queries__{__cortex_queries__="{\"Concat\":[\"sum(count by(instance) (count by(drive, instance) (bar1)))\"]}"}`,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			shardingware := NewQueryShardingMiddleware(
				log.NewNopLogger(),
				engine,
				2,
				nil,
			)

			// todo(ctovena): move this to its own package and test.
			mapper, err := astmapper.NewSharding(2, nil)
			require.Nil(t, err)
			expr, err := parser.ParseExpr(tc.query)
			require.Nil(t, err)
			mapped, err := mapper.Map(expr)
			require.Nil(t, err)
			require.Equal(t, tc.mapped, mapped.String())

			downstream := &downstreamHandler{
				engine:    engine,
				queryable: shardAwareQueryable,
			}

			r := req.WithQuery(tc.query)
			shardedRes, err := shardingware.Wrap(downstream).Do(context.Background(), r)
			require.Nil(t, err)

			res, err := downstream.Do(context.Background(), r)
			require.Nil(t, err)

			approximatelyEquals(t, res.(*PrometheusResponse), shardedRes.(*PrometheusResponse))
		})
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
