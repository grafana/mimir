// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
)

func TestOTLPDeltaMetrics(t *testing.T) {
	md := otlpDeltaMetrics("run", time.Unix(120, 0))
	ms := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
	require.Equal(t, 2, ms.Len())
	require.Equal(t, otlpDeltaCounter, ms.At(0).Name())
	require.True(t, ms.At(0).Sum().IsMonotonic())
	require.Equal(t, pmetric.AggregationTemporalityDelta, ms.At(0).Sum().AggregationTemporality())
	require.Equal(t, pmetric.AggregationTemporalityDelta, ms.At(1).ExponentialHistogram().AggregationTemporality())

	for i, value := range []int64{5, 1, 2} {
		p := ms.At(0).Sum().DataPoints().At(i)
		require.Equal(t, value, p.IntValue())
		require.Equal(t, time.Unix(int64(80+i*20), 0).UTC(), p.Timestamp().AsTime())
		require.Equal(t, 20*time.Second, p.Timestamp().AsTime().Sub(p.StartTimestamp().AsTime()))

		label, ok := p.Attributes().Get("test_run_id")
		require.True(t, ok)
		require.Equal(t, "run", label.Str())

		h := ms.At(1).ExponentialHistogram().DataPoints().At(i)
		require.Equal(t, p.Timestamp(), h.Timestamp())
		require.Equal(t, p.StartTimestamp(), h.StartTimestamp())
		require.Equal(t, uint64(value+1), h.Count())
		require.Equal(t, float64(2*value), h.Sum())
		require.Equal(t, uint64(1), h.ZeroCount())
		require.Equal(t, []uint64{uint64(value)}, h.Positive().BucketCounts().AsRaw())
	}
}

func TestCheckOTLPDeltaResult(t *testing.T) {
	start := time.Unix(80, 0)
	floats := func() model.Matrix {
		return model.Matrix{
			&model.SampleStream{
				Values: []model.SamplePair{
					{Timestamp: 80000, Value: 5},
					{Timestamp: 100000, Value: 1},
					{Timestamp: 120000, Value: 2},
				},
			},
		}
	}
	require.NoError(t, checkOTLPDeltaResult(floats(), start, false, false))

	// Cumulative conversion must fail even when all sample timestamps are correct.
	cumulative := floats()
	cumulative[0].Values[1].Value = 6
	require.ErrorContains(t, checkOTLPDeltaResult(cumulative, start, false, false), "expected 1")

	missing := floats()
	missing[0].Values = missing[0].Values[:2]
	require.EqualError(t, checkOTLPDeltaResult(missing, start, false, false), "expected three float samples, got 2 floats and 0 histograms")

	duplicate := append(floats(), floats()...)
	require.EqualError(t, checkOTLPDeltaResult(duplicate, start, false, false), "expected one series, got 2")

	timestamps := floats()
	for i, value := range []model.SampleValue{80, 100, 120} {
		timestamps[0].Values[i].Value = value
	}
	require.NoError(t, checkOTLPDeltaResult(timestamps, start, false, true))

	// A missing sample can fall back to an older sample through PromQL lookback.
	timestamps[0].Values[1].Value = 80
	require.ErrorContains(t, checkOTLPDeltaResult(timestamps, start, false, true), "expected 100")

	hist := model.Matrix{
		&model.SampleStream{
			Histograms: []model.SampleHistogramPair{
				{Timestamp: 80000, Histogram: &model.SampleHistogram{Count: 6, Sum: 10}},
				{Timestamp: 100000, Histogram: &model.SampleHistogram{Count: 2, Sum: 2}},
				{Timestamp: 120000, Histogram: &model.SampleHistogram{Count: 3, Sum: 4}},
			},
		},
	}
	require.NoError(t, checkOTLPDeltaResult(hist, start, true, false))

	hist[0].Histograms[1].Histogram.Count = 8
	require.ErrorContains(t, checkOTLPDeltaResult(hist, start, true, false), "expected count=2 sum=2")
}

func TestOTLPDeltaConfig(t *testing.T) {
	var cfg OTLPDeltaTestConfig
	flagext.DefaultValues(&cfg)
	require.NoError(t, cfg.Validate())

	cfg.Enabled = true
	require.EqualError(t, cfg.Validate(), "tests.otlp-delta-test.write-endpoint must be an absolute HTTP(S) URL without credentials or a fragment")

	cfg.WriteEndpoint = "http://alloy:4318/v1/metrics"
	require.NoError(t, cfg.Validate())

	cfg.HistoricalAge = cfg.MaxQueryAge
	require.EqualError(t, cfg.Validate(), "tests.otlp-delta-test.max-query-age must be greater than historical-age, and historical-age must be positive")
}

type deltaTestClient struct {
	writeErr error
	writes   int
	queries  []time.Time
	missing  bool
}

func (c *deltaTestClient) WriteOTLPMetrics(context.Context, string, pmetric.Metrics) (int, error) {
	c.writes++
	return 200, c.writeErr
}

func (c *deltaTestClient) QueryRange(_ context.Context, q string, start, end time.Time, step time.Duration, options ...RequestOption) (model.Matrix, error) {
	c.queries = append(c.queries, end)
	if c.missing {
		return nil, nil
	}
	s := &model.SampleStream{}
	for ts := start; !ts.After(end); ts = ts.Add(step) {
		v := float64(ts.Unix()/20%5 + 1)
		if strings.HasPrefix(q, "timestamp(") {
			s.Values = append(s.Values, model.SamplePair{
				Timestamp: model.Time(ts.UnixMilli()),
				Value:     model.SampleValue(ts.Unix()),
			})
		} else if strings.Contains(q, otlpDeltaHistogram) {
			s.Histograms = append(s.Histograms, model.SampleHistogramPair{
				Timestamp: model.Time(ts.UnixMilli()),
				Histogram: &model.SampleHistogram{
					Count: model.FloatString(v + 1),
					Sum:   model.FloatString(v * 2),
				},
			})
		} else {
			s.Values = append(s.Values, model.SamplePair{
				Timestamp: model.Time(ts.UnixMilli()),
				Value:     model.SampleValue(v),
			})
		}
	}

	return model.Matrix{s}, nil
}

func TestOTLPDeltaTestRun(t *testing.T) {
	var cfg OTLPDeltaTestConfig
	flagext.DefaultValues(&cfg)

	cfg.Enabled = true

	cfg.WriteEndpoint = "http://alloy:4318/v1/metrics"
	cfg.HistoricalAge = time.Minute
	cfg.MaxQueryAge = 5 * time.Minute

	c := &deltaTestClient{}
	test := NewOTLPDeltaTest(cfg, c, log.NewNopLogger(), prometheus.NewPedanticRegistry())
	now := time.Unix(140, 0)
	require.NoError(t, test.Init(t.Context(), now))
	require.NoError(t, test.Run(t.Context(), now))
	require.Equal(t, 1, c.writes)
	require.Len(t, c.queries, 4)

	require.NoError(t, test.Run(t.Context(), now.Add(20*time.Second)))
	require.Equal(t, 1, c.writes, "do not send overlapping batches")

	require.NoError(t, test.Run(t.Context(), now.Add(time.Minute)))
	require.Equal(t, 2, c.writes)
	require.Len(t, c.queries, 12)
	require.Equal(t, time.Unix(120, 0).UTC(), c.queries[11], "query old samples after the historical age")

	require.NoError(t, test.Run(t.Context(), now.Add(10*time.Minute)))
	require.Len(t, test.history, 1, "discard expired history")
}

func TestOTLPDeltaTestFailuresAndDisabled(t *testing.T) {
	var cfg OTLPDeltaTestConfig
	flagext.DefaultValues(&cfg)

	c := &deltaTestClient{}
	test := NewOTLPDeltaTest(cfg, c, log.NewNopLogger(), prometheus.NewPedanticRegistry())
	require.NoError(t, test.Init(t.Context(), time.Now()))
	require.NoError(t, test.Run(t.Context(), time.Now()))
	require.Zero(t, c.writes)

	cfg.Enabled = true

	cfg.WriteEndpoint = "http://alloy:4318/v1/metrics"
	cfg.ReadinessTimeout = 10 * time.Millisecond
	test = NewOTLPDeltaTest(cfg, c, log.NewNopLogger(), prometheus.NewPedanticRegistry())
	require.NoError(t, test.Init(t.Context(), time.Now()))

	c.writeErr = errors.New("rejected")
	require.ErrorIs(t, test.Run(t.Context(), time.Now()), c.writeErr)
	require.Empty(t, c.queries)
	require.Empty(t, test.history)

	c.writeErr = nil
	c.missing = true
	require.ErrorContains(t, test.Run(t.Context(), time.Now()), "expected one series, got 0")
}

func TestClientWriteOTLPMetrics(t *testing.T) {
	for _, tc := range []struct {
		name      string
		response  string
		status    int
		wantError string
	}{
		{
			name:   "success",
			status: 200,
		},
		{
			name:      "partial success",
			status:    200,
			response:  "partial",
			wantError: "OTLP partial success: rejected 1 data points: dropped",
		},
		{
			name:      "invalid protobuf",
			status:    200,
			response:  "invalid",
			wantError: "decode OTLP response",
		},
		{
			name:      "rejection",
			status:    400,
			response:  "rejected",
			wantError: "HTTP status 400",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			received := make(chan pmetricotlp.ExportRequest, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/metrics" || r.Header.Get("X-Scope-OrgID") != "delta-tenant" {
					t.Error("unexpected path or tenant")
				}

				gz, err := gzip.NewReader(r.Body)
				if err != nil {
					t.Error(err)
					w.WriteHeader(500)
					return
				}

				body, err := io.ReadAll(gz)
				if err != nil {
					t.Error(err)
					w.WriteHeader(500)
					return
				}
				if err := gz.Close(); err != nil {
					t.Error(err)
				}

				req := pmetricotlp.NewExportRequest()
				if err := req.UnmarshalProto(body); err != nil {
					t.Error(err)
					w.WriteHeader(500)
					return
				}

				received <- req
				w.WriteHeader(tc.status)

				var response []byte
				// HTTP 200 can still report rejected data points in an OTLP response.
				if tc.response == "partial" {
					resp := pmetricotlp.NewExportResponse()
					resp.PartialSuccess().SetRejectedDataPoints(1)
					resp.PartialSuccess().SetErrorMessage("dropped")
					response, err = resp.MarshalProto()
					if err != nil {
						t.Error(err)
						return
					}
				} else {
					response = []byte(tc.response)
				}

				if _, err := w.Write(response); err != nil {
					t.Error(err)
				}
			}))
			defer server.Close()

			var cfg ClientConfig
			flagext.DefaultValues(&cfg)
			cfg.TenantID = "delta-tenant"
			require.NoError(t, cfg.WriteBaseEndpoint.Set(server.URL))
			require.NoError(t, cfg.ReadBaseEndpoint.Set(server.URL))

			client, err := NewClient(cfg, log.NewNopLogger(), prometheus.NewPedanticRegistry())
			require.NoError(t, err)

			status, err := client.WriteOTLPMetrics(t.Context(), server.URL+"/v1/metrics", otlpDeltaMetrics("run", time.Unix(120, 0)))
			require.Equal(t, tc.status, status)
			if tc.wantError == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.wantError)
			}

			select {
			case req := <-received:
				require.Equal(t,
					pmetric.AggregationTemporalityDelta,
					req.Metrics().ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Sum().AggregationTemporality(),
				)
			default:
				t.Fatal("server received no metrics")
			}
		})
	}
}
