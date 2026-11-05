// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
)

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
				if r.URL.Path != "/mimir/otlp/v1/metrics" || r.Header.Get("X-Scope-OrgID") != "delta-tenant" {
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
			require.NoError(t, cfg.WriteBaseEndpoint.Set(server.URL+"/mimir"))
			require.NoError(t, cfg.ReadBaseEndpoint.Set(server.URL))

			client, err := NewClient(cfg, log.NewNopLogger(), prometheus.NewPedanticRegistry())
			require.NoError(t, err)

			status, err := (&otlpDeltaWriter{
				writer: client.otlpClient,
			}).sendWriteRequest(t.Context(), &prompb.WriteRequest{
				Timeseries: generateSineWaveSeries(otlpDeltaCounter, time.Unix(120, 0), 1),
			})
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

func TestDeltaWriterBatchingAndIsolation(t *testing.T) {
	requests := make(chan pmetricotlp.ExportRequest, 8)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/otlp/v1/metrics" {
			t.Errorf("unexpected path: %s", r.URL.Path)
			w.WriteHeader(404)
			return
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
			w.WriteHeader(500)
			return
		}

		request := pmetricotlp.NewExportRequest()
		if err := request.UnmarshalProto(body); err != nil {
			t.Error(err)
			w.WriteHeader(500)
			return
		}
		requests <- request
	}))
	defer server.Close()

	var cfg ClientConfig
	flagext.DefaultValues(&cfg)
	cfg.WriteBatchSize = 2
	require.NoError(t, cfg.WriteBaseEndpoint.Set(server.URL))
	require.NoError(t, cfg.ReadBaseEndpoint.Set(server.URL))

	reg := prometheus.NewPedanticRegistry()
	client, err := NewClient(cfg, log.NewNopLogger(), reg)
	require.NoError(t, err)

	originalWriter := client.writeClient

	var seriesCfg WriteReadSeriesTestConfig
	flagext.DefaultValues(&seriesCfg)
	seriesCfg.NumSeries = 3
	seriesCfg.WithHistograms = true
	normal := NewWriteReadSeriesTest(seriesCfg, client, cfg.WriteProtocol, log.NewNopLogger(), reg)
	delta := NewOTLPDeltaTest(OTLPDeltaTestConfig{
		Enabled: true,
	}, seriesCfg, client, log.NewNopLogger(), reg)
	require.Equal(t, "write-read-series", normal.Name())
	require.Equal(t, "otlp-delta", delta.Name())
	require.Same(t, originalWriter, client.writeClient)
	require.Equal(t, writeProtocolPrometheus, client.Protocol())
	require.Equal(t, writeProtocolOtelHttp, delta.client.Protocol())

	now := time.Unix(86440, 0).UTC()
	for _, histograms := range []bool{false, true} {
		var series []prompb.TimeSeries
		metadata := delta.floatMetadata
		if histograms {
			profile := delta.histProfiles[0]
			series = profile.generateSeries(profile.metricName, now, 3)
			metadata = profile.metadata
		} else {
			series = generateSineWaveSeries(delta.floatName, now, 3)
		}

		status, err := delta.client.WriteSeries(t.Context(), series, metadata)
		require.NoError(t, err)
		require.Equal(t, 200, status)
		require.Len(t, requests, 2)

		total := 0
		for _, batchSize := range []int{2, 1} {
			req := <-requests
			resources := req.Metrics().ResourceMetrics()
			require.Equal(t, batchSize, resources.Len())
			for i := 0; i < resources.Len(); i++ {
				metric := resources.At(i).ScopeMetrics().At(0).Metrics().At(0)
				if histograms {
					require.Equal(t, pmetric.MetricTypeExponentialHistogram, metric.Type())
					hist := metric.ExponentialHistogram()
					require.Equal(t, pmetric.AggregationTemporalityDelta, hist.AggregationTemporality())

					point := hist.DataPoints().At(0)
					require.Equal(t, now.UnixNano(), int64(point.Timestamp()))
					require.Equal(t, now.Add(-writeInterval).UnixNano(), int64(point.StartTimestamp()))

					expected := delta.histProfiles[0].generateSampleHistogram(now, 1)
					require.Equal(t, float64(expected.Count), float64(point.Count()))
					require.Equal(t, float64(expected.Sum), point.Sum())
				} else {
					require.Equal(t, pmetric.MetricTypeSum, metric.Type())
					sum := metric.Sum()
					require.True(t, sum.IsMonotonic())
					require.Equal(t, pmetric.AggregationTemporalityDelta, sum.AggregationTemporality())

					point := sum.DataPoints().At(0)
					require.Equal(t, now.UnixNano(), int64(point.Timestamp()))
					require.Equal(t, now.Add(-writeInterval).UnixNano(), int64(point.StartTimestamp()))
					require.Equal(t, generateSineWaveValue(now), point.DoubleValue())

					id, ok := point.Attributes().Get("series_id")
					require.True(t, ok)
					require.Equal(t, strconv.Itoa(total), id.Str())
				}
				total++
			}
		}
	}
}

func TestDeltaConfig(t *testing.T) {
	var cfg OTLPDeltaTestConfig
	flagext.DefaultValues(&cfg)
	require.False(t, cfg.Enabled)
}

func TestOTLPDeltaTestDisabled(t *testing.T) {
	test := NewOTLPDeltaTest(OTLPDeltaTestConfig{}, WriteReadSeriesTestConfig{}, &Client{}, log.NewNopLogger(), prometheus.NewPedanticRegistry())

	test.client = nil
	require.NoError(t, test.Init(t.Context(), time.Now()))
	require.NoError(t, test.Run(t.Context(), time.Now()))
}

func TestOTLPDeltaTestQueryWindows(t *testing.T) {
	var cfg WriteReadSeriesTestConfig
	flagext.DefaultValues(&cfg)
	now := time.Unix(3*86400, 0).UTC()

	for _, age := range []time.Duration{30 * time.Minute, 25 * time.Hour, 60 * time.Hour} {
		t.Run(age.String(), func(t *testing.T) {
			records := &MetricHistory{
				queryMinTime: now.Add(-age),
				queryMaxTime: now,
			}
			normal := NewWriteReadSeriesTest(cfg, nil, writeProtocolDefault, log.NewNopLogger(), prometheus.NewPedanticRegistry())
			delta := NewOTLPDeltaTest(OTLPDeltaTestConfig{
				Enabled: true,
			}, cfg, &Client{}, log.NewNopLogger(), prometheus.NewPedanticRegistry())
			expectedRanges, expectedInstants, err := normal.getQueryTimeRanges(now, records)
			require.NoError(t, err)
			ranges, instants, err := delta.getQueryTimeRanges(now, records)
			require.NoError(t, err)
			require.Len(t, ranges, len(expectedRanges))
			require.Len(t, instants, len(expectedInstants))

			// The final window and instant are random in both implementations.
			require.Equal(t, expectedRanges[:len(expectedRanges)-1], ranges[:len(ranges)-1])
			require.Equal(t, expectedInstants[:len(expectedInstants)-1], instants[:len(instants)-1])
			randomRange := ranges[len(ranges)-1]
			require.False(t, randomRange[0].Before(maxTime(records.queryMinTime, now.Add(-cfg.MaxQueryAge))))
			require.False(t, randomRange[1].After(now))
			require.False(t, randomRange[1].Before(randomRange[0]))
		})
	}
}

func TestDeltaStartTimestampQuery(t *testing.T) {
	for _, tc := range []struct {
		name     string
		fanOut   bool
		expected string
	}{
		{
			name:     "exact metric",
			expected: "sum(abs(start_timestamp(delta_counter) - (timestamp(delta_counter) - 20)))",
		},
		{
			name:     "fan-out",
			fanOut:   true,
			expected: `sum(abs(start_timestamp({__name__=~"delta_counter.*"}) - (timestamp({__name__=~"delta_counter.*"}) - 20)))`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			checks := deltaQueryChecks("delta_counter", tc.fanOut, generateSineWaveValue, nil)
			require.Equal(t, tc.expected, checks[0].query)
			require.Zero(t, checks[0].history)
			require.Zero(t, checks[0].value(time.Unix(120, 0)))
		})
	}
}

func TestDeltaStartTimestampResultValidation(t *testing.T) {
	now := time.Unix(120, 0).UTC()
	check := deltaQueryChecks(otlpDeltaCounter, false, generateSineWaveValue, nil)[0]

	for _, tc := range []struct {
		name      string
		result    model.Matrix
		wantError bool
	}{
		{
			name: "all intervals match",
			result: model.Matrix{{
				Values: []model.SamplePair{{
					Timestamp: model.TimeFromUnix(120),
					Value:     0,
				}},
			}},
		},
		{
			name: "nonzero interval error",
			result: model.Matrix{{
				Values: []model.SamplePair{{
					Timestamp: model.TimeFromUnix(120),
					Value:     2,
				}},
			}},
			wantError: true,
		},
		{
			name:      "missing result",
			wantError: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := newMockClient()
			client.On("Protocol").Return(writeProtocolOtelHttp)
			client.On("QueryRange", mock.Anything, check.query, now, now, writeInterval, mock.Anything).Return(tc.result, nil)

			test := &OTLPDeltaTest{
				cfg:     WriteReadSeriesTestConfig{NumSeries: 2},
				client:  client,
				logger:  log.NewNopLogger(),
				metrics: NewTestMetrics("otlp-delta", prometheus.NewPedanticRegistry()),
			}
			history := &MetricHistory{
				queryMinTime: now,
				queryMaxTime: now,
			}
			err := test.runRangeQueryAndVerifyResult(t.Context(), now, now, false, "float", check.query, check.value, nil, history)
			if tc.wantError {
				require.ErrorContains(t, err, "range query result check failed")
			} else {
				require.NoError(t, err)
			}

			client.AssertExpectations(t)
		})
	}
}
