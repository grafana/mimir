// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
)

func TestWriteSamplesOTLPFailures(t *testing.T) {
	partial := pmetricotlp.NewExportResponse()
	partial.PartialSuccess().SetRejectedDataPoints(1)
	partial.PartialSuccess().SetErrorMessage("series limit")
	partialBody, err := partial.MarshalProto()
	require.NoError(t, err)

	for _, delta := range []bool{false, true} {
		for _, tc := range []struct {
			name      string
			status    int
			body      []byte
			retry     bool
			wantError string
		}{
			{name: "partial rejection", status: 200, body: partialBody},
			{name: "client rejection", status: 400, body: []byte("invalid series")},
			{name: "invalid response", status: 200, body: []byte("invalid protobuf"), retry: true, wantError: "decode OTLP response"},
			{name: "server error", status: 500, body: []byte("unavailable"), retry: true, wantError: "HTTP status 500"},
		} {
			t.Run(fmt.Sprintf("delta=%t/%s", delta, tc.name), func(t *testing.T) {
				requests := 0
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					requests++
					if requests <= 2 {
						w.WriteHeader(tc.status)
						_, err := w.Write(tc.body)
						if err != nil {
							t.Error(err)
						}
						return
					}
					w.WriteHeader(http.StatusOK)
				}))
				defer server.Close()

				var cfg ClientConfig
				flagext.DefaultValues(&cfg)
				cfg.WriteProtocol = writeProtocolOtelHttp
				require.NoError(t, cfg.WriteBaseEndpoint.Set(server.URL))
				require.NoError(t, cfg.ReadBaseEndpoint.Set(server.URL))
				client, err := NewClient(cfg, log.NewNopLogger(), prometheus.NewRegistry())
				require.NoError(t, err)
				if delta {
					client.writeClient = &otlpDeltaWriter{writer: client.otlpClient}
				}

				previous := time.Unix(100, 0)
				history := MetricHistory{
					lastWrittenTimestamp: previous,
					queryMinTime:         previous,
					queryMaxTime:         previous,
				}
				original := history
				metrics := NewTestMetrics("test", prometheus.NewRegistry())
				var logs bytes.Buffer
				logger := log.NewLogfmtLogger(&logs)

				// Persistent rejection must not pin subsequent runs to the same interval.
				for attempt := 0; attempt < 2; attempt++ {
					ts := history.lastWrittenTimestamp.Add(writeInterval)
					err := writeSamples(t.Context(), floatTypeLabel, ts, generateSineWaveSeries(otlpDeltaCounter, ts, 1), nil, &history, client, metrics, logger)
					if tc.retry {
						require.ErrorContains(t, err, tc.wantError)
						require.Equal(t, original, history)
					} else {
						require.NoError(t, err)
						require.Equal(t, ts, history.lastWrittenTimestamp)
						require.True(t, history.queryMinTime.IsZero())
						require.True(t, history.queryMaxTime.IsZero())
					}
				}

				require.Equal(t, float64(2), testutil.ToFloat64(metrics.writesTotal.WithLabelValues(floatTypeLabel)))
				require.Equal(t, float64(2), testutil.ToFloat64(metrics.writesFailedTotal.WithLabelValues(strconv.Itoa(tc.status), floatTypeLabel)))
				require.NotContains(t, logs.String(), "Remote write series succeeded")
				require.Contains(t, logs.String(), "Failed to remote write series")

				ts := history.lastWrittenTimestamp.Add(writeInterval)
				require.NoError(t, writeSamples(t.Context(), floatTypeLabel, ts, generateSineWaveSeries(otlpDeltaCounter, ts, 1), nil, &history, client, metrics, logger))
				require.Equal(t, ts, history.lastWrittenTimestamp)
				require.Equal(t, ts, history.queryMaxTime)
				if tc.retry {
					require.Equal(t, previous, history.queryMinTime)
				} else {
					require.Equal(t, ts, history.queryMinTime)
				}
				require.Equal(t, float64(3), testutil.ToFloat64(metrics.writesTotal.WithLabelValues(floatTypeLabel)))
				require.Equal(t, float64(2), testutil.ToFloat64(metrics.writesFailedTotal.WithLabelValues(strconv.Itoa(tc.status), floatTypeLabel)))
			})
		}
	}
}
