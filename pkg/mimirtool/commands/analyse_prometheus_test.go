// SPDX-License-Identifier: AGPL-3.0-only

package commands

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	"github.com/grafana/dskit/backoff"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithBackoff_DefaultBoundsDelayRetries(t *testing.T) {
	var attempts int
	start := time.Now()

	err := withBackoff(context.Background(), func() error {
		attempts++
		if attempts < 2 {
			return &v1.Error{Type: v1.ErrServer, Msg: "server error: 503"}
		}
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, 2, attempts)
	assert.GreaterOrEqual(t, time.Since(start), defaultBackoffConfig.MinBackoff, "default backoff must wait at least MinBackoff between retries")
}

func TestWithBackoffConfig_NonRetryableErrorsStopImmediately(t *testing.T) {
	cfg := backoff.Config{
		MinBackoff: 10 * time.Millisecond,
		MaxBackoff: 50 * time.Millisecond,
		MaxRetries: 5,
	}

	t.Run("bad_data 400 error is not retried", func(t *testing.T) {
		var attempts int
		err := withBackoffConfig(context.Background(), cfg, func() error {
			attempts++
			return &v1.Error{Type: v1.ErrBadData, Msg: "parse error"}
		})
		require.Error(t, err)
		assert.Equal(t, 1, attempts)
	})

	t.Run("client error 400 is not retried", func(t *testing.T) {
		var attempts int
		err := withBackoffConfig(context.Background(), cfg, func() error {
			attempts++
			return &v1.Error{Type: v1.ErrClient, Msg: "client error: 400"}
		})
		require.Error(t, err)
		assert.Equal(t, 1, attempts)
	})

	t.Run("client error 429 is retried", func(t *testing.T) {
		var attempts int
		err := withBackoffConfig(context.Background(), cfg, func() error {
			attempts++
			if attempts < 3 {
				return &v1.Error{Type: v1.ErrClient, Msg: "client error: 429"}
			}
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 3, attempts)
	})
}

func TestPrometheusAnalyzeCommand_BackpressureAndStatusHandling(t *testing.T) {
	t.Run("retries 429 with Retry-After and succeeds", func(t *testing.T) {
		var queryAttempts atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			attempt := queryAttempts.Add(1)
			if attempt == 1 {
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte(`rate limit exceeded`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"job":"api"},"value":[1700000000,"42"]}]}}`))
		}))
		defer srv.Close()

		cmd := &PrometheusAnalyzeCommand{
			address:     srv.URL,
			readTimeout: 5 * time.Second,
			backoffConfig: backoff.Config{
				MinBackoff: 10 * time.Millisecond,
				MaxBackoff: 50 * time.Millisecond,
				MaxRetries: 5,
			},
			logger: log.NewNopLogger(),
		}
		v1api, err := cmd.newAPI()
		require.NoError(t, err)

		start := time.Now()
		res, err := AnalyzePrometheusWithBackoff(
			v1api,
			model.LabelValues{"up"},
			func(model.LabelValue) bool { return false },
			1,
			cmd.readTimeout,
			cmd.backoffConfig,
			cmd.logger,
		)
		elapsed := time.Since(start)
		require.NoError(t, err)
		assert.Equal(t, int32(2), queryAttempts.Load())
		assert.Equal(t, uint64(42), res.cardinality)
		assert.Empty(t, res.metricErrors)
		assert.GreaterOrEqual(t, elapsed, 900*time.Millisecond, "should honour 1s Retry-After header on 429")
	})

	t.Run("does not retry non-retryable 400 response", func(t *testing.T) {
		var queryAttempts atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			queryAttempts.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"status":"error","errorType":"bad_data","error":"invalid parameter 'query'"}`))
		}))
		defer srv.Close()

		cmd := &PrometheusAnalyzeCommand{
			address:     srv.URL,
			readTimeout: 5 * time.Second,
			backoffConfig: backoff.Config{
				MinBackoff: 50 * time.Millisecond,
				MaxBackoff: 200 * time.Millisecond,
				MaxRetries: 5,
			},
			logger: log.NewNopLogger(),
		}
		v1api, err := cmd.newAPI()
		require.NoError(t, err)

		res, err := AnalyzePrometheusWithBackoff(
			v1api,
			model.LabelValues{"up"},
			func(model.LabelValue) bool { return false },
			1,
			cmd.readTimeout,
			cmd.backoffConfig,
			cmd.logger,
		)
		require.NoError(t, err)
		assert.Equal(t, int32(1), queryAttempts.Load(), "400 bad_data should not be retried")
		require.Len(t, res.metricErrors, 1)
	})
}

func TestAnalyzeCommand_PrometheusBackoffFlags(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/v1/label/__name__/values" {
			_, _ = w.Write([]byte(`{"status":"success","data":["up"]}`))
			return
		}
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"job":"api"},"value":[1700000000,"1"]}]}}`))
	}))
	defer srv.Close()

	tmpDir := t.TempDir()
	grafanaFile := tmpDir + "/metrics-in-grafana.json"
	outFile := tmpDir + "/prometheus-metrics.json"
	require.NoError(t, os.WriteFile(grafanaFile, []byte(`{"metricsUsed":["up"]}`), 0o600))

	app := kingpin.New("mimirtool", "")
	cmd := &AnalyzeCommand{}
	cmd.Register(app, NewEnvVarsWithPrefix("MIMIR"), &LoggerConfig{})

	_, err := app.Parse([]string{
		"analyze", "prometheus",
		"--address=" + srv.URL,
		"--grafana-metrics-file=" + grafanaFile,
		"--output=" + outFile,
		"--backoff-min-period=25ms",
		"--backoff-max-period=100ms",
		"--backoff-retries=3",
	})
	require.NoError(t, err)
}
