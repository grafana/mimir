// SPDX-License-Identifier: AGPL-3.0-only

package log_test

import (
	"os"
	"testing"
	"time"

	gokitlog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/server"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/log"
)

// Check that debug lines are correctly filtered out and that rate limit is satisfied.
func ExampleInitLogger() {
	// Kludge a couple of things so we can do tests repeatably.
	saveStderr := os.Stderr
	os.Stderr = os.Stdout
	saveTimestamp := gokitlog.DefaultTimestampUTC
	gokitlog.DefaultTimestampUTC = gokitlog.TimestampFormat(
		func() time.Time { return time.Unix(0, 0).UTC() },
		time.RFC3339Nano,
	)

	cfg := server.Config{}
	_ = cfg.LogLevel.Set("info")
	rateLimitedCfg := log.RateLimitedLoggerCfg{
		Enabled:            true,
		LogsPerSecond:      1,
		LogsPerSecondBurst: 4,
		Registry:           prometheus.NewPedanticRegistry(),
	}
	cfg.Log = log.InitLogger(cfg.LogFormat, cfg.LogLevel, false, rateLimitedCfg)

	for i := 0; i < 1000; i++ {
		level.Info(log.Logger).Log("msg", "log.Logger", "test", i+1)
		level.Debug(log.Logger).Log("msg", "log.Logger", "test", i+1)
		level.Info(cfg.Log).Log("msg", "cfg.Log", "test", i+1)
		level.Debug(cfg.Log).Log("msg", "log.Logger", "test", i+1)
	}

	// Output:
	// ts=1970-01-01T00:00:00Z caller=log_test.go:41 level=info msg=log.Logger test=1
	// ts=1970-01-01T00:00:00Z caller=log_test.go:43 level=info msg=cfg.Log test=1
	// ts=1970-01-01T00:00:00Z caller=log_test.go:41 level=info msg=log.Logger test=2
	// ts=1970-01-01T00:00:00Z caller=log_test.go:43 level=info msg=cfg.Log test=2

	os.Stderr = saveStderr
	gokitlog.DefaultTimestampUTC = saveTimestamp
}

// Check the overhead of debug logging which gets filtered out.
func BenchmarkDebugLog(b *testing.B) {
	cfg := server.Config{}
	require.NoError(b, cfg.LogLevel.Set("info"))
	log.InitLogger(cfg.LogFormat, cfg.LogLevel, false, log.RateLimitedLoggerCfg{})
	b.ResetTimer()
	dl := level.Debug(log.Logger)
	for i := 0; i < b.N; i++ {
		dl.Log("something", "happened")
	}
}
