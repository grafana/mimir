// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMetrics(t *testing.T) {
	t.Run("metric names and label sets", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		m := newMetrics(reg)

		// Touch every metric so all series are created.
		m.hedgeAttemptsTotal.Inc()
		m.hedgeWinsTotal.Inc()
		m.hedgeAttemptsSuppressedTotal.WithLabelValues("warmup").Inc()
		m.lingerFlushesTotal.Inc()
		m.lingerBufferBytes.Set(1024)
		m.produceRequestsTotal.Inc()
		m.produceRequestsFailedTotal.WithLabelValues("timeout").Inc()

		require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
			# HELP hedge_attempts_total Total number of produce requests for which a fanout to per-partition secondaries was dispatched. Includes both latency-triggered hedges (primary still in flight) and primary-failure retries.
			# TYPE hedge_attempts_total counter
			hedge_attempts_total 1

			# HELP hedge_wins_total Total number of produce requests where the per-partition secondary fanout produced the winning response. Includes both races where the secondaries beat the primary and retries where the primary had already failed.
			# TYPE hedge_wins_total counter
			hedge_wins_total 1

			# HELP hedge_attempts_suppressed_total Total number of produce requests where hedging was suppressed.
			# TYPE hedge_attempts_suppressed_total counter
			hedge_attempts_suppressed_total{reason="warmup"} 1

			# HELP linger_flushes_total Total number of partition batch flushes triggered by the linger buffer.
			# TYPE linger_flushes_total counter
			linger_flushes_total 1

			# HELP linger_buffer_bytes Current number of bytes buffered in the linger buffer awaiting flush.
			# TYPE linger_buffer_bytes gauge
			linger_buffer_bytes 1024

			# HELP produce_requests_total Total number of produce requests attempted.
			# TYPE produce_requests_total counter
			produce_requests_total 1

			# HELP produce_requests_failed_total Total number of produce requests that failed, by failure reason.
			# TYPE produce_requests_failed_total counter
			produce_requests_failed_total{reason="timeout"} 1
		`)))
	})

	t.Run("second registration panics on duplicate", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		newMetrics(reg)
		assert.Panics(t, func() { newMetrics(reg) })
	})
}
