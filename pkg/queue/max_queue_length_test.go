// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

const maxQueueLengthMetricName = "cortex_query_scheduler_max_queue_length"

func gatherMaxQueueLength(t *testing.T, reg *prometheus.Registry, expected string) {
	t.Helper()
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expected), maxQueueLengthMetricName))
}

func expectedMaxQueueLength(series string) string {
	return "# HELP cortex_query_scheduler_max_queue_length Maximum number of queries observed in a tenant's queue since the last metric collection (reset on each scrape). Captures the true peak queue depth between scrapes.\n" +
		"# TYPE cortex_query_scheduler_max_queue_length gauge\n" +
		series
}

func TestMaxQueueLengthGauge_TracksPeak(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	g := NewMaxQueueLengthGauge()
	reg.MustRegister(g)

	g.inc("a")
	g.inc("a")
	g.inc("a")

	gatherMaxQueueLength(t, reg, expectedMaxQueueLength(`cortex_query_scheduler_max_queue_length{user="a"} 3`+"\n"))
}

func TestMaxQueueLengthGauge_PeakIsIntraWindowMaxNotEndValue(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	g := NewMaxQueueLengthGauge()
	reg.MustRegister(g)

	// Within a single scrape window: rise to 3, drain to 1, rise to 2.
	g.inc("a") // 1
	g.inc("a") // 2
	g.inc("a") // 3 (peak)
	g.dec("a") // 2
	g.dec("a") // 1
	g.inc("a") // 2

	// The reported value is the intra-window peak (3), not the depth at scrape time (2).
	gatherMaxQueueLength(t, reg, expectedMaxQueueLength(`cortex_query_scheduler_max_queue_length{user="a"} 3`+"\n"))
}

func TestMaxQueueLengthGauge_ResetsToStandingDepth(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	g := NewMaxQueueLengthGauge()
	reg.MustRegister(g)

	g.inc("a")
	g.inc("a")
	g.inc("a") // current=3, max=3

	// First scrape reports the peak and reseeds max to the standing depth (3).
	gatherMaxQueueLength(t, reg, expectedMaxQueueLength(`cortex_query_scheduler_max_queue_length{user="a"} 3`+"\n"))

	// Queue drains with no new enqueues in this window.
	g.dec("a")
	g.dec("a") // current=1, max still 3

	// The window began at standing depth 3, so the peak for it is still 3 (not under-reported as 1).
	gatherMaxQueueLength(t, reg, expectedMaxQueueLength(`cortex_query_scheduler_max_queue_length{user="a"} 3`+"\n"))

	// With no further activity, the next window reflects the new standing depth (1).
	gatherMaxQueueLength(t, reg, expectedMaxQueueLength(`cortex_query_scheduler_max_queue_length{user="a"} 1`+"\n"))
}

func TestMaxQueueLengthGauge_DecNeverGoesNegative(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	g := NewMaxQueueLengthGauge()
	reg.MustRegister(g)

	// dec on an unknown / already-empty tenant must be a no-op, not a negative depth.
	g.dec("a")
	g.inc("a") // 1
	g.dec("a") // 0
	g.dec("a") // still 0

	gatherMaxQueueLength(t, reg, expectedMaxQueueLength(`cortex_query_scheduler_max_queue_length{user="a"} 1`+"\n"))
	// Standing depth is 0 after the reset.
	gatherMaxQueueLength(t, reg, expectedMaxQueueLength(`cortex_query_scheduler_max_queue_length{user="a"} 0`+"\n"))
}

func TestMaxQueueLengthGauge_PerTenantAndDelete(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	g := NewMaxQueueLengthGauge()
	reg.MustRegister(g)

	g.inc("a")
	g.inc("a")
	g.inc("b")

	gatherMaxQueueLength(t, reg, expectedMaxQueueLength(
		`cortex_query_scheduler_max_queue_length{user="a"} 2`+"\n"+
			`cortex_query_scheduler_max_queue_length{user="b"} 1`+"\n"))

	g.DeleteTenant("a")

	gatherMaxQueueLength(t, reg, expectedMaxQueueLength(`cortex_query_scheduler_max_queue_length{user="b"} 1`+"\n"))
}
