// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"bytes"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetrics_UpdateRound_AddsAndDeletesGauges(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	m := newMetrics(reg)

	// First round: partitions {0, 1, 2} and instances {a, b}.
	m.updateRound(map[int32]float64{0: 1.0, 1: 2.0, 2: 3.0}, map[string]float64{"ingester-a": 10.0, "ingester-b": 20.0})

	got := mustGather(t, reg)
	assert.Contains(t, got, `cortex_nautilus_rebalancer_partition_query_samples_ewma{partition="0"} 1`)
	assert.Contains(t, got, `cortex_nautilus_rebalancer_partition_query_samples_ewma{partition="1"} 2`)
	assert.Contains(t, got, `cortex_nautilus_rebalancer_partition_query_samples_ewma{partition="2"} 3`)
	assert.Contains(t, got, `cortex_nautilus_rebalancer_unnamed_query_samples_ewma{instance="ingester-a"} 10`)
	assert.Contains(t, got, `cortex_nautilus_rebalancer_unnamed_query_samples_ewma{instance="ingester-b"} 20`)

	// Second round: partition 1 dropped (e.g. became inactive); ingester-a left.
	// Stale series must be deleted, not flat-line at the previous value.
	m.updateRound(map[int32]float64{0: 4.0, 2: 5.0}, map[string]float64{"ingester-b": 25.0})

	got = mustGather(t, reg)
	assert.Contains(t, got, `cortex_nautilus_rebalancer_partition_query_samples_ewma{partition="0"} 4`)
	assert.Contains(t, got, `cortex_nautilus_rebalancer_partition_query_samples_ewma{partition="2"} 5`)
	assert.NotContains(t, got, `partition="1"`, "partition 1 dropped between rounds; gauge must not flat-line")
	assert.Contains(t, got, `cortex_nautilus_rebalancer_unnamed_query_samples_ewma{instance="ingester-b"} 25`)
	assert.NotContains(t, got, `instance="ingester-a"`, "ingester-a dropped between rounds; gauge must not flat-line")
}

func TestMetrics_UpdateRound_NilSafe(t *testing.T) {
	var m *metrics
	m.updateRound(map[int32]float64{0: 1.0}, map[string]float64{"a": 1.0}) // must not panic
}

func mustGather(t *testing.T, reg *prometheus.Registry) string {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	var buf bytes.Buffer
	enc := expfmt.NewEncoder(&buf, expfmt.NewFormat(expfmt.TypeTextPlain))
	for _, mf := range mfs {
		require.NoError(t, enc.Encode(mf))
	}
	return buf.String()
}
