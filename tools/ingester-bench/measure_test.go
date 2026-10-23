// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCountersPicksWhatChangesTheCost(t *testing.T) {
	m := map[string]float64{
		"cortex_ingester_tsdb_compactions_total":                            2,
		"cortex_ingester_tsdb_checkpoint_creations_total":                   1,
		"cortex_ingester_tsdb_head_truncations_total":                       3,
		"cortex_ingester_seriesstore_head_snapshots_total{outcome=success}": 4,
		"cortex_ingester_utilization_limited_read_requests_total":           0,
		"cortex_ingester_ingested_samples_total":                            1e6,
		"cortex_ingester_tsdb_compaction_duration_seconds_count":            2,
	}
	require.Equal(t, map[string]float64{
		"cortex_ingester_tsdb_compactions_total":                            2,
		"cortex_ingester_tsdb_checkpoint_creations_total":                   1,
		"cortex_ingester_tsdb_head_truncations_total":                       3,
		"cortex_ingester_seriesstore_head_snapshots_total{outcome=success}": 4,
		"cortex_ingester_utilization_limited_read_requests_total":           0,
	}, counters(m))
}

func TestReplayCPUIsFromPastTheFirstIntervalToReady(t *testing.T) {
	ready := map[string]float64{"process_cpu_seconds_total": 30, "cortex_ingester_ingested_samples_total": 5e6}
	require.Equal(t, ReplayCPU{CPUSeconds: 20, Samples: 4e6, CPUSecondsPerSample: 5e-6, Coverage: 0.5}, replayCPU(10, 1e6, true, ready, 8e6))
	// Ready before the bench saw it past the first interval: nothing to measure.
	require.Equal(t, ReplayCPU{}, replayCPU(0, 0, false, ready, 8e6))
}
