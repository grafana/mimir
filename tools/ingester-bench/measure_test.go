// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"os"
	"path/filepath"
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

func TestHeadSnapshotBytesIsTheTenantsLastSnapshots(t *testing.T) {
	dir := t.TempDir()
	write := func(path string, size int) {
		path = filepath.Join(dir, "tsdb", path)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, make([]byte, size), 0o644))
	}
	write("tenant-1/seriesstore/snapshot", 100)
	write("tenant-2/seriesstore/snapshot", 20)
	// A round's snapshot of tenant-2 being written, which replaces its last only once it's whole.
	write("tenant-2/seriesstore/snapshot.tmp", 5)
	// The chunk files the snapshots refer to, which a restart maps rather than restores.
	write("tenant-1/seriesstore/shard-000/000001", 1000)
	require.Equal(t, 120.0, headSnapshotBytes(dir))
	// An ingester whose directory the bench can't see, as one in another container.
	require.Zero(t, headSnapshotBytes(filepath.Join(dir, "missing")))
}
