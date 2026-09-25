// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestClusterMetrics_PerCluster(t *testing.T) {
	t.Run("compartments disabled omits the write_compartment label", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		metrics := newClusterMetrics(reg, false, 1)
		require.Len(t, metrics, 1)

		metrics[0].startOffset.WithLabelValues("1").Set(42)
		metrics[0].flushFailed.Inc()
		metrics[0].endOffsetProbeFailed.Add(2)

		require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_blockbuilder_scheduler_partition_start_offset The observed start offset of each partition.
			# TYPE cortex_blockbuilder_scheduler_partition_start_offset gauge
			cortex_blockbuilder_scheduler_partition_start_offset{partition="1"} 42

			# HELP cortex_blockbuilder_scheduler_flush_failed_total The total number of times flushing the cluster's committed offsets to Kafka failed.
			# TYPE cortex_blockbuilder_scheduler_flush_failed_total counter
			cortex_blockbuilder_scheduler_flush_failed_total 1

			# HELP cortex_blockbuilder_scheduler_end_offset_probe_failed_total The total number of times probing the cluster's end offsets failed.
			# TYPE cortex_blockbuilder_scheduler_end_offset_probe_failed_total counter
			cortex_blockbuilder_scheduler_end_offset_probe_failed_total 2
		`),
			"cortex_blockbuilder_scheduler_partition_start_offset",
			"cortex_blockbuilder_scheduler_flush_failed_total",
			"cortex_blockbuilder_scheduler_end_offset_probe_failed_total",
		))
	})

	t.Run("compartments enabled labels each cluster with its write_compartment", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		metrics := newClusterMetrics(reg, true, 2)
		require.Len(t, metrics, 2)

		metrics[0].startOffset.WithLabelValues("1").Set(42)
		metrics[1].startOffset.WithLabelValues("1").Set(99)
		metrics[1].flushFailed.Inc()
		metrics[0].endOffsetProbeFailed.Add(2)

		require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_blockbuilder_scheduler_partition_start_offset The observed start offset of each partition.
			# TYPE cortex_blockbuilder_scheduler_partition_start_offset gauge
			cortex_blockbuilder_scheduler_partition_start_offset{partition="1",write_compartment="0"} 42
			cortex_blockbuilder_scheduler_partition_start_offset{partition="1",write_compartment="1"} 99

			# HELP cortex_blockbuilder_scheduler_flush_failed_total The total number of times flushing the cluster's committed offsets to Kafka failed.
			# TYPE cortex_blockbuilder_scheduler_flush_failed_total counter
			cortex_blockbuilder_scheduler_flush_failed_total{write_compartment="0"} 0
			cortex_blockbuilder_scheduler_flush_failed_total{write_compartment="1"} 1

			# HELP cortex_blockbuilder_scheduler_end_offset_probe_failed_total The total number of times probing the cluster's end offsets failed.
			# TYPE cortex_blockbuilder_scheduler_end_offset_probe_failed_total counter
			cortex_blockbuilder_scheduler_end_offset_probe_failed_total{write_compartment="0"} 2
			cortex_blockbuilder_scheduler_end_offset_probe_failed_total{write_compartment="1"} 0
		`),
			"cortex_blockbuilder_scheduler_partition_start_offset",
			"cortex_blockbuilder_scheduler_flush_failed_total",
			"cortex_blockbuilder_scheduler_end_offset_probe_failed_total",
		))
	})
}
