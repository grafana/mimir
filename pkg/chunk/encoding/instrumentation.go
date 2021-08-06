// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package encoding

import "github.com/prometheus/client_golang/prometheus"

// Usually, a separate file for instrumentation is frowned upon. Metrics should
// be close to where they are used. However, the metrics below are set all over
// the place, so we go for a separate instrumentation file in this case.
var (
	Ops = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "chunk_ops_total",
			Help:      "The total number of chunk operations by their type.",
		},
		[]string{OpTypeLabel},
	)
	DescOps = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "chunkdesc_ops_total",
			Help:      "The total number of chunk descriptor operations by their type.",
		},
		[]string{OpTypeLabel},
	)
	NumMemDescs = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "memory_chunkdescs",
		Help:      "The current number of chunk descriptors in memory.",
	})
)

const (
	namespace = "prometheus"
	subsystem = "local_storage"

	// OpTypeLabel is the label name for chunk operation types.
	OpTypeLabel = "type"

	// Op-types for ChunkOps.

	// CreateAndPin is the label value for create-and-pin chunk ops.
	CreateAndPin = "create" // A Desc creation with refCount=1.
	// PersistAndUnpin is the label value for persist chunk ops.
	PersistAndUnpin = "persist"
	// Pin is the label value for pin chunk ops (excludes pin on creation).
	Pin = "pin"
	// Unpin is the label value for unpin chunk ops (excludes the unpin on persisting).
	Unpin = "unpin"
	// Transcode is the label value for transcode chunk ops.
	Transcode = "transcode"
	// Drop is the label value for drop chunk ops.
	Drop = "drop"

	// Op-types for ChunkOps and ChunkDescOps.

	// Evict is the label value for evict chunk desc ops.
	Evict = "evict"
	// Load is the label value for load chunk and chunk desc ops.
	Load = "load"
)

func init() {
	prometheus.MustRegister(Ops)
	prometheus.MustRegister(DescOps)
	prometheus.MustRegister(NumMemDescs)
}

// NumMemChunks is the total number of chunks in memory. This is a global
// counter, also used internally, so not implemented as metrics. Collected in
// MemorySeriesStorage.
// TODO(beorn7): Having this as an exported global variable is really bad.
var NumMemChunks int64
