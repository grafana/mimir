// SPDX-License-Identifier: AGPL-3.0-only

// Package metrics holds Prometheus metrics named like the Go ingester's, so the same dashboards
// and recording rules read both. Cost attribution trackers that are not internal go to a
// separate registry served on Mimir's `-cost-attribution.registry-path`.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/limits"
)

var (
	// Registry is the one `/metrics` serves.
	Registry = prometheus.NewRegistry()
	// UsageRegistry holds the attributed series of trackers that are not internal.
	UsageRegistry = prometheus.NewRegistry()
)

func expBuckets(start, factor float64, count int) []float64 {
	return prometheus.ExponentialBuckets(start, factor, count)
}

var (
	DiscardedSamples = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_discarded_samples_total", Help: "The total number of samples that were discarded.",
	}, []string{"reason", "user", "group"})
	DiscardedMetadata = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_discarded_metadata_total", Help: "The total number of metadata that were discarded.",
	}, []string{"reason", "user"})
	IngestedSamples = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_ingester_ingested_samples_total", Help: "The total number of samples ingested per user.",
	}, []string{"user"})
	IngestedSamplesFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_ingester_ingested_samples_failures_total", Help: "The total number of samples that errored on ingestion per user.",
	}, []string{"user"})
	IngestedExemplars = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cortex_ingester_ingested_exemplars_total", Help: "The total number of exemplars ingested.",
	})
	IngestedExemplarsFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cortex_ingester_ingested_exemplars_failures_total", Help: "The total number of exemplars that errored on ingestion.",
	})
	IngestedMetadata = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cortex_ingester_ingested_metadata_total", Help: "The total number of metadata ingested.",
	})
	IngestedMetadataFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cortex_ingester_ingested_metadata_failures_total", Help: "The total number of metadata that errored on ingestion.",
	})
	ExemplarsInStorage = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cortex_ingester_tsdb_exemplar_exemplars_in_storage", Help: "Number of TSDB exemplars currently in storage.",
	})
	ExemplarsAppended = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_ingester_tsdb_exemplar_exemplars_appended_total", Help: "Total number of TSDB exemplars appended.",
	}, []string{"user"})
	SeriesWithExemplars = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage", Help: "Number of TSDB series with exemplars currently in storage.",
	}, []string{"user"})
	LastExemplarsTimestamp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds",
		Help: "The timestamp of the oldest exemplar stored in circular storage. Useful to check for what time range the current exemplar buffer limit allows. This usually means the last timestamp for all exemplars for a typical setup. This is not true though if one of the series timestamp is in future compared to rest series.",
	}, []string{"user"})
	OutOfOrderExemplars = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total", Help: "Total number of out-of-order exemplar ingestion failed attempts.",
	})
	OutOfOrderSamplesAppended = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_ingester_tsdb_out_of_order_samples_appended_total", Help: "Total number of out-of-order samples appended.",
	}, []string{"user"})
	MemorySeriesCreated = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_ingester_memory_series_created_total", Help: "The total number of series that were created per user.",
	}, []string{"user"})
	MemorySeriesRemoved = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_ingester_memory_series_removed_total", Help: "The total number of series that were removed per user.",
	}, []string{"user"})
	EarlyCompactionNonOwned = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_ingester_tsdb_early_compaction_non_owned_series_triggered_total", Help: "Total number of triggered early head compactions of non-owned series, per tenant.",
	}, []string{"user"})
	MemoryMetadataCreated = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_ingester_memory_metadata_created_total", Help: "The total number of metadata that were created per user",
	}, []string{"user"})
	MemoryMetadataRemoved = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_ingester_memory_metadata_removed_total", Help: "The total number of metadata that were removed per user.",
	}, []string{"user"})
	OwnedSeries = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_ingester_owned_series", Help: "Number of currently owned series per user.",
	}, []string{"user"})
	LocalLimits = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_ingester_local_limits", Help: "Local per-user limits used by this ingester.",
		ConstLabels: prometheus.Labels{"limit": "max_global_series_per_user"},
	}, []string{"user"})
	InstanceLimits = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_ingester_instance_limits", Help: "Instance limits used by this ingester.",
	}, []string{"limit"})
	HeadChunks = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cortex_ingester_tsdb_head_chunks", Help: "Total number of chunks in the TSDB head block.",
	})
	HeadChunksCreated = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_ingester_tsdb_head_chunks_created_total", Help: "Total number of series created in the TSDB head.",
	}, []string{"user"})
	HeadChunksRemoved = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_ingester_tsdb_head_chunks_removed_total", Help: "Total number of series removed in the TSDB head.",
	}, []string{"user"})
	HeadMinTimestamp = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cortex_ingester_tsdb_head_min_timestamp_seconds", Help: "Minimum timestamp of the head block across all tenants.",
	})
	HeadMaxTimestamp = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cortex_ingester_tsdb_head_max_timestamp_seconds", Help: "Maximum timestamp of the head block across all tenants.",
	})
	IngestionRateGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cortex_ingester_ingestion_rate_samples_per_second", Help: "Current ingestion rate in samples/sec that ingester is using to limit access.",
	})
	Queries = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cortex_ingester_queries_total", Help: "The total number of queries the ingester has handled.",
	})
	QueriedSamples = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "cortex_ingester_queried_samples", Help: "The total number of samples returned from queries.",
		Buckets: expBuckets(10, 8, 8),
	})
	QueriedExemplars = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "cortex_ingester_queried_exemplars", Help: "The total number of exemplars returned from queries.",
		Buckets: expBuckets(10, 5, 5),
	})
	QueriedSeries = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "cortex_ingester_queried_series", Help: "The total number of series returned from queries.",
		Buckets: expBuckets(10, 8, 6),
	}, []string{"stage"})
	CostAttributionCardinality = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_cost_attribution_active_series_tracker_cardinality", Help: "The cardinality of a cost attribution active series tracker for each user.",
	}, []string{"user", "tracker"})
	CostAttributionOverflown = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_cost_attribution_active_series_tracker_overflown", Help: "This metric is exported with value 1 when an active series tracker for a user is overflown. It's not exported otherwise.",
	}, []string{"user", "tracker"})
	MemorySeries = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cortex_ingester_memory_series", Help: "The current number of series in memory.",
	})
	MemoryUsers = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cortex_ingester_memory_users", Help: "The current number of users in memory.",
	})
	MemoryMetadata = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cortex_ingester_memory_metadata", Help: "The current number of metadata in memory.",
	})
	ActiveSeries = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_ingester_active_series", Help: "Number of currently active series per user.",
	}, []string{"user"})
	ActiveSeriesCustomTracker = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_ingester_active_series_custom_tracker", Help: "Number of currently active series matching a pre-configured label matchers per user.",
	}, []string{"user", "name"})
	ActiveNativeHistogramSeries = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_ingester_active_native_histogram_series", Help: "Number of currently active native histogram series per user.",
	}, []string{"user"})
	ActiveNativeHistogramSeriesCustomTracker = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_ingester_active_native_histogram_series_custom_tracker", Help: "Number of currently active native histogram series matching a pre-configured label matchers per user.",
	}, []string{"user", "name"})
	ActiveNativeHistogramBuckets = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_ingester_active_native_histogram_buckets", Help: "Number of currently active native histogram buckets per user.",
	}, []string{"user"})
	ActiveNativeHistogramBucketsCustomTracker = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_ingester_active_native_histogram_buckets_custom_tracker", Help: "Number of currently active native histogram buckets matching a pre-configured label matchers per user.",
	}, []string{"user", "name"})
	ActiveSeriesLoading = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cortex_ingester_active_series_loading", Help: "1 if active series counts are still warming up and may be underreported, 0 once they are accurate.",
	})
	// dskit's request duration histogram as Mimir configures it: native buckets growing by 1.1, at
	// most 100 of them, reset at most hourly, next to the classic ones.
	RequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "cortex_request_duration_seconds", Help: "Time (in seconds) spent serving HTTP requests.",
		Buckets:                         []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 25, 50, 100},
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"method", "route", "status_code", "ws"})
	InflightRequests = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_inflight_requests", Help: "Current number of inflight requests.",
	}, []string{"method", "route"})
	CircuitBreakerTransitions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_ingester_circuit_breaker_transitions_total", Help: "Number of times the circuit breaker has entered a state.",
	}, []string{"request_type", "state"})
	CircuitBreakerResults = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_ingester_circuit_breaker_results_total", Help: "Results of executing requests via the circuit breaker.",
	}, []string{"request_type", "result"})
	CircuitBreakerRequestTimeouts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_ingester_circuit_breaker_request_timeouts_total", Help: "Number of times the circuit breaker recorded a request that reached timeout.",
	}, []string{"request_type"})
	CircuitBreakerCurrentState = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_ingester_circuit_breaker_current_state", Help: "Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.",
	}, []string{"request_type", "state"})
	UtilizationLimitedReads = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_ingester_utilization_limited_read_requests_total", Help: "Total number of times read requests have been rejected due to utilization based limiting.",
	}, []string{"reason"})
	UtilizationCPU = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "utilization_limiter_current_cpu_load", Help: "Current average CPU load calculated by utilization based limiter.",
	})
	UtilizationMemory = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "utilization_limiter_current_memory_usage_bytes", Help: "Current memory usage calculated by utilization based limiter.",
	})
)

func init() {
	Registry.MustRegister(
		DiscardedSamples, DiscardedMetadata, IngestedSamples, IngestedSamplesFailures,
		IngestedExemplars, IngestedExemplarsFailures, IngestedMetadata, IngestedMetadataFailures,
		ExemplarsInStorage, ExemplarsAppended, SeriesWithExemplars, LastExemplarsTimestamp,
		OutOfOrderExemplars, OutOfOrderSamplesAppended, MemorySeriesCreated, MemorySeriesRemoved,
		EarlyCompactionNonOwned, MemoryMetadataCreated, MemoryMetadataRemoved, OwnedSeries,
		LocalLimits, InstanceLimits, HeadChunks, HeadChunksCreated, HeadChunksRemoved,
		HeadMinTimestamp, HeadMaxTimestamp, IngestionRateGauge, Queries, QueriedSamples,
		QueriedExemplars, QueriedSeries, CostAttributionCardinality, CostAttributionOverflown,
		MemorySeries, MemoryUsers, MemoryMetadata, ActiveSeries, ActiveSeriesCustomTracker,
		ActiveNativeHistogramSeries, ActiveNativeHistogramSeriesCustomTracker,
		ActiveNativeHistogramBuckets, ActiveNativeHistogramBucketsCustomTracker,
		ActiveSeriesLoading, RequestDuration, InflightRequests, CircuitBreakerTransitions,
		CircuitBreakerResults, CircuitBreakerRequestTimeouts, CircuitBreakerCurrentState,
		UtilizationLimitedReads, UtilizationCPU, UtilizationMemory,
	)
	// The runtime config's metrics live with its loader.
	Registry.MustRegister(limits.Collectors()...)
}

// RegisterProcessMetrics adds the process and Go runtime collectors, which only the binary wants:
// tests share the registry.
func RegisterProcessMetrics() {
	_ = Registry.Register(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	_ = Registry.Register(prometheus.NewGoCollector())
}

// IngestionRate is Mimir's `EwmaRate` with alpha 0.2 over one-second ticks, fed the ingested
// sample total.
type IngestionRate struct {
	lastTotal uint64
	rate      float64
	started   bool
}

// Tick takes the ingested sample total and returns the rate.
func (r *IngestionRate) Tick(total uint64) float64 {
	var events uint64
	if total > r.lastTotal {
		events = total - r.lastTotal
	}
	r.lastTotal = total
	instant := float64(events)
	if r.started {
		r.rate += 0.2 * (instant - r.rate)
	} else {
		r.rate = instant
		r.started = true
	}
	return r.rate
}
