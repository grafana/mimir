// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/metrics.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/atomic"

	util_math "github.com/grafana/mimir/pkg/util/math"
	"github.com/grafana/mimir/pkg/util/validation"
)

type ingesterMetrics struct {
	ingestedSamples       *prometheus.CounterVec
	ingestedExemplars     prometheus.Counter
	ingestedMetadata      prometheus.Counter
	ingestedSamplesFail   *prometheus.CounterVec
	ingestedExemplarsFail prometheus.Counter
	ingestedMetadataFail  prometheus.Counter

	queries              prometheus.Counter
	queriedSamples       prometheus.Histogram
	queriedExemplars     prometheus.Histogram
	queriedSeries        *prometheus.HistogramVec
	discardedSeriesRatio prometheus.Histogram

	memMetadata             prometheus.Gauge
	memUsers                prometheus.Gauge
	memMetadataCreatedTotal *prometheus.CounterVec
	memMetadataRemovedTotal *prometheus.CounterVec

	activeSeriesLoading                               *prometheus.GaugeVec
	activeSeriesPerUser                               *prometheus.GaugeVec
	activeSeriesPerUserOTLP                           *prometheus.GaugeVec
	activeSeriesCustomTrackersPerUser                 *prometheus.GaugeVec
	activeSeriesPerUserNativeHistograms               *prometheus.GaugeVec
	activeSeriesCustomTrackersPerUserNativeHistograms *prometheus.GaugeVec
	activeNativeHistogramBucketsPerUser               *prometheus.GaugeVec
	activeNativeHistogramBucketsCustomTrackersPerUser *prometheus.GaugeVec

	attributedActiveSeriesFailuresPerUser *prometheus.CounterVec

	// Owned series
	ownedSeriesPerUser *prometheus.GaugeVec

	// Global limit metrics
	maxUsersGauge                prometheus.GaugeFunc
	maxSeriesGauge               prometheus.GaugeFunc
	maxIngestionRate             prometheus.GaugeFunc
	ingestionRate                prometheus.GaugeFunc
	maxInflightPushRequests      prometheus.GaugeFunc
	maxInflightPushRequestsBytes prometheus.GaugeFunc
	inflightRequests             prometheus.GaugeFunc
	inflightRequestsBytes        prometheus.GaugeFunc
	inflightRequestsSummary      prometheus.Summary

	// Local limit metrics
	maxLocalSeriesPerUser *prometheus.GaugeVec

	// Head compactions metrics.
	compactionsTriggered               prometheus.Counter
	compactionsFailed                  prometheus.Counter
	forcedCompactionInProgress         prometheus.Gauge
	perTenantEarlyCompactionsTriggered prometheus.Counter
	appenderAddDuration                prometheus.Histogram
	appenderCommitDuration             prometheus.Histogram
	idleTsdbChecks                     *prometheus.CounterVec

	// Reference counter for forced/idle compactions across all user TSDBs.
	// Used to set forcedCompactionInProgress to 1 when any compaction is running, 0 when all complete.
	forcedCompactionsCount int64
	forcedCompactionsMtx   sync.Mutex

	// Open all existing TSDBs metrics
	openExistingTSDB prometheus.Counter

	discarded *discardedMetrics
	rejected  *prometheus.CounterVec

	// Discarded metadata
	discardedMetadataPerUserMetadataLimit   *prometheus.CounterVec
	discardedMetadataPerMetricMetadataLimit *prometheus.CounterVec

	// Shutdown marker for ingester scale down
	shutdownMarker prometheus.Gauge

	// Count number of requests rejected due to utilization based limiting.
	utilizationLimitedRequests *prometheus.CounterVec

	// Index lookup planning comparison outcomes.
	indexLookupComparisonOutcomes *prometheus.CounterVec
}

func newIngesterMetrics(
	r prometheus.Registerer,
	activeSeriesEnabled bool,
	instanceLimitsFn func() *InstanceLimits,
	ingestionRate *util_math.EwmaRate,
	inflightRequests *atomic.Int64,
	inflightRequestsBytes *atomic.Int64,
) *ingesterMetrics {
	const (
		instanceLimits     = "cortex_ingester_instance_limits"
		instanceLimitsHelp = "Instance limits used by this ingester." // Must be same for all registrations.
		limitLabel         = "limit"
	)

	idleTsdbChecks := promauto.With(r).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_ingester_idle_tsdb_checks_total",
		Help: "The total number of various results for idle TSDB checks.",
	}, []string{"result"})

	idleTsdbChecks.WithLabelValues(string(tsdbShippingDisabled))
	idleTsdbChecks.WithLabelValues(string(tsdbNotIdle))
	idleTsdbChecks.WithLabelValues(string(tsdbNotCompacted))
	idleTsdbChecks.WithLabelValues(string(tsdbNotShipped))
	idleTsdbChecks.WithLabelValues(string(tsdbCheckFailed))
	idleTsdbChecks.WithLabelValues(string(tsdbCloseFailed))
	idleTsdbChecks.WithLabelValues(string(tsdbNotActive))
	idleTsdbChecks.WithLabelValues(string(tsdbDataRemovalFailed))
	idleTsdbChecks.WithLabelValues(string(tsdbTenantMarkedForDeletion))
	idleTsdbChecks.WithLabelValues(string(tsdbIdleClosed))

	// Active series metrics are registered only if enabled.
	var activeSeriesReg prometheus.Registerer
	if activeSeriesEnabled {
		activeSeriesReg = r
	}

	m := &ingesterMetrics{
		ingestedSamples: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingester_ingested_samples_total",
			Help: "The total number of samples ingested per user.",
		}, []string{"user"}),
		ingestedExemplars: promauto.With(r).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingester_ingested_exemplars_total",
			Help: "The total number of exemplars ingested.",
		}),
		ingestedMetadata: promauto.With(r).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingester_ingested_metadata_total",
			Help: "The total number of metadata ingested.",
		}),
		ingestedSamplesFail: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingester_ingested_samples_failures_total",
			Help: "The total number of samples that errored on ingestion per user.",
		}, []string{"user"}),
		ingestedExemplarsFail: promauto.With(r).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingester_ingested_exemplars_failures_total",
			Help: "The total number of exemplars that errored on ingestion.",
		}),
		ingestedMetadataFail: promauto.With(r).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingester_ingested_metadata_failures_total",
			Help: "The total number of metadata that errored on ingestion.",
		}),
		queries: promauto.With(r).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingester_queries_total",
			Help: "The total number of queries the ingester has handled.",
		}),
		queriedSamples: promauto.With(r).NewHistogram(prometheus.HistogramOpts{
			Name: "cortex_ingester_queried_samples",
			Help: "The total number of samples returned from queries.",
			// Could easily return 10m samples per query - 10*(8^(8-1)) = 20.9m.
			Buckets: prometheus.ExponentialBuckets(10, 8, 8),
		}),
		queriedExemplars: promauto.With(r).NewHistogram(prometheus.HistogramOpts{
			Name: "cortex_ingester_queried_exemplars",
			Help: "The total number of exemplars returned from queries.",
			// A reasonable upper bound is around 6k - 10*(5^(5-1)) = 6250.
			Buckets: prometheus.ExponentialBuckets(10, 5, 5),
		}),
		queriedSeries: promauto.With(r).NewHistogramVec(prometheus.HistogramOpts{
			Name: "cortex_ingester_queried_series",
			Help: "The total number of series returned from queries.",
			// A reasonable upper bound is around 100k - 10*(8^(6-1)) = 327k.
			Buckets:                         prometheus.ExponentialBuckets(10, 8, 6),
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: 1 * time.Hour,
		}, []string{"stage"}),
		discardedSeriesRatio: promauto.With(r).NewHistogram(prometheus.HistogramOpts{
			Name:                            "cortex_ingester_discarded_series_ratio",
			Help:                            `Ratio of discarded series during query processing. These are series fetched from the index, but then discarded because they don't match the vector selector. This is the ratio of cortex_ingester_queried_series{stage="index"} over {stage="send"}.`,
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: 1 * time.Hour,
		}),
		memMetadata: promauto.With(r).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_ingester_memory_metadata",
			Help: "The current number of metadata in memory.",
		}),
		memUsers: promauto.With(r).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_ingester_memory_users",
			Help: "The current number of users in memory.",
		}),
		memMetadataCreatedTotal: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingester_memory_metadata_created_total",
			Help: "The total number of metadata that were created per user",
		}, []string{"user"}),
		memMetadataRemovedTotal: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingester_memory_metadata_removed_total",
			Help: "The total number of metadata that were removed per user.",
		}, []string{"user"}),
		utilizationLimitedRequests: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingester_utilization_limited_read_requests_total",
			Help: "Total number of times read requests have been rejected due to utilization based limiting.",
		}, []string{"reason"}),

		ownedSeriesPerUser: promauto.With(r).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_ingester_owned_series",
			Help: "Number of currently owned series per user.",
		}, []string{"user"}),
		attributedActiveSeriesFailuresPerUser: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingester_attributed_active_series_failure",
			Help: "The total number of failed active series decrement per user",
		}, []string{"user"}),
		maxUsersGauge: promauto.With(r).NewGaugeFunc(prometheus.GaugeOpts{
			Name:        instanceLimits,
			Help:        instanceLimitsHelp,
			ConstLabels: map[string]string{limitLabel: "max_tenants"},
		}, func() float64 {
			if g := instanceLimitsFn(); g != nil {
				return float64(g.MaxInMemoryTenants)
			}
			return 0
		}),

		maxSeriesGauge: promauto.With(r).NewGaugeFunc(prometheus.GaugeOpts{
			Name:        instanceLimits,
			Help:        instanceLimitsHelp,
			ConstLabels: map[string]string{limitLabel: "max_series"},
		}, func() float64 {
			if g := instanceLimitsFn(); g != nil {
				return float64(g.MaxInMemorySeries)
			}
			return 0
		}),

		maxIngestionRate: promauto.With(r).NewGaugeFunc(prometheus.GaugeOpts{
			Name:        instanceLimits,
			Help:        instanceLimitsHelp,
			ConstLabels: map[string]string{limitLabel: "max_ingestion_rate"},
		}, func() float64 {
			if g := instanceLimitsFn(); g != nil {
				return g.MaxIngestionRate
			}
			return 0
		}),

		maxInflightPushRequests: promauto.With(r).NewGaugeFunc(prometheus.GaugeOpts{
			Name:        instanceLimits,
			Help:        instanceLimitsHelp,
			ConstLabels: map[string]string{limitLabel: "max_inflight_push_requests"},
		}, func() float64 {
			if g := instanceLimitsFn(); g != nil {
				return float64(g.MaxInflightPushRequests)
			}
			return 0
		}),

		maxInflightPushRequestsBytes: promauto.With(r).NewGaugeFunc(prometheus.GaugeOpts{
			Name:        instanceLimits,
			Help:        instanceLimitsHelp,
			ConstLabels: map[string]string{limitLabel: "max_inflight_push_requests_bytes"},
		}, func() float64 {
			if g := instanceLimitsFn(); g != nil {
				return float64(g.MaxInflightPushRequestsBytes)
			}
			return 0
		}),

		ingestionRate: promauto.With(r).NewGaugeFunc(prometheus.GaugeOpts{
			Name: "cortex_ingester_ingestion_rate_samples_per_second",
			Help: "Current ingestion rate in samples/sec that ingester is using to limit access.",
		}, func() float64 {
			if ingestionRate != nil {
				return ingestionRate.Rate()
			}
			return 0
		}),

		inflightRequests: promauto.With(r).NewGaugeFunc(prometheus.GaugeOpts{
			Name: "cortex_ingester_inflight_push_requests",
			Help: "Current number of inflight push requests in ingester.",
		}, func() float64 {
			if inflightRequests != nil {
				return float64(inflightRequests.Load())
			}
			return 0
		}),

		inflightRequestsBytes: promauto.With(r).NewGaugeFunc(prometheus.GaugeOpts{
			Name: "cortex_ingester_inflight_push_requests_bytes",
			Help: "Total sum of inflight push request sizes in ingester in bytes.",
		}, func() float64 {
			if inflightRequestsBytes != nil {
				return float64(inflightRequestsBytes.Load())
			}
			return 0
		}),

		inflightRequestsSummary: promauto.With(r).NewSummary(prometheus.SummaryOpts{
			Name:       "cortex_ingester_inflight_push_requests_summary",
			Help:       "Number of inflight requests sampled at a regular interval. Quantile buckets keep track of inflight requests over the last 60s.",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.95: 0.01, 0.99: 0.001, 1.00: 0.001},
			MaxAge:     time.Minute,
			AgeBuckets: 6,
		}),

		maxLocalSeriesPerUser: promauto.With(r).NewGaugeVec(prometheus.GaugeOpts{
			Name:        "cortex_ingester_local_limits",
			Help:        "Local per-user limits used by this ingester.",
			ConstLabels: map[string]string{"limit": "max_global_series_per_user"},
		}, []string{"user"}),

		// Not registered automatically, but only if activeSeriesEnabled is true.
		activeSeriesLoading: promauto.With(activeSeriesReg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_ingester_active_series_loading",
			Help: "Indicates that active series configuration is being reloaded, and waiting to become stable. While this metric is non zero, values from active series metrics shouldn't be considered.",
		}, []string{"user"}),

		// Not registered automatically, but only if activeSeriesEnabled is true.
		activeSeriesPerUser: promauto.With(activeSeriesReg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_ingester_active_series",
			Help: "Number of currently active series per user.",
		}, []string{"user"}),

		// Not registered automatically, but only if activeSeriesEnabled is true.
		activeSeriesPerUserOTLP: promauto.With(activeSeriesReg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_ingester_active_otlp_series",
			Help: "Number of currently active series per user ingested via OTLP.",
		}, []string{"user"}),

		// Not registered automatically, but only if activeSeriesEnabled is true.
		activeSeriesCustomTrackersPerUser: promauto.With(activeSeriesReg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_ingester_active_series_custom_tracker",
			Help: "Number of currently active series matching a pre-configured label matchers per user.",
		}, []string{"user", "name"}),

		// Not registered automatically, but only if activeSeriesEnabled is true.
		activeSeriesPerUserNativeHistograms: promauto.With(activeSeriesReg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_ingester_active_native_histogram_series",
			Help: "Number of currently active native histogram series per user.",
		}, []string{"user"}),

		// Not registered automatically, but only if activeSeriesEnabled is true.
		activeSeriesCustomTrackersPerUserNativeHistograms: promauto.With(activeSeriesReg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_ingester_active_native_histogram_series_custom_tracker",
			Help: "Number of currently active native histogram series matching a pre-configured label matchers per user.",
		}, []string{"user", "name"}),

		// Not registered automatically, but only if activeSeriesEnabled is true.
		activeNativeHistogramBucketsPerUser: promauto.With(activeSeriesReg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_ingester_active_native_histogram_buckets",
			Help: "Number of currently active native histogram buckets per user.",
		}, []string{"user"}),

		// Not registered automatically, but only if activeSeriesEnabled is true.
		activeNativeHistogramBucketsCustomTrackersPerUser: promauto.With(activeSeriesReg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_ingester_active_native_histogram_buckets_custom_tracker",
			Help: "Number of currently active native histogram buckets matching a pre-configured label matchers per user.",
		}, []string{"user", "name"}),

		compactionsTriggered: promauto.With(r).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingester_tsdb_compactions_triggered_total",
			Help: "Total number of triggered compactions.",
		}),
		compactionsFailed: promauto.With(r).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingester_tsdb_compactions_failed_total",
			Help: "Total number of compactions that failed.",
		}),
		forcedCompactionInProgress: promauto.With(r).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_ingester_tsdb_forced_compactions_in_progress",
			Help: "Reports 1 if there's a forced or idle TSDB head compaction in progress, 0 otherwise.",
		}),
		perTenantEarlyCompactionsTriggered: promauto.With(r).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingester_tsdb_per_tenant_early_compactions_triggered_total",
			Help: "Total number of triggered per-tenant early compactions.",
		}),

		appenderAddDuration: promauto.With(r).NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_ingester_tsdb_appender_add_duration_seconds",
			Help:    "The total time it takes for a push request to add samples to the TSDB appender.",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}),
		appenderCommitDuration: promauto.With(r).NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_ingester_tsdb_appender_commit_duration_seconds",
			Help:    "The total time it takes for a push request to commit samples appended to TSDB.",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}),

		idleTsdbChecks: idleTsdbChecks,

		openExistingTSDB: promauto.With(r).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingester_tsdb_open_duration_seconds_total",
			Help: "The total time it takes to open all existing TSDBs at ingester startup. This time also includes the TSDBs WAL replay duration.",
		}),

		discarded: newDiscardedMetrics(r),
		rejected: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingester_instance_rejected_requests_total",
			Help: "Requests rejected for hitting per-instance limits",
		}, []string{"reason"}),

		discardedMetadataPerUserMetadataLimit:   validation.DiscardedMetadataCounter(r, perUserMetadataLimit),
		discardedMetadataPerMetricMetadataLimit: validation.DiscardedMetadataCounter(r, perMetricMetadataLimit),

		shutdownMarker: promauto.With(r).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_ingester_prepare_shutdown_requested",
			Help: "If the ingester has been requested to prepare for shutdown via endpoint or marker file.",
		}),

		indexLookupComparisonOutcomes: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingester_index_lookup_planning_comparison_outcomes_total",
			Help: "Total number of index lookup planning comparison outcomes when using mirrored chunk querier.",
		}, []string{"outcome", "user"}),
	}

	// Initialize expected rejected request labels
	m.rejected.WithLabelValues(reasonIngesterMaxIngestionRate)
	m.rejected.WithLabelValues(reasonIngesterMaxTenants)
	m.rejected.WithLabelValues(reasonIngesterMaxInMemorySeries)
	m.rejected.WithLabelValues(reasonIngesterMaxInflightPushRequests)
	m.rejected.WithLabelValues(reasonIngesterMaxInflightPushRequestsBytes)
	m.rejected.WithLabelValues(reasonIngesterMaxInflightReadRequests)

	return m
}

func (m *ingesterMetrics) deletePerUserMetrics(userID string) {
	m.ingestedSamples.DeleteLabelValues(userID)
	m.ingestedSamplesFail.DeleteLabelValues(userID)
	m.memMetadataCreatedTotal.DeleteLabelValues(userID)
	m.memMetadataRemovedTotal.DeleteLabelValues(userID)

	filter := prometheus.Labels{"user": userID}
	m.discarded.DeletePartialMatch(filter)

	m.discardedMetadataPerUserMetadataLimit.DeleteLabelValues(userID)
	m.discardedMetadataPerMetricMetadataLimit.DeleteLabelValues(userID)

	m.maxLocalSeriesPerUser.DeleteLabelValues(userID)
	m.ownedSeriesPerUser.DeleteLabelValues(userID)
	m.attributedActiveSeriesFailuresPerUser.DeleteLabelValues(userID)
}

func (m *ingesterMetrics) deletePerGroupMetricsForUser(userID, group string) {
	m.discarded.DeleteLabelValues(userID, group)
}

func (m *ingesterMetrics) deletePerUserCustomTrackerMetrics(userID string, customTrackerMetrics []string) {
	m.activeSeriesLoading.DeleteLabelValues(userID)
	m.activeSeriesPerUser.DeleteLabelValues(userID)
	m.activeSeriesPerUserOTLP.DeleteLabelValues(userID)
	m.activeSeriesPerUserNativeHistograms.DeleteLabelValues(userID)
	m.activeNativeHistogramBucketsPerUser.DeleteLabelValues(userID)
	for _, name := range customTrackerMetrics {
		m.activeSeriesCustomTrackersPerUser.DeleteLabelValues(userID, name)
		m.activeSeriesCustomTrackersPerUserNativeHistograms.DeleteLabelValues(userID, name)
		m.activeNativeHistogramBucketsCustomTrackersPerUser.DeleteLabelValues(userID, name)
	}
}

func (m *ingesterMetrics) increaseForcedCompactions() {
	m.forcedCompactionsMtx.Lock()
	defer m.forcedCompactionsMtx.Unlock()
	m.forcedCompactionsCount++
	if m.forcedCompactionsCount == 1 {
		m.forcedCompactionInProgress.Set(1)
	}
}

func (m *ingesterMetrics) decreaseForcedCompactions() {
	m.forcedCompactionsMtx.Lock()
	defer m.forcedCompactionsMtx.Unlock()
	m.forcedCompactionsCount--
	if m.forcedCompactionsCount == 0 {
		m.forcedCompactionInProgress.Set(0)
	}
}

func (m *ingesterMetrics) resetForcedCompactions() {
	m.forcedCompactionsMtx.Lock()
	defer m.forcedCompactionsMtx.Unlock()
	m.forcedCompactionsCount = 0
	m.forcedCompactionInProgress.Set(0)
}

type discardedMetrics struct {
	sampleTimestampTooOld  *prometheus.CounterVec
	sampleOutOfOrder       *prometheus.CounterVec
	sampleTooOld           *prometheus.CounterVec
	sampleTooFarInFuture   *prometheus.CounterVec
	newValueForTimestamp   *prometheus.CounterVec
	perUserSeriesLimit     *prometheus.CounterVec
	perMetricSeriesLimit   *prometheus.CounterVec
	invalidNativeHistogram *prometheus.CounterVec
	labelsNotSorted        *prometheus.CounterVec
}

func newDiscardedMetrics(r prometheus.Registerer) *discardedMetrics {
	return &discardedMetrics{
		sampleTimestampTooOld:  validation.DiscardedSamplesCounter(r, reasonSampleTimestampTooOld),
		sampleOutOfOrder:       validation.DiscardedSamplesCounter(r, reasonSampleOutOfOrder),
		sampleTooOld:           validation.DiscardedSamplesCounter(r, reasonSampleTooOld),
		sampleTooFarInFuture:   validation.DiscardedSamplesCounter(r, reasonSampleTooFarInFuture),
		newValueForTimestamp:   validation.DiscardedSamplesCounter(r, reasonNewValueForTimestamp),
		perUserSeriesLimit:     validation.DiscardedSamplesCounter(r, reasonPerUserSeriesLimit),
		perMetricSeriesLimit:   validation.DiscardedSamplesCounter(r, reasonPerMetricSeriesLimit),
		invalidNativeHistogram: validation.DiscardedSamplesCounter(r, reasonInvalidNativeHistogram),
		labelsNotSorted:        validation.DiscardedSamplesCounter(r, reasonLabelsNotSorted),
	}
}

func (m *discardedMetrics) DeletePartialMatch(filter prometheus.Labels) {
	m.sampleTimestampTooOld.DeletePartialMatch(filter)
	m.sampleOutOfOrder.DeletePartialMatch(filter)
	m.sampleTooOld.DeletePartialMatch(filter)
	m.sampleTooFarInFuture.DeletePartialMatch(filter)
	m.newValueForTimestamp.DeletePartialMatch(filter)
	m.perUserSeriesLimit.DeletePartialMatch(filter)
	m.perMetricSeriesLimit.DeletePartialMatch(filter)
	m.invalidNativeHistogram.DeletePartialMatch(filter)
	m.labelsNotSorted.DeletePartialMatch(filter)
}

func (m *discardedMetrics) DeleteLabelValues(userID string, group string) {
	m.sampleTimestampTooOld.DeleteLabelValues(userID, group)
	m.sampleOutOfOrder.DeleteLabelValues(userID, group)
	m.sampleTooOld.DeleteLabelValues(userID, group)
	m.sampleTooFarInFuture.DeleteLabelValues(userID, group)
	m.newValueForTimestamp.DeleteLabelValues(userID, group)
	m.perUserSeriesLimit.DeleteLabelValues(userID, group)
	m.perMetricSeriesLimit.DeleteLabelValues(userID, group)
	m.invalidNativeHistogram.DeleteLabelValues(userID, group)
	m.labelsNotSorted.DeleteLabelValues(userID, group)
}
