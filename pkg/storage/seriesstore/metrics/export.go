// SPDX-License-Identifier: AGPL-3.0-only

package metrics

import (
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/grafana/mimir/pkg/storage/seriesstore/limits"
	"github.com/grafana/mimir/pkg/storage/seriesstore/trackers"
)

// ActiveSeriesReport is a tenant's active series, metadata and exemplars, as the store reports
// them on each active series update.
type ActiveSeriesReport struct {
	Tenant                       string
	Series                       uint64
	Active                       uint64
	ActiveNativeHistograms       uint64
	ActiveNativeHistogramBuckets uint64
	// Per custom tracker name: active series, native histogram series and buckets.
	CustomTrackers   []TrackerCounts
	CostAttribution  []AttributedSeries
	Metadata         uint64
	Exemplars        uint64
	ExemplarSeries   uint64
	OldestExemplarMs int64
	HasOldest        bool
}

// TrackerCounts are a custom tracker's active series, native histogram series and buckets.
type TrackerCounts struct {
	Name   string
	Counts [3]uint64
}

// AttributedSeries is a cost attribution tracker's active series by attribution value.
type AttributedSeries struct {
	Tracker      string
	Internal     bool
	OutputLabels []string
	// Attribution values and their active series, native histogram series and buckets. An
	// overflowing tracker reports a single `__overflow__` entry.
	Values   []AttributedValue
	Overflow bool
	// Distinct attribution values, whether or not the tracker overflows.
	Cardinality uint64
}

// AttributedValue is one attribution's values and counts.
type AttributedValue struct {
	Values []string
	Counts [3]uint64
}

// HeadReport is a tenant's emulated Go head, as the store reports it on each head tick.
type HeadReport struct {
	Tenant       string
	MemorySeries uint64
	// Series created and removed since the last tick.
	SeriesCreated uint64
	SeriesRemoved uint64
	OwnedSeries   uint64
	HeadChunks    uint64
	HeadMinTime   int64
	HeadMaxTime   int64
	// Series evicted from the head as non-owned in this tick.
	NonOwnedEvicted uint64
	ActiveSeries    uint64
	// Whether a compaction truncated the head.
	Truncated bool
}

// attributedFamilies are attributed active series, one family per registry; internal trackers
// use the main one.
type attributedFamilies struct {
	activeSeries           *prometheus.GaugeVec
	overflowLabels         *prometheus.GaugeVec
	nativeHistogramSeries  *prometheus.GaugeVec
	nativeHistogramBuckets *prometheus.GaugeVec
}

type attributedKey struct {
	internal bool
	labels   string
}

var (
	attributedMu sync.Mutex
	attributed   = map[attributedKey]*attributedFamilies{}
)

// attributedSeries registers families lazily per tracker label set, because their label names
// come from the tracker configuration.
func attributedSeries(internal bool, labelNames []string) *attributedFamilies {
	attributedMu.Lock()
	defer attributedMu.Unlock()
	key := attributedKey{internal: internal, labels: strings.Join(labelNames, "\x00")}
	if family, ok := attributed[key]; ok {
		return family
	}
	registry := UsageRegistry
	if internal {
		registry = Registry
	}
	names := append(append([]string{}, labelNames...), "tenant", "tracker")
	family := func(name, help string) *prometheus.GaugeVec {
		gauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help}, names)
		// Another label set can already own the name in this registry; Prometheus clients reject
		// mixed label names within one family, so that tracker's series are not exported.
		_ = registry.Register(gauge)
		return gauge
	}
	series := &attributedFamilies{
		activeSeries: family("cortex_ingester_attributed_active_series",
			"The total number of active series per user and attribution."),
		overflowLabels: family("cortex_attributed_series_overflow_labels",
			"The overflow labels for this tenant. This metric is always 1 for tenants with active series, it is only used to have the overflow labels available in the recording rules without knowing their names."),
		nativeHistogramSeries: family("cortex_ingester_attributed_active_native_histogram_series",
			"The total number of active native histogram series per user and attribution."),
		nativeHistogramBuckets: family("cortex_ingester_attributed_active_native_histogram_buckets",
			"The total number of active native histogram buckets per user and attribution."),
	}
	attributed[key] = series
	return series
}

// resetAttributedSeries clears every attributed series, before an active series update sets the
// live ones again.
func resetAttributedSeries() {
	attributedMu.Lock()
	defer attributedMu.Unlock()
	for _, family := range attributed {
		family.activeSeries.Reset()
		family.overflowLabels.Reset()
		family.nativeHistogramSeries.Reset()
		family.nativeHistogramBuckets.Reset()
	}
}

// ExportActiveSeries replaces the per-tenant active series, memory and exemplar gauges with
// reports. Custom tracker series with no active series are not exported, like in Mimir.
func ExportActiveSeries(reports []ActiveSeriesReport) {
	for _, gauge := range []*prometheus.GaugeVec{
		ActiveSeries, ActiveNativeHistogramSeries, ActiveNativeHistogramBuckets,
		ActiveSeriesCustomTracker, ActiveNativeHistogramSeriesCustomTracker,
		ActiveNativeHistogramBucketsCustomTracker, SeriesWithExemplars,
		CostAttributionCardinality, CostAttributionOverflown, LastExemplarsTimestamp,
	} {
		gauge.Reset()
	}
	resetAttributedSeries()
	var metadata, exemplars uint64
	for i := range reports {
		report := &reports[i]
		tenant := report.Tenant
		metadata += report.Metadata
		// Like Mimir, a tenant without active series has none of these.
		for _, entry := range []struct {
			gauge *prometheus.GaugeVec
			value uint64
		}{
			{ActiveSeries, report.Active},
			{ActiveNativeHistogramSeries, report.ActiveNativeHistograms},
			{ActiveNativeHistogramBuckets, report.ActiveNativeHistogramBuckets},
		} {
			if entry.value > 0 {
				entry.gauge.WithLabelValues(tenant).Set(float64(entry.value))
			}
		}
		exemplars += report.Exemplars
		// Like the per-tenant TSDB metrics, exported for every tenant with a head.
		SeriesWithExemplars.WithLabelValues(tenant).Set(float64(report.ExemplarSeries))
		oldest := 0.0
		if report.HasOldest {
			oldest = float64(report.OldestExemplarMs) / 1000
		}
		LastExemplarsTimestamp.WithLabelValues(tenant).Set(oldest)
		for _, tracker := range report.CustomTrackers {
			active, histograms, buckets := tracker.Counts[0], tracker.Counts[1], tracker.Counts[2]
			if active > 0 {
				ActiveSeriesCustomTracker.WithLabelValues(tenant, tracker.Name).Set(float64(active))
			}
			if histograms > 0 {
				ActiveNativeHistogramSeriesCustomTracker.WithLabelValues(tenant, tracker.Name).Set(float64(histograms))
				ActiveNativeHistogramBucketsCustomTracker.WithLabelValues(tenant, tracker.Name).Set(float64(buckets))
			}
		}
		for _, tracker := range report.CostAttribution {
			CostAttributionCardinality.WithLabelValues(tenant, tracker.Tracker).Set(float64(tracker.Cardinality))
			if tracker.Overflow {
				CostAttributionOverflown.WithLabelValues(tenant, tracker.Tracker).Set(1)
			}
			family := attributedSeries(tracker.Internal, tracker.OutputLabels)
			labels := make([]string, 0, len(tracker.OutputLabels)+2)
			for range tracker.OutputLabels {
				labels = append(labels, trackers.OverflowValue)
			}
			labels = append(labels, tenant, tracker.Tracker)
			family.overflowLabels.WithLabelValues(labels...).Set(1)
			for _, value := range tracker.Values {
				labels = append(labels[:0], value.Values...)
				labels = append(labels, tenant, tracker.Tracker)
				family.activeSeries.WithLabelValues(labels...).Set(float64(value.Counts[0]))
				if value.Counts[1] > 0 {
					family.nativeHistogramSeries.WithLabelValues(labels...).Set(float64(value.Counts[1]))
					family.nativeHistogramBuckets.WithLabelValues(labels...).Set(float64(value.Counts[2]))
				}
			}
		}
	}
	MemoryUsers.Set(float64(len(reports)))
	MemoryMetadata.Set(float64(metadata))
	ExemplarsInStorage.Set(float64(exemplars))
}

// LocalSeriesLimit is a tenant's local series limit, for `cortex_ingester_local_limits`.
type LocalSeriesLimit struct {
	Tenant string
	Limit  uint64
}

func counterValue(counter prometheus.Counter) uint64 {
	var metric dto.Metric
	if err := counter.Write(&metric); err != nil {
		return 0
	}
	return uint64(metric.GetCounter().GetValue())
}

// ExportHead exports the emulated Go head state, the per-tenant local series limit and instance
// limits.
func ExportHead(reports []HeadReport, localSeriesLimits []LocalSeriesLimit, instance limits.InstanceLimits, ownedSeries bool) {
	OwnedSeries.Reset()
	LocalLimits.Reset()
	var series, chunks uint64
	minTime, maxTime := int64(1<<63-1), int64(-1<<63)
	for i := range reports {
		report := &reports[i]
		tenant := report.Tenant
		series += report.MemorySeries
		chunks += report.HeadChunks
		if report.SeriesCreated > 0 {
			MemorySeriesCreated.WithLabelValues(tenant).Add(float64(report.SeriesCreated))
		}
		if report.SeriesRemoved > 0 {
			MemorySeriesRemoved.WithLabelValues(tenant).Add(float64(report.SeriesRemoved))
		}
		if report.NonOwnedEvicted > 0 {
			EarlyCompactionNonOwned.WithLabelValues(tenant).Inc()
		}
		if ownedSeries {
			OwnedSeries.WithLabelValues(tenant).Set(float64(report.OwnedSeries))
		}
		// Removed chunks are the created ones no longer in the head, which never decreases.
		created := counterValue(HeadChunksCreated.WithLabelValues(tenant))
		removed := HeadChunksRemoved.WithLabelValues(tenant)
		var nowRemoved uint64
		if created > report.HeadChunks {
			nowRemoved = created - report.HeadChunks
		}
		if already := counterValue(removed); nowRemoved > already {
			removed.Add(float64(nowRemoved - already))
		}
		// A truncated head keeps its time range when compactions emptied it.
		if report.MemorySeries > 0 || report.Truncated {
			minTime = min(minTime, report.HeadMinTime)
			maxTime = max(maxTime, report.HeadMaxTime)
		}
	}
	for _, limit := range localSeriesLimits {
		LocalLimits.WithLabelValues(limit.Tenant).Set(float64(limit.Limit))
	}
	MemorySeries.Set(float64(series))
	HeadChunks.Set(float64(chunks))
	// Like Go, no head data reports 0.
	if minTime == int64(1<<63-1) {
		HeadMinTimestamp.Set(0)
	} else {
		HeadMinTimestamp.Set(float64(minTime) / 1000)
	}
	if maxTime == int64(-1<<63) {
		HeadMaxTimestamp.Set(0)
	} else {
		HeadMaxTimestamp.Set(float64(maxTime) / 1000)
	}
	for _, entry := range []struct {
		name  string
		value float64
	}{
		{"max_ingestion_rate", instance.MaxIngestionRate},
		{"max_tenants", float64(instance.MaxTenants)},
		{"max_series", float64(instance.MaxSeries)},
		{"max_inflight_push_requests", float64(instance.MaxInflightPushRequests)},
		{"max_inflight_push_requests_bytes", float64(instance.MaxInflightPushRequestsBytes)},
	} {
		InstanceLimits.WithLabelValues(entry.name).Set(entry.value)
	}
}
