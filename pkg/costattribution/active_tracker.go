// SPDX-License-Identifier: AGPL-3.0-only

package costattribution

import (
	"bytes"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"go.uber.org/atomic"
)

type asMetrics struct {
	asCount     *atomic.Int64
	nhCount     *atomic.Int64
	nhBucketNum *atomic.Int64
}

type ActiveSeriesTracker struct {
	userID                                         string
	activeSeriesPerUserAttribution                 *prometheus.Desc
	activeNativeHistogramSeriesPerUserAttribution  *prometheus.Desc
	activeNativeHistogramBucketsPerUserAttribution *prometheus.Desc
	logger                                         log.Logger

	labels         []string
	overflowLabels []string

	maxCardinality   int
	cooldownDuration time.Duration

	observedMtx   sync.RWMutex
	observed      map[string]asMetrics
	overflowSince time.Time

	overflowCounter asMetrics
}

func NewActiveSeriesTracker(userID string, trackedLabels []string, limit int, cooldownDuration time.Duration, logger log.Logger) *ActiveSeriesTracker {
	// Create a map for overflow labels to export when overflow happens
	overflowLabels := make([]string, len(trackedLabels)+2)
	for i := range trackedLabels {
		overflowLabels[i] = overflowValue
	}

	overflowLabels[len(trackedLabels)] = userID
	overflowLabels[len(trackedLabels)+1] = overflowValue

	ast := &ActiveSeriesTracker{
		userID:           userID,
		labels:           trackedLabels,
		maxCardinality:   limit,
		observed:         make(map[string]asMetrics),
		logger:           logger,
		overflowLabels:   overflowLabels,
		cooldownDuration: cooldownDuration,
	}

	variableLabels := slices.Clone(trackedLabels)
	variableLabels = append(variableLabels, tenantLabel, "reason")

	ast.activeSeriesPerUserAttribution = prometheus.NewDesc("cortex_ingester_attributed_active_series",
		"The total number of active series per user and attribution.", variableLabels[:len(variableLabels)-1],
		prometheus.Labels{trackerLabel: defaultTrackerName})
	ast.activeNativeHistogramSeriesPerUserAttribution = prometheus.NewDesc("cortex_ingester_attributed_active_native_histogram_series",
		"The total number of active native histogram series per user and attribution.", variableLabels[:len(variableLabels)-1],
		prometheus.Labels{trackerLabel: defaultTrackerName})
	ast.activeNativeHistogramBucketsPerUserAttribution = prometheus.NewDesc("cortex_ingester_attributed_active_native_histogram_buckets",
		"The total number of active native histogram buckets per user and attribution.", variableLabels[:len(variableLabels)-1],
		prometheus.Labels{trackerLabel: defaultTrackerName})
	ast.overflowCounter = asMetrics{
		asCount:     atomic.NewInt64(0),
		nhCount:     atomic.NewInt64(0),
		nhBucketNum: atomic.NewInt64(0),
	}
	return ast
}

func (at *ActiveSeriesTracker) hasSameLabels(labels []string) bool {
	return slices.Equal(at.labels, labels)
}

// Increment increases the active series count for the given labels.
// If nativeHistogramBucketNum is not -1, it also increments the native histogram counter and the corresponding bucket.
// Otherwise, only the active series count is updated.
func (at *ActiveSeriesTracker) Increment(lbls labels.Labels, now time.Time, nativeHistogramBucketNum int) {
	if at == nil {
		return
	}
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)
	at.fillKeyFromLabels(lbls, buf)

	at.observedMtx.RLock()
	as, ok := at.observed[string(buf.Bytes())]
	if ok {
		as.asCount.Inc()
		if nativeHistogramBucketNum >= 0 {
			as.nhCount.Inc()
			as.nhBucketNum.Add(int64(nativeHistogramBucketNum))
		}
		at.observedMtx.RUnlock()
		return
	}

	if !at.overflowSince.IsZero() {
		at.observedMtx.RUnlock()
		at.overflowCounter.asCount.Inc()
		if nativeHistogramBucketNum >= 0 {
			at.overflowCounter.nhCount.Inc()
			at.overflowCounter.nhBucketNum.Add(int64(nativeHistogramBucketNum))
		}
		return
	}

	as, ok = at.observed[string(buf.Bytes())]
	if ok {
		as.asCount.Inc()
		if nativeHistogramBucketNum >= 0 {
			as.nhCount.Inc()
			as.nhBucketNum.Add(int64(nativeHistogramBucketNum))
		}
		at.observedMtx.RUnlock()
		return
	}

	if !at.overflowSince.IsZero() {
		at.observedMtx.RUnlock()
		at.overflowCounter.asCount.Inc()
		if nativeHistogramBucketNum >= 0 {
			at.overflowCounter.nhCount.Inc()
			at.overflowCounter.nhBucketNum.Add(int64(nativeHistogramBucketNum))
		}
		return
	}
	at.observedMtx.RUnlock()

	at.observedMtx.Lock()
	defer at.observedMtx.Unlock()

	as, ok = at.observed[string(buf.Bytes())]
	if ok {
		as.asCount.Inc()
		if nativeHistogramBucketNum >= 0 {
			as.nhCount.Inc()
			as.nhBucketNum.Add(int64(nativeHistogramBucketNum))
		}
		return
	}

	if !at.overflowSince.IsZero() {
		at.overflowCounter.asCount.Inc()
		if nativeHistogramBucketNum >= 0 {
			at.overflowCounter.nhCount.Inc()
			at.overflowCounter.nhBucketNum.Add(int64(nativeHistogramBucketNum))
		}
		return
	}

	if len(at.observed) >= at.maxCardinality {
		at.overflowSince = now
		at.overflowCounter.asCount.Inc()
		if nativeHistogramBucketNum >= 0 {
			at.overflowCounter.nhCount.Inc()
			at.overflowCounter.nhBucketNum.Add(int64(nativeHistogramBucketNum))
		}
		return
	}

	nhCount := atomic.NewInt64(0)
	nhBucketNum := atomic.NewInt64(0)
	if nativeHistogramBucketNum >= 0 {
		nhCount.Inc()
		nhBucketNum.Add(int64(nativeHistogramBucketNum))
	}
	at.observed[string(buf.Bytes())] = asMetrics{
		asCount:     atomic.NewInt64(1),
		nhCount:     nhCount,
		nhBucketNum: nhBucketNum,
	}
}

func (at *ActiveSeriesTracker) Decrement(lbls labels.Labels, nativeHistogramBucketNum int) {
	if at == nil {
		return
	}
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)
	at.fillKeyFromLabels(lbls, buf)
	at.observedMtx.RLock()
	as, ok := at.observed[string(buf.Bytes())]
	if ok {
		nv := as.asCount.Dec()
		if nativeHistogramBucketNum >= 0 && as.nhCount.Load() > 0 {
			as.nhCount.Dec()
			as.nhBucketNum.Sub(int64(nativeHistogramBucketNum))
		}
		if nv > 0 {
			at.observedMtx.RUnlock()
			return
		}
		at.observedMtx.RUnlock()
		at.observedMtx.Lock()
		as, ok := at.observed[string(buf.Bytes())]
		if ok && as.asCount.Load() == 0 {
			// use buf.String() instead of string(buf.Bytes()) to fix the lint issue
			delete(at.observed, buf.String())
		}
		at.observedMtx.Unlock()
		return
	}
	at.observedMtx.RUnlock()

	at.observedMtx.RLock()
	defer at.observedMtx.RUnlock()

	if !at.overflowSince.IsZero() {
		at.overflowCounter.asCount.Dec()
		if nativeHistogramBucketNum >= 0 && at.overflowCounter.nhCount.Load() > 0 {
			at.overflowCounter.nhCount.Dec()
			at.overflowCounter.nhBucketNum.Sub(int64(nativeHistogramBucketNum))
		}
		return
	}
	panic(fmt.Errorf("decrementing non-existent active series: labels=%v, cost attribution keys: %v, the current observation map length: %d, the current cost attribution key: %s", lbls, at.labels, len(at.observed), buf.String()))
}

func (at *ActiveSeriesTracker) Collect(out chan<- prometheus.Metric) {
	at.observedMtx.RLock()
	if !at.overflowSince.IsZero() {
		var activeSeries int64
		var nativeHistogram int64
		var nhBucketNum int64
		for _, as := range at.observed {
			activeSeries += as.asCount.Load()
			nativeHistogram += as.nhCount.Load()
			nhBucketNum += as.nhBucketNum.Load()
		}
		at.observedMtx.RUnlock()
		out <- prometheus.MustNewConstMetric(at.activeSeriesPerUserAttribution, prometheus.GaugeValue, float64(activeSeries+at.overflowCounter.asCount.Load()), at.overflowLabels[:len(at.overflowLabels)-1]...)
		out <- prometheus.MustNewConstMetric(at.activeNativeHistogramSeriesPerUserAttribution, prometheus.GaugeValue, float64(nativeHistogram+at.overflowCounter.nhCount.Load()), at.overflowLabels[:len(at.overflowLabels)-1]...)
		out <- prometheus.MustNewConstMetric(at.activeNativeHistogramBucketsPerUserAttribution, prometheus.GaugeValue, float64(nhBucketNum+at.overflowCounter.nhBucketNum.Load()), at.overflowLabels[:len(at.overflowLabels)-1]...)
		return
	}
	// We don't know the performance of out receiver, so we don't want to hold the lock for too long
	var prometheusMetrics []prometheus.Metric
	for key, as := range at.observed {
		keys := strings.Split(key, string(sep))
		keys = append(keys, at.userID)
		prometheusMetrics = append(prometheusMetrics,
			prometheus.MustNewConstMetric(at.activeSeriesPerUserAttribution, prometheus.GaugeValue, float64(as.asCount.Load()), keys...),
			prometheus.MustNewConstMetric(at.activeNativeHistogramSeriesPerUserAttribution, prometheus.GaugeValue, float64(as.nhCount.Load()), keys...),
			prometheus.MustNewConstMetric(at.activeNativeHistogramBucketsPerUserAttribution, prometheus.GaugeValue, float64(as.nhBucketNum.Load()), keys...),
		)
	}
	at.observedMtx.RUnlock()

	for _, m := range prometheusMetrics {
		out <- m
	}
}

func (at *ActiveSeriesTracker) fillKeyFromLabels(lbls labels.Labels, buf *bytes.Buffer) {
	buf.Reset()
	for idx, cal := range at.labels {
		if idx > 0 {
			buf.WriteRune(sep)
		}
		v := lbls.Get(cal)
		if v != "" {
			buf.WriteString(v)
		} else {
			buf.WriteString(missingValue)
		}
	}
}
