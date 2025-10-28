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

	"github.com/grafana/mimir/pkg/costattribution/costattributionmodel"
)

const trackedSeriesFactor = 2

type counters struct {
	activeSeries           atomic.Int64
	nativeHistograms       atomic.Int64
	nativeHistogramBuckets atomic.Int64
}

type ActiveSeriesTracker struct {
	userID                                         string
	activeSeriesPerUserAttribution                 *prometheus.Desc
	attributedOverflowLabels                       *prometheus.Desc
	activeNativeHistogramSeriesPerUserAttribution  *prometheus.Desc
	activeNativeHistogramBucketsPerUserAttribution *prometheus.Desc
	logger                                         log.Logger

	labels         []costattributionmodel.Label
	overflowLabels []string

	// maxCardinality is the cardinality at which tracker enters the overflow state.
	// There are up to trackedSeriesFactor*maxCardinality series in observed map.
	maxCardinality   int
	cooldownDuration time.Duration

	observedMtx   sync.RWMutex
	observed      map[string]*counters
	overflowSince time.Time

	overflowCounter counters

	isInvalid atomic.Bool
}

func NewActiveSeriesTracker(userID string, trackedLabels []costattributionmodel.Label, limit int, cooldownDuration time.Duration, logger log.Logger) *ActiveSeriesTracker {
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
		observed:         make(map[string]*counters),
		logger:           logger,
		overflowLabels:   overflowLabels,
		cooldownDuration: cooldownDuration,
	}

	variableLabels := make([]string, 0, len(trackedLabels)+2)
	for _, label := range trackedLabels {
		variableLabels = append(variableLabels, label.OutputLabel())
	}
	variableLabels = append(variableLabels, tenantLabel, "reason")

	ast.activeSeriesPerUserAttribution = prometheus.NewDesc("cortex_ingester_attributed_active_series",
		"The total number of active series per user and attribution.", variableLabels[:len(variableLabels)-1],
		prometheus.Labels{trackerLabel: defaultTrackerName})
	ast.attributedOverflowLabels = prometheus.NewDesc("cortex_attributed_series_overflow_labels",
		"The overflow labels for this tenant. This metric is always 1 for tenants with active series, it is only used to have the overflow labels available in the recording rules without knowing their names.", variableLabels[:len(variableLabels)-1],
		prometheus.Labels{trackerLabel: defaultTrackerName})
	ast.activeNativeHistogramSeriesPerUserAttribution = prometheus.NewDesc("cortex_ingester_attributed_active_native_histogram_series",
		"The total number of active native histogram series per user and attribution.", variableLabels[:len(variableLabels)-1],
		prometheus.Labels{trackerLabel: defaultTrackerName})
	ast.activeNativeHistogramBucketsPerUserAttribution = prometheus.NewDesc("cortex_ingester_attributed_active_native_histogram_buckets",
		"The total number of active native histogram buckets per user and attribution.", variableLabels[:len(variableLabels)-1],
		prometheus.Labels{trackerLabel: defaultTrackerName})
	return ast
}

func (at *ActiveSeriesTracker) hasSameLabels(labels []costattributionmodel.Label) bool {
	return slices.Equal(at.labels, labels)
}

// Increment increases the active series count for the given labels.
// If nativeHistogramBucketNum is not -1, it also increments the native histogram counter and the corresponding bucket.
// Otherwise, only the active series count is updated.
func (at *ActiveSeriesTracker) Increment(lbls labels.Labels, now time.Time, nativeHistogramBucketNum int) {
	if at == nil || at.isInvalid.Load() {
		return
	}
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)
	at.fillKeyFromLabels(lbls, buf)

	// Happy path, maybe we're already tracking this cost-attribution labels combination.
	// First try just with a read lock.
	at.observedMtx.RLock()
	c, ok := at.observed[string(buf.Bytes())]
	if ok {
		// We have this combination already tracked, just increase the counter for it.
		c.activeSeries.Inc()
		if nativeHistogramBucketNum >= 0 {
			c.nativeHistograms.Inc()
			c.nativeHistogramBuckets.Add(int64(nativeHistogramBucketNum))
		}
		at.observedMtx.RUnlock()
		return
	}

	// We don't have this combination tracked.
	// If we are at capacity, there's no need to grab the write lock to try to add a new series.
	if len(at.observed) >= trackedSeriesFactor*at.maxCardinality {
		at.overflowCounter.activeSeries.Inc()
		if nativeHistogramBucketNum >= 0 {
			at.overflowCounter.nativeHistograms.Inc()
			at.overflowCounter.nativeHistogramBuckets.Add(int64(nativeHistogramBucketNum))
		}
		at.observedMtx.RUnlock()
		return
	}
	at.observedMtx.RUnlock()

	at.observedMtx.Lock()
	defer at.observedMtx.Unlock()

	// Are we alredy tracking this series?
	c, ok = at.observed[string(buf.Bytes())]
	if ok {
		c.activeSeries.Inc()
		if nativeHistogramBucketNum >= 0 {
			c.nativeHistograms.Inc()
			c.nativeHistogramBuckets.Add(int64(nativeHistogramBucketNum))
		}
		return
	}

	// If we can't store more series, we count this series in the overflow counter.
	if len(at.observed) >= trackedSeriesFactor*at.maxCardinality {
		at.overflowCounter.activeSeries.Inc()
		if nativeHistogramBucketNum >= 0 {
			at.overflowCounter.nativeHistograms.Inc()
			at.overflowCounter.nativeHistogramBuckets.Add(int64(nativeHistogramBucketNum))
		}
		return
	}

	// We still can store more series, add it to the map.
	counter := &counters{}
	counter.activeSeries.Inc()
	if nativeHistogramBucketNum >= 0 {
		counter.nativeHistograms.Inc()
		counter.nativeHistogramBuckets.Add(int64(nativeHistogramBucketNum))
	}
	at.observed[string(buf.Bytes())] = counter

	// Check whether we entered the overflow state.
	if len(at.observed) > at.maxCardinality && at.overflowSince.IsZero() {
		at.overflowSince = now
	}
}

func (at *ActiveSeriesTracker) Decrement(lbls labels.Labels, nativeHistogramBucketNum int) {
	if at == nil || at.isInvalid.Load() {
		return
	}
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)
	at.fillKeyFromLabels(lbls, buf)
	at.observedMtx.RLock()
	c, ok := at.observed[string(buf.Bytes())]
	if ok {
		nv := c.activeSeries.Dec()
		if nativeHistogramBucketNum >= 0 {
			c.nativeHistograms.Dec()
			c.nativeHistogramBuckets.Sub(int64(nativeHistogramBucketNum))
		}
		if nv > 0 {
			at.observedMtx.RUnlock()
			return
		}
		at.observedMtx.RUnlock()
		at.observedMtx.Lock()
		c, ok := at.observed[string(buf.Bytes())]
		if ok && c.activeSeries.Load() == 0 {
			// use buf.String() instead of string(buf.Bytes()) to fix the lint issue
			delete(at.observed, buf.String())
		}
		at.observedMtx.Unlock()
		return
	}
	defer at.observedMtx.RUnlock()

	if !at.overflowSince.IsZero() {
		at.overflowCounter.activeSeries.Dec()
		if nativeHistogramBucketNum >= 0 {
			at.overflowCounter.nativeHistograms.Dec()
			at.overflowCounter.nativeHistogramBuckets.Sub(int64(nativeHistogramBucketNum))
		}
		return
	}
	panic(fmt.Errorf("decrementing non-existent active series: labels=%v, cost attribution keys: %v, the current observation map length: %d, the current cost attribution key: %s", lbls, at.labels, len(at.observed), buf.String()))
}

func (at *ActiveSeriesTracker) Collect(out chan<- prometheus.Metric) (retErr error) {
	if at == nil || at.isInvalid.Load() {
		return nil
	}
	defer func() {
		at.isInvalid.Store(retErr != nil)
	}()
	if metric, err := prometheus.NewConstMetric(at.attributedOverflowLabels, prometheus.GaugeValue, 1, at.overflowLabels[:len(at.overflowLabels)-1]...); err != nil {
		return at.collectErr(err)
	} else {
		out <- metric
	}

	at.observedMtx.RLock()
	if !at.overflowSince.IsZero() {
		var (
			activeSeries      int64
			nativeHistogram   int64
			nhBucketNum       int64
			prometheusMetrics = make([]prometheus.Metric, 0, 3)
		)
		for _, c := range at.observed {
			activeSeries += c.activeSeries.Load()
			nativeHistogram += c.nativeHistograms.Load()
			nhBucketNum += c.nativeHistogramBuckets.Load()
		}
		at.observedMtx.RUnlock()

		if metric, err := prometheus.NewConstMetric(at.activeSeriesPerUserAttribution, prometheus.GaugeValue, float64(activeSeries+at.overflowCounter.activeSeries.Load()), at.overflowLabels[:len(at.overflowLabels)-1]...); err != nil {
			return at.collectErr(err)
		} else {
			prometheusMetrics = append(prometheusMetrics, metric)
		}

		if nhcounter := float64(nativeHistogram + at.overflowCounter.nativeHistograms.Load()); nhcounter > 0 {
			if metric, err := prometheus.NewConstMetric(at.activeNativeHistogramSeriesPerUserAttribution, prometheus.GaugeValue, nhcounter, at.overflowLabels[:len(at.overflowLabels)-1]...); err != nil {
				return at.collectErr(err)
			} else {
				prometheusMetrics = append(prometheusMetrics, metric)
			}
		}

		if bcounter := float64(nhBucketNum + at.overflowCounter.nativeHistogramBuckets.Load()); bcounter > 0 {
			if metric, err := prometheus.NewConstMetric(at.activeNativeHistogramBucketsPerUserAttribution, prometheus.GaugeValue, bcounter, at.overflowLabels[:len(at.overflowLabels)-1]...); err != nil {
				return at.collectErr(err)
			} else {
				prometheusMetrics = append(prometheusMetrics, metric)
			}
		}
		for _, m := range prometheusMetrics {
			out <- m
		}
		return nil
	}
	// We don't know the performance of out receiver, so we don't want to hold the lock for too long
	var prometheusMetrics []prometheus.Metric
	for key, c := range at.observed {
		keys := strings.Split(key, string(sep))
		keys = append(keys, at.userID)
		if metric, err := prometheus.NewConstMetric(at.activeSeriesPerUserAttribution, prometheus.GaugeValue, float64(c.activeSeries.Load()), keys...); err != nil {
			return at.collectErr(err)
		} else {
			prometheusMetrics = append(prometheusMetrics, metric)
		}

		if nhcounter := c.nativeHistograms.Load(); nhcounter > 0 {
			if metric, err := prometheus.NewConstMetric(at.activeNativeHistogramSeriesPerUserAttribution, prometheus.GaugeValue, float64(nhcounter), keys...); err != nil {
				return at.collectErr(err)
			} else {
				prometheusMetrics = append(prometheusMetrics, metric)
			}
		}
		if nbcounter := c.nativeHistogramBuckets.Load(); nbcounter > 0 {
			if metric, err := prometheus.NewConstMetric(at.activeNativeHistogramBucketsPerUserAttribution, prometheus.GaugeValue, float64(nbcounter), keys...); err != nil {
				return at.collectErr(err)
			} else {
				prometheusMetrics = append(prometheusMetrics, metric)
			}
		}
	}
	at.observedMtx.RUnlock()

	for _, m := range prometheusMetrics {
		out <- m
	}
	return nil
}

func (at *ActiveSeriesTracker) collectErr(err error) error {
	return fmt.Errorf("it was impossible to collect metrics of ActiveSeriesTracker for tenant %s and labels %s: %w", at.userID, costattributionmodel.FromCostAttributionLabelsToString(at.labels), err)
}

func (at *ActiveSeriesTracker) fillKeyFromLabels(lbls labels.Labels, buf *bytes.Buffer) {
	buf.Reset()
	for idx, cal := range at.labels {
		if idx > 0 {
			buf.WriteRune(sep)
		}
		v := lbls.Get(cal.Input)
		if v != "" {
			buf.WriteString(v)
		} else {
			buf.WriteString(missingValue)
		}
	}
}

func (at *ActiveSeriesTracker) cardinality() (cardinality int, overflown bool) {
	at.observedMtx.RLock()
	defer at.observedMtx.RUnlock()
	return len(at.observed), !at.overflowSince.IsZero()
}
