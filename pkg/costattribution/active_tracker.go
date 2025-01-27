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

type ActiveSeriesTracker struct {
	userID                         string
	activeSeriesPerUserAttribution *prometheus.Desc
	logger                         log.Logger

	labels         []string
	overflowLabels []string

	maxCardinality   int
	cooldownDuration time.Duration

	observedMtx   sync.RWMutex
	observed      map[string]*atomic.Int64
	overflowSince time.Time

	overflowCounter atomic.Int64
}

func addLabelsPrefix(labels []string) []string {
	out := make([]string, 0, len(labels))
	for _, l := range labels {
		out = append(out, strings.Join([]string{usagePrefix, l}, ""))
	}
	return out
}

func newActiveSeriesTracker(userID string, trackedLabels []string, limit int, cooldownDuration time.Duration, logger log.Logger) *ActiveSeriesTracker {
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
		observed:         make(map[string]*atomic.Int64),
		logger:           logger,
		overflowLabels:   overflowLabels,
		cooldownDuration: cooldownDuration,
	}

	labelsWithPrefix := addLabelsPrefix(trackedLabels)
	labelsWithPrefix = append(labelsWithPrefix, tenantLabel)
	ast.activeSeriesPerUserAttribution = prometheus.NewDesc("cortex_ingester_attributed_active_series",
		"The total number of active series per user and attribution.", labelsWithPrefix,
		prometheus.Labels{trackerLabel: defaultTrackerName})

	return ast
}

func (at *ActiveSeriesTracker) hasSameLabels(labels []string) bool {
	return slices.Equal(at.labels, labels)
}

func (at *ActiveSeriesTracker) Increment(lbls labels.Labels, now time.Time) {
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
		as.Inc()
		at.observedMtx.RUnlock()
		return
	}

	if !at.overflowSince.IsZero() {
		at.observedMtx.RUnlock()
		at.overflowCounter.Inc()
		return
	}

	as, ok = at.observed[string(buf.Bytes())]
	if ok {
		as.Inc()
		at.observedMtx.RUnlock()
		return
	}

	if !at.overflowSince.IsZero() {
		at.observedMtx.RUnlock()
		at.overflowCounter.Inc()
		return
	}
	at.observedMtx.RUnlock()

	at.observedMtx.Lock()
	defer at.observedMtx.Unlock()

	as, ok = at.observed[string(buf.Bytes())]
	if ok {
		as.Inc()
		return
	}

	if !at.overflowSince.IsZero() {
		at.overflowCounter.Inc()
		return
	}

	if len(at.observed) >= at.maxCardinality {
		at.overflowSince = now
		at.overflowCounter.Inc()
		return
	}
	at.observed[string(buf.Bytes())] = atomic.NewInt64(1)
}

func (at *ActiveSeriesTracker) Decrement(lbls labels.Labels) {
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
		nv := as.Dec()
		if nv > 0 {
			at.observedMtx.RUnlock()
			return
		}
		at.observedMtx.RUnlock()
		at.observedMtx.Lock()
		as, ok := at.observed[string(buf.Bytes())]
		if ok && as.Load() == 0 {
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
		at.overflowCounter.Dec()
		return
	}
	panic(fmt.Errorf("decrementing non-existent active series: labels=%v, cost attribution keys: %v, the current observation map length: %d, the current cost attribution key: %s", lbls, at.labels, len(at.observed), buf.String()))
}

func (at *ActiveSeriesTracker) Collect(out chan<- prometheus.Metric) {
	at.observedMtx.RLock()
	if !at.overflowSince.IsZero() {
		var activeSeries int64
		for _, as := range at.observed {
			activeSeries += as.Load()
		}
		at.observedMtx.RUnlock()
		out <- prometheus.MustNewConstMetric(at.activeSeriesPerUserAttribution, prometheus.GaugeValue, float64(activeSeries+at.overflowCounter.Load()), at.overflowLabels[:len(at.overflowLabels)-1]...)
		return
	}
	// We don't know the performance of out receiver, so we don't want to hold the lock for too long
	var prometheusMetrics []prometheus.Metric
	for key, as := range at.observed {
		keys := strings.Split(key, string(sep))
		keys = append(keys, at.userID)
		prometheusMetrics = append(prometheusMetrics, prometheus.MustNewConstMetric(at.activeSeriesPerUserAttribution, prometheus.GaugeValue, float64(as.Load()), keys...))
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
