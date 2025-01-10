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
	labels                         []string
	maxCardinality                 int
	activeSeriesPerUserAttribution *prometheus.Desc
	overflowLabels                 []string
	observed                       map[string]*atomic.Int64
	observedMtx                    sync.RWMutex
	overflowSince                  atomic.Int64
	overflowCounter                atomic.Int64
	cooldownDuration               time.Duration
	logger                         log.Logger
}

func newActiveSeriesTracker(userID string, trackedLabels []string, limit int, cooldown time.Duration, logger log.Logger) *ActiveSeriesTracker {
	orderedLables := slices.Clone(trackedLabels)
	slices.Sort(orderedLables)

	// Create a map for overflow labels to export when overflow happens
	overflowLabels := make([]string, len(orderedLables)+2)
	for i := range orderedLables {
		overflowLabels[i] = overflowValue
	}

	overflowLabels[len(orderedLables)] = userID
	overflowLabels[len(orderedLables)+1] = overflowValue

	tracker := &ActiveSeriesTracker{
		userID:         userID,
		labels:         orderedLables,
		maxCardinality: limit,
		observed:       make(map[string]*atomic.Int64),
		logger:         logger,
		overflowLabels: overflowLabels,
	}

	variableLabels := slices.Clone(orderedLables)
	variableLabels = append(variableLabels, tenantLabel, "reason")

	tracker.activeSeriesPerUserAttribution = prometheus.NewDesc("cortex_ingester_attributed_active_series",
		"The total number of active series per user and attribution.", variableLabels[:len(variableLabels)-1],
		prometheus.Labels{trackerLabel: defaultTrackerName})

	return tracker
}

func (t *ActiveSeriesTracker) hasSameLabels(labels []string) bool {
	return slices.Equal(t.labels, labels)
}

func (t *ActiveSeriesTracker) Increment(lbls labels.Labels, now time.Time) {
	if t == nil {
		return
	}
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)
	t.fillKeyFromLabels(lbls, buf)
	t.observedMtx.RLock()
	as, ok := t.observed[string(buf.Bytes())]
	if ok {
		as.Inc()
		t.observedMtx.RUnlock()
		return
	}
	t.observedMtx.RUnlock()

	if t.overflowSince.Load() > 0 {
		t.overflowCounter.Inc()
		return
	}

	t.observedMtx.Lock()
	defer t.observedMtx.Unlock()
	as, ok = t.observed[string(buf.Bytes())]
	if ok {
		as.Inc()
		return
	}

	if t.overflowSince.Load() > 0 {
		t.overflowCounter.Inc()
		return
	}

	if len(t.observed) >= t.maxCardinality {
		t.overflowSince.Store(now.Unix())
		t.overflowCounter.Inc()
		return
	}

	t.observed[string(buf.Bytes())] = atomic.NewInt64(1)

}

func (t *ActiveSeriesTracker) Decrement(lbls labels.Labels) {
	if t == nil {
		return
	}
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)
	t.fillKeyFromLabels(lbls, buf)
	t.observedMtx.RLock()
	as, ok := t.observed[string(buf.Bytes())]
	if ok {
		nv := as.Dec()
		if nv > 0 {
			t.observedMtx.RUnlock()
			return
		}
		t.observedMtx.RUnlock()
		t.observedMtx.Lock()
		as, ok := t.observed[string(buf.Bytes())]
		if ok && as.Load() == 0 {
			// use buf.String() instead of string(buf.Bytes()) to fix the lint issue
			delete(t.observed, buf.String())
		}
		t.observedMtx.Unlock()
		return
	}
	t.observedMtx.RUnlock()

	if t.overflowSince.Load() > 0 {
		t.overflowCounter.Dec()
		return
	}

	t.observedMtx.RLock()
	defer t.observedMtx.RUnlock()
	panic(fmt.Errorf("decrementing non-existent active series: labels=%v, cost attribution keys: %v, the current observation map length: %d, the current cost attribution key: %s", lbls, t.labels, len(t.observed), buf.String()))
}

func (t *ActiveSeriesTracker) Collect(out chan<- prometheus.Metric) {
	if t.overflowSince.Load() > 0 {
		var activeSeries int64
		t.observedMtx.RLock()
		for _, as := range t.observed {
			activeSeries += as.Load()
		}
		t.observedMtx.RUnlock()
		out <- prometheus.MustNewConstMetric(t.activeSeriesPerUserAttribution, prometheus.GaugeValue, float64(activeSeries+t.overflowCounter.Load()), t.overflowLabels[:len(t.overflowLabels)-1]...)
		return
	}
	// We don't know the performance of out receiver, so we don't want to hold the lock for too long
	var prometheusMetrics []prometheus.Metric
	t.observedMtx.RLock()
	for key, as := range t.observed {
		keys := strings.Split(key, string(sep))
		keys = append(keys, t.userID)
		prometheusMetrics = append(prometheusMetrics, prometheus.MustNewConstMetric(t.activeSeriesPerUserAttribution, prometheus.GaugeValue, float64(as.Load()), keys...))
	}
	t.observedMtx.RUnlock()

	for _, m := range prometheusMetrics {
		out <- m
	}
}

func (t *ActiveSeriesTracker) fillKeyFromLabels(lbls labels.Labels, buf *bytes.Buffer) {
	buf.Reset()
	for idx, cal := range t.labels {
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
