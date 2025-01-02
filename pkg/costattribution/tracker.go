// SPDX-License-Identifier: AGPL-3.0-only

package costattribution

import (
	"bytes"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/mimirpb"
)

const sep = rune(0x80)

type observation struct {
	lastUpdate         atomic.Int64
	activeSerie        atomic.Float64
	receivedSample     atomic.Float64
	discardedSampleMtx sync.Mutex
	discardedSample    map[string]*atomic.Float64
	totalDiscarded     atomic.Float64
}

type Tracker struct {
	userID                         string
	labels                         []string
	maxCardinality                 int
	activeSeriesPerUserAttribution *prometheus.Desc
	receivedSamplesAttribution     *prometheus.Desc
	discardedSampleAttribution     *prometheus.Desc
	overflowLabels                 []string
	observed                       map[string]*observation
	observedMtx                    sync.RWMutex
	isOverflow                     atomic.Bool
	overflowCounter                *observation
	totalFailedActiveSeries        *atomic.Float64
	cooldownDuration               time.Duration
	cooldownUntil                  time.Time
	logger                         log.Logger
}

func newTracker(userID string, trackedLabels []string, limit int, cooldown time.Duration, logger log.Logger) *Tracker {
	orderedLables := slices.Clone(trackedLabels)
	slices.Sort(orderedLables)

	// Create a map for overflow labels to export when overflow happens
	overflowLabels := make([]string, len(orderedLables)+2)
	for i := range orderedLables {
		overflowLabels[i] = overflowValue
	}

	overflowLabels[len(orderedLables)] = userID
	overflowLabels[len(orderedLables)+1] = overflowValue

	tracker := &Tracker{
		userID:                  userID,
		labels:                  orderedLables,
		maxCardinality:          limit,
		observed:                make(map[string]*observation),
		cooldownDuration:        cooldown,
		logger:                  logger,
		overflowLabels:          overflowLabels,
		totalFailedActiveSeries: atomic.NewFloat64(0),
		overflowCounter:         &observation{},
	}

	variableLabels := slices.Clone(orderedLables)
	variableLabels = append(variableLabels, tenantLabel, "reason")
	tracker.discardedSampleAttribution = prometheus.NewDesc("cortex_discarded_attributed_samples_total",
		"The total number of samples that were discarded per attribution.",
		variableLabels,
		prometheus.Labels{trackerLabel: defaultTrackerName})

	tracker.receivedSamplesAttribution = prometheus.NewDesc("cortex_received_attributed_samples_total",
		"The total number of samples that were received per attribution.",
		variableLabels[:len(variableLabels)-1],
		prometheus.Labels{trackerLabel: defaultTrackerName})

	tracker.activeSeriesPerUserAttribution = prometheus.NewDesc("cortex_ingester_attributed_active_series",
		"The total number of active series per user and attribution.", variableLabels[:len(variableLabels)-1],
		prometheus.Labels{trackerLabel: defaultTrackerName})

	return tracker
}

func (t *Tracker) hasSameLabels(labels []string) bool {
	return slices.Equal(t.labels, labels)
}

var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func (t *Tracker) cleanupTrackerAttribution(key string) {
	t.observedMtx.Lock()
	defer t.observedMtx.Unlock()
	delete(t.observed, key)
}

func (t *Tracker) IncrementActiveSeries(lbs labels.Labels, now time.Time) {
	if t == nil {
		return
	}
	t.updateCounters(lbs, now, 1, 0, 0, nil, true)
}

func (t *Tracker) DecrementActiveSeries(lbs labels.Labels) {
	if t == nil {
		return
	}
	t.updateCounters(lbs, time.Time{}, -1, 0, 0, nil, false)
}

func (t *Tracker) Collect(out chan<- prometheus.Metric) {
	if t.isOverflow.Load() {
		var activeSeries float64
		t.observedMtx.RLock()
		for _, o := range t.observed {
			activeSeries += o.activeSerie.Load()
		}
		t.observedMtx.RUnlock()
		out <- prometheus.MustNewConstMetric(t.activeSeriesPerUserAttribution, prometheus.GaugeValue, activeSeries+t.overflowCounter.activeSerie.Load(), t.overflowLabels[:len(t.overflowLabels)-1]...)
		out <- prometheus.MustNewConstMetric(t.receivedSamplesAttribution, prometheus.CounterValue, t.overflowCounter.receivedSample.Load(), t.overflowLabels[:len(t.overflowLabels)-1]...)
		out <- prometheus.MustNewConstMetric(t.discardedSampleAttribution, prometheus.CounterValue, t.overflowCounter.totalDiscarded.Load(), t.overflowLabels...)
		return
	}
	// We don't know the performance of out receiver, so we don't want to hold the lock for too long
	var prometheusMetrics []prometheus.Metric
	t.observedMtx.RLock()
	for key, o := range t.observed {
		keys := strings.Split(key, string(sep))
		keys = append(keys, t.userID)
		if o.activeSerie.Load() > 0 {
			prometheusMetrics = append(prometheusMetrics, prometheus.MustNewConstMetric(t.activeSeriesPerUserAttribution, prometheus.GaugeValue, o.activeSerie.Load(), keys...))
		}
		if o.receivedSample.Load() > 0 {
			prometheusMetrics = append(prometheusMetrics, prometheus.MustNewConstMetric(t.receivedSamplesAttribution, prometheus.CounterValue, o.receivedSample.Load(), keys...))
		}
		o.discardedSampleMtx.Lock()
		for reason, discarded := range o.discardedSample {
			prometheusMetrics = append(prometheusMetrics, prometheus.MustNewConstMetric(t.discardedSampleAttribution, prometheus.CounterValue, discarded.Load(), append(keys, reason)...))
		}
		o.discardedSampleMtx.Unlock()
	}
	t.observedMtx.RUnlock()

	for _, m := range prometheusMetrics {
		out <- m
	}
}

func (t *Tracker) IncrementDiscardedSamples(lbs []mimirpb.LabelAdapter, value float64, reason string, now time.Time) {
	if t == nil {
		return
	}
	t.updateCountersWithLabelAdapter(lbs, now, 0, 0, value, &reason, true)
}

func (t *Tracker) IncrementReceivedSamples(req *mimirpb.WriteRequest, now time.Time) {
	if t == nil {
		return
	}

	// We precompute the cost attribution per request before update Observations and State to avoid frequently update the atomic counters
	dict := make(map[string]int)
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)
	for _, ts := range req.Timeseries {
		t.fillKeyFromLabelAdapters(ts.Labels, buf)
		dict[string(buf.Bytes())] += len(ts.TimeSeries.Samples) + len(ts.TimeSeries.Histograms)
	}

	// Update the observations for each label set and update the state per request,
	// this would be less precised than per sample but it's more efficient
	var total float64
	for k, v := range dict {
		count := float64(v)
		t.updateObservations(k, now, 0, count, 0, nil, true)
		total += count
	}
}

func (t *Tracker) updateCountersWithLabelAdapter(lbls []mimirpb.LabelAdapter, ts time.Time, activeSeriesIncrement, receivedSampleIncrement, discardedSampleIncrement float64, reason *string, createIfDoesNotExist bool) {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)
	t.fillKeyFromLabelAdapters(lbls, buf)
	t.updateObservations(buf.String(), ts, activeSeriesIncrement, receivedSampleIncrement, discardedSampleIncrement, reason, createIfDoesNotExist)
}

func (t *Tracker) fillKeyFromLabelAdapters(lbls []mimirpb.LabelAdapter, buf *bytes.Buffer) {
	buf.Reset()
	var exists bool
	for idx, cal := range t.labels {
		if idx > 0 {
			buf.WriteRune(sep)
		}
		exists = false
		for _, l := range lbls {
			if l.Name == cal {
				exists = true
				buf.WriteString(l.Value)
				break
			}
		}
		if !exists {
			buf.WriteString(missingValue)
		}
	}
}

func (t *Tracker) fillKeyFromLabels(lbls labels.Labels, buf *bytes.Buffer) {
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

func (t *Tracker) updateCounters(lbls labels.Labels, ts time.Time, activeSeriesIncrement, receivedSampleIncrement, discardedSampleIncrement float64, reason *string, createIfDoesNotExist bool) {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)
	t.fillKeyFromLabels(lbls, buf)
	t.updateObservations(buf.String(), ts, activeSeriesIncrement, receivedSampleIncrement, discardedSampleIncrement, reason, createIfDoesNotExist)
}

// updateObservations updates or creates a new observation in the 'observed' map.
func (t *Tracker) updateObservations(key string, ts time.Time, activeSeriesIncrement, receivedSampleIncrement, discardedSampleIncrement float64, reason *string, createIfDoesNotExist bool) {
	t.observedMtx.RLock()
	o, known := t.observed[key]
	t.observedMtx.RUnlock()

	if !known {
		if !createIfDoesNotExist {
			return
		}
		// We don't want to restart the tracker when we are not sure overflow is fixed, so we keep a observation with 2 times the max cardinality, as soon as
		// the tracker is still above the max cardinality, we will keep the overflow state.
		t.observedMtx.RLock()
		if len(t.observed) < t.maxCardinality*2 {
			t.observedMtx.RUnlock()

			// If adding the new observation would exceed the max cardinality, we need to update the state, it is fine only call it here
			// because we are sure that the new observation will be added to the map
			t.updateState(ts)

			// If we are not in overflow mode, we can create a new observation with input values, otherwise we create a new observation with 0 values
			if !t.isOverflow.Load() {
				t.createNewObservationAndUpdateState(key, ts, activeSeriesIncrement, receivedSampleIncrement, discardedSampleIncrement, reason)
				return
			}
			t.createNewObservationAndUpdateState(key, ts, 0, 0, 0, nil)
		} else {
			t.observedMtx.RUnlock()
		}
		// If we are in overflow mode (including the case that observed map size exceed 2 times max cardinality), we update the overflow counter
		o = t.overflowCounter
	}

	o.lastUpdate.Store(ts.Unix())
	if activeSeriesIncrement != 0 {
		o.activeSerie.Add(activeSeriesIncrement)
	}
	if receivedSampleIncrement > 0 {
		o.receivedSample.Add(receivedSampleIncrement)
	}
	if discardedSampleIncrement > 0 && reason != nil {
		o.discardedSampleMtx.Lock()
		if _, ok := o.discardedSample[*reason]; ok {
			o.discardedSample[*reason].Add(discardedSampleIncrement)
		} else {
			o.discardedSample[*reason] = atomic.NewFloat64(discardedSampleIncrement)
		}
		o.discardedSampleMtx.Unlock()
	}
}

// updateState checks if the tracker has exceeded its max cardinality and updates overflow state if necessary.
// This function is not thread-safe and should be called with the t.observedMtx read lock.
func (t *Tracker) updateState(ts time.Time) {
	if !t.isOverflow.Load() && len(t.observed) >= t.maxCardinality {
		// Update state to overflow and set cooldown time
		t.isOverflow.Store(true)
		if t.cooldownUntil.IsZero() {
			t.cooldownUntil = ts.Add(t.cooldownDuration)
		}
		t.logger.Log("msg", "tracker is in overflow mode", "userID", t.userID, "maxCardinality", t.maxCardinality)
	}
}

// createNewObservationAndUpdateState creates a new observation in the 'observed' map. Check if the tracker is in overflow mode and updates the state.
func (t *Tracker) createNewObservationAndUpdateState(key string, ts time.Time, activeSeriesIncrement, receivedSampleIncrement, discardedSampleIncrement float64, reason *string) {
	t.observedMtx.Lock()
	defer t.observedMtx.Unlock()
	if _, exists := t.observed[key]; exists {
		return
	}

	t.observed[key] = &observation{
		lastUpdate:         *atomic.NewInt64(ts.Unix()),
		activeSerie:        *atomic.NewFloat64(activeSeriesIncrement),
		receivedSample:     *atomic.NewFloat64(receivedSampleIncrement),
		discardedSample:    make(map[string]*atomic.Float64),
		discardedSampleMtx: sync.Mutex{},
	}
	if discardedSampleIncrement > 0 && reason != nil {
		t.observed[key].discardedSampleMtx.Lock()
		t.observed[key].discardedSample[*reason] = atomic.NewFloat64(discardedSampleIncrement)
		t.observed[key].discardedSampleMtx.Unlock()
	}
}

func (t *Tracker) recoveredFromOverflow(deadline time.Time) bool {
	t.observedMtx.RLock()
	if !t.cooldownUntil.IsZero() && t.cooldownUntil.Before(deadline) {
		if len(t.observed) <= t.maxCardinality {
			t.observedMtx.RUnlock()
			return true
		}
		t.observedMtx.RUnlock()

		// Increase the cooldown duration if the number of observations is still above the max cardinality
		t.observedMtx.Lock()
		if len(t.observed) <= t.maxCardinality {
			t.observedMtx.Unlock()
			return true
		}
		t.cooldownUntil = deadline.Add(t.cooldownDuration)
		t.observedMtx.Unlock()
	} else {
		t.observedMtx.RUnlock()
	}
	return false
}

func (t *Tracker) inactiveObservations(deadline time.Time) []string {
	// otherwise, we need to check all observations and clean up the ones that are inactive
	var invalidKeys []string
	t.observedMtx.RLock()
	defer t.observedMtx.RUnlock()
	for labkey, ob := range t.observed {
		if ob != nil && ob.lastUpdate.Load() <= deadline.Unix() {
			invalidKeys = append(invalidKeys, labkey)
		}
	}

	return invalidKeys
}
