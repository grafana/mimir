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

type TrackerState int

const (
	Normal TrackerState = iota
	Overflow
)

const sep = rune(0x80)

type observation struct {
	lastUpdate         *atomic.Int64
	activeSerie        atomic.Float64
	receivedSample     atomic.Float64
	discardedSampleMtx sync.Mutex
	discardedSample    map[string]atomic.Float64
	totalDiscarded     atomic.Float64
}

type Tracker struct {
	userID                         string
	labels                         []string
	maxCardinality                 int
	activeSeriesPerUserAttribution *prometheus.Desc
	receivedSamplesAttribution     *prometheus.Desc
	discardedSampleAttribution     *prometheus.Desc
	failedActiveSeriesDecrement    *prometheus.Desc
	overflowLabels                 []string
	observed                       map[string]*observation
	observedMtx                    sync.RWMutex
	cooldownUntil                  int64
	hashBuffer                     []byte
	state                          TrackerState
	overflowCounter                *observation
	totalFailedActiveSeries        *atomic.Float64
	cooldownDuration               int64
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
		hashBuffer:              make([]byte, 0, 1024),
		cooldownDuration:        int64(cooldown.Seconds()),
		logger:                  logger,
		overflowLabels:          overflowLabels,
		totalFailedActiveSeries: atomic.NewFloat64(0),
		cooldownUntil:           0,
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
	tracker.failedActiveSeriesDecrement = prometheus.NewDesc("cortex_ingester_attributed_active_series_failure",
		"The total number of failed active series decrement per user and tracker.", []string{tenantLabel},
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
	t.updateCounters(lbs, now.Unix(), 1, 0, 0, nil, true)
}

func (t *Tracker) DecrementActiveSeries(lbs labels.Labels) {
	if t == nil {
		return
	}
	t.updateCounters(lbs, -1, -1, 0, 0, nil, false)
}

func (t *Tracker) Collect(out chan<- prometheus.Metric) {
	switch t.state {
	case Overflow:
		out <- prometheus.MustNewConstMetric(t.activeSeriesPerUserAttribution, prometheus.GaugeValue, t.overflowCounter.activeSerie.Load(), t.overflowLabels[:len(t.overflowLabels)-1]...)
		out <- prometheus.MustNewConstMetric(t.receivedSamplesAttribution, prometheus.CounterValue, t.overflowCounter.receivedSample.Load(), t.overflowLabels[:len(t.overflowLabels)-1]...)
		out <- prometheus.MustNewConstMetric(t.discardedSampleAttribution, prometheus.CounterValue, t.overflowCounter.totalDiscarded.Load(), t.overflowLabels...)
	case Normal:
		// Collect metrics for all observed keys
		t.observedMtx.RLock()
		defer t.observedMtx.RUnlock()
		for key, o := range t.observed {
			if key == "" {
				continue
			}
			keys := strings.Split(key, string(sep))

			keys = append(keys, t.userID)
			if o.activeSerie.Load() > 0 {
				out <- prometheus.MustNewConstMetric(t.activeSeriesPerUserAttribution, prometheus.GaugeValue, o.activeSerie.Load(), keys...)
			}
			if o.receivedSample.Load() > 0 {
				out <- prometheus.MustNewConstMetric(t.receivedSamplesAttribution, prometheus.CounterValue, o.receivedSample.Load(), keys...)
			}
			o.discardedSampleMtx.Lock()
			for reason, discarded := range o.discardedSample {
				out <- prometheus.MustNewConstMetric(t.discardedSampleAttribution, prometheus.CounterValue, discarded.Load(), append(keys, reason)...)
			}
			o.discardedSampleMtx.Unlock()
		}
	}
	if t.totalFailedActiveSeries.Load() > 0 {
		out <- prometheus.MustNewConstMetric(t.failedActiveSeriesDecrement, prometheus.CounterValue, t.totalFailedActiveSeries.Load(), t.userID)
	}
}

func (t *Tracker) IncrementDiscardedSamples(lbs []mimirpb.LabelAdapter, value float64, reason string, now time.Time) {
	if t == nil {
		return
	}
	t.updateCountersWithLabelAdapter(lbs, now.Unix(), 0, 0, value, &reason, true)
}

func (t *Tracker) IncrementReceivedSamples(lbs []mimirpb.LabelAdapter, value float64, now time.Time) {
	if t == nil {
		return
	}
	t.updateCountersWithLabelAdapter(lbs, now.Unix(), 0, value, 0, nil, true)
}

func (t *Tracker) IncrementActiveSeriesFailure() {
	if t == nil {
		return
	}
	t.totalFailedActiveSeries.Add(1)
}

func (t *Tracker) updateCountersWithLabelAdapter(lbls []mimirpb.LabelAdapter, ts int64, activeSeriesIncrement, receivedSampleIncrement, discardedSampleIncrement float64, reason *string, createIfDoesNotExist bool) {
	extractValues := func() []string {
		labelValues := make([]string, len(t.labels))
		for idx, cal := range t.labels {
			for _, l := range lbls {
				if l.Name == cal {
					labelValues[idx] = l.Value
					break
				}
			}
			if labelValues[idx] == "" {
				labelValues[idx] = missingValue
			}
		}
		return labelValues
	}
	t.updateCountersCommon(extractValues, ts, activeSeriesIncrement, receivedSampleIncrement, discardedSampleIncrement, reason, createIfDoesNotExist)
}

func (t *Tracker) updateCounters(lbls labels.Labels, ts int64, activeSeriesIncrement, receivedSampleIncrement, discardedSampleIncrement float64, reason *string, createIfDoesNotExist bool) {
	extractValues := func() []string {
		labelValues := make([]string, len(t.labels))
		for idx, cal := range t.labels {
			labelValues[idx] = lbls.Get(cal)
			if labelValues[idx] == "" {
				labelValues[idx] = missingValue
			}
		}
		return labelValues
	}
	t.updateCountersCommon(extractValues, ts, activeSeriesIncrement, receivedSampleIncrement, discardedSampleIncrement, reason, createIfDoesNotExist)
}

func (t *Tracker) updateCountersCommon(
	extractValues func() []string,
	ts int64,
	activeSeriesIncrement, receivedSampleIncrement, discardedSampleIncrement float64,
	reason *string,
	createIfDoesNotExist bool,
) {
	// Extract label values
	labelValues := extractValues()

	// Reuse buffer from pool for building the observation key
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	// Construct the observation key by joining label values
	for i, value := range labelValues {
		if i > 0 {
			buf.WriteRune(sep)
		}
		buf.WriteString(value)
	}

	// Lock access to the observation map
	t.observedMtx.Lock()
	defer t.observedMtx.Unlock()

	// Update observations and state
	t.updateObservations(buf.Bytes(), ts, activeSeriesIncrement, receivedSampleIncrement, discardedSampleIncrement, reason, createIfDoesNotExist)
	t.updateState(ts, activeSeriesIncrement, receivedSampleIncrement, discardedSampleIncrement)
}

// updateObservations updates or creates a new observation in the 'observed' map.
func (t *Tracker) updateObservations(key []byte, ts int64, activeSeriesIncrement, receivedSampleIncrement, discardedSampleIncrement float64, reason *string, createIfDoesNotExist bool) {
	if o, known := t.observed[string(key)]; known && o.lastUpdate != nil {
		o.lastUpdate.Store(ts)
		if activeSeriesIncrement != 0 {
			o.activeSerie.Add(activeSeriesIncrement)
		}
		if receivedSampleIncrement > 0 {
			o.receivedSample.Add(receivedSampleIncrement)
		}
		if discardedSampleIncrement > 0 && reason != nil {
			o.discardedSampleMtx.Lock()
			if v, ok := o.discardedSample[*reason]; ok {
				v.Add(discardedSampleIncrement)
				o.discardedSample[*reason] = v
			} else {
				o.discardedSample[*reason] = *atomic.NewFloat64(discardedSampleIncrement)
			}
			o.discardedSampleMtx.Unlock()
		}
	} else if len(t.observed) < t.maxCardinality*2 && createIfDoesNotExist {
		// When createIfDoesNotExist is false, it means that the method is called from DecrementActiveSeries, when key doesn't exist we should ignore the call
		// Otherwise create a new observation for the key
		t.createNewObservation(key, ts, activeSeriesIncrement, receivedSampleIncrement, discardedSampleIncrement, reason)
	}
}

// updateState checks if the tracker has exceeded its max cardinality and updates overflow state if necessary.
func (t *Tracker) updateState(ts int64, activeSeriesIncrement, receivedSampleIncrement, discardedSampleIncrement float64) {
	// Transition to overflow mode if maximum cardinality is exceeded.
	previousState := t.state
	if t.state == Normal && len(t.observed) > t.maxCardinality {
		t.state = Overflow
		// Initialize the overflow counter.
		t.overflowCounter = &observation{
			lastUpdate:     atomic.NewInt64(ts),
			activeSerie:    *atomic.NewFloat64(0),
			receivedSample: *atomic.NewFloat64(0),
			totalDiscarded: *atomic.NewFloat64(0),
		}

		// Aggregate active series from all keys into the overflow counter.
		for _, o := range t.observed {
			if o != nil {
				t.overflowCounter.activeSerie.Add(o.activeSerie.Load())
			}
		}
		t.cooldownUntil = ts + t.cooldownDuration
	}

	if t.state == Overflow {
		// if already in overflow mode, update the overflow counter. If it was normal mode, the active series are already applied.
		if previousState == Overflow && activeSeriesIncrement != 0 {
			t.overflowCounter.activeSerie.Add(activeSeriesIncrement)
		}
		if receivedSampleIncrement > 0 {
			t.overflowCounter.receivedSample.Add(receivedSampleIncrement)
		}
		if discardedSampleIncrement > 0 {
			t.overflowCounter.totalDiscarded.Add(discardedSampleIncrement)
		}
	}
}

// createNewObservation creates a new observation in the 'observed' map.
func (t *Tracker) createNewObservation(key []byte, ts int64, activeSeriesIncrement, receivedSampleIncrement, discardedSampleIncrement float64, reason *string) {
	t.observed[string(key)] = &observation{
		lastUpdate:         atomic.NewInt64(ts),
		activeSerie:        *atomic.NewFloat64(activeSeriesIncrement),
		receivedSample:     *atomic.NewFloat64(receivedSampleIncrement),
		discardedSample:    map[string]atomic.Float64{},
		discardedSampleMtx: sync.Mutex{},
	}
	if discardedSampleIncrement > 0 && reason != nil {
		t.observed[string(key)].discardedSampleMtx.Lock()
		t.observed[string(key)].discardedSample[*reason] = *atomic.NewFloat64(discardedSampleIncrement)
		t.observed[string(key)].discardedSampleMtx.Unlock()
	}
}

func (t *Tracker) recoveredFromOverflow(deadline int64) bool {
	t.observedMtx.RLock()
	if t.cooldownUntil > 0 && t.cooldownUntil < deadline {
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
		t.cooldownUntil = deadline + t.cooldownDuration
		t.observedMtx.Unlock()
	} else {
		t.observedMtx.RUnlock()
	}
	return false
}

func (t *Tracker) inactiveObservations(deadline int64) []string {
	// otherwise, we need to check all observations and clean up the ones that are inactive
	var invalidKeys []string
	t.observedMtx.RLock()
	defer t.observedMtx.RUnlock()
	for labkey, ob := range t.observed {
		if ob != nil && ob.lastUpdate != nil && ob.lastUpdate.Load() <= deadline {
			invalidKeys = append(invalidKeys, labkey)
		}
	}

	return invalidKeys
}