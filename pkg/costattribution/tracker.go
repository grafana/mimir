// SPDX-License-Identifier: AGPL-3.0-only

package costattribution

import (
	"bytes"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"go.uber.org/atomic"
)

type TrackerState int

const (
	Normal TrackerState = iota
	Overflow
	OverflowComplete
)

const sep = rune(0x80)

type Observation struct {
	lastUpdate       *atomic.Int64
	activeSerie      *atomic.Float64
	receivedSample   *atomic.Float64
	discardSamplemtx sync.Mutex
	discardedSample  map[string]*atomic.Float64
	totalDiscarded   *atomic.Float64
}

type Tracker struct {
	userID                         string
	caLabels                       []string
	caLabelMap                     map[string]int
	maxCardinality                 int
	activeSeriesPerUserAttribution *prometheus.Desc
	receivedSamplesAttribution     *prometheus.Desc
	discardedSampleAttribution     *prometheus.Desc
	failedActiveSeriesDecrement    *prometheus.Desc
	overflowLabels                 []string
	obseveredMtx                   sync.RWMutex
	observed                       map[string]*Observation
	hashBuffer                     []byte
	state                          TrackerState
	overflowCounter                *Observation
	cooldownUntil                  *atomic.Int64
	totalFailedActiveSeries        *atomic.Float64
	cooldownDuration               int64
	logger                         log.Logger
}

func newTracker(userID string, trackedLabels []string, limit int, cooldown time.Duration, logger log.Logger) *Tracker {
	sort.Slice(trackedLabels, func(i, j int) bool {
		return trackedLabels[i] < trackedLabels[j]
	})

	// Create a map for fast lookup, and overflow labels to export when overflow happens
	caLabelMap := make(map[string]int, len(trackedLabels))
	overflowLabels := make([]string, len(trackedLabels)+2)
	for i, label := range trackedLabels {
		caLabelMap[label] = i
		overflowLabels[i] = overflowValue
	}

	overflowLabels[len(trackedLabels)] = userID
	overflowLabels[len(trackedLabels)+1] = overflowValue

	tracker := &Tracker{
		userID:                  userID,
		caLabels:                trackedLabels,
		caLabelMap:              caLabelMap,
		maxCardinality:          limit,
		observed:                make(map[string]*Observation),
		hashBuffer:              make([]byte, 0, 1024),
		cooldownDuration:        int64(cooldown.Seconds()),
		logger:                  logger,
		overflowLabels:          overflowLabels,
		totalFailedActiveSeries: atomic.NewFloat64(0),
	}

	tracker.discardedSampleAttribution = prometheus.NewDesc("cortex_discarded_attributed_samples_total",
		"The total number of samples that were discarded per attribution.",
		append(trackedLabels, tenantLabel, "reason"),
		prometheus.Labels{trackerLabel: defaultTrackerName})

	tracker.receivedSamplesAttribution = prometheus.NewDesc("cortex_received_attributed_samples_total",
		"The total number of samples that were received per attribution.",
		append(trackedLabels, tenantLabel),
		prometheus.Labels{trackerLabel: defaultTrackerName})

	tracker.activeSeriesPerUserAttribution = prometheus.NewDesc("cortex_ingester_attributed_active_series",
		"The total number of active series per user and attribution.", append(trackedLabels, tenantLabel),
		prometheus.Labels{trackerLabel: defaultTrackerName})
	tracker.failedActiveSeriesDecrement = prometheus.NewDesc("cortex_ingester_attributed_active_series_failure",
		"The total number of failed active series decrement per user and tracker.", []string{tenantLabel},
		prometheus.Labels{trackerLabel: defaultTrackerName})
	return tracker
}

func (t *Tracker) CompareCALabels(currentLabels []string) bool {
	if t == nil {
		return len(currentLabels) == 0
	}
	if len(t.caLabels) != len(currentLabels) {
		return false
	}
	for _, v := range currentLabels {
		if _, exists := t.caLabelMap[v]; !exists {
			return false
		}
	}
	return true
}

func (t *Tracker) MaxCardinality() int {
	if t == nil {
		return 0
	}
	return t.maxCardinality
}

func (t *Tracker) CooldownDuration() int64 {
	if t == nil {
		return 0
	}
	return t.cooldownDuration
}

var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func (t *Tracker) cleanupTrackerAttribution(key string) {
	if t == nil {
		return
	}
	t.obseveredMtx.Lock()
	defer t.obseveredMtx.Unlock()
	delete(t.observed, key)
}

func (t *Tracker) IncrementActiveSeries(lbs labels.Labels, now time.Time) {
	if t == nil {
		return
	}
	t.updateCounters(lbs, now.Unix(), 1, 0, 0, nil)
}

func (t *Tracker) DecrementActiveSeries(lbs labels.Labels) {
	if t == nil {
		return
	}
	t.updateCounters(lbs, -1, -1, 0, 0, nil)
}

func (t *Tracker) Collect(out chan<- prometheus.Metric) {
	switch t.state {
	case Overflow:
		out <- prometheus.MustNewConstMetric(t.activeSeriesPerUserAttribution, prometheus.GaugeValue, t.overflowCounter.activeSerie.Load(), t.overflowLabels[:len(t.overflowLabels)-1]...)
		out <- prometheus.MustNewConstMetric(t.receivedSamplesAttribution, prometheus.CounterValue, t.overflowCounter.receivedSample.Load(), t.overflowLabels[:len(t.overflowLabels)-1]...)
		out <- prometheus.MustNewConstMetric(t.discardedSampleAttribution, prometheus.CounterValue, t.overflowCounter.totalDiscarded.Load(), t.overflowLabels...)
	case Normal:
		// Collect metrics for all observed streams
		t.obseveredMtx.RLock()
		defer t.obseveredMtx.RUnlock()
		for key, o := range t.observed {
			keys := strings.Split(key, string(sep))
			keys = append(keys, t.userID)
			if o.activeSerie.Load() > 0 {
				out <- prometheus.MustNewConstMetric(t.activeSeriesPerUserAttribution, prometheus.GaugeValue, o.activeSerie.Load(), keys...)
			}
			if o.receivedSample.Load() > 0 {
				out <- prometheus.MustNewConstMetric(t.receivedSamplesAttribution, prometheus.CounterValue, o.receivedSample.Load(), keys...)
			}
			o.discardSamplemtx.Lock()
			for reason, discarded := range o.discardedSample {
				out <- prometheus.MustNewConstMetric(t.discardedSampleAttribution, prometheus.CounterValue, discarded.Load(), append(keys, reason)...)
			}
			o.discardSamplemtx.Unlock()
		}
	}
	if t.totalFailedActiveSeries.Load() > 0 {
		out <- prometheus.MustNewConstMetric(t.failedActiveSeriesDecrement, prometheus.CounterValue, t.totalFailedActiveSeries.Load(), t.userID)
	}
}

func (t *Tracker) IncrementDiscardedSamples(lbs labels.Labels, value float64, reason string, now time.Time) {
	if t == nil {
		return
	}
	t.updateCounters(lbs, now.Unix(), 0, 0, value, &reason)
}

func (t *Tracker) IncrementReceivedSamples(lbs labels.Labels, value float64, now time.Time) {
	if t == nil {
		return
	}
	t.updateCounters(lbs, now.Unix(), 0, value, 0, nil)
}

func (t *Tracker) IncrementActiveSeriesFailure(value float64) {
	if t == nil {
		return
	}
	t.totalFailedActiveSeries.Add(value)
}

func (t *Tracker) updateCounters(lbls labels.Labels, ts int64, activeSeriesIncrement, receivedSampleIncrement, discardedSampleIncrement float64, reason *string) {
	labelValues := make([]string, len(t.caLabels))
	lbls.Range(func(l labels.Label) {
		if idx, ok := t.caLabelMap[l.Name]; ok {
			labelValues[idx] = l.Value
		}
	})
	for i := 0; i < len(labelValues); i++ {
		if labelValues[i] == "" {
			labelValues[i] = missingValue
		}
	}

	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	// Build the stream key
	for i, value := range labelValues {
		if i > 0 {
			buf.WriteRune(sep)
		}
		buf.WriteString(value)
	}

	t.obseveredMtx.Lock()
	defer t.obseveredMtx.Unlock()

	t.updateOverflow(buf.String(), ts, activeSeriesIncrement, receivedSampleIncrement, discardedSampleIncrement, reason)
}

// handleObservation updates or creates a new stream observation in the 'observed' map.
func (t *Tracker) handleObservation(stream string, ts int64, activeSeriesIncrement, receivedSampleIncrement, discardedSampleIncrement float64, reason *string) {
	if o, known := t.observed[stream]; known && o.lastUpdate != nil {
		// Update the timestamp if needed
		if o.lastUpdate.Load() < ts {
			o.lastUpdate.Store(ts)
		}
		if activeSeriesIncrement != 0 {
			o.activeSerie.Add(activeSeriesIncrement)
		}
		if receivedSampleIncrement > 0 {
			o.receivedSample.Add(receivedSampleIncrement)
		}
		if discardedSampleIncrement > 0 && reason != nil {
			o.discardSamplemtx.Lock()
			o.discardedSample[*reason] = atomic.NewFloat64(discardedSampleIncrement)
			o.discardSamplemtx.Unlock()
		}
	} else if len(t.observed) < t.maxCardinality*2 {
		// If the ts is negative, it means that the method is called from DecrementActiveSeries, when key doesn't exist we should ignore the call
		// Otherwise create a new observation for the stream
		if ts >= 0 {
			t.createNewObservation(stream, ts, activeSeriesIncrement, receivedSampleIncrement, discardedSampleIncrement, reason)
		}
	}
}

func (t *Tracker) updateOverflow(stream string, ts int64, activeSeriesIncrement, receivedSampleIncrement, discardedSampleIncrement float64, reason *string) {
	// Update the stream in the observed map
	t.handleObservation(stream, ts, activeSeriesIncrement, receivedSampleIncrement, discardedSampleIncrement, reason)
	t.handleOverflow(ts, activeSeriesIncrement, receivedSampleIncrement, discardedSampleIncrement)
}

// handleOverflow checks if the tracker has exceeded its max cardinality and updates overflow state if necessary.
func (t *Tracker) handleOverflow(ts int64, activeSeriesIncrement, receivedSampleIncrement, discardedSampleIncrement float64) {
	// Transition to overflow mode if maximum cardinality is exceeded.
	previousState := t.state
	if t.state == Normal && len(t.observed) > t.maxCardinality {
		t.state = Overflow
		// Initialize the overflow counter.
		t.overflowCounter = &Observation{
			lastUpdate:     atomic.NewInt64(ts),
			activeSerie:    atomic.NewFloat64(0),
			receivedSample: atomic.NewFloat64(0),
			totalDiscarded: atomic.NewFloat64(0),
		}

		// Aggregate active series from all streams into the overflow counter.
		for _, o := range t.observed {
			if o != nil {
				t.overflowCounter.activeSerie.Add(o.activeSerie.Load())
			}
		}
		t.cooldownUntil = atomic.NewInt64(ts + t.cooldownDuration)
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
func (t *Tracker) createNewObservation(stream string, ts int64, activeSeriesIncrement, receivedSampleIncrement, discardedSampleIncrement float64, reason *string) {
	t.observed[stream] = &Observation{
		lastUpdate:       atomic.NewInt64(ts),
		activeSerie:      atomic.NewFloat64(activeSeriesIncrement),
		receivedSample:   atomic.NewFloat64(receivedSampleIncrement),
		discardedSample:  map[string]*atomic.Float64{},
		discardSamplemtx: sync.Mutex{},
	}
	if discardedSampleIncrement > 0 && reason != nil {
		t.observed[stream].discardSamplemtx.Lock()
		t.observed[stream].discardedSample[*reason] = atomic.NewFloat64(discardedSampleIncrement)
		t.observed[stream].discardSamplemtx.Unlock()
	}
}

func (t *Tracker) inactiveObservations(deadline int64) []string {
	// otherwise, we need to check all observations and clean up the ones that are inactive
	var invalidKeys []string
	t.obseveredMtx.RLock()
	defer t.obseveredMtx.RUnlock()
	for labkey, ob := range t.observed {
		if ob != nil && ob.lastUpdate != nil && ob.lastUpdate.Load() <= deadline {
			invalidKeys = append(invalidKeys, labkey)
		}
	}

	return invalidKeys
}

func (t *Tracker) UpdateMaxCardinality(limit int) {
	if t == nil {
		return
	}
	t.maxCardinality = limit
}

func (t *Tracker) UpdateCooldownDuration(cooldownDuration int64) {
	if t == nil {
		return
	}
	t.cooldownDuration = cooldownDuration
}
