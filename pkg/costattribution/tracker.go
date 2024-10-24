// SPDX-License-Identifier: AGPL-3.0-only

package costattribution

import (
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"go.uber.org/atomic"
)

type observation struct {
	lvalues    []string
	lastUpdate *atomic.Int64
}

func (t *Tracker) cleanupTrackerAttribution(vals []string) {
	t.activeSeriesPerUserAttribution.DeleteLabelValues(vals...)
	t.receivedSamplesAttribution.DeleteLabelValues(vals...)
	t.discardedSampleAttribution.DeleteLabelValues(vals...)
}

func (t *Tracker) cleanupTracker(userID string) {
	filter := prometheus.Labels{"user": userID}
	t.activeSeriesPerUserAttribution.DeletePartialMatch(filter)
	t.receivedSamplesAttribution.DeletePartialMatch(filter)
	t.discardedSampleAttribution.DeletePartialMatch(filter)
}

type Tracker struct {
	userID                         string
	trackedLabels                  []string
	maxCardinality                 int
	activeSeriesPerUserAttribution *prometheus.GaugeVec
	receivedSamplesAttribution     *prometheus.CounterVec
	discardedSampleAttribution     *prometheus.CounterVec

	// oLock protects the observed map
	oLock    sync.RWMutex
	observed map[uint64]*observation

	hashBuffer []byte
}

func (t *Tracker) IncrementActiveSeries(lbs labels.Labels, now time.Time) {
	vals := t.getKeyValues(lbs, now.Unix())
	t.activeSeriesPerUserAttribution.WithLabelValues(vals...).Inc()
}

func (t *Tracker) IncrementDiscardedSamples(lbs labels.Labels, value float64, reason string, now time.Time) {
	vals := t.getKeyValues(lbs, now.Unix())
	t.discardedSampleAttribution.WithLabelValues(vals...).Add(value)
}

func (t *Tracker) IncrementReceivedSamples(lbs labels.Labels, value float64, now time.Time) {
	vals := t.getKeyValues(lbs, now.Unix())
	t.receivedSamplesAttribution.WithLabelValues(vals...).Add(value)
}

func (t *Tracker) getKeyValues(lbls labels.Labels, ts int64) []string {
	values := make([]string, len(t.trackedLabels)+1)
	for i, l := range t.trackedLabels {
		values[i] = lbls.Get(l)
		if values[i] == "" {
			values[i] = missingValue
		}
	}
	values[len(values)-1] = t.userID

	var stream uint64
	stream, t.hashBuffer = lbls.HashForLabels(t.hashBuffer, t.trackedLabels...)
	if t.overflow(stream, values, ts) {
		// Omit last label.
		for i := range values[:len(values)-1] {
			values[i] = overflowValue
		}
	}

	return values
}

func (t *Tracker) overflow(stream uint64, values []string, ts int64) bool {
	// If the maximum cardinality is hit all streams become `__overflow__`.
	if len(t.observed) > t.maxCardinality {
		return true
	}

	if o, known := t.observed[stream]; known && o.lastUpdate != nil && o.lastUpdate.Load() < ts {
		o.lastUpdate.Store(ts)
	} else {
		t.observed[stream] = &observation{
			lvalues:    values,
			lastUpdate: atomic.NewInt64(ts),
		}
	}

	return false
}

// we need the time stamp, since active series could have entered active stripe long time ago, and already evicted
// from the observed map but still in the active Stripe
func (t *Tracker) DecrementActiveSeries(lbs labels.Labels, value int64, ts time.Time) {
	vals := t.getKeyValues(lbs, ts.Unix())
	t.activeSeriesPerUserAttribution.WithLabelValues(vals...).Dec()
}

func newTracker(trackedLabels []string, limit int) (*Tracker, error) {
	// keep tracked labels sorted for consistent metric labels
	sort.Strings(trackedLabels)
	m := &Tracker{
		trackedLabels:  trackedLabels,
		maxCardinality: limit,
		oLock:          sync.RWMutex{},
		observed:       map[uint64]*observation{},
		//nolint:faillint // the metrics are registered in the mimir package
		discardedSampleAttribution: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_discarded_samples_attribution_total",
			Help: "The total number of samples that were discarded per attribution.",
		}, append(trackedLabels, "user")),
		//nolint:faillint
		receivedSamplesAttribution: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_received_samples_attribution_total",
			Help: "The total number of samples that were received per attribution.",
		}, append(trackedLabels, "user")),
		//nolint:faillint
		activeSeriesPerUserAttribution: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_ingester_active_series_attribution",
			Help: "The total number of active series per user and attribution.",
		}, append(trackedLabels, "user")),
	}
	return m, nil
}

func (t *Tracker) Collect(out chan<- prometheus.Metric) {
	t.activeSeriesPerUserAttribution.Collect(out)
	t.receivedSamplesAttribution.Collect(out)
	t.discardedSampleAttribution.Collect(out)
}

// Describe implements prometheus.Collector.
func (t *Tracker) Describe(chan<- *prometheus.Desc) {
	// this is an unchecked collector
}

func (t *Tracker) PurgeInactiveObservations(deadline int64) []*observation {
	obs := t.observed
	if obs == nil {
		return nil
	}

	var invalidKeys []uint64
	for labHash, ob := range obs {
		if ob != nil && ob.lastUpdate != nil && ob.lastUpdate.Load() <= deadline {
			invalidKeys = append(invalidKeys, labHash)
		}
	}

	if len(invalidKeys) == 0 {
		return nil
	}

	t.oLock.Lock()
	defer t.oLock.Unlock()

	// Cleanup inactive observations and return all invalid observations to clean up metrics for them
	res := make([]*observation, len(invalidKeys))
	for i := 0; i < len(invalidKeys); {
		inactiveLab := invalidKeys[i]
		ob := t.observed[inactiveLab]
		if ob != nil && ob.lastUpdate != nil && ob.lastUpdate.Load() <= deadline {
			delete(t.observed, inactiveLab)
			res[i] = ob
			i++
		} else {
			invalidKeys[i] = invalidKeys[len(invalidKeys)-1]
			invalidKeys = invalidKeys[:len(invalidKeys)-1]
		}
	}

	return res[:len(invalidKeys)]
}

func (t *Tracker) updateMaxCardinality(limit int) {
	// if we are reducing limit, we can just set it
	if t.maxCardinality >= limit {
		t.maxCardinality = limit
		return
	}
	// if we are increasing limit, we need to check if we are already in overflow,
	// if yes, reset the counter, otherwise the counters won't be correct
	t.oLock.Lock()
	defer t.oLock.Unlock()
	if len(t.observed) > t.maxCardinality {
		t.observed = map[uint64]*observation{}
	}
	t.maxCardinality = limit
}
