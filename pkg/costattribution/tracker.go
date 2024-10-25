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

type Tracker interface {
	IncrementActiveSeries(labels.Labels, time.Time)
	IncrementDiscardedSamples(labels.Labels, float64, string, time.Time)
	IncrementReceivedSamples(labels.Labels, float64, time.Time)
	DecrementActiveSeries(labels.Labels, time.Time)
	PurgeInactiveObservations(int64) []*Observation
	UpdateMaxCardinality(int)
	GetMaxCardinality() int
	GetCALabels() []string
}

type Observation struct {
	lvalues    []string
	lastUpdate *atomic.Int64
}

func (t *TrackerImp) GetCALabels() []string {
	return t.caLabels
}

func (t *TrackerImp) GetMaxCardinality() int {
	return t.maxCardinality
}

func (t *TrackerImp) cleanupTrackerAttribution(vals []string) {
	t.activeSeriesPerUserAttribution.DeleteLabelValues(vals...)
	t.receivedSamplesAttribution.DeleteLabelValues(vals...)
	t.discardedSampleAttribution.DeleteLabelValues(vals...)
}

func (t *TrackerImp) cleanupTracker(userID string) {
	filter := prometheus.Labels{"user": userID}
	t.activeSeriesPerUserAttribution.DeletePartialMatch(filter)
	t.receivedSamplesAttribution.DeletePartialMatch(filter)
	t.discardedSampleAttribution.DeletePartialMatch(filter)
}

type TrackerImp struct {
	userID                         string
	caLabels                       []string
	maxCardinality                 int
	activeSeriesPerUserAttribution *prometheus.GaugeVec
	receivedSamplesAttribution     *prometheus.CounterVec
	discardedSampleAttribution     *prometheus.CounterVec

	// obseveredMtx protects the observed map
	obseveredMtx sync.RWMutex
	observed     map[uint64]*Observation

	hashBuffer []byte
}

func (t *TrackerImp) IncrementActiveSeries(lbs labels.Labels, now time.Time) {
	vals := t.getKeyValues(lbs, now.Unix(), nil)
	t.activeSeriesPerUserAttribution.WithLabelValues(vals...).Inc()
}

func (t *TrackerImp) IncrementDiscardedSamples(lbs labels.Labels, value float64, reason string, now time.Time) {
	vals := t.getKeyValues(lbs, now.Unix(), &reason)
	t.discardedSampleAttribution.WithLabelValues(vals...).Add(value)
}

func (t *TrackerImp) IncrementReceivedSamples(lbs labels.Labels, value float64, now time.Time) {
	vals := t.getKeyValues(lbs, now.Unix(), nil)
	t.receivedSamplesAttribution.WithLabelValues(vals...).Add(value)
}

func (t *TrackerImp) getKeyValues(lbls labels.Labels, ts int64, reason *string) []string {
	values := make([]string, len(t.caLabels)+2)
	for i, l := range t.caLabels {
		values[i] = lbls.Get(l)
		if values[i] == "" {
			values[i] = missingValue
		}
	}
	values[len(values)-2] = t.userID
	if reason != nil {
		values[len(values)-1] = *reason
	}
	var stream uint64
	stream, t.hashBuffer = lbls.HashForLabels(t.hashBuffer, t.caLabels...)
	if t.overflow(stream, values, ts) {
		// Omit last label.
		for i := range values[:len(values)-2] {
			values[i] = overflowValue
		}
	}
	if reason == nil {
		return values[:len(values)-1]
	}
	return values
}

func (t *TrackerImp) overflow(stream uint64, values []string, ts int64) bool {
	// If the maximum cardinality is hit all streams become `__overflow__`.
	if len(t.observed) > t.maxCardinality {
		return true
	}

	if o, known := t.observed[stream]; known && o.lastUpdate != nil && o.lastUpdate.Load() < ts {
		o.lastUpdate.Store(ts)
	} else {
		t.observed[stream] = &Observation{
			lvalues:    values,
			lastUpdate: atomic.NewInt64(ts),
		}
	}

	return false
}

// we need the time stamp, since active series could have entered active stripe long time ago, and already evicted
// from the observed map but still in the active Stripe
func (t *TrackerImp) DecrementActiveSeries(lbs labels.Labels, ts time.Time) {
	vals := t.getKeyValues(lbs, ts.Unix(), nil)
	t.activeSeriesPerUserAttribution.WithLabelValues(vals...).Dec()
}

func newTracker(trackedLabels []string, limit int) (*TrackerImp, error) {
	// keep tracked labels sorted for consistent metric labels
	sort.Slice(trackedLabels, func(i, j int) bool {
		return trackedLabels[i] < trackedLabels[j]
	})
	m := &TrackerImp{
		caLabels:       trackedLabels,
		maxCardinality: limit,
		obseveredMtx:   sync.RWMutex{},
		observed:       map[uint64]*Observation{},
		//lint:ignore faillint the metrics are registered in the mimir package
		discardedSampleAttribution: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_discarded_attributed_samples_total",
			Help: "The total number of samples that were discarded per attribution.",
		}, append(trackedLabels, "user", "reason")),
		//lint:ignore faillint the metrics are registered in the mimir package
		receivedSamplesAttribution: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_received_attributed_samples_total",
			Help: "The total number of samples that were received per attribution.",
		}, append(trackedLabels, "user")),
		//lint:ignore faillint the metrics are registered in the mimir package
		activeSeriesPerUserAttribution: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_ingester_attributed_active_series",
			Help: "The total number of active series per user and attribution.",
		}, append(trackedLabels, "user")),
	}
	return m, nil
}

func (t *TrackerImp) Collect(out chan<- prometheus.Metric) {
	t.activeSeriesPerUserAttribution.Collect(out)
	t.receivedSamplesAttribution.Collect(out)
	t.discardedSampleAttribution.Collect(out)
}

// Describe implements prometheus.Collector.
func (t *TrackerImp) Describe(chan<- *prometheus.Desc) {
}

func (t *TrackerImp) PurgeInactiveObservations(deadline int64) []*Observation {
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

	t.obseveredMtx.Lock()
	defer t.obseveredMtx.Unlock()

	// Cleanup inactive observations and return all invalid observations to clean up metrics for them
	res := make([]*Observation, len(invalidKeys))
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

func (t *TrackerImp) UpdateMaxCardinality(limit int) {
	// if we are reducing limit, we can just set it
	if t.maxCardinality >= limit {
		t.maxCardinality = limit
		return
	}
	// if we are increasing limit, we need to check if we are already in overflow,
	// if yes, reset the counter, otherwise the counters won't be correct
	t.obseveredMtx.Lock()
	defer t.obseveredMtx.Unlock()
	if len(t.observed) > t.maxCardinality {
		t.observed = map[uint64]*Observation{}
	}
	t.maxCardinality = limit
}

type NoopTracker struct{}

func NewNoopTracker() *NoopTracker {
	return &NoopTracker{}
}
func (*NoopTracker) IncrementActiveSeries(labels.Labels, time.Time)                      {}
func (*NoopTracker) IncrementDiscardedSamples(labels.Labels, float64, string, time.Time) {}
func (*NoopTracker) IncrementReceivedSamples(labels.Labels, float64, time.Time)          {}
func (*NoopTracker) DecrementActiveSeries(labels.Labels, time.Time)                      {}
func (*NoopTracker) PurgeInactiveObservations(int64) []*Observation                      { return nil }
func (*NoopTracker) UpdateMaxCardinality(int)                                            {}
func (*NoopTracker) GetMaxCardinality() int                                              { return 0 }
func (*NoopTracker) GetCALabels() []string                                               { return nil }
