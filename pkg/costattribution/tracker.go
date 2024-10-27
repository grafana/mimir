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

	hashBuffer   []byte
	overflowHash uint64
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

// TODO: bug here, we can update values in the overflow, the reason is that when overflow, we need to change also the values for the overflow hash
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
	// If the maximum cardinality is hit all streams become `__overflow__`, the function would return true.
	// the origin labels ovserved time is not updated, but the overflow hash is updated.
	isOverflow := false
	if len(t.observed) > t.maxCardinality {
		isOverflow = true
		stream = t.overflowHash
	}

	if o, known := t.observed[stream]; known && o.lastUpdate != nil && o.lastUpdate.Load() < ts {
		o.lastUpdate.Store(ts)
	} else {
		t.observed[stream] = &Observation{
			lvalues:    values,
			lastUpdate: atomic.NewInt64(ts),
		}
	}

	return isOverflow
}

// we need the time stamp, since active series could have entered active stripe long time ago, and already evicted
// from the observed map but still in the active Stripe
func (t *TrackerImp) DecrementActiveSeries(lbs labels.Labels, ts time.Time) {
	vals := t.getKeyValues(lbs, ts.Unix(), nil)
	t.activeSeriesPerUserAttribution.WithLabelValues(vals...).Dec()
}

func newTracker(userID string, trackedLabels []string, limit int) (*TrackerImp, error) {
	// keep tracked labels sorted for consistent metric labels
	sort.Slice(trackedLabels, func(i, j int) bool {
		return trackedLabels[i] < trackedLabels[j]
	})
	m := &TrackerImp{
		userID:         userID,
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
		hashBuffer: make([]byte, 0, 1024),
	}
	m.updateOverFlowHash()
	return m, nil
}

func (t *TrackerImp) updateOverFlowHash() {
	b := labels.NewScratchBuilder(len(t.caLabels))
	for _, lb := range t.caLabels {
		b.Add(lb, overflowValue)
	}
	b.Sort()
	t.overflowHash = b.Labels().Hash()
}

func (t *TrackerImp) Collect(out chan<- prometheus.Metric) {
	t.activeSeriesPerUserAttribution.Collect(out)
	t.receivedSamplesAttribution.Collect(out)
	t.discardedSampleAttribution.Collect(out)
}

// Describe implements prometheus.Collector.
func (t *TrackerImp) Describe(chan<- *prometheus.Desc) {
}

// resetObservedIfNeeded checks if the overflow hash is in the observed map and if it is, when dealine is 0, means that
// we just need to clean up the observed map and metrics without checking the deadline.
// Otherwise, we need to check if the last update time of the overflow hash is less than or equal to the deadline.
// return true if the observed map is cleaned up, otherwise false.
func (t *TrackerImp) resetObservedIfNeeded(deadline int64) bool {
	t.obseveredMtx.Lock()
	defer t.obseveredMtx.Unlock()
	if ob, ok := t.observed[t.overflowHash]; ok {
		if deadline == 0 || (ob != nil && ob.lastUpdate != nil && ob.lastUpdate.Load() <= deadline) {
			t.observed = map[uint64]*Observation{}
			t.cleanupTracker(t.userID)
			return true
		}
	}
	return false
}

func (t *TrackerImp) PurgeInactiveObservations(deadline int64) []*Observation {
	// if overflow is in the observed map and it is reached dealine, we need to clean up the observed map and metrics
	isReset := t.resetObservedIfNeeded(deadline)
	if isReset {
		return []*Observation{}
	}

	// otherwise, we need to check all observations and clean up the ones that are inactive
	var invalidKeys []uint64
	for labHash, ob := range t.observed {
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
	// if we are reducing limit, we can just set it, if it hits the limit, we can't do much about it.
	if t.maxCardinality >= limit {
		t.maxCardinality = limit
		return
	}
	// if we have hit the limit, we need to clear the observed map. The way to tell that we have hit the limit is
	// by checking if the overflow hash is in the observed map. This is handled in the resetObservedIfNeeded function. 0 here means no deadline check is needed.
	t.resetObservedIfNeeded(0)
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
