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

type Observation struct {
	lvalues    []string
	lastUpdate *atomic.Int64
}

const (
	TrackerLabel = "tracker"
	TenantLabel  = "tenant"
)

type Tracker struct {
	userID                         string
	caLabels                       []string
	maxCardinality                 int
	activeSeriesPerUserAttribution *prometheus.GaugeVec
	receivedSamplesAttribution     *prometheus.CounterVec
	discardedSampleAttribution     *prometheus.CounterVec

	// obseveredMtx protects the observed map
	obseveredMtx sync.RWMutex
	observed     map[uint64]*Observation

	hashBuffer       []byte
	isOverflow       bool
	cooldownUntil    *atomic.Int64
	cooldownDuration int64
}

func newTracker(userID string, trackedLabels []string, limit int, cooldown time.Duration) (*Tracker, error) {
	// keep tracked labels sorted for consistent metric labels
	sort.Slice(trackedLabels, func(i, j int) bool {
		return trackedLabels[i] < trackedLabels[j]
	})
	m := &Tracker{
		userID:         userID,
		caLabels:       trackedLabels,
		maxCardinality: limit,
		obseveredMtx:   sync.RWMutex{},
		observed:       map[uint64]*Observation{},
		//lint:ignore faillint the metrics are registered in the mimir package
		discardedSampleAttribution: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "cortex_discarded_attributed_samples_total",
			Help:        "The total number of samples that were discarded per attribution.",
			ConstLabels: prometheus.Labels{TrackerLabel: "custom_attribution"},
		}, append(trackedLabels, TenantLabel, "reason")),
		//lint:ignore faillint the metrics are registered in the mimir package
		receivedSamplesAttribution: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "cortex_received_attributed_samples_total",
			Help:        "The total number of samples that were received per attribution.",
			ConstLabels: prometheus.Labels{TrackerLabel: "custom_attribution"},
		}, append(trackedLabels, TenantLabel)),
		//lint:ignore faillint the metrics are registered in the mimir package
		activeSeriesPerUserAttribution: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "cortex_ingester_attributed_active_series",
			Help:        "The total number of active series per user and attribution.",
			ConstLabels: prometheus.Labels{TrackerLabel: "custom_attribution"},
		}, append(trackedLabels, TenantLabel)),
		hashBuffer:       make([]byte, 0, 1024),
		cooldownDuration: int64(cooldown.Seconds()),
	}
	return m, nil
}

func (t *Tracker) CALabels() []string {
	if t == nil {
		return nil
	}
	return t.caLabels
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

func (t *Tracker) cleanupTrackerAttribution(vals []string) {
	if t == nil {
		return
	}
	t.activeSeriesPerUserAttribution.DeleteLabelValues(vals...)
	t.receivedSamplesAttribution.DeleteLabelValues(vals...)

	// except for discarded sample metrics, there is reason label that is not part of the key, we need to delete all partial matches
	filter := prometheus.Labels{}
	for i := 0; i < len(t.caLabels); i++ {
		filter[t.caLabels[i]] = vals[i]
	}
	filter[TenantLabel] = vals[len(vals)-1]
	t.discardedSampleAttribution.DeletePartialMatch(filter)
}

func (t *Tracker) cleanupTracker(userID string) {
	if t == nil {
		return
	}
	filter := prometheus.Labels{TenantLabel: userID}
	t.activeSeriesPerUserAttribution.DeletePartialMatch(filter)
	t.receivedSamplesAttribution.DeletePartialMatch(filter)
	t.discardedSampleAttribution.DeletePartialMatch(filter)
}

func (t *Tracker) IncrementActiveSeries(lbs labels.Labels, now time.Time) {
	if t == nil {
		return
	}
	vals := t.getKeyValues(lbs, now.Unix())
	t.activeSeriesPerUserAttribution.WithLabelValues(vals...).Inc()
}

func (t *Tracker) DecrementActiveSeries(lbs labels.Labels, now time.Time) {
	if t == nil {
		return
	}
	vals := t.getKeyValues(lbs, now.Unix())
	t.activeSeriesPerUserAttribution.WithLabelValues(vals...).Dec()
}

func (t *Tracker) IncrementDiscardedSamples(lbs labels.Labels, value float64, reason string, now time.Time) {
	if t == nil {
		return
	}
	vals := t.getKeyValues(lbs, now.Unix())
	if t.isOverflow {
		vals = append(vals, overflowValue)
	} else {
		vals = append(vals, reason)
	}
	t.discardedSampleAttribution.WithLabelValues(vals...).Add(value)
}

func (t *Tracker) IncrementReceivedSamples(lbs labels.Labels, value float64, now time.Time) {
	if t == nil {
		return
	}
	vals := t.getKeyValues(lbs, now.Unix())
	t.receivedSamplesAttribution.WithLabelValues(vals...).Add(value)
}

func (t *Tracker) Collect(out chan<- prometheus.Metric) {
	if t == nil {
		return
	}
	t.activeSeriesPerUserAttribution.Collect(out)
	t.receivedSamplesAttribution.Collect(out)
	t.discardedSampleAttribution.Collect(out)
}

// Describe implements prometheus.Collector.
func (t *Tracker) Describe(chan<- *prometheus.Desc) {
	// this is an unchecked collector
	if t == nil {
		return
	}
}

func (t *Tracker) getKeyValues(lbls labels.Labels, ts int64) []string {
	if t == nil {
		return nil
	}
	values := make([]string, len(t.caLabels)+1)
	for i, l := range t.caLabels {
		values[i] = lbls.Get(l)
		if values[i] == "" {
			values[i] = missingValue
		}
	}
	values[len(values)-1] = t.userID
	var stream uint64
	stream, _ = lbls.HashForLabels(t.hashBuffer, t.caLabels...)

	if t.overflow(stream, values, ts) {
		// Omit last label.
		for i := range values[:len(values)-1] {
			values[i] = overflowValue
		}
	}
	return values
}

func (t *Tracker) overflow(stream uint64, values []string, ts int64) bool {
	if t == nil {
		return false
	}

	// we store up to 2 * maxCardinality observations, if we have seen the stream before, we update the last update time
	if o, known := t.observed[stream]; known && o.lastUpdate != nil && o.lastUpdate.Load() < ts {
		o.lastUpdate.Store(ts)
	} else if len(t.observed) < t.maxCardinality*2 {
		t.observed[stream] = &Observation{
			lvalues:    values,
			lastUpdate: atomic.NewInt64(ts),
		}
	}

	// If the maximum cardinality is hit all streams become `__overflow__`, the function would return true.
	// the origin labels ovserved time is not updated, but the overflow hash is updated.
	if !t.isOverflow && len(t.observed) > t.maxCardinality {
		t.isOverflow = true
		t.cooldownUntil = atomic.NewInt64(ts + t.cooldownDuration)
	}

	return t.isOverflow
}

func (t *Tracker) PurgeInactiveObservations(deadline int64) []*Observation {
	if t == nil {
		return nil
	}

	// otherwise, we need to check all observations and clean up the ones that are inactive
	var invalidKeys []uint64
	t.obseveredMtx.Lock()
	defer t.obseveredMtx.Unlock()
	for labHash, ob := range t.observed {
		if ob != nil && ob.lastUpdate != nil && ob.lastUpdate.Load() <= deadline {
			invalidKeys = append(invalidKeys, labHash)
		}
	}

	if len(invalidKeys) == 0 {
		return nil
	}

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
