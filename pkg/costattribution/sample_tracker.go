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
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/validation"
)

const sep = rune(0x80)

type observation struct {
	lastUpdate         atomic.Int64
	receivedSample     atomic.Float64
	discardedSampleMtx sync.RWMutex
	discardedSample    map[string]*atomic.Float64
	totalDiscarded     atomic.Float64
}

type SampleTrackers struct {
	maxCardinality   int
	cooldownDuration time.Duration
	trackers         map[string]*SampleTracker
	mtx              sync.RWMutex
}
type SampleTracker struct {
	userID                     string
	receivedSamplesAttribution *prometheus.Desc
	discardedSampleAttribution *prometheus.Desc
	logger                     log.Logger

	labels         []string
	overflowLabels []string

	observedMtx   sync.RWMutex
	observed      map[string]*observation
	overflowSince time.Time

	overflowCounter observation
}

func newSampleTrackers(userID string, maxCardinality int, cooldownDuration time.Duration, logger log.Logger, tksCfg []validation.CostAttributionTracker) *SampleTrackers {
	simpleTrackers := &SampleTrackers{
		trackers:         make(map[string]*SampleTracker),
		maxCardinality:   maxCardinality,
		cooldownDuration: cooldownDuration,
	}
	for _, tkCfg := range tksCfg {
		// sort the labels to ensure the order is consistent
		orderedLables := slices.Clone(tkCfg.Labels)
		slices.Sort(orderedLables)
		simpleTrackers.trackers[tkCfg.Name] = newSampleTracker(userID, orderedLables, logger)
	}
	return simpleTrackers
}

func newSampleTracker(userID string, trackedLabels []string, logger log.Logger) *SampleTracker {
	// Create a map for overflow labels to export when overflow happens
	overflowLabels := make([]string, len(trackedLabels)+2)
	for i := range trackedLabels {
		overflowLabels[i] = overflowValue
	}

	overflowLabels[len(trackedLabels)] = userID
	overflowLabels[len(trackedLabels)+1] = overflowValue

	tracker := &SampleTracker{
		userID:          userID,
		labels:          trackedLabels,
		observed:        make(map[string]*observation),
		logger:          logger,
		overflowLabels:  overflowLabels,
		overflowCounter: observation{},
	}

	variableLabels := slices.Clone(trackedLabels)
	variableLabels = append(variableLabels, tenantLabel, "reason")
	tracker.discardedSampleAttribution = prometheus.NewDesc("cortex_discarded_attributed_samples_total",
		"The total number of samples that were discarded per attribution.",
		variableLabels,
		prometheus.Labels{trackerLabel: defaultTrackerName})

	tracker.receivedSamplesAttribution = prometheus.NewDesc("cortex_distributor_received_attributed_samples_total",
		"The total number of samples that were received per attribution.",
		variableLabels[:len(variableLabels)-1],
		prometheus.Labels{trackerLabel: defaultTrackerName})
	return tracker
}

func (st *SampleTrackers) hasSameLabels(name string, labels []string) bool {
	if tk, exists := st.trackers[name]; !exists || !slices.Equal(tk.labels, labels) {
		return false
	}
	return true
}

var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func (st *SampleTracker) Collect(out chan<- prometheus.Metric) {
	// We don't know the performance of out receiver, so we don't want to hold the lock for too long
	var prometheusMetrics []prometheus.Metric
	st.observedMtx.RLock()

	if !st.overflowSince.IsZero() {
		st.observedMtx.RUnlock()
		out <- prometheus.MustNewConstMetric(st.receivedSamplesAttribution, prometheus.CounterValue, st.overflowCounter.receivedSample.Load(), st.overflowLabels[:len(st.overflowLabels)-1]...)
		out <- prometheus.MustNewConstMetric(st.discardedSampleAttribution, prometheus.CounterValue, st.overflowCounter.totalDiscarded.Load(), st.overflowLabels...)
		return
	}

	for key, o := range st.observed {
		keys := strings.Split(key, string(sep))
		keys = append(keys, st.userID)
		if o.receivedSample.Load() > 0 {
			prometheusMetrics = append(prometheusMetrics, prometheus.MustNewConstMetric(st.receivedSamplesAttribution, prometheus.CounterValue, o.receivedSample.Load(), keys...))
		}
		o.discardedSampleMtx.RLock()
		for reason, discarded := range o.discardedSample {
			prometheusMetrics = append(prometheusMetrics, prometheus.MustNewConstMetric(st.discardedSampleAttribution, prometheus.CounterValue, discarded.Load(), append(keys, reason)...))
		}
		o.discardedSampleMtx.RUnlock()
	}
	st.observedMtx.RUnlock()

	for _, m := range prometheusMetrics {
		out <- m
	}
}

func (st *SampleTracker) IncrementDiscardedSamples(lbls []mimirpb.LabelAdapter, value float64, reason string, now time.Time) {
	if st == nil {
		return
	}
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)
	st.fillKeyFromLabelAdapters(lbls, buf)
	st.updateObservations(buf.String(), now, 0, value, &reason)
}

func (st *SampleTracker) IncrementReceivedSamples(req *mimirpb.WriteRequest, now time.Time) {
	if st == nil {
		return
	}

	// We precompute the cost attribution per request before update Observations and State to avoid frequently update the atomic counters.
	// This is based on the assumption that usually a single WriteRequest will have samples that belong to the same or few cost attribution groups.
	dict := make(map[string]int)
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)
	for _, ts := range req.Timeseries {
		st.fillKeyFromLabelAdapters(ts.Labels, buf)
		dict[string(buf.Bytes())] += len(ts.TimeSeries.Samples) + len(ts.TimeSeries.Histograms)
	}

	// Update the observations for each label set and update the state per request,
	// this would be less precised than per sample but it's more efficient
	var total float64
	for k, v := range dict {
		count := float64(v)
		st.updateObservations(k, now, count, 0, nil)
		total += count
	}
}

func (st *SampleTracker) fillKeyFromLabelAdapters(lbls []mimirpb.LabelAdapter, buf *bytes.Buffer) {
	buf.Reset()
	var exists bool
	for idx, cal := range st.labels {
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

// updateObservations updates or creates a new observation in the 'observed' map.
func (st *SampleTracker) updateObservations(key string, ts time.Time, receivedSampleIncrement, discardedSampleIncrement float64, reason *string) {
	// if not overflow, we need to check if the key exists in the observed map,
	// if yes, we update the observation, otherwise we create a new observation, and set the overflowSince if the max cardinality is exceeded
	st.observedMtx.RLock()

	// if overflowSince is set, we only update the overflow counter, this is after the read lock since overflowSince can only be set when holding observedMtx write lock
	// check it after read lock would make sure that we don't miss any updates
	if !st.overflowSince.IsZero() {
		st.overflowCounter.receivedSample.Add(receivedSampleIncrement)
		if discardedSampleIncrement > 0 && reason != nil {
			st.overflowCounter.totalDiscarded.Add(discardedSampleIncrement)
		}
		st.observedMtx.RUnlock()
		return
	}

	o, known := st.observed[key]
	if known {
		o.lastUpdate.Store(ts.Unix())
		o.receivedSample.Add(receivedSampleIncrement)
		if discardedSampleIncrement > 0 && reason != nil {
			o.discardedSampleMtx.RLock()
			if r, ok := o.discardedSample[*reason]; ok {
				r.Add(discardedSampleIncrement)
				o.discardedSampleMtx.RUnlock()
			} else {
				o.discardedSampleMtx.RUnlock()
				o.discardedSampleMtx.Lock()
				if r, ok := o.discardedSample[*reason]; ok {
					r.Add(discardedSampleIncrement)
				} else {
					o.discardedSample[*reason] = atomic.NewFloat64(discardedSampleIncrement)
				}
				o.discardedSampleMtx.Unlock()
			}
		}
		st.observedMtx.RUnlock()
		return
	}
	st.observedMtx.RUnlock()

	// If it is not known, we take the write lock, but still check whether the key is added in the meantime
	st.observedMtx.Lock()
	defer st.observedMtx.Unlock()
	// If not in overflow, we update the observation if it exists, otherwise we check if create a new observation would exceed the max cardinality
	// if it does, we set the overflowSince
	if st.overflowSince.IsZero() {
		o, known = st.observed[key]
		if known {
			o.lastUpdate.Store(ts.Unix())
			o.receivedSample.Add(receivedSampleIncrement)
			if discardedSampleIncrement > 0 && reason != nil {
				o.discardedSampleMtx.RLock()
				if r, ok := o.discardedSample[*reason]; ok {
					r.Add(discardedSampleIncrement)
					o.discardedSampleMtx.RUnlock()
				} else {
					o.discardedSampleMtx.RUnlock()
					o.discardedSampleMtx.Lock()
					if r, ok := o.discardedSample[*reason]; ok {
						r.Add(discardedSampleIncrement)
					} else {
						o.discardedSample[*reason] = atomic.NewFloat64(discardedSampleIncrement)
					}
					o.discardedSampleMtx.Unlock()
				}
			}
			return
		}
		// if it is not known, we need to check if the max cardinality is exceeded
		if len(st.observed) >= st.maxCardinality {
			st.overflowSince = ts
		}
	}

	// if overflowSince is set, we only update the overflow counter
	if !st.overflowSince.IsZero() {
		st.overflowCounter.receivedSample.Add(receivedSampleIncrement)
		if discardedSampleIncrement > 0 && reason != nil {
			st.overflowCounter.totalDiscarded.Add(discardedSampleIncrement)
		}
		return
	}

	// create a new observation
	st.observed[key] = &observation{
		lastUpdate:      *atomic.NewInt64(ts.Unix()),
		discardedSample: make(map[string]*atomic.Float64),
		receivedSample:  *atomic.NewFloat64(receivedSampleIncrement),
	}

	if discardedSampleIncrement > 0 && reason != nil {
		st.observed[key].discardedSampleMtx.Lock()
		st.observed[key].discardedSample[*reason] = atomic.NewFloat64(discardedSampleIncrement)
		st.observed[key].discardedSampleMtx.Unlock()
	}
}

func (st *SampleTracker) recoveredFromOverflow(deadline time.Time) bool {
	st.observedMtx.RLock()
	if !st.overflowSince.IsZero() && st.overflowSince.Add(st.cooldownDuration).Before(deadline) {
		if len(st.observed) < st.maxCardinality {
			st.observedMtx.RUnlock()
			return true
		}
		st.observedMtx.RUnlock()

		// Increase the cooldown duration if the number of observations is still above the max cardinality
		st.observedMtx.Lock()
		if len(st.observed) < st.maxCardinality {
			st.observedMtx.Unlock()
			return true
		}
		st.overflowSince = deadline
		st.observedMtx.Unlock()
	} else {
		st.observedMtx.RUnlock()
	}
	return false
}

func (sts *SampleTrackers) cleanupInactiveObservations(deadline time.Time) {
	sts.mtx.RLock()
	for _, st := range sts.trackers {
		st.cleanupInactiveObservations(deadline)
	}
	sts.mtx.RUnlock()
}

func (st *SampleTracker) cleanupInactiveObservations(deadline time.Time) {
	// otherwise, we need to check all observations and clean up the ones that are inactive
	var invalidKeys []string
	st.observedMtx.RLock()
	for labkey, ob := range st.observed {
		if ob != nil && ob.lastUpdate.Load() <= deadline.Unix() {
			invalidKeys = append(invalidKeys, labkey)
		}
	}
	st.observedMtx.RUnlock()

	st.observedMtx.Lock()
	for _, key := range invalidKeys {
		delete(st.observed, key)
	}
	st.observedMtx.Unlock()
}
