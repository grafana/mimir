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
)

const sep = rune(0x80)

type observation struct {
	lastUpdate         atomic.Int64
	receivedSample     atomic.Float64
	discardedSampleMtx sync.Mutex
	discardedSample    map[string]*atomic.Float64
	totalDiscarded     atomic.Float64
}

type SampleTracker struct {
	userID                     string
	labels                     []string
	maxCardinality             int
	receivedSamplesAttribution *prometheus.Desc
	discardedSampleAttribution *prometheus.Desc
	overflowLabels             []string
	observed                   map[string]*observation
	observedMtx                sync.RWMutex
	overflowSince              atomic.Int64
	overflowCounter            *observation
	cooldownDuration           time.Duration
	logger                     log.Logger
}

func newSampleTracker(userID string, trackedLabels []string, limit int, cooldown time.Duration, logger log.Logger) *SampleTracker {
	orderedLables := slices.Clone(trackedLabels)
	slices.Sort(orderedLables)

	// Create a map for overflow labels to export when overflow happens
	overflowLabels := make([]string, len(orderedLables)+2)
	for i := range orderedLables {
		overflowLabels[i] = overflowValue
	}

	overflowLabels[len(orderedLables)] = userID
	overflowLabels[len(orderedLables)+1] = overflowValue

	tracker := &SampleTracker{
		userID:           userID,
		labels:           orderedLables,
		maxCardinality:   limit,
		observed:         make(map[string]*observation),
		cooldownDuration: cooldown,
		logger:           logger,
		overflowLabels:   overflowLabels,
		overflowCounter:  &observation{},
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
	return tracker
}

func (t *SampleTracker) hasSameLabels(labels []string) bool {
	return slices.Equal(t.labels, labels)
}

var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func (t *SampleTracker) cleanupTrackerAttribution(key string) {
	t.observedMtx.Lock()
	defer t.observedMtx.Unlock()
	delete(t.observed, key)
}

func (t *SampleTracker) Collect(out chan<- prometheus.Metric) {
	if t.overflowSince.Load() > 0 {
		t.observedMtx.RLock()
		t.observedMtx.RUnlock()
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

func (t *SampleTracker) IncrementDiscardedSamples(lbs []mimirpb.LabelAdapter, value float64, reason string, now time.Time) {
	if t == nil {
		return
	}
	t.updateCountersWithLabelAdapter(lbs, now, 0, 0, value, &reason)
}

func (t *SampleTracker) IncrementReceivedSamples(req *mimirpb.WriteRequest, now time.Time) {
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
		t.updateObservations(k, now, count, 0, nil)
		total += count
	}
}

func (t *SampleTracker) updateCountersWithLabelAdapter(lbls []mimirpb.LabelAdapter, ts time.Time, activeSeriesIncrement, receivedSampleIncrement, discardedSampleIncrement float64, reason *string) {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)
	t.fillKeyFromLabelAdapters(lbls, buf)
	t.updateObservations(buf.String(), ts, receivedSampleIncrement, discardedSampleIncrement, reason)
}

func (t *SampleTracker) fillKeyFromLabelAdapters(lbls []mimirpb.LabelAdapter, buf *bytes.Buffer) {
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

// updateObservations updates or creates a new observation in the 'observed' map.
func (t *SampleTracker) updateObservations(key string, ts time.Time, receivedSampleIncrement, discardedSampleIncrement float64, reason *string) {
	// if overflowSince is set, we only update the overflow counter
	if t.overflowSince.Load() > 0 {
		t.overflowCounter.receivedSample.Add(receivedSampleIncrement)
		if discardedSampleIncrement > 0 && reason != nil {
			t.overflowCounter.totalDiscarded.Add(discardedSampleIncrement)
		}
		return
	}

	// if not overflow, we need to check if the key exists in the observed map,
	// if yes, we update the observation, otherwise we create a new observation, and set the overflowSince if the max cardinality is exceeded
	t.observedMtx.Lock()
	defer t.observedMtx.Unlock()
	o, known := t.observed[key]
	if known && t.overflowSince.Load() == 0 {
		o.lastUpdate.Store(ts.Unix())
		o.receivedSample.Add(receivedSampleIncrement)
		if discardedSampleIncrement > 0 && reason != nil {
			o.discardedSampleMtx.Lock()
			if _, ok := o.discardedSample[*reason]; ok {
				o.discardedSample[*reason].Add(discardedSampleIncrement)
			} else {
				o.discardedSample[*reason] = atomic.NewFloat64(discardedSampleIncrement)
			}
			o.discardedSampleMtx.Unlock()
		}
		return
	}

	if t.overflowSince.Load() > 0 {
		t.overflowCounter.receivedSample.Add(receivedSampleIncrement)
		if discardedSampleIncrement > 0 && reason != nil {
			t.overflowCounter.totalDiscarded.Add(discardedSampleIncrement)
		}
		return
	}

	// If adding the new observation would exceed the max cardinality, we need to update the state
	t.observed[key] = &observation{
		lastUpdate:         *atomic.NewInt64(ts.Unix()),
		discardedSample:    make(map[string]*atomic.Float64),
		receivedSample:     *atomic.NewFloat64(receivedSampleIncrement),
		discardedSampleMtx: sync.Mutex{},
	}

	if discardedSampleIncrement > 0 && reason != nil {
		t.observed[key].discardedSampleMtx.Lock()
		t.observed[key].discardedSample[*reason] = atomic.NewFloat64(discardedSampleIncrement)
		t.observed[key].discardedSampleMtx.Unlock()
	}

	if len(t.observed) >= t.maxCardinality {
		t.overflowSince.Store(ts.Unix())
	}
}

func (t *SampleTracker) recoveredFromOverflow(deadline time.Time) bool {
	t.observedMtx.RLock()
	if t.overflowSince.Load() > 0 && time.Unix(t.overflowSince.Load(), 0).Add(t.cooldownDuration).Before(deadline) {
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
		t.overflowSince.Store(deadline.Unix())
		t.observedMtx.Unlock()
	} else {
		t.observedMtx.RUnlock()
	}
	return false
}

func (t *SampleTracker) inactiveObservations(deadline time.Time) []string {
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
