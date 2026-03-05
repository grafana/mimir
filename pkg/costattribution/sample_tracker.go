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
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/costattribution/costattributionmodel"
	"github.com/grafana/mimir/pkg/mimirpb"
)

const sep = rune(0x80)

// sliceToMapThreshold is the number of unique attribution keys at which
// IncrementReceivedSamples switches from a slice-based linear scan to a map
// for aggregating sample counts. For typical workloads (1-10 unique keys),
// the slice approach is much faster and avoids allocations.
const sliceToMapThreshold = 16

type observation struct {
	lastUpdate         atomic.Int64
	receivedSample     atomic.Float64
	discardedSampleMtx sync.RWMutex
	discardedSample    map[string]*atomic.Float64
	totalDiscarded     atomic.Float64
}

// keyCount pairs an attribution key with its aggregated sample count.
type keyCount struct {
	key   string
	count int
}

type SampleTracker struct {
	userID                     string
	receivedSamplesAttribution *descriptor
	discardedSampleAttribution *descriptor
	logger                     log.Logger

	labels         costattributionmodel.Labels
	overflowLabels []string

	maxCardinality   int
	cooldownDuration time.Duration

	observedMtx   sync.RWMutex
	observed      map[string]*observation
	overflowSince time.Time

	overflowCounter observation
}

func newSampleTracker(userID string, trackedLabels costattributionmodel.Labels, limit int, cooldown time.Duration, logger log.Logger) (*SampleTracker, error) {
	// Create a map for overflow labels to export when overflow happens
	overflowLabels := make([]string, len(trackedLabels)+2)
	for i := range trackedLabels {
		overflowLabels[i] = overflowValue
	}

	overflowLabels[len(trackedLabels)] = userID
	overflowLabels[len(trackedLabels)+1] = overflowValue

	tracker := &SampleTracker{
		userID:           userID,
		labels:           trackedLabels,
		maxCardinality:   limit,
		observed:         make(map[string]*observation),
		cooldownDuration: cooldown,
		logger:           logger,
		overflowLabels:   overflowLabels,
		overflowCounter:  observation{},
	}

	if err := tracker.createAndValidateDescriptors(trackedLabels); err != nil {
		return nil, fmt.Errorf("failed to create a sample tracker for tenant %s: %w", userID, err)
	}

	return tracker, nil
}

func (st *SampleTracker) createAndValidateDescriptors(trackedLabels costattributionmodel.Labels) error {
	variableLabels := make([]string, 0, len(trackedLabels)+2)
	for _, label := range trackedLabels {
		variableLabels = append(variableLabels, label.OutputLabel())
	}
	variableLabels = append(variableLabels, tenantLabel, reasonLabel)

	var err error
	if st.receivedSamplesAttribution, err = newDescriptor("cortex_distributor_received_attributed_samples_total",
		"The total number of samples that were received per attribution.",
		variableLabels[:len(variableLabels)-1],
		prometheus.Labels{trackerLabel: defaultTrackerName}); err != nil {
		return err
	}

	if st.discardedSampleAttribution, err = newDescriptor("cortex_discarded_attributed_samples_total",
		"The total number of samples that were discarded per attribution.",
		variableLabels,
		prometheus.Labels{trackerLabel: defaultTrackerName}); err != nil {
		return err
	}
	return nil
}

func (st *SampleTracker) hasSameLabels(labels costattributionmodel.Labels) bool {
	return slices.Equal(st.labels, labels)
}

var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func (st *SampleTracker) collectCostAttribution(out chan<- prometheus.Metric) {
	// We don't know the performance of out receiver, so we don't want to hold the lock for too long
	var prometheusMetrics []prometheus.Metric
	st.observedMtx.RLock()

	if !st.overflowSince.IsZero() {
		st.observedMtx.RUnlock()
		out <- st.receivedSamplesAttribution.counter(st.overflowCounter.receivedSample.Load(), st.overflowLabels[:len(st.overflowLabels)-1]...)
		out <- st.discardedSampleAttribution.counter(st.overflowCounter.totalDiscarded.Load(), st.overflowLabels...)
		return
	}

	for key, o := range st.observed {
		keys := strings.Split(key, string(sep))
		keys = append(keys, st.userID)
		if o.receivedSample.Load() > 0 {
			prometheusMetrics = append(prometheusMetrics, st.receivedSamplesAttribution.counter(o.receivedSample.Load(), keys...))
		}
		o.discardedSampleMtx.RLock()
		for reason, discarded := range o.discardedSample {
			prometheusMetrics = append(prometheusMetrics, st.discardedSampleAttribution.counter(discarded.Load(), append(keys, reason)...))
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
	//
	// We use a slice instead of a map to aggregate counts by key. The number of unique attribution
	// keys per request is typically very small (1-5), so linear scan is fast and avoids map overhead
	// and per-timeseries string allocations. A string is only allocated when a new unique key is first
	// seen. The comparison `existingKey == string(bufBytes)` is optimized by the Go compiler to avoid
	// allocating a new string.
	// If the number of unique keys exceeds sliceToMapThreshold, we fall back to a map for efficiency.
	var keyCounts []keyCount
	var dict map[string]int
	useMap := false
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)
	for _, ts := range req.Timeseries {
		st.fillKeyFromLabelAdapters(ts.Labels, buf)
		sampleCount := len(ts.Samples) + len(ts.Histograms)
		bufBytes := buf.Bytes()

		if useMap {
			dict[string(bufBytes)] += sampleCount
			continue
		}

		// Linear scan for matching key; typically very few unique keys per request.
		found := false
		for i := range keyCounts {
			if keyCounts[i].key == string(bufBytes) {
				keyCounts[i].count += sampleCount
				found = true
				break
			}
		}
		if !found {
			// Only allocate a string for genuinely new keys.
			key := string(bufBytes)
			keyCounts = append(keyCounts, keyCount{key: key, count: sampleCount})

			// If too many unique keys, switch to map-based aggregation.
			if len(keyCounts) >= sliceToMapThreshold {
				dict = make(map[string]int, len(keyCounts))
				for _, kc := range keyCounts {
					dict[kc.key] = kc.count
				}
				keyCounts = nil
				useMap = true
			}
		}
	}

	// Update the observations for each label set and update the state per request,
	// this would be less precised than per sample but it's more efficient
	if useMap {
		for k, v := range dict {
			st.updateObservations(k, now, float64(v), 0, nil)
		}
	} else {
		for _, kc := range keyCounts {
			st.updateObservations(kc.key, now, float64(kc.count), 0, nil)
		}
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
			if l.Name == cal.Input {
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

func (st *SampleTracker) cardinality() (cardinality int, overflown bool) {
	st.observedMtx.RLock()
	defer st.observedMtx.RUnlock()
	return len(st.observed), !st.overflowSince.IsZero()
}
