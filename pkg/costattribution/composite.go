// SPDX-License-Identifier: AGPL-3.0-only

package costattribution

import (
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/costattribution/costattributionmodel"
	"github.com/grafana/mimir/pkg/mimirpb"
)

// SampleTracker delegates to one or more internal sample trackers.
type SampleTracker struct {
	trackers   []*sampleTracker
	configHash uint64
}

// nolint:unused
func (c *SampleTracker) getTrackers() []*sampleTracker { return c.trackers }

// nolint:unused
func (c *SampleTracker) getConfigHash() uint64 { return c.configHash }

// nolint:unused
func (c *SampleTracker) purge(now, deadline time.Time) int {
	cardinality := 0
	c.each(func(t *sampleTracker) { cardinality += t.purge(now, deadline) })
	return cardinality
}

// nolint:unused
func (c *SampleTracker) collectCostAttribution(out chan<- prometheus.Metric) {
	c.eachFiltered(isNotInternal, func(t *sampleTracker) { t.collectCostAttribution(out) })
}

// nolint:unused
func (c *SampleTracker) collectInternalCostAttribution(out chan<- prometheus.Metric) {
	c.eachFiltered(isInternal, func(t *sampleTracker) { t.collectCostAttribution(out) })
}

func (c *SampleTracker) each(do func(*sampleTracker)) {
	if c == nil {
		return
	}
	for _, t := range c.trackers {
		do(t)
	}
}

func (c *SampleTracker) eachFiltered(filter func(tracker individualTracker) bool, do func(tracker *sampleTracker)) {
	if c == nil {
		return
	}
	for _, t := range c.trackers {
		if filter(t) {
			do(t)
		}
	}
}

func newSampleTrackerComposite(trackers []*sampleTracker, configHash uint64) *SampleTracker {
	return &SampleTracker{trackers: trackers, configHash: configHash}
}

func (c *SampleTracker) IncrementReceivedSamples(req *mimirpb.WriteRequest, now time.Time) {
	c.each(func(t *sampleTracker) { t.IncrementReceivedSamples(req, now) })
}

func (c *SampleTracker) IncrementDiscardedSamples(lbls []mimirpb.LabelAdapter, value float64, reason string, now time.Time) {
	c.each(func(t *sampleTracker) { t.IncrementDiscardedSamples(lbls, value, reason, now) })
}

// ActiveSeriesTracker delegates to one or more internal active series trackers.
type ActiveSeriesTracker struct {
	trackers   []*activeSeriesTracker
	configHash uint64
}

// nolint:unused
func (c *ActiveSeriesTracker) getTrackers() []*activeSeriesTracker { return c.trackers }

// nolint:unused
func (c *ActiveSeriesTracker) getConfigHash() uint64 { return c.configHash }

// nolint:unused
func (c *ActiveSeriesTracker) purge(now, deadline time.Time) int {
	cardinality := 0
	c.each(func(t *activeSeriesTracker) { cardinality += t.purge(now, deadline) })
	return cardinality
}

// nolint:unused
func (c *ActiveSeriesTracker) collectCostAttribution(out chan<- prometheus.Metric) {
	c.eachFiltered(isNotInternal, func(t *activeSeriesTracker) { t.collectCostAttribution(out) })
}

// nolint:unused
func (c *ActiveSeriesTracker) collectInternalCostAttribution(out chan<- prometheus.Metric) {
	c.eachFiltered(isInternal, func(t *activeSeriesTracker) { t.collectCostAttribution(out) })
}

func (c *ActiveSeriesTracker) each(do func(tracker *activeSeriesTracker)) {
	if c == nil {
		return
	}
	for _, t := range c.trackers {
		do(t)
	}
}

func (c *ActiveSeriesTracker) eachFiltered(filter func(tracker individualTracker) bool, do func(tracker *activeSeriesTracker)) {
	if c == nil {
		return
	}
	for _, t := range c.trackers {
		if filter(t) {
			do(t)
		}
	}
}

func newActiveSeriesTrackerComposite(trackers []*activeSeriesTracker, configHash uint64) *ActiveSeriesTracker {
	return &ActiveSeriesTracker{
		trackers:   trackers,
		configHash: configHash,
	}
}

func (c *ActiveSeriesTracker) Increment(lbls labels.Labels, now time.Time, nativeHistogramBucketNum int) {
	c.each(func(t *activeSeriesTracker) { t.Increment(lbls, now, nativeHistogramBucketNum) })
}

func (c *ActiveSeriesTracker) Decrement(lbls labels.Labels, nativeHistogramBucketNum int) {
	c.each(func(t *activeSeriesTracker) { t.Decrement(lbls, nativeHistogramBucketNum) })
}

// Equals returns true if both composites are the same instance.
// If an ActiveSeriesTracker was re-created with same config after a purge,
// it still should be considered different as we need to re-track the series we missed while racing with the purge.
func (c *ActiveSeriesTracker) Equals(other *ActiveSeriesTracker) bool {
	return c == other
}

// NewActiveSeriesTrackerForTests creates an ActiveSeriesTracker with a single tracker.
// This is a test helper for the activeseries package.
func NewActiveSeriesTrackerForTests(userID, trackerName string, trackedLabels costattributionmodel.Labels, limit int, cooldownDuration time.Duration, logger log.Logger) (*ActiveSeriesTracker, error) {
	t, err := newActiveSeriesTracker(userID, trackerName, trackedLabels, false, limit, cooldownDuration, logger)
	if err != nil {
		return nil, err
	}
	return &ActiveSeriesTracker{trackers: []*activeSeriesTracker{t}}, nil
}

func isInternal(tracker individualTracker) bool {
	_, internal, _, _ := tracker.config()
	return internal
}

func isNotInternal(tracker individualTracker) bool {
	_, internal, _, _ := tracker.config()
	return !internal
}
