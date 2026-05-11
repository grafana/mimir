// SPDX-License-Identifier: AGPL-3.0-only

package costattribution

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// CompositeSampleTracker delegates to multiple SampleTrackers.
type CompositeSampleTracker struct {
	trackers []*SampleTracker
}

func newCompositeSampleTracker(trackers []*SampleTracker) *CompositeSampleTracker {
	if len(trackers) == 0 {
		return nil
	}
	return &CompositeSampleTracker{trackers: trackers}
}

func (c *CompositeSampleTracker) IncrementReceivedSamples(req *mimirpb.WriteRequest, now time.Time) {
	if c == nil {
		return
	}
	for _, t := range c.trackers {
		t.IncrementReceivedSamples(req, now)
	}
}

func (c *CompositeSampleTracker) IncrementDiscardedSamples(lbls []mimirpb.LabelAdapter, value float64, reason string, now time.Time) {
	if c == nil {
		return
	}
	for _, t := range c.trackers {
		t.IncrementDiscardedSamples(lbls, value, reason, now)
	}
}

// CompositeActiveSeriesTracker delegates to multiple ActiveSeriesTrackers.
type CompositeActiveSeriesTracker struct {
	trackers []*ActiveSeriesTracker
}

func NewCompositeActiveSeriesTracker(trackers []*ActiveSeriesTracker) *CompositeActiveSeriesTracker {
	if len(trackers) == 0 {
		return nil
	}
	return &CompositeActiveSeriesTracker{trackers: trackers}
}

func (c *CompositeActiveSeriesTracker) Increment(lbls labels.Labels, now time.Time, nativeHistogramBucketNum int) {
	if c == nil {
		return
	}
	for _, t := range c.trackers {
		t.Increment(lbls, now, nativeHistogramBucketNum)
	}
}

func (c *CompositeActiveSeriesTracker) Decrement(lbls labels.Labels, nativeHistogramBucketNum int) {
	if c == nil {
		return
	}
	for _, t := range c.trackers {
		t.Decrement(lbls, nativeHistogramBucketNum)
	}
}

func (c *CompositeActiveSeriesTracker) Collect(out chan<- prometheus.Metric) {
	if c == nil {
		return
	}
	for _, t := range c.trackers {
		t.Collect(out)
	}
}

// Equals returns true if both composites contain the same tracker pointers in the same order.
func (c *CompositeActiveSeriesTracker) Equals(other *CompositeActiveSeriesTracker) bool {
	if c == nil && other == nil {
		return true
	}
	if c == nil || other == nil {
		return false
	}
	if len(c.trackers) != len(other.trackers) {
		return false
	}
	for i, t := range c.trackers {
		if t != other.trackers[i] {
			return false
		}
	}
	return true
}
