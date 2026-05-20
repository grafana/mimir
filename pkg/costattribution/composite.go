// SPDX-License-Identifier: AGPL-3.0-only

package costattribution

import (
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/costattribution/costattributionmodel"
	"github.com/grafana/mimir/pkg/mimirpb"
)

// SampleTracker delegates to one or more internal sample trackers.
type SampleTracker struct {
	trackers   []*sampleTracker
	configHash uint64
}

func (c *SampleTracker) getTrackers() []*sampleTracker { return c.trackers }
func (c *SampleTracker) getConfigHash() uint64         { return c.configHash }

func newSampleTrackerComposite(trackers []*sampleTracker, configHash uint64) *SampleTracker {
	return &SampleTracker{trackers: trackers, configHash: configHash}
}

func (c *SampleTracker) IncrementReceivedSamples(req *mimirpb.WriteRequest, now time.Time) {
	if c == nil {
		return
	}
	for _, t := range c.trackers {
		t.IncrementReceivedSamples(req, now)
	}
}

func (c *SampleTracker) IncrementDiscardedSamples(lbls []mimirpb.LabelAdapter, value float64, reason string, now time.Time) {
	if c == nil {
		return
	}
	for _, t := range c.trackers {
		t.IncrementDiscardedSamples(lbls, value, reason, now)
	}
}

// ActiveSeriesTracker delegates to one or more internal active series trackers.
type ActiveSeriesTracker struct {
	trackers   []*activeSeriesTracker
	configHash uint64
}

func (c *ActiveSeriesTracker) getTrackers() []*activeSeriesTracker { return c.trackers }
func (c *ActiveSeriesTracker) getConfigHash() uint64               { return c.configHash }

func newActiveSeriesTrackerComposite(trackers []*activeSeriesTracker, configHash uint64) *ActiveSeriesTracker {
	return &ActiveSeriesTracker{trackers: trackers, configHash: configHash}
}

// NewActiveSeriesTracker creates an ActiveSeriesTracker with a single tracker.
func NewActiveSeriesTracker(userID, trackerName string, trackedLabels costattributionmodel.Labels, limit int, cooldownDuration time.Duration, logger log.Logger) (*ActiveSeriesTracker, error) {
	t, err := newActiveSeriesTracker(userID, trackerName, trackedLabels, limit, cooldownDuration, logger)
	if err != nil {
		return nil, err
	}
	return &ActiveSeriesTracker{trackers: []*activeSeriesTracker{t}}, nil
}

func (c *ActiveSeriesTracker) Increment(lbls labels.Labels, now time.Time, nativeHistogramBucketNum int) {
	if c == nil {
		return
	}
	for _, t := range c.trackers {
		t.Increment(lbls, now, nativeHistogramBucketNum)
	}
}

func (c *ActiveSeriesTracker) Decrement(lbls labels.Labels, nativeHistogramBucketNum int) {
	if c == nil {
		return
	}
	for _, t := range c.trackers {
		t.Decrement(lbls, nativeHistogramBucketNum)
	}
}

// Equals returns true if both composites contain the same tracker pointers in the same order.
func (c *ActiveSeriesTracker) Equals(other *ActiveSeriesTracker) bool {
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
