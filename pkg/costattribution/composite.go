// SPDX-License-Identifier: AGPL-3.0-only

// nolint:unused // This file uses generics and unused linter fails to detect their usage.
package costattribution

import (
	"slices"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/costattribution/costattributionmodel"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/validation"
)

// SampleTracker delegates to one or more internal sample trackers.
type SampleTracker struct {
	trackers   []*sampleTracker
	configHash uint64
}

func (c *SampleTracker) getTrackers() []*sampleTracker { return c.trackers }

func (c *SampleTracker) getConfigHash() uint64 { return c.configHash }

func (c *SampleTracker) purge(now, deadline time.Time) (cardinality int, shouldRecreate bool) {
	c.each(func(t *sampleTracker) { cardinality += t.purge(now, deadline) })
	// When a sampleTracker recovers from overflow it just resets, it doesn't need to be re-created.
	return cardinality, false
}

func (c *SampleTracker) collectCostAttribution(out chan<- prometheus.Metric) {
	c.eachFiltered(isNotInternal, func(t *sampleTracker) { t.collectCostAttribution(out) })
}

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

// rebuild returns a new composite for the given config, reusing the individual trackers whose
// config didn't change and recreating only the ones that did (or were added). Reusing trackers
// preserves their accumulated counts, which is the desired behavior for sample trackers.
// The receiver may be nil, in which case all trackers are built from scratch.
func (c *SampleTracker) rebuild(userID string, cfg validation.CostAttributionConfig, configHash uint64, logger log.Logger, creationErrors *prometheus.CounterVec) *SampleTracker {
	existing := make(map[string]*sampleTracker)
	c.each(func(t *sampleTracker) { existing[t.name] = t })

	trackers := make([]*sampleTracker, 0, len(cfg.Trackers))
	for name, tcfg := range cfg.Trackers {
		if t, ok := existing[name]; ok {
			lbls, internal, maxCardinality, cooldown := t.config()
			if maxCardinality == cfg.MaxCardinality && internal == tcfg.Internal && cooldown == cfg.Cooldown && slices.Equal(lbls, tcfg.Labels) {
				trackers = append(trackers, t)
				continue
			}
		}
		t, err := newSampleTracker(userID, name, tcfg.Labels, tcfg.Internal, cfg.MaxCardinality, cfg.Cooldown, logger)
		if err != nil {
			creationErrors.With(prometheus.Labels{"user": userID, "tracker_name": name}).Inc()
			level.Warn(logger).Log("msg", "error creating cost attribution tracker, skipping it", "user", userID, "tracker_name", name, "error", err)
			continue
		}
		trackers = append(trackers, t)
	}
	slices.SortFunc(trackers, func(a, b *sampleTracker) int { return strings.Compare(a.name, b.name) })
	return newSampleTrackerComposite(trackers, configHash)
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

func (c *ActiveSeriesTracker) getTrackers() []*activeSeriesTracker { return c.trackers }

func (c *ActiveSeriesTracker) getConfigHash() uint64 { return c.configHash }

func (c *ActiveSeriesTracker) purge(now, deadline time.Time) (cardinality int, shouldRecreate bool) {
	c.each(func(t *activeSeriesTracker) {
		trackerCardinality, trackerShouldRecreate := t.purge(now, deadline)
		cardinality += trackerCardinality
		shouldRecreate = shouldRecreate || trackerShouldRecreate
	})
	return cardinality, shouldRecreate
}

func (c *ActiveSeriesTracker) collectCostAttribution(out chan<- prometheus.Metric) {
	c.eachFiltered(isNotInternal, func(t *activeSeriesTracker) { t.collectCostAttribution(out) })
}

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

// rebuild returns a new composite for the given config, always building all individual trackers
// from scratch. Unlike sample trackers, active series trackers can't be reused across composites:
// the ingester detects the composite swap (via Equals) and re-tracks all series, so reusing a
// tracker that still holds its observations would double-count them. Building fresh, empty
// trackers ensures the re-track counts each series exactly once.
// The receiver is ignored (and may be nil).
func (c *ActiveSeriesTracker) rebuild(userID string, cfg validation.CostAttributionConfig, configHash uint64, logger log.Logger, creationErrors *prometheus.CounterVec) *ActiveSeriesTracker {
	trackers := make([]*activeSeriesTracker, 0, len(cfg.Trackers))
	for name, tcfg := range cfg.Trackers {
		t, err := newActiveSeriesTracker(userID, name, tcfg.Labels, tcfg.Internal, cfg.MaxCardinality, cfg.Cooldown, logger)
		if err != nil {
			creationErrors.With(prometheus.Labels{"user": userID, "tracker_name": name}).Inc()
			level.Warn(logger).Log("msg", "error creating cost attribution tracker, skipping it", "user", userID, "tracker_name", name, "error", err)
			continue
		}
		trackers = append(trackers, t)
	}
	slices.SortFunc(trackers, func(a, b *activeSeriesTracker) int { return strings.Compare(a.name, b.name) })
	return newActiveSeriesTrackerComposite(trackers, configHash)
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
