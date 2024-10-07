// SPDX-License-Identifier: AGPL-3.0-only

package caimpl

import (
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"
)

type Tracker struct {
	trackedLabel                   string
	attributionLimit               int
	activeSeriesPerUserAttribution *prometheus.GaugeVec
	receivedSamplesAttribution     *prometheus.CounterVec
	discardedSampleAttribution     *prometheus.CounterVec
	attributionTimestamps          map[string]*atomic.Int64
	coolDownDeadline               *atomic.Int64
}

func (t *Tracker) cleanupTrackerAttribution(userID, attribution string) {
	t.activeSeriesPerUserAttribution.DeleteLabelValues(userID, attribution)
	t.receivedSamplesAttribution.DeleteLabelValues(userID, attribution)
	t.discardedSampleAttribution.DeleteLabelValues(userID, attribution)
}

func (t *Tracker) cleanupTracker(userID string) {
	filter := prometheus.Labels{"user": userID}
	t.activeSeriesPerUserAttribution.DeletePartialMatch(filter)
	t.receivedSamplesAttribution.DeletePartialMatch(filter)
	t.discardedSampleAttribution.DeletePartialMatch(filter)
}

func newTracker(trackedLabel string, limit int) (*Tracker, error) {
	m := &Tracker{
		trackedLabel:          trackedLabel,
		attributionLimit:      limit,
		attributionTimestamps: map[string]*atomic.Int64{},
		coolDownDeadline:      atomic.NewInt64(0),
		//nolint:faillint // the metrics are registered in the mimir package
		discardedSampleAttribution: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_discarded_samples_attribution_total",
			Help: "The total number of samples that were discarded per attribution.",
		}, []string{"user", trackedLabel}),
		//nolint:faillint
		receivedSamplesAttribution: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_received_samples_attribution_total",
			Help: "The total number of samples that were received per attribution.",
		}, []string{"user", trackedLabel}),
		//nolint:faillint
		activeSeriesPerUserAttribution: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_ingester_active_series_attribution",
			Help: "The total number of active series per user and attribution.",
		}, []string{"user", trackedLabel}),
	}
	return m, nil
}

func (t *Tracker) Collect(out chan<- prometheus.Metric) {
	t.activeSeriesPerUserAttribution.Collect(out)
	t.receivedSamplesAttribution.Collect(out)
	t.discardedSampleAttribution.Collect(out)
}

// Describe implements prometheus.Collector.
func (t *Tracker) Describe(chan<- *prometheus.Desc) {
	// this is an unchecked collector
}
