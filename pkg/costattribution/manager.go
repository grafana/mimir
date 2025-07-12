// SPDX-License-Identifier: AGPL-3.0-only

package costattribution

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"

	model "github.com/grafana/mimir/pkg/costattribution/model"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	trackerLabel       = "tracker"
	tenantLabel        = "tenant"
	defaultTrackerName = "cost-attribution"
	missingValue       = "__missing__"
	overflowValue      = "__overflow__"
)

type Manager struct {
	services.Service
	logger log.Logger
	limits *validation.Overrides

	sampleTrackerCardinalityDesc       *prometheus.Desc
	sampleTrackerOverflowDesc          *prometheus.Desc
	activeSeriesTrackerCardinalityDesc *prometheus.Desc
	activeSeriesTrackerOverflowDesc    *prometheus.Desc

	inactiveTimeout time.Duration
	cleanupInterval time.Duration

	stmtx                  sync.RWMutex
	sampleTrackersByUserID map[string]*SampleTracker

	atmtx                  sync.RWMutex
	activeTrackersByUserID map[string]*ActiveSeriesTracker
}

func NewManager(cleanupInterval, inactiveTimeout time.Duration, logger log.Logger, limits *validation.Overrides, reg, costAttributionReg prometheus.Registerer) (*Manager, error) {
	m := &Manager{
		stmtx:                  sync.RWMutex{},
		sampleTrackersByUserID: make(map[string]*SampleTracker),

		atmtx:                  sync.RWMutex{},
		activeTrackersByUserID: make(map[string]*ActiveSeriesTracker),

		sampleTrackerCardinalityDesc: prometheus.NewDesc("cortex_cost_attribution_sample_tracker_cardinality",
			"The cardinality of a cost attribution sample tracker for each user.",
			[]string{"user"},
			prometheus.Labels{trackerLabel: defaultTrackerName},
		),
		sampleTrackerOverflowDesc: prometheus.NewDesc("cortex_cost_attribution_sample_tracker_overflown",
			"This metric is exported with value 1 when a sample tracker for a user is overflown. It's not exported otherwise.",
			[]string{"user"},
			prometheus.Labels{trackerLabel: defaultTrackerName},
		),
		activeSeriesTrackerCardinalityDesc: prometheus.NewDesc("cortex_cost_attribution_active_series_tracker_cardinality",
			"The cardinality of a cost attribution active series tracker for each user.",
			[]string{"user"},
			prometheus.Labels{trackerLabel: defaultTrackerName},
		),
		activeSeriesTrackerOverflowDesc: prometheus.NewDesc("cortex_cost_attribution_active_series_tracker_overflown",
			"This metric is exported with value 1 when an active series tracker for a user is overflown. It's not exported otherwise.",
			[]string{"user"},
			prometheus.Labels{trackerLabel: defaultTrackerName},
		),

		limits:          limits,
		inactiveTimeout: inactiveTimeout,
		logger:          logger,
		cleanupInterval: cleanupInterval,
	}

	m.Service = services.NewTimerService(cleanupInterval, nil, m.iteration, nil).WithName("cost attribution manager")
	if err := reg.Register(m); err != nil {
		return nil, fmt.Errorf("can't register operational metrics: %w", err)
	}
	if err := costAttributionReg.Register(costAttributionCollector{m}); err != nil {
		return nil, fmt.Errorf("can't register cost attribution metrics: %w", err)
	}
	return m, nil
}

func (m *Manager) iteration(_ context.Context) error {
	return m.purgeInactiveAttributionsUntil(time.Now().Add(-m.inactiveTimeout))
}

func (m *Manager) enabledForUser(userID string) bool {
	if m == nil {
		return false
	}

	return len(m.limits.CostAttributionLabels(userID)) > 0 || len(m.limits.CostAttributionLabelsStructured(userID)) > 0
}

func (m *Manager) labels(userID string) []model.Label {
	// We prefer the structured labels over the string labels, if provided.
	if s := m.limits.CostAttributionLabelsStructured(userID); len(s) > 0 {
		return s
	}
	return model.ParseCostAttributionLabels(m.limits.CostAttributionLabels(userID))
}

func (m *Manager) SampleTracker(userID string) *SampleTracker {
	if !m.enabledForUser(userID) {
		return nil
	}

	// Check if the tracker already exists, if exists return it. Otherwise lock and create a new tracker.
	m.stmtx.RLock()
	tracker, exists := m.sampleTrackersByUserID[userID]
	m.stmtx.RUnlock()
	if exists {
		return tracker
	}

	// We need to create a new tracker, get all the necessary information from the limits before locking and creating the tracker.
	labels := m.labels(userID)
	maxCardinality := m.limits.MaxCostAttributionCardinality(userID)
	cooldownDuration := m.limits.CostAttributionCooldown(userID)

	m.stmtx.Lock()
	defer m.stmtx.Unlock()
	if tracker, exists = m.sampleTrackersByUserID[userID]; exists {
		return tracker
	}

	// sort the labels to ensure the order is consistent.
	slices.SortFunc(labels, func(a, b model.Label) int {
		return strings.Compare(a.Input, b.Input)
	})

	tracker = newSampleTracker(userID, labels, maxCardinality, cooldownDuration, m.logger)
	m.sampleTrackersByUserID[userID] = tracker
	return tracker
}

func (m *Manager) ActiveSeriesTracker(userID string) *ActiveSeriesTracker {
	if !m.enabledForUser(userID) {
		return nil
	}

	// Check if the tracker already exists, if exists return it. Otherwise lock and create a new tracker.
	m.atmtx.RLock()
	tracker, exists := m.activeTrackersByUserID[userID]
	m.atmtx.RUnlock()
	if exists {
		return tracker
	}

	// We need to create a new tracker, get all the necessary information from the limits before locking and creating the tracker.
	labels := m.labels(userID)
	maxCardinality := m.limits.MaxCostAttributionCardinality(userID)
	cooldownDuration := m.limits.CostAttributionCooldown(userID)

	m.atmtx.Lock()
	defer m.atmtx.Unlock()
	if tracker, exists = m.activeTrackersByUserID[userID]; exists {
		return tracker
	}

	// sort the labels to ensure the order is consistent
	slices.SortFunc(labels, func(a, b model.Label) int {
		return strings.Compare(a.Input, b.Input)
	})

	tracker = NewActiveSeriesTracker(userID, labels, maxCardinality, cooldownDuration, m.logger)
	m.activeTrackersByUserID[userID] = tracker
	return tracker
}

func (m *Manager) Collect(out chan<- prometheus.Metric) {
	// Clone both maps to avoid holding the locks while collecting metrics.
	m.stmtx.RLock()
	sampleTrackersByUserID := maps.Clone(m.sampleTrackersByUserID)
	m.stmtx.RUnlock()

	m.atmtx.RLock()
	activeTrackersByUserID := maps.Clone(m.activeTrackersByUserID)
	m.atmtx.RUnlock()

	for _, tracker := range sampleTrackersByUserID {
		cardinality, overflown := tracker.cardinality()

		out <- prometheus.MustNewConstMetric(
			m.sampleTrackerCardinalityDesc,
			prometheus.GaugeValue,
			float64(cardinality),
			tracker.userID,
		)
		if overflown {
			out <- prometheus.MustNewConstMetric(
				m.sampleTrackerOverflowDesc,
				prometheus.GaugeValue,
				1,
				tracker.userID,
			)
		}
	}

	for _, tracker := range activeTrackersByUserID {
		cardinality, overflown := tracker.cardinality()

		out <- prometheus.MustNewConstMetric(
			m.activeSeriesTrackerCardinalityDesc,
			prometheus.GaugeValue,
			float64(cardinality),
			tracker.userID,
		)
		if overflown {
			out <- prometheus.MustNewConstMetric(
				m.activeSeriesTrackerOverflowDesc,
				prometheus.GaugeValue,
				1,
				tracker.userID,
			)
		}
	}
}

func (m *Manager) Describe(chan<- *prometheus.Desc) {
	// Describe is not implemented because the metrics include dynamic labels. The Manager functions as an unchecked exporter.
	// For more details, refer to the documentation: https://pkg.go.dev/github.com/prometheus/client_golang/prometheus#hdr-Custom_Collectors_and_constant_Metrics
}

func (m *Manager) collectCostAttribution(out chan<- prometheus.Metric) {
	// Clone both maps to avoid holding the locks while collecting metrics.
	m.stmtx.RLock()
	sampleTrackersByUserID := maps.Clone(m.sampleTrackersByUserID)
	m.stmtx.RUnlock()

	m.atmtx.RLock()
	activeTrackersByUserID := maps.Clone(m.activeTrackersByUserID)
	m.atmtx.RUnlock()

	for _, tracker := range sampleTrackersByUserID {
		tracker.collectCostAttribution(out)
	}

	for _, tracker := range activeTrackersByUserID {
		tracker.Collect(out)
	}
}

func (m *Manager) describeCostAttribution(chan<- *prometheus.Desc) {
	// Describe is not implemented because the metrics include dynamic labels. The Manager functions as an unchecked exporter.
	// For more details, refer to the documentation: https://pkg.go.dev/github.com/prometheus/client_golang/prometheus#hdr-Custom_Collectors_and_constant_Metrics
}

func (m *Manager) deleteSampleTracker(userID string) {
	m.stmtx.Lock()
	delete(m.sampleTrackersByUserID, userID)
	m.stmtx.Unlock()
}

func (m *Manager) deleteActiveTracker(userID string) {
	m.atmtx.Lock()
	delete(m.activeTrackersByUserID, userID)
	m.atmtx.Unlock()
}

func (m *Manager) updateTracker(userID string) (*SampleTracker, *ActiveSeriesTracker) {
	if !m.enabledForUser(userID) {
		m.deleteSampleTracker(userID)
		m.deleteActiveTracker(userID)
		return nil, nil
	}

	st := m.SampleTracker(userID)
	at := m.ActiveSeriesTracker(userID)
	labels := m.labels(userID)

	// sort the labels to ensure the order is consistent
	slices.SortFunc(labels, func(a, b model.Label) int {
		return strings.Compare(a.Input, b.Input)
	})

	// if the labels have changed or the max cardinality or cooldown duration have changed, create a new tracker
	newMaxCardinality := m.limits.MaxCostAttributionCardinality(userID)
	newCooldownDuration := m.limits.CostAttributionCooldown(userID)

	if !st.hasSameLabels(labels) || st.maxCardinality != newMaxCardinality || st.cooldownDuration != newCooldownDuration {
		m.stmtx.Lock()
		st = newSampleTracker(userID, labels, newMaxCardinality, newCooldownDuration, m.logger)
		m.sampleTrackersByUserID[userID] = st
		m.stmtx.Unlock()
	}

	if !at.hasSameLabels(labels) || at.maxCardinality != newMaxCardinality || at.cooldownDuration != newCooldownDuration {
		m.atmtx.Lock()
		at = NewActiveSeriesTracker(userID, labels, newMaxCardinality, newCooldownDuration, m.logger)
		m.activeTrackersByUserID[userID] = at
		m.atmtx.Unlock()
	}

	return st, at
}

func (m *Manager) purgeInactiveAttributionsUntil(deadline time.Time) error {
	m.stmtx.RLock()
	userIDs := make([]string, 0, len(m.sampleTrackersByUserID))
	for userID := range m.sampleTrackersByUserID {
		userIDs = append(userIDs, userID)
	}
	m.stmtx.RUnlock()

	for _, userID := range userIDs {
		st, at := m.updateTracker(userID)
		if st == nil || at == nil {
			continue
		}

		st.cleanupInactiveObservations(deadline)

		// only sample tracker can recovered from overflow, the activeseries tracker after the cooldown would just be deleted and recreated
		if st.recoveredFromOverflow(deadline) {
			m.deleteSampleTracker(userID)
		}

		at.observedMtx.RLock()
		// if the activeseries tracker has been in overflow for more than the cooldown duration, delete it
		if !at.overflowSince.IsZero() && at.overflowSince.Add(at.cooldownDuration).Before(deadline) {
			at.observedMtx.RUnlock()
			m.deleteActiveTracker(userID)
		} else {
			at.observedMtx.RUnlock()
		}
	}
	return nil
}

var _ prometheus.Collector = (*costAttributionCollector)(nil)

// costAttributionCollector is a prometheus collector that collects cost attribution metrics.
// It collects metrics from methods that are explicit on their purpose: they are cost attribution metrics.
// This way it's clear which are the usual operational metrics and which ones are the cost attribution metrics.
type costAttributionCollector struct {
	ca interface {
		describeCostAttribution(chan<- *prometheus.Desc)
		collectCostAttribution(chan<- prometheus.Metric)
	}
}

func (c costAttributionCollector) Describe(descs chan<- *prometheus.Desc) {
	c.ca.describeCostAttribution(descs)
}

func (c costAttributionCollector) Collect(metrics chan<- prometheus.Metric) {
	c.ca.collectCostAttribution(metrics)
}
