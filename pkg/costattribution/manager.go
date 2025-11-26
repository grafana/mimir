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
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/costattribution/costattributionmodel"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	trackerLabel        = "tracker"
	tenantLabel         = "tenant"
	reasonLabel         = "reason"
	defaultTrackerName  = "cost-attribution"
	missingValue        = "__missing__"
	overflowValue       = "__overflow__"
	activeSeriesTracker = "active-series"
	samplesTracker      = "samples"
)

type Manager struct {
	services.Service
	logger log.Logger
	limits *validation.Overrides

	sampleTrackerCardinalityDesc       *descriptor
	sampleTrackerOverflowDesc          *descriptor
	activeSeriesTrackerCardinalityDesc *descriptor
	activeSeriesTrackerOverflowDesc    *descriptor
	trackerCreationErrors              *prometheus.CounterVec

	inactiveTimeout time.Duration

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

		trackerCreationErrors: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_cost_attribution_tracker_creation_errors_total",
			Help: "The total number of errors creating cost attribution trackers for each user.",
		}, []string{"user", trackerLabel}),

		limits:          limits,
		inactiveTimeout: inactiveTimeout,
		logger:          logger,
	}

	if err := m.createAndValidateDescriptors(); err != nil {
		return nil, err
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

func (m *Manager) createAndValidateDescriptors() error {
	var err error
	if m.sampleTrackerCardinalityDesc, err = newDescriptor("cortex_cost_attribution_sample_tracker_cardinality",
		"The cardinality of a cost attribution sample tracker for each user.",
		[]string{"user"},
		prometheus.Labels{trackerLabel: defaultTrackerName}); err != nil {
		return err
	}
	if m.sampleTrackerOverflowDesc, err = newDescriptor("cortex_cost_attribution_sample_tracker_overflown",
		"This metric is exported with value 1 when a sample tracker for a user is overflown. It's not exported otherwise.",
		[]string{"user"},
		prometheus.Labels{trackerLabel: defaultTrackerName}); err != nil {
		return err
	}
	if m.activeSeriesTrackerCardinalityDesc, err = newDescriptor("cortex_cost_attribution_active_series_tracker_cardinality",
		"The cardinality of a cost attribution active series tracker for each user.",
		[]string{"user"},
		prometheus.Labels{trackerLabel: defaultTrackerName}); err != nil {
		return err
	}
	if m.activeSeriesTrackerOverflowDesc, err = newDescriptor("cortex_cost_attribution_active_series_tracker_overflown",
		"This metric is exported with value 1 when an active series tracker for a user is overflown. It's not exported otherwise.",
		[]string{"user"},
		prometheus.Labels{trackerLabel: defaultTrackerName}); err != nil {
		return err
	}
	return nil
}

func (m *Manager) iteration(_ context.Context) error {
	m.purgeInactiveAttributionsUntil(time.Now())
	return nil
}

func (m *Manager) enabledForUser(userID string) bool {
	if m == nil {
		return false
	}

	return len(m.limits.CostAttributionLabelsStructured(userID)) > 0
}

func (m *Manager) labels(userID string) costattributionmodel.Labels {
	return m.limits.CostAttributionLabelsStructured(userID)
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
	slices.SortFunc(labels, func(a, b costattributionmodel.Label) int {
		return strings.Compare(a.Input, b.Input)
	})

	tracker, err := newSampleTracker(userID, labels, maxCardinality, cooldownDuration, m.logger)
	if err != nil {
		m.trackerCreationErrors.WithLabelValues(userID, samplesTracker).Inc()
		return nil
	}
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
	slices.SortFunc(labels, func(a, b costattributionmodel.Label) int {
		return strings.Compare(a.Input, b.Input)
	})

	tracker, err := NewActiveSeriesTracker(userID, labels, maxCardinality, cooldownDuration, m.logger)
	if err != nil {
		m.trackerCreationErrors.WithLabelValues(userID, activeSeriesTracker).Inc()
		return nil
	}
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

		out <- m.sampleTrackerCardinalityDesc.gauge(float64(cardinality), tracker.userID)
		if overflown {
			out <- m.sampleTrackerOverflowDesc.gauge(1, tracker.userID)
		}
	}

	for _, tracker := range activeTrackersByUserID {
		cardinality, overflown := tracker.cardinality()

		out <- m.activeSeriesTrackerCardinalityDesc.gauge(float64(cardinality), tracker.userID)
		if overflown {
			out <- m.activeSeriesTrackerOverflowDesc.gauge(1, tracker.userID)
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

	if at == nil || st == nil {
		return nil, nil
	}

	labels := m.labels(userID)

	// sort the labels to ensure the order is consistent
	slices.SortFunc(labels, func(a, b costattributionmodel.Label) int {
		return strings.Compare(a.Input, b.Input)
	})

	// if the labels have changed or the max cardinality or cooldown duration have changed, create a new tracker
	newMaxCardinality := m.limits.MaxCostAttributionCardinality(userID)
	newCooldownDuration := m.limits.CostAttributionCooldown(userID)

	if !st.hasSameLabels(labels) || st.maxCardinality != newMaxCardinality || st.cooldownDuration != newCooldownDuration {
		m.stmtx.Lock()
		var err error
		st, err = newSampleTracker(userID, labels, newMaxCardinality, newCooldownDuration, m.logger)
		if err != nil {
			m.trackerCreationErrors.WithLabelValues(userID, samplesTracker).Inc()
			delete(m.sampleTrackersByUserID, userID)
		} else {
			m.sampleTrackersByUserID[userID] = st
		}
		m.stmtx.Unlock()
	}

	if !at.hasSameLabels(labels) || at.maxCardinality != newMaxCardinality || at.cooldownDuration != newCooldownDuration {
		m.atmtx.Lock()
		var err error
		at, err = NewActiveSeriesTracker(userID, labels, newMaxCardinality, newCooldownDuration, m.logger)
		if err != nil {
			m.trackerCreationErrors.WithLabelValues(userID, activeSeriesTracker).Inc()
			delete(m.activeTrackersByUserID, userID)
		} else {
			m.activeTrackersByUserID[userID] = at
		}
		m.atmtx.Unlock()
	}

	return st, at
}

func (m *Manager) purgeInactiveAttributionsUntil(now time.Time) {
	deadline := now.Add(-m.inactiveTimeout)
	m.stmtx.RLock()
	userIDs := make(map[string]struct{}, len(m.sampleTrackersByUserID)+len(m.activeTrackersByUserID))
	for userID := range m.sampleTrackersByUserID {
		userIDs[userID] = struct{}{}
	}
	for userID := range m.activeTrackersByUserID {
		userIDs[userID] = struct{}{}
	}
	m.stmtx.RUnlock()

	for userID := range userIDs {
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
		// If the activeseries tracker has been in overflow for more than the cooldown duration,
		// check if it recovered. If it recovered, delete the tracker to reset its state (and account the overflown series correctly),
		// If it didn't recover, reset the overflowSince to now to start a new cooldown period.
		isOverflowedAndShouldCheck := !at.overflowSince.IsZero() && at.overflowSince.Add(at.cooldownDuration).Before(deadline)

		// NOTE: If there are still series in the overflow counter we can't tell how much cardinality those add.
		// The best we can say is "it's at least 1", so we just ignore here (with a limit set to thousands, doing this plus one won't change much).
		// If we wanted to be more accurate, we should do "(len(at.observed) + (at.overflowCounter.activeSeries.Load() > 0 ? 1 : 0)) < at.maxCardinality".
		recovered := len(at.observed) <= at.maxCardinality
		at.observedMtx.RUnlock()

		if isOverflowedAndShouldCheck {
			if recovered {
				m.deleteActiveTracker(userID)
			} else {
				at.observedMtx.Lock()
				at.overflowSince = now
				at.observedMtx.Unlock()
			}
		}
	}
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
