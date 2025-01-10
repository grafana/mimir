// SPDX-License-Identifier: AGPL-3.0-only

package costattribution

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"

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
	reg    *prometheus.Registry

	mstx                   sync.RWMutex
	sampleTrackersByUserID map[string]*SampleTracker
	inactiveTimeout        time.Duration
	cleanupInterval        time.Duration

	matx                   sync.RWMutex
	activeTrackersByUserID map[string]*ActiveSeriesTracker
}

func NewManager(cleanupInterval, inactiveTimeout time.Duration, logger log.Logger, limits *validation.Overrides, reg *prometheus.Registry) (*Manager, error) {
	m := &Manager{
		mstx:                   sync.RWMutex{},
		sampleTrackersByUserID: make(map[string]*SampleTracker),

		matx:                   sync.RWMutex{},
		activeTrackersByUserID: make(map[string]*ActiveSeriesTracker),

		limits:          limits,
		inactiveTimeout: inactiveTimeout,
		logger:          logger,
		reg:             reg,
		cleanupInterval: cleanupInterval,
	}

	m.Service = services.NewTimerService(cleanupInterval, nil, m.iteration, nil).WithName("cost attribution manager")
	if err := reg.Register(m); err != nil {
		return nil, err
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
	return len(m.limits.CostAttributionLabels(userID)) > 0
}

func (m *Manager) SampleTracker(userID string) *SampleTracker {
	if !m.enabledForUser(userID) {
		return nil
	}

	// Check if the tracker already exists, if exists return it. Otherwise lock and create a new tracker.
	m.mstx.RLock()
	tracker, exists := m.sampleTrackersByUserID[userID]
	m.mstx.RUnlock()
	if exists {
		return tracker
	}

	// We need to create a new tracker, get all the necessary information from the limits before locking and creating the tracker.
	labels := m.limits.CostAttributionLabels(userID)
	maxCardinality := m.limits.MaxCostAttributionCardinalityPerUser(userID)
	cooldownDuration := m.limits.CostAttributionCooldown(userID)

	m.mstx.Lock()
	defer m.mstx.Unlock()
	if tracker, exists = m.sampleTrackersByUserID[userID]; exists {
		return tracker
	}
	tracker = newSampleTracker(userID, labels, maxCardinality, cooldownDuration, m.logger)
	m.sampleTrackersByUserID[userID] = tracker
	return tracker
}

func (m *Manager) ActiveSeriesTracker(userID string) *ActiveSeriesTracker {
	if !m.enabledForUser(userID) {
		return nil
	}

	// Check if the tracker already exists, if exists return it. Otherwise lock and create a new tracker.
	m.matx.RLock()
	tracker, exists := m.activeTrackersByUserID[userID]
	m.matx.RUnlock()
	if exists {
		return tracker
	}

	// We need to create a new tracker, get all the necessary information from the limits before locking and creating the tracker.
	labels := m.limits.CostAttributionLabels(userID)
	maxCardinality := m.limits.MaxCostAttributionCardinalityPerUser(userID)
	cooldownDuration := m.limits.CostAttributionCooldown(userID)

	m.matx.Lock()
	defer m.matx.Unlock()
	if tracker, exists = m.activeTrackersByUserID[userID]; exists {
		return tracker
	}
	tracker = newActiveSeriesTracker(userID, labels, maxCardinality, cooldownDuration, m.logger)
	m.activeTrackersByUserID[userID] = tracker
	return tracker
}

func (m *Manager) Collect(out chan<- prometheus.Metric) {
	m.mstx.RLock()
	for _, tracker := range m.sampleTrackersByUserID {
		tracker.Collect(out)
	}
	m.mstx.RUnlock()

	m.matx.RLock()
	for _, tracker := range m.activeTrackersByUserID {
		tracker.Collect(out)
	}
	m.matx.RUnlock()
}

func (m *Manager) Describe(chan<- *prometheus.Desc) {
	// Describe is not implemented because the metrics include dynamic labels. The Manager functions as an unchecked exporter.
	// For more details, refer to the documentation: https://pkg.go.dev/github.com/prometheus/client_golang/prometheus#hdr-Custom_Collectors_and_constant_Metrics
}

func (m *Manager) deleteSampleTracker(userID string) {
	m.mstx.Lock()
	delete(m.sampleTrackersByUserID, userID)
	m.mstx.Unlock()
}

func (m *Manager) deleteActiveTracker(userID string) {
	m.matx.Lock()
	delete(m.activeTrackersByUserID, userID)
	m.matx.Unlock()
}

func (m *Manager) updateTracker(userID string) (*SampleTracker, *ActiveSeriesTracker) {
	if !m.enabledForUser(userID) {
		m.deleteSampleTracker(userID)
		m.deleteActiveTracker(userID)
		return nil, nil
	}

	st := m.SampleTracker(userID)
	at := m.ActiveSeriesTracker(userID)
	lbls := slices.Clone(m.limits.CostAttributionLabels(userID))

	// sort the labels to ensure the order is consistent
	slices.Sort(lbls)

	// if the labels have changed or the max cardinality or cooldown duration have changed, create a new tracker
	if !st.hasSameLabels(lbls) || st.maxCardinality != m.limits.MaxCostAttributionCardinalityPerUser(userID) || st.cooldownDuration != m.limits.CostAttributionCooldown(userID) {
		m.mstx.Lock()
		st = newSampleTracker(userID, lbls, m.limits.MaxCostAttributionCardinalityPerUser(userID), m.limits.CostAttributionCooldown(userID), m.logger)
		m.sampleTrackersByUserID[userID] = st
		m.mstx.Unlock()
	}

	if !at.hasSameLabels(lbls) || at.maxCardinality != m.limits.MaxCostAttributionCardinalityPerUser(userID) || at.cooldownDuration != m.limits.CostAttributionCooldown(userID) {
		m.matx.Lock()
		at = newActiveSeriesTracker(userID, lbls, m.limits.MaxCostAttributionCardinalityPerUser(userID), m.limits.CostAttributionCooldown(userID), m.logger)
		m.activeTrackersByUserID[userID] = at
		m.matx.Unlock()
	}

	return st, at
}

func (m *Manager) purgeInactiveAttributionsUntil(deadline time.Time) error {
	m.mstx.RLock()
	userIDs := make([]string, 0, len(m.sampleTrackersByUserID))
	for userID := range m.sampleTrackersByUserID {
		userIDs = append(userIDs, userID)
	}
	m.mstx.RUnlock()

	for _, userID := range userIDs {
		st, at := m.updateTracker(userID)
		if st == nil && at == nil {
			continue
		}

		invalidKeys := st.inactiveObservations(deadline)
		for _, key := range invalidKeys {
			st.cleanupTrackerAttribution(key)
		}

		// only sample tracker can recovered from overflow, the activeseries tracker after the cooldown would just be deleted and recreated
		if st.recoveredFromOverflow(deadline) {
			m.deleteSampleTracker(userID)
		}

		// if the activeseries tracker has been in overflow for more than the cooldown duration, delete it
		if at.overflowSince.Load() > 0 && time.Unix(at.overflowSince.Load(), 0).Add(at.cooldownDuration).Before(deadline) {
			m.deleteActiveTracker(userID)
		}
	}
	return nil
}
