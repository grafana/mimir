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
	logger          log.Logger
	inactiveTimeout time.Duration
	limits          *validation.Overrides

	mtx              sync.RWMutex
	trackersByUserID map[string]*Tracker
	reg              *prometheus.Registry
	cleanupInterval  time.Duration
}

func NewManager(cleanupInterval, inactiveTimeout time.Duration, logger log.Logger, limits *validation.Overrides, reg *prometheus.Registry) (*Manager, error) {
	m := &Manager{
		trackersByUserID: make(map[string]*Tracker),
		limits:           limits,
		mtx:              sync.RWMutex{},
		inactiveTimeout:  inactiveTimeout,
		logger:           logger,
		reg:              reg,
		cleanupInterval:  cleanupInterval,
	}

	m.Service = services.NewTimerService(cleanupInterval, nil, m.iteration, nil).WithName("cost attribution manager")
	if err := reg.Register(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *Manager) iteration(_ context.Context) error {
	return m.purgeInactiveAttributionsUntil(time.Now().Add(-m.inactiveTimeout).Unix())
}

func (m *Manager) EnabledForUser(userID string) bool {
	if m == nil {
		return false
	}
	return len(m.limits.CostAttributionLabels(userID)) > 0
}

func (m *Manager) Tracker(userID string) *Tracker {
	if !m.EnabledForUser(userID) {
		return nil
	}

	// Check if the tracker already exists, if exists return it. Otherwise lock and create a new tracker.
	m.mtx.RLock()
	tracker, exists := m.trackersByUserID[userID]
	m.mtx.RUnlock()
	if exists {
		return tracker
	}

	m.mtx.Lock()
	defer m.mtx.Unlock()
	if tracker, exists = m.trackersByUserID[userID]; exists {
		return tracker
	}
	tracker = newTracker(userID, m.limits.CostAttributionLabels(userID), m.limits.MaxCostAttributionCardinalityPerUser(userID), m.limits.CostAttributionCooldown(userID), m.logger)
	m.trackersByUserID[userID] = tracker
	return tracker
}

func (m *Manager) Collect(out chan<- prometheus.Metric) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()
	for _, tracker := range m.trackersByUserID {
		tracker.Collect(out)
	}
}

func (m *Manager) Describe(chan<- *prometheus.Desc) {
	// Describe is not implemented because the metrics include dynamic labels. The Manager functions as an unchecked exporter.
	// For more details, refer to the documentation: https://pkg.go.dev/github.com/prometheus/client_golang/prometheus#hdr-Custom_Collectors_and_constant_Metrics
}

func (m *Manager) deleteTracker(userID string) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	delete(m.trackersByUserID, userID)
}

func (m *Manager) updateTracker(userID string) *Tracker {
	if !m.EnabledForUser(userID) {
		m.deleteTracker(userID)
		return nil
	}

	t := m.Tracker(userID)

	lbls := m.limits.CostAttributionLabels(userID)

	newTrackedLabels := make([]string, len(lbls))
	copy(newTrackedLabels, lbls)

	// sort the labels to ensure the order is consistent
	slices.Sort(newTrackedLabels)

	// if the labels have changed or the max cardinality or cooldown duration have changed, create a new tracker
	if !t.hasSameLabels(newTrackedLabels) || t.maxCardinality != m.limits.MaxCostAttributionCardinalityPerUser(userID) || t.cooldownDuration != int64(m.limits.CostAttributionCooldown(userID).Seconds()) {
		m.mtx.Lock()
		t = newTracker(userID, newTrackedLabels, m.limits.MaxCostAttributionCardinalityPerUser(userID), m.limits.CostAttributionCooldown(userID), m.logger)
		m.trackersByUserID[userID] = t
		m.mtx.Unlock()
		return t
	}

	return t
}

func (m *Manager) purgeInactiveAttributionsUntil(deadline int64) error {
	m.mtx.RLock()
	userIDs := make([]string, 0, len(m.trackersByUserID))
	for userID := range m.trackersByUserID {
		userIDs = append(userIDs, userID)
	}
	m.mtx.RUnlock()

	for _, userID := range userIDs {
		t := m.updateTracker(userID)
		if t == nil {
			continue
		}

		invalidKeys := t.inactiveObservations(deadline)
		for _, key := range invalidKeys {
			t.cleanupTrackerAttribution(key)
		}

		if t.recoveredFromOverflow(deadline) {
			m.deleteTracker(userID)
		}
	}
	return nil
}
