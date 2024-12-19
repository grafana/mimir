// SPDX-License-Identifier: AGPL-3.0-only

package costattribution

import (
	"context"
	"sort"
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

	mtx                   sync.RWMutex
	trackersByUserID      map[string]*Tracker
	reg                   *prometheus.Registry
	cleanupInterval       time.Duration
	metricsExportInterval time.Duration
}

func NewManager(cleanupInterval, exportInterval, inactiveTimeout time.Duration, logger log.Logger, limits *validation.Overrides, reg *prometheus.Registry) (*Manager, error) {
	m := &Manager{
		trackersByUserID:      make(map[string]*Tracker),
		limits:                limits,
		mtx:                   sync.RWMutex{},
		inactiveTimeout:       inactiveTimeout,
		logger:                logger,
		reg:                   reg,
		cleanupInterval:       cleanupInterval,
		metricsExportInterval: exportInterval,
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

func (m *Manager) purgeInactiveAttributionsUntil(deadline int64) error {
	m.mtx.RLock()
	userIDs := make([]string, 0, len(m.trackersByUserID))
	for userID := range m.trackersByUserID {
		userIDs = append(userIDs, userID)
	}
	m.mtx.RUnlock()

	for _, userID := range userIDs {
		if !m.EnabledForUser(userID) {
			m.deleteTracker(userID)
			continue
		}

		invalidKeys := m.inactiveObservationsForUser(userID, deadline)
		cat := m.Tracker(userID)
		for _, key := range invalidKeys {
			cat.cleanupTrackerAttribution(key)
		}

		if cat != nil && cat.cooldownUntil != nil && cat.cooldownUntil.Load() < deadline {
			if len(cat.observed) <= cat.MaxCardinality() {
				cat.state = OverflowComplete
				m.deleteTracker(userID)
			} else {
				cat.cooldownUntil.Store(deadline + cat.cooldownDuration)
			}
		}
	}
	return nil
}

func (m *Manager) inactiveObservationsForUser(userID string, deadline int64) []string {
	cat := m.Tracker(userID)
	newTrackedLabels := m.limits.CostAttributionLabels(userID)
	sort.Slice(newTrackedLabels, func(i, j int) bool {
		return newTrackedLabels[i] < newTrackedLabels[j]
	})

	if !cat.CompareCALabels(newTrackedLabels) {
		m.mtx.Lock()
		cat = newTracker(userID, newTrackedLabels, m.limits.MaxCostAttributionCardinalityPerUser(userID), m.limits.CostAttributionCooldown(userID), m.logger)
		m.trackersByUserID[userID] = cat
		m.mtx.Unlock()
		return nil
	}
	maxCardinality := m.limits.MaxCostAttributionCardinalityPerUser(userID)
	if cat.MaxCardinality() != maxCardinality {
		cat.UpdateMaxCardinality(maxCardinality)
	}

	cooldown := int64(m.limits.CostAttributionCooldown(userID).Seconds())
	if cooldown != cat.CooldownDuration() {
		cat.UpdateCooldownDuration(cooldown)
	}

	return cat.InactiveObservations(deadline)
}
