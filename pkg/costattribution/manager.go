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
	missingValue  = "__missing__"
	overflowValue = "__overflow__"
)

type Manager struct {
	services.Service
	logger          log.Logger
	inactiveTimeout time.Duration
	limits          *validation.Overrides

	// mu protects the trackersByUserID map
	mtx              sync.RWMutex
	trackersByUserID map[string]*Tracker
	reg              *prometheus.Registry
	cleanupInterval  time.Duration
}

// NewManager creates a new cost attribution manager. which is responsible for managing the cost attribution of series.
// It will clean up inactive series and update the cost attribution of series every 3 minutes.
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

	m.Service = services.NewBasicService(nil, m.running, nil).WithName("cost attribution manager")
	if err := reg.Register(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *Manager) running(ctx context.Context) error {
	if m == nil {
		return nil
	}
	t := time.NewTicker(m.cleanupInterval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			err := m.purgeInactiveAttributionsUntil(time.Now().Add(-m.inactiveTimeout).Unix())
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		}
	}
}

// EnabledForUser returns true if the cost attribution is enabled for the user
func (m *Manager) EnabledForUser(userID string) bool {
	if m == nil {
		return false
	}
	return len(m.limits.CostAttributionLabels(userID)) > 0
}

func (m *Manager) TrackerForUser(userID string) *Tracker {
	// if manager is not initialized or cost attribution is not enabled, return nil
	if m == nil || !m.EnabledForUser(userID) {
		return nil
	}

	m.mtx.Lock()
	defer m.mtx.Unlock()

	// if not exists, create a new tracker
	if _, exists := m.trackersByUserID[userID]; !exists {
		m.trackersByUserID[userID], _ = newTracker(userID, m.limits.CostAttributionLabels(userID), m.limits.MaxCostAttributionCardinalityPerUser(userID), m.limits.CostAttributionCooldown(userID), m.logger)
	}
	return m.trackersByUserID[userID]
}

func (m *Manager) Collect(out chan<- prometheus.Metric) {
	if m == nil {
		return
	}
	m.mtx.RLock()
	defer m.mtx.RUnlock()
	for _, tracker := range m.trackersByUserID {
		tracker.Collect(out)
	}
}

// Describe implements prometheus.Collector.
func (m *Manager) Describe(chan<- *prometheus.Desc) {
	// this is an unchecked collector
}

// deleteUserTracer is delete user tracker since the user is disabled for cost attribution
func (m *Manager) deleteUserTracer(userID string) {
	if m == nil {
		return
	}
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if _, exists := m.trackersByUserID[userID]; exists {
		// clean up tracker metrics and delete the tracker
		m.trackersByUserID[userID].cleanupTracker()
		delete(m.trackersByUserID, userID)
	}
}

func (m *Manager) purgeInactiveAttributionsUntil(deadline int64) error {
	if m == nil {
		return nil
	}
	// Get all userIDs from the map
	m.mtx.RLock()
	userIDs := make([]string, 0, len(m.trackersByUserID))
	for userID := range m.trackersByUserID {
		userIDs = append(userIDs, userID)
	}
	m.mtx.RUnlock()

	// Iterate over all userIDs and purge inactive attributions of each user
	for _, userID := range userIDs {
		// if cost attribution is not enabled for the user, delete the user tracker and continue
		if !m.EnabledForUser(userID) {
			m.deleteUserTracer(userID)
			continue
		}

		// get all inactive attributions for the user and clean up the tracker
		invalidKeys := m.getInactiveObservationsForUser(userID, deadline)

		cat := m.TrackerForUser(userID)
		for _, key := range invalidKeys {
			cat.cleanupTrackerAttribution(key)
		}

		// if the tracker is no longer overflowed, and it is currently in overflow state, check the cooldown and create new tracker
		if cat != nil && cat.cooldownUntil != nil && cat.cooldownUntil.Load() < deadline {
			if len(cat.observed) <= cat.MaxCardinality() {
				m.deleteUserTracer(userID)
			} else {
				cat.cooldownUntil.Store(deadline + cat.cooldownDuration)
			}
		}
	}
	return nil
}

// compare two sorted string slices
// true if they are equal, otherwise false
func CompareCALabels(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func (m *Manager) getInactiveObservationsForUser(userID string, deadline int64) []string {
	cat := m.TrackerForUser(userID)
	if cat == nil {
		return nil
	}

	newTrackedLabels := m.limits.CostAttributionLabels(userID)
	sort.Slice(newTrackedLabels, func(i, j int) bool {
		return newTrackedLabels[i] < newTrackedLabels[j]
	})

	// if they are different, we need to update the tracker, we don't mind, just reinitialized the tracker
	if !CompareCALabels(cat.CALabels(), newTrackedLabels) {
		m.mtx.Lock()
		m.trackersByUserID[userID], _ = newTracker(userID, m.limits.CostAttributionLabels(userID), m.limits.MaxCostAttributionCardinalityPerUser(userID), m.limits.CostAttributionCooldown(userID), m.logger)
		// update the tracker with the new tracker
		cat = m.trackersByUserID[userID]
		m.mtx.Unlock()
	} else {
		maxCardinality := m.limits.MaxCostAttributionCardinalityPerUser(userID)
		if cat.MaxCardinality() != maxCardinality {
			cat.UpdateMaxCardinality(maxCardinality)
		}

		cooldown := int64(m.limits.CostAttributionCooldown(userID).Seconds())
		if cooldown != cat.CooldownDuration() {
			cat.UpdateCooldownDuration(cooldown)
		}
	}

	return cat.GetInactiveObservations(deadline)
}
