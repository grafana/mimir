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
	cooldownTimeout time.Duration

	// mu protects the trackersByUserID map
	tlock            sync.RWMutex
	trackersByUserID map[string]*Tracker
}

// NewManager creates a new cost attribution manager. which is responsible for managing the cost attribution of series.
// It will clean up inactive series and update the cost attribution of series every 3 minutes.
func NewManager(cleanupInterval, inactiveTimeout time.Duration, cooldownTimeout time.Duration, logger log.Logger, limits *validation.Overrides) *Manager {
	s := &Manager{
		trackersByUserID: make(map[string]*Tracker),
		limits:           limits,
		tlock:            sync.RWMutex{},
		cooldownTimeout:  cooldownTimeout,
		inactiveTimeout:  inactiveTimeout,
		logger:           logger,
	}

	s.Service = services.NewTimerService(cleanupInterval, nil, s.iteration, nil).WithName("cost attribution manager")
	return s
}

func (m *Manager) iteration(_ context.Context) error {
	m.purgeInactiveAttributions(m.inactiveTimeout)
	return nil
}

// EnabledForUser returns true if the cost attribution is enabled for the user
func (m *Manager) EnabledForUser(userID string) bool {
	return len(m.limits.CostAttributionLabel(userID)) > 0
}

func (m *Manager) TrackerForUser(userID string) *Tracker {
	// if cost attribution is not enabled, return nil
	if !m.EnabledForUser(userID) {
		return nil
	}
	m.tlock.Lock()
	defer m.tlock.Unlock()

	// if not exists, create a new tracker
	if _, exists := m.trackersByUserID[userID]; !exists {
		m.trackersByUserID[userID], _ = newTracker(m.limits.CostAttributionLabel(userID), m.limits.MaxCostAttributionPerUser(userID))
	}
	return m.trackersByUserID[userID]
}

func (m *Manager) Collect(out chan<- prometheus.Metric) {
	m.tlock.RLock()
	defer m.tlock.RUnlock()
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
	m.tlock.Lock()
	defer m.tlock.Unlock()
	if _, exists := m.trackersByUserID[userID]; !exists {
		return
	}
	// clean up tracker metrics and delete the tracker
	m.trackersByUserID[userID].cleanupTracker(userID)
	delete(m.trackersByUserID, userID)
}

func (m *Manager) purgeInactiveAttributions(inactiveTimeout time.Duration) {

	// Get all userIDs from the map
	m.tlock.RLock()
	userIDs := make([]string, 0, len(m.trackersByUserID))
	for userID := range m.trackersByUserID {
		userIDs = append(userIDs, userID)
	}
	m.tlock.RUnlock()

	// Iterate over all userIDs and purge inactive attributions of each user
	currentTime := time.Now()
	for _, userID := range userIDs {
		// if cost attribution is not enabled for the user, delete the user tracker and continue
		if len(m.limits.CostAttributionLabel(userID)) == 0 || m.limits.MaxCostAttributionPerUser(userID) <= 0 {
			m.deleteUserTracer(userID)
			continue
		}
		// get all inactive attributions for the user and clean up the tracker
		inactiveObs := m.purgeInactiveObservationsForUser(userID, currentTime.Add(-inactiveTimeout).UnixNano())

		for _, ob := range inactiveObs {
			m.trackersByUserID[userID].cleanupTrackerAttribution(ob.lvalues)
		}
	}
}

// compare two sorted string slices
func compareStringSlice(a, b []string) bool {
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

func (m *Manager) purgeInactiveObservationsForUser(userID string, deadline int64) []*observation {
	cat := m.TrackerForUser(userID)
	if cat == nil {
		return nil
	}

	newTrackedLabels := sort.StringSlice(m.limits.CostAttributionLabel(userID))
	// if they are different, we need to update the tracker, we don't mind, just reinitalized the tracker
	if !compareStringSlice(cat.trackedLabels, newTrackedLabels) {
		m.tlock.Lock()
		m.trackersByUserID[userID], _ = newTracker(m.limits.CostAttributionLabel(userID), m.limits.MaxCostAttributionPerUser(userID))
		// update the tracker with the new tracker
		cat = m.trackersByUserID[userID]
		m.tlock.Unlock()
	} else if maxCardinality := m.limits.MaxCostAttributionPerUser(userID); cat.maxCardinality != maxCardinality {
		// if the maxCardinality is different, update the tracker
		cat.updateMaxCardinality(maxCardinality)
	}

	return cat.PurgeInactiveObservations(deadline)
}
