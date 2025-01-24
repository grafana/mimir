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
	limits          *validation.Overrides
	reg             *prometheus.Registry
	inactiveTimeout time.Duration
	cleanupInterval time.Duration

	stmtx                  sync.RWMutex
	sampleTrackersByUserID map[string]*SampleTrackers

	atmtx                  sync.RWMutex
	activeTrackersByUserID map[string]*ActiveSeriesTrackers
}

func NewManager(cleanupInterval, inactiveTimeout time.Duration, logger log.Logger, limits *validation.Overrides, reg *prometheus.Registry) (*Manager, error) {
	m := &Manager{
		stmtx:                  sync.RWMutex{},
		sampleTrackersByUserID: make(map[string]*SampleTrackers),

		atmtx:                  sync.RWMutex{},
		activeTrackersByUserID: make(map[string]*ActiveSeriesTrackers),

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
	return len(m.limits.CostAttributionTrackers(userID)) > 0
}

// SampleTracker returns the SampleTrackers for the given userID. If the user is not enabled for cost attribution, it returns nil.
func (m *Manager) SampleTracker(userID string) *SampleTrackers {
	if !m.enabledForUser(userID) {
		return nil
	}

	// Check if the tracker already exists, if exists return it. Otherwise lock and create a new tracker.
	m.stmtx.RLock()
	simpleTrackers, exists := m.sampleTrackersByUserID[userID]
	m.stmtx.RUnlock()
	if exists {
		return simpleTrackers
	}

	// We need to create a new tracker, get all the necessary information from the limits before locking and creating the tracker.
	tksCfg := m.limits.CostAttributionTrackers(userID)
	maxCardinality := m.limits.MaxCostAttributionCardinalityPerUser(userID)
	cooldownDuration := m.limits.CostAttributionCooldown(userID)

	m.stmtx.Lock()
	defer m.stmtx.Unlock()
	if simpleTrackers, exists = m.sampleTrackersByUserID[userID]; exists {
		return simpleTrackers
	}

	m.sampleTrackersByUserID[userID] = newSampleTrackers(userID, maxCardinality, cooldownDuration, m.logger, tksCfg)
	return simpleTrackers
}

func (m *Manager) ActiveSeriesTracker(userID string) *ActiveSeriesTrackers {
	if !m.enabledForUser(userID) {
		return nil
	}

	// Check if the tracker already exists, if exists return it. Otherwise lock and create a new tracker.
	m.atmtx.RLock()
	acts, exists := m.activeTrackersByUserID[userID]
	m.atmtx.RUnlock()
	if exists {
		return acts
	}

	// We need to create a new tracker, get all the necessary information from the limits before locking and creating the tracker.
	tks := m.limits.CostAttributionTrackers(userID)
	maxCardinality := m.limits.MaxCostAttributionCardinalityPerUser(userID)
	cooldownDuration := m.limits.CostAttributionCooldown(userID)

	m.atmtx.Lock()
	defer m.atmtx.Unlock()
	if acts, exists = m.activeTrackersByUserID[userID]; exists {
		return acts
	}

	m.activeTrackersByUserID[userID] = newActiveSeriesTrackers(userID, maxCardinality, cooldownDuration, m.logger, tks)
	return acts
}

func (m *Manager) Collect(out chan<- prometheus.Metric) {
	m.stmtx.RLock()
	for _, tracker := range m.sampleTrackersByUserID {
		for _, t := range tracker.trackers {
			t.Collect(out)
		}
	}
	m.stmtx.RUnlock()

	m.atmtx.RLock()
	for _, tracker := range m.activeTrackersByUserID {
		for _, t := range tracker.trackers {
			t.Collect(out)
		}
	}
	m.atmtx.RUnlock()
}

func (m *Manager) Describe(chan<- *prometheus.Desc) {
	// Describe is not implemented because the metrics include dynamic labels. The Manager functions as an unchecked exporter.
	// For more details, refer to the documentation: https://pkg.go.dev/github.com/prometheus/client_golang/prometheus#hdr-Custom_Collectors_and_constant_Metrics
}

func (m *Manager) deleteSampleTrackerByTrackerName(userID, trackerName string) {
	m.stmtx.RLock()
	tk := m.sampleTrackersByUserID[userID]
	tk.mtx.Lock()
	delete(tk.trackers, trackerName)
	tk.mtx.Unlock()
	m.stmtx.RUnlock()
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

func (m *Manager) updateTrackers(userID string) (*SampleTrackers, *ActiveSeriesTrackers) {
	if !m.enabledForUser(userID) {
		m.deleteSampleTracker(userID)
		m.deleteActiveTracker(userID)
		return nil, nil
	}

	st := m.SampleTracker(userID)
	at := m.ActiveSeriesTracker(userID)
	tks := m.limits.CostAttributionTrackers(userID)

	newMaxCardinality := m.limits.MaxCostAttributionCardinalityPerUser(userID)
	newCooldownDuration := m.limits.CostAttributionCooldown(userID)

	// for the active series tracker, we would reinitialize it as soon as one of the trackers has changed, since we need to inform active stripe to recompute the active series
	shouldReinit := false
	for _, tk := range tks {
		lbls := slices.Clone(tk.Labels)
		slices.Sort(lbls)
		if !st.hasSameLabels(tk.Name, lbls) {
			shouldReinit = true
		}
	}

	if shouldReinit || at.maxCardinality != newMaxCardinality || st.cooldownDuration != newCooldownDuration {
		m.atmtx.Lock()
		m.activeTrackersByUserID[userID] = newActiveSeriesTrackers(userID, newMaxCardinality, newCooldownDuration, m.logger, tks)
		m.atmtx.Unlock()
	}

	// when the max cardinality or cooldown duration has changed, create new trackers for user, delete the old ones
	if st.maxCardinality != newMaxCardinality || st.cooldownDuration != newCooldownDuration {
		m.stmtx.Lock()
		st = newSampleTrackers(userID, newMaxCardinality, newCooldownDuration, m.logger, tks)
		m.sampleTrackersByUserID[userID] = st
		m.stmtx.Unlock()
	}

	// check each tracker of the user to see if the labels have changed, if so create a new tracker
	for _, tk := range tks {
		lbls := slices.Clone(tk.Labels)
		// sort the labels to ensure the order is consistent
		slices.Sort(lbls)

		// if the labels have changed or the max cardinality or cooldown duration have changed, create a new tracker
		if !st.hasSameLabels(tk.Name, lbls) {
			m.stmtx.RLock()
			str := m.sampleTrackersByUserID[userID]
			m.stmtx.RUnlock()

			str.mtx.Lock()
			str.trackers[tk.Name] = newSampleTracker(userID, lbls, m.logger)
			str.mtx.Unlock()
		}
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
		sts, ats := m.updateTrackers(userID)
		if sts == nil || ats == nil {
			continue
		}

		sts.cleanupInactiveObservations(deadline)

		// only sample tracker can recovered from overflow, the activeseries tracker after the cooldown would just be deleted and recreated
		sts.mtx.RLock()
		for name, st := range sts.trackers {
			if st.recoveredFromOverflow(deadline) {
				m.deleteSampleTrackerByTrackerName(userID, name)
			}
		}
		sts.mtx.RUnlock()

		for _, at := range ats.trackers {
			at.observedMtx.RLock()
			// if the activeseries tracker has been in overflow for more than the cooldown duration, delete entire active series tracker
			if !at.overflowSince.IsZero() && at.overflowSince.Add(ats.cooldownDuration).Before(deadline) {
				at.observedMtx.RUnlock()
				m.deleteActiveTracker(userID)
				break
			} else {
				at.observedMtx.RUnlock()
			}
		}
	}
	return nil
}
