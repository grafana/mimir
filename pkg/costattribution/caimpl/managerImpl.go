package caimpl

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/util/validation"
)

type ManagerImpl struct {
	services.Service
	logger             log.Logger
	attributionTracker *AttributionTrackerGroup
	inactiveTimeout    time.Duration
	invalidValue       string
}

// NewManager creates a new cost attribution manager. which is responsible for managing the cost attribution of series.
// It will clean up inactive series and update the cost attribution of series every 3 minutes.
func NewManager(cleanupInterval, inactiveTimeout time.Duration, cooldownTimeout time.Duration, logger log.Logger, limits *validation.Overrides) *ManagerImpl {
	s := &ManagerImpl{
		attributionTracker: newAttributionTrackerGroup(limits, cooldownTimeout),
		inactiveTimeout:    inactiveTimeout,
		logger:             logger,
		invalidValue:       "__unaccounted__",
	}

	s.Service = services.NewTimerService(cleanupInterval, nil, s.iteration, nil).WithName("cost attribution manager")
	return s
}

func (m *ManagerImpl) iteration(_ context.Context) error {
	m.attributionTracker.purgeInactiveAttributions(m.inactiveTimeout)
	return nil
}

// EnabledForUser returns true if the cost attribution is enabled for the user
func (m *ManagerImpl) EnabledForUser(userID string) bool {
	return m.attributionTracker.limits.CostAttributionLabel(userID) != ""
}

// GetUserAttributionLabel returns the cost attribution label for the user, first it will try to get the label from the cache,
// If not found, it will get the label from the config
// If the user is not enabled for cost attribution, it would clean the cache and return empty string
func (m *ManagerImpl) GetUserAttributionLabel(userID string) string {
	if m.EnabledForUser(userID) {
		return m.attributionTracker.getUserAttributionLabelFromCache(userID)
	}
	m.attributionTracker.deleteUserTracerFromCache(userID)
	return ""
}

// GetUserAttributionLimit returns the cost attribution limit for the user, first it will try to get the limit from the cache,
// If not found, it will get the limit from the config
// If the user is not enabled for cost attribution, it would clean the cache and return 0
func (m *ManagerImpl) GetUserAttributionLimit(userID string) int {
	if m.EnabledForUser(userID) {
		return m.attributionTracker.getUserAttributionLimitFromCache(userID)
	}
	m.attributionTracker.deleteUserTracerFromCache(userID)
	return 0
}

func (m *ManagerImpl) UpdateAttributionTimestamp(user string, lbs labels.Labels, now time.Time) string {
	// if cost attribution is not enabled for the user, return empty string
	if !m.EnabledForUser(user) {
		m.attributionTracker.deleteUserTracerFromCache(user)
		return ""
	}

	// when cost attribution is enabled, the label has to be set. the cache would be updated with the label
	lb := m.attributionTracker.getUserAttributionLabelFromCache(user)
	// this should not happened, if user is enabled for cost attribution, the label has to be set
	if lb == "" {
		return ""
	}
	val := lbs.Get(lb)

	if m.attributionTracker.attributionLimitExceeded(user, val, now) {
		val = m.invalidValue
		level.Error(m.logger).Log("msg", fmt.Sprintf("set attribution label to \"%s\" since user has reached the limit of cost attribution labels", m.invalidValue))
	}
	m.attributionTracker.updateAttributionCacheForUser(user, lb, val, now)
	return val
}

// SetActiveSeries adjust the input attribution and sets the active series gauge for the given user and attribution
func (m *ManagerImpl) SetActiveSeries(userID, attribution string, value float64) {
	attribution = m.adjustUserAttribution(userID, attribution)

	m.attributionTracker.mu.Lock()
	defer m.attributionTracker.mu.Unlock()
	if tracker, exists := m.attributionTracker.trackersByUserID[userID]; exists {
		tracker.activeSeriesPerUserAttribution.WithLabelValues(userID, attribution).Set(value)
	}
}

// IncrementDiscardedSamples increments the discarded samples counter for a given user and attribution
func (m *ManagerImpl) IncrementDiscardedSamples(userID, attribution string, value float64) {
	attribution = m.adjustUserAttribution(userID, attribution)
	m.attributionTracker.mu.RLock()
	defer m.attributionTracker.mu.RUnlock()
	if tracker, exists := m.attributionTracker.trackersByUserID[userID]; exists {
		tracker.discardedSampleAttribution.WithLabelValues(userID, attribution).Add(value)
	}
}

// IncrementReceivedSamples increments the received samples counter for a given user and attribution
func (m *ManagerImpl) IncrementReceivedSamples(userID, attribution string, value float64) {
	attribution = m.adjustUserAttribution(userID, attribution)
	m.attributionTracker.mu.RLock()
	defer m.attributionTracker.mu.RUnlock()
	if tracker, exists := m.attributionTracker.trackersByUserID[userID]; exists {
		tracker.receivedSamplesAttribution.WithLabelValues(userID, attribution).Add(value)
	}
}

func (m *ManagerImpl) adjustUserAttribution(userID, attribution string) string {
	if m.attributionTracker.attributionLimitExceeded(userID, attribution, time.Now()) {
		return m.invalidValue
	}
	return attribution
}

func (m *ManagerImpl) Collect(out chan<- prometheus.Metric) {
	m.attributionTracker.mu.RLock()
	defer m.attributionTracker.mu.RUnlock()
	for _, tracker := range m.attributionTracker.trackersByUserID {
		tracker.Collect(out)
	}
}

// Describe implements prometheus.Collector.
func (m *ManagerImpl) Describe(chan<- *prometheus.Desc) {
	// this is an unchecked collector
}
