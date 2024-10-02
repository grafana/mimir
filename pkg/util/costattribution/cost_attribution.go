// SPDX-License-Identifier: AGPL-3.0-only

package costattribution

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/grafana/mimir/pkg/util/validation"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"go.uber.org/atomic"
)

type Tracker struct {
	trackedLabel                   string
	activeSeriesPerUserAttribution *prometheus.GaugeVec
	receivedSamplesAttribution     *prometheus.CounterVec
	discardedSampleAttribution     *prometheus.CounterVec
	attributionTimestamps          map[string]*atomic.Int64
	coolDownDeadline               *atomic.Int64
}

func (m *Tracker) RemoveAttributionMetricsForUser(userID, attribution string) {
	m.activeSeriesPerUserAttribution.DeleteLabelValues(userID, attribution)
	m.receivedSamplesAttribution.DeleteLabelValues(userID, attribution)
	m.discardedSampleAttribution.DeleteLabelValues(userID, attribution)
}

func NewCostAttributionTracker(reg prometheus.Registerer, trackedLabel string) *Tracker {
	m := &Tracker{
		trackedLabel:          trackedLabel,
		attributionTimestamps: map[string]*atomic.Int64{},
		coolDownDeadline:      atomic.NewInt64(0),
		discardedSampleAttribution: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_discarded_samples_attribution_total",
			Help: "The total number of samples that were discarded per attribution.",
		}, []string{"user", trackedLabel}),
		receivedSamplesAttribution: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_received_samples_attribution_total",
			Help: "The total number of samples that were received per attribution.",
		}, []string{"user", trackedLabel}),
		activeSeriesPerUserAttribution: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_ingester_active_series_attribution",
			Help: "The total number of active series per user and attribution.",
		}, []string{"user", trackedLabel}),
	}
	return m
}

type CostAttribution struct {
	mu       sync.RWMutex
	trackers map[string]*Tracker
	limits   *validation.Overrides
	reg      prometheus.Registerer
}

func NewCostAttribution(limits *validation.Overrides, reg prometheus.Registerer) *CostAttribution {
	return &CostAttribution{
		trackers: make(map[string]*Tracker),
		limits:   limits,
		reg:      reg,
		mu:       sync.RWMutex{},
	}
}

// UpdateAttributionTimestampForUser function is only guaranteed to update to the
// timestamp provided even if it is smaller than the existing value
func (ca *CostAttribution) UpdateAttributionTimestampForUser(userID, attribution string, now time.Time) {
	// If the limit is set to 0, we don't need to track the attribution
	if ca.limits.MaxCostAttributionPerUser(userID) <= 0 {
		return
	}

	ts := now.UnixNano()
	ca.mu.Lock()
	// create new tracker if not exists
	if _, exists := ca.trackers[userID]; !exists {
		// the attribution label and values should be managed by cache
		ca.trackers[userID] = NewCostAttributionTracker(ca.reg, ca.limits.CostAttributionLabel(userID))
	}
	ca.mu.Unlock()
	ca.mu.RLock()
	if groupTs := ca.trackers[userID].attributionTimestamps[attribution]; groupTs != nil {
		groupTs.Store(ts)
		return
	}
	ca.mu.RUnlock()
	ca.mu.Lock()
	defer ca.mu.Unlock()
	ca.trackers[userID].attributionTimestamps[attribution] = atomic.NewInt64(ts)
}

func (ca *CostAttribution) purgeInactiveAttributionsForUser(userID string, deadline int64) []string {
	ca.mu.RLock()
	var inactiveAttributions []string
	if ca.trackers[userID] == nil || ca.trackers[userID].attributionTimestamps == nil {
		return nil
	}

	attributionTimestamps := ca.trackers[userID].attributionTimestamps
	for attr, ts := range attributionTimestamps {
		if ts.Load() <= deadline {
			inactiveAttributions = append(inactiveAttributions, attr)
		}
	}
	ca.mu.RUnlock()

	if len(inactiveAttributions) == 0 {
		return nil
	}

	// Cleanup inactive groups
	ca.mu.Lock()
	defer ca.mu.Unlock()

	for i := 0; i < len(inactiveAttributions); {
		inactiveAttribution := inactiveAttributions[i]
		groupTs := ca.trackers[userID].attributionTimestamps[inactiveAttribution]
		if groupTs != nil && groupTs.Load() <= deadline {
			delete(ca.trackers[userID].attributionTimestamps, inactiveAttribution)
			i++
		} else {
			inactiveAttributions[i] = inactiveAttributions[len(inactiveAttributions)-1]
			inactiveAttributions = inactiveAttributions[:len(inactiveAttributions)-1]
		}
	}

	return inactiveAttributions
}

func (ca *CostAttribution) purgeInactiveAttributions(inactiveTimeout time.Duration) {
	ca.mu.RLock()
	userIDs := make([]string, 0, len(ca.trackers))
	for userID := range ca.trackers {
		userIDs = append(userIDs, userID)
	}
	ca.mu.RUnlock()

	currentTime := time.Now()
	for _, userID := range userIDs {
		inactiveAttributions := ca.purgeInactiveAttributionsForUser(userID, currentTime.Add(-inactiveTimeout).UnixNano())
		for _, attribution := range inactiveAttributions {
			ca.trackers[userID].RemoveAttributionMetricsForUser(userID, attribution)
		}
	}
}

func (ca *CostAttribution) attributionLimitExceeded(userID, attribution string) bool {
	// if we are still at the cooldown period, we will consider the limit reached
	ca.mu.RLock()
	defer ca.mu.RUnlock()
	// if the user is not exist, we don't need to check the limit
	if ca.trackers[userID] == nil {
		return false
	}

	now := time.Now()
	if v := ca.trackers[userID].coolDownDeadline; v != nil && v.Load() > now.UnixNano() {
		return true
	}

	// if the user attribution is already exist and we are not in the cooldown period, we don't need to check the limit
	_, exists := ca.trackers[userID].attributionTimestamps[attribution]
	if exists {
		return false
	}

	// if the user has reached the limit, we will set the cooldown period which is 20 minutes
	maxReached := len(ca.trackers[userID].attributionTimestamps) >= ca.limits.MaxCostAttributionPerUser(userID)
	if maxReached {
		ca.mu.Lock()
		ca.trackers[userID].coolDownDeadline.Store(now.Add(20 * time.Minute).UnixNano())
		ca.mu.Unlock()
		return true
	}

	return maxReached
}

type CostAttributionCleanupService struct {
	services.Service
	logger          log.Logger
	costAttribution *CostAttribution
	inactiveTimeout time.Duration
	invalidValue    string
}

type CostAttributionMetricsCleaner interface {
	RemoveAttributionMetricsForUser(userID, attribution string)
}

func NewCostAttributionCleanupService(cleanupInterval, inactiveTimeout time.Duration, logger log.Logger, limits *validation.Overrides, reg prometheus.Registerer) *CostAttributionCleanupService {
	s := &CostAttributionCleanupService{
		costAttribution: NewCostAttribution(limits, reg),
		inactiveTimeout: inactiveTimeout,
		logger:          logger,
		invalidValue:    "__unaccounted__",
	}

	s.Service = services.NewTimerService(cleanupInterval, nil, s.iteration, nil).WithName("cost attribution cleanup")
	return s
}

// IncrementReceivedSamples increments the received samples counter for a given user and attribution
func (s *CostAttributionCleanupService) IncrementReceivedSamples(userID, attribution string, value float64) {
	attribution = s.GetUserAttribution(userID, attribution)
	s.costAttribution.mu.RLock()
	defer s.costAttribution.mu.RUnlock()
	if tracker, exists := s.costAttribution.trackers[userID]; exists {
		tracker.receivedSamplesAttribution.WithLabelValues(userID, attribution).Add(value)
	}
}

// IncrementDiscardedSamples increments the discarded samples counter for a given user and attribution
func (s *CostAttributionCleanupService) IncrementDiscardedSamples(userID, attribution string, value float64) {
	attribution = s.GetUserAttribution(userID, attribution)
	s.costAttribution.mu.RLock()
	defer s.costAttribution.mu.RUnlock()
	if tracker, exists := s.costAttribution.trackers[userID]; exists {
		tracker.discardedSampleAttribution.WithLabelValues(userID, attribution).Add(value)
	}
}

// SetActiveSeries sets the active series gauge for a given user and attribution
func (s *CostAttributionCleanupService) SetActiveSeries(userID, attribution string, value float64) {
	attribution = s.GetUserAttribution(userID, attribution)
	s.costAttribution.mu.RLock()
	defer s.costAttribution.mu.RUnlock()
	if tracker, exists := s.costAttribution.trackers[userID]; exists {
		tracker.activeSeriesPerUserAttribution.WithLabelValues(userID, attribution).Set(value)
	}
}

func (s *CostAttributionCleanupService) GetUserAttribution(userID, attribution string) string {
	// not tracking cost attribution for this user, this shouldn't happen
	if s.costAttribution.limits.MaxCostAttributionPerUser(userID) <= 0 {
		return attribution
	}
	if s.costAttribution.attributionLimitExceeded(userID, attribution) {
		return s.invalidValue
	}
	return attribution
}

func (s *CostAttributionCleanupService) GetUserAttributionLabel(userID string) string {
	s.costAttribution.mu.RLock()
	defer s.costAttribution.mu.RUnlock()
	if s.costAttribution != nil {
		if val, exists := s.costAttribution.trackers[userID]; exists {
			return val.trackedLabel
		}
	}
	return ""
}

func (s *CostAttributionCleanupService) GetUserAttributionLimit(userID string) int {
	return s.costAttribution.limits.MaxCostAttributionPerUser(userID)
}

func (s *CostAttributionCleanupService) UpdateAttributionTimestamp(user string, lbs labels.Labels, now time.Time) string {
	if s.costAttribution.trackers[user] == nil || s.costAttribution.trackers[user].trackedLabel == "" {
		return ""
	}
	attribution := lbs.Get(s.costAttribution.trackers[user].trackedLabel)
	// empty label is not normal, if user set attribution label, the metrics send has to include the label
	if attribution == "" {
		level.Error(s.logger).Log("msg", "set attribution label to \"\" since missing cost attribution label in metrics")
		return attribution
	}

	if s.costAttribution.attributionLimitExceeded(user, attribution) {
		attribution = s.invalidValue
		level.Error(s.logger).Log("msg", fmt.Sprintf("set attribution label to \"%s\" since user has reached the limit of cost attribution labels", s.invalidValue))
	}

	s.costAttribution.UpdateAttributionTimestampForUser(user, attribution, now)
	return attribution
}

func (s *CostAttributionCleanupService) iteration(_ context.Context) error {
	s.costAttribution.purgeInactiveAttributions(s.inactiveTimeout)
	return nil
}
