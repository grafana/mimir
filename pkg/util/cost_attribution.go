// SPDX-License-Identifier: AGPL-3.0-only

package util

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"go.uber.org/atomic"
)

type CostAttribution struct {
	mu                sync.RWMutex
	timestampsPerUser map[string]map[string]*atomic.Int64 // map[user][group] -> timestamp
	coolDownDeadline  map[string]*atomic.Int64
}

func NewCostAttribution() *CostAttribution {
	return &CostAttribution{
		timestampsPerUser: map[string]map[string]*atomic.Int64{},
		coolDownDeadline:  map[string]*atomic.Int64{},
	}
}

// UpdateAttributionTimestampForUser function is only guaranteed to update to the
// timestamp provided even if it is smaller than the existing value
func (ag *CostAttribution) UpdateAttributionTimestampForUser(userID, attribution string, now time.Time) {
	ts := now.UnixNano()
	ag.mu.RLock()
	if groupTs := ag.timestampsPerUser[userID][attribution]; groupTs != nil {
		ag.mu.RUnlock()
		groupTs.Store(ts)
		return
	}
	ag.mu.RUnlock()

	ag.mu.Lock()
	defer ag.mu.Unlock()

	if ag.timestampsPerUser[userID] == nil {
		ag.timestampsPerUser[userID] = map[string]*atomic.Int64{attribution: atomic.NewInt64(ts)}
		return
	}

	if groupTs := ag.timestampsPerUser[userID][attribution]; groupTs != nil {
		groupTs.Store(ts)
		return
	}

	ag.timestampsPerUser[userID][attribution] = atomic.NewInt64(ts)
}

func (ag *CostAttribution) purgeInactiveAttributionsForUser(userID string, deadline int64) []string {
	ag.mu.RLock()
	var inactiveAttributions []string
	attributionTimestamps := ag.timestampsPerUser[userID]

	for attr, ts := range attributionTimestamps {
		if ts.Load() <= deadline {
			inactiveAttributions = append(inactiveAttributions, attr)
		}
	}
	ag.mu.RUnlock()

	if len(inactiveAttributions) == 0 {
		return nil
	}

	// Cleanup inactive groups
	ag.mu.Lock()
	defer ag.mu.Unlock()

	for i := 0; i < len(inactiveAttributions); {
		inactiveAttribution := inactiveAttributions[i]
		groupTs := ag.timestampsPerUser[userID][inactiveAttribution]
		if groupTs != nil && groupTs.Load() <= deadline {
			delete(ag.timestampsPerUser[userID], inactiveAttribution)
			i++
		} else {
			inactiveAttributions[i] = inactiveAttributions[len(inactiveAttributions)-1]
			inactiveAttributions = inactiveAttributions[:len(inactiveAttributions)-1]
		}
	}

	return inactiveAttributions
}

func (ca *CostAttribution) purgeInactiveAttributions(inactiveTimeout time.Duration, cleanupFuncs ...func(string, string)) {
	ca.mu.RLock()
	userIDs := make([]string, 0, len(ca.timestampsPerUser))
	for userID := range ca.timestampsPerUser {
		userIDs = append(userIDs, userID)
	}
	ca.mu.RUnlock()

	currentTime := time.Now()
	for _, userID := range userIDs {
		inactiveAttributions := ca.purgeInactiveAttributionsForUser(userID, currentTime.Add(-inactiveTimeout).UnixNano())
		for _, attribution := range inactiveAttributions {
			for _, cleanupFn := range cleanupFuncs {
				cleanupFn(userID, attribution)
			}
		}
	}
}

func (ca *CostAttribution) attributionLimitExceeded(userID, attribution string, now time.Time, limit int) bool {
	// if we are still at the cooldown period, we will consider the limit reached
	ca.mu.RLock()
	defer ca.mu.RUnlock()

	if v, exists := ca.coolDownDeadline[userID]; exists && v.Load() > now.UnixNano() {
		return true
	}

	// if the user attribution is already exist and we are not in the cooldown period, we don't need to check the limit
	_, exists := ca.timestampsPerUser[userID][attribution]
	if exists {
		return false
	}

	// if the user has reached the limit, we will set the cooldown period which is 20 minutes
	maxReached := len(ca.timestampsPerUser[userID]) >= limit
	if maxReached {
		ca.coolDownDeadline[userID].Store(time.Now().Add(20 * time.Minute).UnixNano())
		return true
	}

	return maxReached
}

type CostAttributionCleanupService struct {
	services.Service
	logger          log.Logger
	costAttribution *CostAttribution
	cleanupFuncs    []func(userID, attribution string)
	inactiveTimeout time.Duration
	invalidValue    string
}

type CostAttributionMetricsCleaner interface {
	RemoveAttributionMetricsForUser(userID, attribution string)
}

func NewCostAttributionCleanupService(cleanupInterval, inactiveTimeout time.Duration, logger log.Logger, cleanupFns ...func(string, string)) *CostAttributionCleanupService {
	s := &CostAttributionCleanupService{
		costAttribution: NewCostAttribution(),
		cleanupFuncs:    cleanupFns,
		inactiveTimeout: inactiveTimeout,
		logger:          logger,
		invalidValue:    "__unaccounted__",
	}

	s.Service = services.NewTimerService(cleanupInterval, nil, s.iteration, nil).WithName("cost attribution cleanup")
	return s
}

func (s *CostAttributionCleanupService) UpdateAttributionTimestamp(user, attribution string, now time.Time, limit int) string {
	// empty label is not normal, if user set attribution label, the metrics send has to include the label
	if attribution == "" {
		attribution = s.invalidValue
		level.Error(s.logger).Log("msg", fmt.Sprintf("set attribution label to \"%s\" since missing cost attribution label in metrics", s.invalidValue))
	} else if s.costAttribution.attributionLimitExceeded(user, attribution, now, limit) {
		attribution = s.invalidValue
		level.Error(s.logger).Log("msg", "set attribution label to \"%s\" since user has reached the limit of cost attribution labels", s.invalidValue)
	}

	s.costAttribution.UpdateAttributionTimestampForUser(user, attribution, now)
	return attribution
}

func (s *CostAttributionCleanupService) iteration(_ context.Context) error {
	s.costAttribution.purgeInactiveAttributions(s.inactiveTimeout, s.cleanupFuncs...)
	return nil
}

// Register registers the cleanup function from metricsCleaner to be called during each cleanup iteration.
// This function is NOT thread safe
func (s *CostAttributionCleanupService) Register(metricsCleaner CostAttributionMetricsCleaner) {
	s.cleanupFuncs = append(s.cleanupFuncs, metricsCleaner.RemoveAttributionMetricsForUser)
}
