// SPDX-License-Identifier: AGPL-3.0-only

package util

import (
	"context"
	"sync"
	"time"

	"github.com/grafana/dskit/services"
	"go.uber.org/atomic"
)

type ActiveGroups struct {
	mu                sync.RWMutex
	timestampsPerUser map[string]map[string]*atomic.Int64 // map[user][group] -> timestamp
	maxGroupsPerUser  int
}

func NewActiveGroups(maxGroupsPerUser int) *ActiveGroups {
	return &ActiveGroups{
		timestampsPerUser: map[string]map[string]*atomic.Int64{},
		maxGroupsPerUser:  maxGroupsPerUser,
	}
}

// UpdateGroupTimestampForUser function is only guaranteed to update to the
// timestamp provided even if it is smaller than the existing value
func (ag *ActiveGroups) UpdateGroupTimestampForUser(userID, group string, now time.Time) {
	ts := now.UnixNano()
	ag.mu.RLock()
	if groupTs := ag.timestampsPerUser[userID][group]; groupTs != nil {
		ag.mu.RUnlock()
		groupTs.Store(ts)
		return
	}
	ag.mu.RUnlock()

	ag.mu.Lock()
	defer ag.mu.Unlock()

	if ag.timestampsPerUser[userID] == nil {
		ag.timestampsPerUser[userID] = map[string]*atomic.Int64{group: atomic.NewInt64(ts)}
		return
	}

	if groupTs := ag.timestampsPerUser[userID][group]; groupTs != nil {
		groupTs.Store(ts)
		return
	}

	ag.timestampsPerUser[userID][group] = atomic.NewInt64(ts)
}

func (ag *ActiveGroups) PurgeInactiveGroupsForUser(userID string, deadline int64) []string {
	ag.mu.RLock()
	var inactiveGroups []string
	groupTimestamps := ag.timestampsPerUser[userID]

	for group, ts := range groupTimestamps {
		if ts.Load() <= deadline {
			inactiveGroups = append(inactiveGroups, group)
		}
	}
	ag.mu.RUnlock()

	if len(inactiveGroups) == 0 {
		return nil
	}

	// Cleanup inactive groups
	ag.mu.Lock()
	defer ag.mu.Unlock()

	for i := 0; i < len(inactiveGroups); {
		inactiveGroup := inactiveGroups[i]
		groupTs := ag.timestampsPerUser[userID][inactiveGroup]
		if groupTs != nil && groupTs.Load() <= deadline {
			delete(ag.timestampsPerUser[userID], inactiveGroup)
			i++
		} else {
			inactiveGroups[i] = inactiveGroups[len(inactiveGroups)-1]
			inactiveGroups = inactiveGroups[:len(inactiveGroups)-1]
		}
	}

	return inactiveGroups
}

func (ag *ActiveGroups) PurgeInactiveGroups(inactiveTimeout time.Duration, cleanupFuncs ...func(string, string)) {
	ag.mu.RLock()
	userIDs := make([]string, 0, len(ag.timestampsPerUser))
	for userID := range ag.timestampsPerUser {
		userIDs = append(userIDs, userID)
	}
	ag.mu.RUnlock()

	currentTime := time.Now()
	for _, userID := range userIDs {
		inactiveGroups := ag.PurgeInactiveGroupsForUser(userID, currentTime.Add(-inactiveTimeout).UnixNano())
		for _, group := range inactiveGroups {
			for _, cleanupFn := range cleanupFuncs {
				cleanupFn(userID, group)
			}
		}
	}
}

type ActiveGroupsCleanupService struct {
	services.Service

	activeGroups    *ActiveGroups
	cleanupFuncs    []func(userID, group string)
	inactiveTimeout time.Duration
}

type UserGroupMetricsCleaner interface {
	RemoveGroupMetricsForUser(userID, group string)
}

func NewActiveGroupsCleanupService(cleanupInterval, inactiveTimeout time.Duration, maxGroupsPerUser int, cleanupFns ...func(string, string)) *ActiveGroupsCleanupService {
	s := &ActiveGroupsCleanupService{
		activeGroups:    NewActiveGroups(maxGroupsPerUser),
		cleanupFuncs:    cleanupFns,
		inactiveTimeout: inactiveTimeout,
	}

	s.Service = services.NewTimerService(cleanupInterval, nil, s.iteration, nil).WithName("active groups cleanup")
	return s
}

func (ag *ActiveGroups) ActiveGroupLimitExceeded(userID, group string) bool {
	ag.mu.RLock()
	defer ag.mu.RUnlock()

	_, containsGroup := ag.timestampsPerUser[userID][group]
	return !containsGroup && len(ag.timestampsPerUser[userID]) >= ag.maxGroupsPerUser
}

func (s *ActiveGroupsCleanupService) UpdateActiveGroupTimestamp(user, group string, now time.Time) string {
	// Does not track empty label
	if s == nil || s.activeGroups == nil || group == "" {
		return group
	}

	if s.activeGroups.ActiveGroupLimitExceeded(user, group) {
		group = "other"
	}
	s.activeGroups.UpdateGroupTimestampForUser(user, group, now)
	return group
}

func (s *ActiveGroupsCleanupService) iteration(_ context.Context) error {
	s.activeGroups.PurgeInactiveGroups(s.inactiveTimeout, s.cleanupFuncs...)
	return nil
}

// Register registers the cleanup function from metricsCleaner to be called during each cleanup iteration.
// This function is NOT thread safe
func (s *ActiveGroupsCleanupService) Register(metricsCleaner UserGroupMetricsCleaner) {
	s.cleanupFuncs = append(s.cleanupFuncs, metricsCleaner.RemoveGroupMetricsForUser)
}
