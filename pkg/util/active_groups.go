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

// UpdateGroupTimestampForUser function is only guaranteed to update to timestamp
// provided even if it is smaller than the existing value
func (ag *ActiveGroups) UpdateGroupTimestampForUser(userID, group string, ts int64) {
	ag.mu.RLock()
	if groupTs := ag.timestampsPerUser[userID][group]; groupTs != nil {
		ag.mu.RUnlock()
		groupTs.Store(ts)
		return
	}
	ag.mu.RUnlock()

	ag.mu.Lock()
	defer ag.mu.Unlock()
	groupTimestamps := ag.timestampsPerUser[userID]

	if groupTimestamps == nil {
		ag.timestampsPerUser[userID] = map[string]*atomic.Int64{group: atomic.NewInt64(ts)}
		return
	}

	groupTs := ag.timestampsPerUser[userID][group]

	if groupTs != nil {
		groupTs.Store(ts)
		return
	}

	ag.timestampsPerUser[userID][group] = atomic.NewInt64(ts)
}

func (ag *ActiveGroups) PurgeInactiveGroupsForUser(userID string, deadline int64) []string {
	ag.mu.RLock()
	totalGroups := len(ag.timestampsPerUser[userID])
	if totalGroups == 0 {
		ag.mu.RUnlock()
		return nil
	}

	inactiveGroups := make([]string, 0, totalGroups)
	groupTimestamps := ag.timestampsPerUser[userID]

	for group, ts := range groupTimestamps {
		if ts.Load() <= deadline {
			inactiveGroups = append(inactiveGroups, group)
		}
	}
	ag.mu.RUnlock()

	if groupTimestamps == nil || len(inactiveGroups) == 0 {
		return nil
	}

	// Cleanup inactive groups
	deletedGroups := make([]string, 0, len(inactiveGroups))
	ag.mu.Lock()
	defer ag.mu.Unlock()

	for _, inactiveGroup := range inactiveGroups {
		groupTs := ag.timestampsPerUser[userID][inactiveGroup]
		if groupTs != nil && groupTs.Load() <= deadline {
			delete(ag.timestampsPerUser[userID], inactiveGroup)
			deletedGroups = append(deletedGroups, inactiveGroup)
		}
	}

	return deletedGroups
}

type ActiveGroupsCleanupService struct {
	services.Service

	activeGroups    *ActiveGroups
	cleanupFuncs    []func(string, string) // Takes userID, group
	inactiveTimeout time.Duration
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

func NewActiveGroupsCleanupWithDefaultValues(maxGroupsPerUser int, cleanupFns ...func(string, string)) *ActiveGroupsCleanupService {
	return NewActiveGroupsCleanupService(1*time.Minute, 20*time.Minute, maxGroupsPerUser, cleanupFns...)
}

func (ag *ActiveGroups) ActiveGroupLimitExceeded(userID, group string) bool {
	ag.mu.RLock()
	defer ag.mu.RUnlock()

	_, containsGroup := ag.timestampsPerUser[userID][group]
	return !containsGroup && len(ag.timestampsPerUser[userID]) >= ag.maxGroupsPerUser
}

func (s *ActiveGroupsCleanupService) UpdateActiveGroupTimestamp(user, group string, now time.Time) string {
	// Does not track empty label
	if group == "" {
		return group
	}

	if s.activeGroups.ActiveGroupLimitExceeded(user, group) {
		group = "other"
	}
	s.activeGroups.UpdateGroupTimestampForUser(user, group, now.UnixNano())
	return group
}

func (s *ActiveGroupsCleanupService) iteration(_ context.Context) error {
	for userID := range s.activeGroups.timestampsPerUser {
		inactiveGroups := s.activeGroups.PurgeInactiveGroupsForUser(userID, time.Now().Add(-s.inactiveTimeout).UnixNano())
		for _, group := range inactiveGroups {
			for _, cleanupFn := range s.cleanupFuncs {
				cleanupFn(userID, group)
			}
		}
	}
	return nil
}
