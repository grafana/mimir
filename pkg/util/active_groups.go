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

func (ag *ActiveGroups) UpdateGroupTimestampForUser(userID, group string, ts int64) {
	ag.mu.RLock()
	groupTimestamps := ag.timestampsPerUser[userID]
	ag.mu.RUnlock()

	if groupTimestamps == nil {
		ag.mu.Lock()
		ag.timestampsPerUser[userID] = map[string]*atomic.Int64{group: atomic.NewInt64(ts)}
		ag.mu.Unlock()
		return
	}

	ag.mu.RLock()
	groupTs := ag.timestampsPerUser[userID][group]
	ag.mu.RUnlock()

	if groupTs != nil {
		groupTs.Store(ts)
		return
	}

	ag.mu.Lock()
	ag.timestampsPerUser[userID][group] = atomic.NewInt64(ts)
	ag.mu.Unlock()
}

func (ag *ActiveGroups) PurgeInactiveGroupsForUser(userID string, deadline int64) []string {
	ag.mu.RLock()
	inactiveGroups := make([]string, 0, len(ag.timestampsPerUser))
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
	for i := 0; i < len(inactiveGroups); {
		group := inactiveGroups[i]
		deleted := false

		ag.mu.Lock()
		groupTs := ag.timestampsPerUser[userID][group]
		if groupTs != nil && groupTs.Load() <= deadline {
			delete(ag.timestampsPerUser[userID], group)
			deleted = true
		}
		ag.mu.Unlock()

		if deleted {
			i++
		} else {
			inactiveGroups = append(inactiveGroups[:i], inactiveGroups[i+1:]...)
		}
	}

	return inactiveGroups
}

type ActiveGroupsCleanupService struct {
	services.Service

	activeGroups    *ActiveGroups
	cleanupFunc     func(string, string) // Takes userID, group
	inactiveTimeout time.Duration
}

func NewActiveGroupsCleanupService(cleanupInterval, inactiveTimeout time.Duration, maxGroupsPerUser int, cleanupFn func(string, string)) *ActiveGroupsCleanupService {
	s := &ActiveGroupsCleanupService{
		activeGroups:    NewActiveGroups(maxGroupsPerUser),
		cleanupFunc:     cleanupFn,
		inactiveTimeout: inactiveTimeout,
	}

	s.Service = services.NewTimerService(cleanupInterval, nil, s.iteration, nil).WithName("active groups cleanup")
	return s
}

func NewActiveGroupsCleanupWithDefaultValues(cleanupFn func(string, string), maxGroupsPerUser int) *ActiveGroupsCleanupService {
	return NewActiveGroupsCleanupService(1*time.Minute, 20*time.Minute, maxGroupsPerUser, cleanupFn)
}

func (s *ActiveGroupsCleanupService) ActiveGroupLimitExceeded(userID, group string) bool {
	if s.activeGroups == nil {
		return false
	}

	s.activeGroups.mu.RLock()
	defer s.activeGroups.mu.RUnlock()

	_, containsGroup := s.activeGroups.timestampsPerUser[userID][group]
	return !containsGroup && len(s.activeGroups.timestampsPerUser[userID]) >= s.activeGroups.maxGroupsPerUser
}

func (s *ActiveGroupsCleanupService) UpdateGroupTimestamp(user, group string, now time.Time) {
	// if active group limit exceeded, group = "other"
	s.activeGroups.UpdateGroupTimestampForUser(user, group, now.UnixNano())
}

func (s *ActiveGroupsCleanupService) iteration(_ context.Context) error {
	for userID := range s.activeGroups.timestampsPerUser {
		inactiveGroups := s.activeGroups.PurgeInactiveGroupsForUser(userID, time.Now().Add(-s.inactiveTimeout).UnixNano())
		for _, group := range inactiveGroups {
			s.cleanupFunc(userID, group)
		}
	}
	return nil
}
