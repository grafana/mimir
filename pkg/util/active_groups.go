// SPDX-License-Identifier: AGPL-3.0-only

package util

import (
	"context"
	"sync"
	"time"

	"github.com/grafana/dskit/services"
	"go.uber.org/atomic"
)

const maxGroupsPerUser int = 100

type ActiveGroups struct {
	mu                sync.RWMutex
	timestampsPerUser map[string]map[string]*atomic.Int64 // map[user][group] -> timestamp
}

func NewActiveGroups() *ActiveGroups {
	return &ActiveGroups{
		timestampsPerUser: map[string]map[string]*atomic.Int64{},
	}
}

func (ag *ActiveGroups) UpdateGroupTimestampForUser(userID, group string, ts int64) {
	// Create new atomic before lock is acquired
	newAtomic := atomic.NewInt64(ts)

	ag.mu.RLock()
	groupTimestamps := ag.timestampsPerUser[userID]
	ag.mu.RUnlock()

	if groupTimestamps == nil {
		ag.mu.Lock()
		ag.timestampsPerUser[userID] = make(map[string]*atomic.Int64)
		ag.timestampsPerUser[userID][group] = newAtomic
		ag.mu.Unlock()
		return
	}

	ag.mu.Lock()
	groupTs := ag.timestampsPerUser[userID][group]
	if groupTs != nil {
		ag.mu.Unlock()

		groupTs.Store(ts)
		return
	}

	ag.timestampsPerUser[userID][group] = newAtomic
	ag.mu.Unlock()

	// ag.mu.Lock()
	// if groupTimestamps := ag.timestampsPerUser[userID]; groupTimestamps == nil {
	// 	ag.timestampsPerUser[userID] = make(map[string]*atomic.Int64)
	// }
	// ag.timestampsPerUser[userID][group] = newAtomic
	// ag.mu.Unlock()
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

func NewActiveGroupsCleanupService(cleanupInterval, inactiveTimeout time.Duration, cleanupFn func(string, string)) *ActiveGroupsCleanupService {
	s := &ActiveGroupsCleanupService{
		activeGroups:    NewActiveGroups(),
		cleanupFunc:     cleanupFn,
		inactiveTimeout: inactiveTimeout,
	}

	s.Service = services.NewTimerService(cleanupInterval, nil, s.iteration, nil).WithName("active groups cleanup")
	return s
}

func NewActiveGroupsCleanupWithDefaultValues(cleanupFn func(string, string)) *ActiveGroupsCleanupService {
	return NewActiveGroupsCleanupService(1*time.Minute, 20*time.Minute, cleanupFn)
}

func (s *ActiveGroupsCleanupService) UpdateGroupTimestamp(user, group string, now time.Time) {
	s.activeGroups.UpdateGroupTimestampForUser(user, group, now.UnixNano())
}

func (s *ActiveGroupsCleanupService) iteration(_ context.Context) error {
	//inactiveUsers := s.activeGroups.PurgeInactiveGroupsForUser(time.Now().Add(-s.inactiveTimeout).UnixNano())
	// Inactive Users - Delete all their metrics
	// Get Active Users
	// Go through and cleanup their metrics (last seen timestamp)

	// for _, userID := range inactiveUsers {
	// 	s.cleanupFunc(userID)
	// }
	return nil
}
