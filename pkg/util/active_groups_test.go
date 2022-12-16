// SPDX-License-Identifier: AGPL-3.0-only

package util

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"golang.org/x/exp/slices"
)

func TestActiveGroups(t *testing.T) {
	ag := NewActiveGroups()

	ag.UpdateGroupTimestampForUser("user1", "group1", 10)
	ag.UpdateGroupTimestampForUser("user1", "group2", 15)
	ag.UpdateGroupTimestampForUser("user1", "group3", 20)

	ag.UpdateGroupTimestampForUser("user2", "group4", 10)
	ag.UpdateGroupTimestampForUser("user2", "group5", 15)
	ag.UpdateGroupTimestampForUser("user2", "group6", 20)

	require.Nil(t, ag.PurgeInactiveGroupsForUser("user1", 5))
	inactiveGroupsForUser1 := ag.PurgeInactiveGroupsForUser("user1", 16)
	require.Equal(t, 2, len(inactiveGroupsForUser1))
	require.True(t, slices.Contains(inactiveGroupsForUser1, "group1"))
	require.True(t, slices.Contains(inactiveGroupsForUser1, "group2"))
	require.False(t, slices.Contains(inactiveGroupsForUser1, "group3"))

	require.Nil(t, ag.PurgeInactiveGroupsForUser("user2", 5))
	inactiveGroupsForUser2 := ag.PurgeInactiveGroupsForUser("user2", 11)
	require.Equal(t, 1, len(inactiveGroupsForUser2))
	require.True(t, slices.Contains(inactiveGroupsForUser2, "group4"))
	require.False(t, slices.Contains(inactiveGroupsForUser2, "group5"))
	require.False(t, slices.Contains(inactiveGroupsForUser2, "group6"))

	ag.UpdateGroupTimestampForUser("user1", "group1", 25)
	inactiveGroupsForUser1 = ag.PurgeInactiveGroupsForUser("user1", 21)
	require.Equal(t, 1, len(inactiveGroupsForUser1))
	require.Equal(t, []string{"group3"}, inactiveGroupsForUser1)
}

func TestActiveGroupsConcurrentUpdateAndPurge(t *testing.T) {
	numGroups := 10
	ag := NewActiveGroups()
	done := sync.WaitGroup{}
	stop := atomic.NewBool(false)
	latestTS := atomic.NewInt64(0)

	for i := 0; i < numGroups; i++ {
		done.Add(1)
		go func() {
			defer done.Done()

			for !stop.Load() {
				ts := latestTS.Inc()
				ag.UpdateGroupTimestampForUser("user1", fmt.Sprintf("%d", ts), ts)
				time.Sleep(1 * time.Millisecond)
			}
		}()
	}

	previousLatest := int64(0)
	for i := 0; i < numGroups; i++ {
		time.Sleep(100 * time.Millisecond)

		latest := latestTS.Load()
		require.True(t, latest > previousLatest)

		previousLatest = latest

		purgedGroups := ag.PurgeInactiveGroupsForUser("user1", latest)
		require.NotEmpty(t, purgedGroups)
	}

	stop.Store(true)
	done.Wait()

	// At this point, map may or may not be empty. Do one final purge
	latest := latestTS.Load()
	_ = ag.PurgeInactiveGroupsForUser("user1", latest)

	// Final purge, should be no more inactive groups
	purgedGroups := ag.PurgeInactiveGroupsForUser("user1", latest)
	require.Empty(t, purgedGroups)
}

func TestActiveGroupLimitExceeded(t *testing.T) {
	agCleanupService := NewActiveGroupsCleanupWithDefaultValues(func(string, string) {})
	ag := agCleanupService.activeGroups

	// Send groups over the limit
	for i := 0; i < 1000; i++ {
		ag.UpdateGroupTimestampForUser("user1", fmt.Sprintf("%d", i), int64(i))
	}

	// Active group limit exceeded when trying to add new group
	require.True(t, agCleanupService.ActiveGroupLimitExceeded("user1", "new-group"))

	// Existing group can still be updated
	require.False(t, agCleanupService.ActiveGroupLimitExceeded("user1", "0"))
}
