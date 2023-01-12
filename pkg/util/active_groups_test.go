// SPDX-License-Identifier: AGPL-3.0-only

package util

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

const maxGroupsPerUser = 100

func TestActiveGroups(t *testing.T) {
	ag := NewActiveGroups(maxGroupsPerUser)

	ag.UpdateGroupTimestampForUser("user1", "group1", time.Unix(0, 10))
	ag.UpdateGroupTimestampForUser("user1", "group2", time.Unix(0, 15))
	ag.UpdateGroupTimestampForUser("user1", "group3", time.Unix(0, 20))

	ag.UpdateGroupTimestampForUser("user2", "group4", time.Unix(0, 10))
	ag.UpdateGroupTimestampForUser("user2", "group5", time.Unix(0, 15))
	ag.UpdateGroupTimestampForUser("user2", "group6", time.Unix(0, 20))

	require.Nil(t, ag.PurgeInactiveGroupsForUser("user1", 5))
	inactiveGroupsForUser1 := ag.PurgeInactiveGroupsForUser("user1", 16)
	require.ElementsMatch(t, inactiveGroupsForUser1, []string{"group1", "group2"})

	require.Nil(t, ag.PurgeInactiveGroupsForUser("user2", 5))
	inactiveGroupsForUser2 := ag.PurgeInactiveGroupsForUser("user2", 11)
	require.ElementsMatch(t, inactiveGroupsForUser2, []string{"group4"})

	ag.UpdateGroupTimestampForUser("user1", "group1", time.Unix(0, 25))
	inactiveGroupsForUser1 = ag.PurgeInactiveGroupsForUser("user1", 21)
	require.ElementsMatch(t, inactiveGroupsForUser1, []string{"group3"})
}

func TestActiveGroupsConcurrentUpdateAndPurge(t *testing.T) {
	numGroups := 10
	ag := NewActiveGroups(maxGroupsPerUser)
	done := sync.WaitGroup{}
	stop := atomic.NewBool(false)
	latestTS := atomic.NewInt64(0)

	for i := 0; i < numGroups; i++ {
		done.Add(1)
		go func() {
			defer done.Done()

			for !stop.Load() {
				ts := latestTS.Inc()
				ag.UpdateGroupTimestampForUser("user1", fmt.Sprintf("%d", ts), time.Unix(0, ts))
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
	ag := NewActiveGroups(maxGroupsPerUser)

	// Send number of groups to the limit
	for i := 0; i < maxGroupsPerUser; i++ {
		ag.UpdateGroupTimestampForUser("user1", fmt.Sprintf("%d", i), time.Unix(0, int64(i)))
	}

	// Active group limit exceeded when trying to add new group
	require.True(t, ag.ActiveGroupLimitExceeded("user1", "new-group"))

	// Existing group can still be updated
	require.False(t, ag.ActiveGroupLimitExceeded("user1", "0"))
}

func TestUpdateActiveGroups(t *testing.T) {
	s := NewActiveGroupsCleanupService(3*time.Minute, 20*time.Minute, maxGroupsPerUser)

	// Hit the group limit for user1
	for i := 0; i < maxGroupsPerUser; i++ {
		s.UpdateActiveGroupTimestamp("user1", fmt.Sprintf("%d", i), time.Unix(1, 0))
	}

	// Add new groups for user1 that should be set as "other"
	finalTs := time.Now().Add(-10 * time.Second)
	s.UpdateActiveGroupTimestamp("user1", "new_group_1", time.Unix(123, 0))
	group := s.UpdateActiveGroupTimestamp("user1", "new_group_2", finalTs)
	require.Equal(t, "other", group)

	// Active groups should now have "other" with timestamp finalTs
	timestamps := s.activeGroups.timestampsPerUser["user1"]
	require.NotEmpty(t, timestamps)
	require.Equal(t, timestamps["other"], atomic.NewInt64(finalTs.UnixNano()))
}
