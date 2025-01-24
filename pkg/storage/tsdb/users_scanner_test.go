// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/tsdb/users_scanner_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package tsdb

import (
	"context"
	"errors"
	"fmt"
	"path"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/bucket"
)

func TestUsersScanner_ScanUsers_ShouldReturnedOwnedUsersOnly(t *testing.T) {
	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("", []string{"user-1", "user-2", "user-3", "user-4"}, nil)
	bucketClient.MockExists(path.Join("user-1", TenantDeletionMarkPath), false, nil)
	bucketClient.MockExists(path.Join("user-3", TenantDeletionMarkPath), true, nil)

	isOwned := func(userID string) (bool, error) {
		return userID == "user-1" || userID == "user-3", nil
	}

	s := NewUsersScanner(bucketClient, isOwned, log.NewNopLogger())
	actual, deleted, err := s.ScanUsers(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"user-1"}, actual)
	assert.Equal(t, []string{"user-3"}, deleted)
}

func TestUsersScanner_ScanUsers_ShouldReturnUsersForWhichOwnerCheckOrTenantDeletionCheckFailed(t *testing.T) {
	users := []string{"user-1", "user-2"}

	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("", users, nil)
	bucketClient.MockExists(path.Join("user-1", TenantDeletionMarkPath), false, nil)
	bucketClient.MockExists(path.Join("user-2", TenantDeletionMarkPath), false, errors.New("fail"))

	isOwned := func(string) (bool, error) {
		return false, errors.New("failed to check if user is owned")
	}

	s := NewUsersScanner(bucketClient, isOwned, log.NewNopLogger())
	actual, deleted, err := s.ScanUsers(context.Background())
	require.NoError(t, err)
	for _, userID := range users {
		assert.Contains(t, actual, userID)
	}
	assert.Empty(t, deleted)
}

func TestUsersScanner_ScanUsers_ShouldNotReturnPrefixedUsedByMimirInternals(t *testing.T) {
	users := []string{"user-1", "user-2"}
	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("", []string{"user-1", "user-2", bucket.MimirInternalsPrefix}, nil)
	bucketClient.MockExists(path.Join("user-1", TenantDeletionMarkPath), false, nil)
	bucketClient.MockExists(path.Join("user-2", TenantDeletionMarkPath), false, nil)

	s := NewUsersScanner(bucketClient, AllUsers, log.NewNopLogger())
	actual, _, err := s.ScanUsers(context.Background())
	require.NoError(t, err)
	for _, userID := range users {
		assert.Contains(t, actual, userID)
	}
}

func TestUsersScanner_ScanUsers_ShouldReturnRandomizedOrder(t *testing.T) {
	var users = make([]string, 20)
	for i := 0; i < len(users); i++ {
		users[i] = fmt.Sprintf("user-%d", i)
	}
	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("", users, nil)
	for i := 0; i < len(users); i++ {
		bucketClient.MockExists(path.Join(users[i], TenantDeletionMarkPath), false, nil)
	}

	isOwned := func(_ string) (bool, error) {
		return true, nil
	}

	s := NewUsersScanner(bucketClient, isOwned, log.NewNopLogger())
	actual, deleted, err := s.ScanUsers(context.Background())
	require.NoError(t, err)
	for _, userID := range users {
		assert.Contains(t, actual, userID)
	}
	assert.Empty(t, deleted)
	assert.NotEqual(t, actual, users)
}
