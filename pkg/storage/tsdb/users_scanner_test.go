// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/tsdb/users_scanner_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package tsdb

import (
	"errors"
	"path"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

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

	s := NewUsersScanner(bucketClient, isOwned, time.Minute, log.NewNopLogger(), nil)
	for range 2 {
		actual, deleted, err := s.ScanUsers(t.Context())
		require.NoError(t, err)
		assert.Equal(t, []string{"user-1"}, actual)
		assert.Equal(t, []string{"user-3"}, deleted)
	}
	s.Stop()
}

func TestUsersScanner_ScanUsers_ShouldReturnUsersForWhichOwnerCheckOrTenantDeletionCheckFailed(t *testing.T) {
	expected := []string{"user-1", "user-2"}

	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("", expected, nil)
	bucketClient.MockExists(path.Join("user-1", TenantDeletionMarkPath), false, nil)
	bucketClient.MockExists(path.Join("user-2", TenantDeletionMarkPath), false, errors.New("fail"))

	isOwned := func(string) (bool, error) {
		return false, errors.New("failed to check if user is owned")
	}

	s := NewUsersScanner(bucketClient, isOwned, time.Minute, log.NewNopLogger(), nil)
	actual, deleted, err := s.ScanUsers(t.Context())
	require.NoError(t, err)
	assert.Equal(t, expected, actual)
	assert.Empty(t, deleted)
}

func TestUsersScanner_ScanUsers_ShouldNotReturnPrefixedUsedByMimirInternals(t *testing.T) {
	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("", []string{"user-1", "user-2", bucket.MimirInternalsPrefix}, nil)
	bucketClient.MockExists(path.Join("user-1", TenantDeletionMarkPath), false, nil)
	bucketClient.MockExists(path.Join("user-2", TenantDeletionMarkPath), false, nil)

	s := NewUsersScanner(bucketClient, AllUsers, time.Minute, log.NewNopLogger(), nil)
	actual, _, err := s.ScanUsers(t.Context())
	require.NoError(t, err)
	assert.Equal(t, []string{"user-1", "user-2"}, actual)
}

func TestListUsersInBuckets(t *testing.T) {
	t.Run("reports the buckets of each user", func(t *testing.T) {
		newBucket := &bucket.ClientMock{}
		newBucket.MockIter("", []string{"user-1/", "user-3/", bucket.MimirInternalsPrefix + "/"}, nil)
		oldBucket := &bucket.ClientMock{}
		oldBucket.MockIter("", []string{"user-1/", "user-2/"}, nil)
		emptyBucket := &bucket.ClientMock{}
		emptyBucket.MockIter("", nil, nil)

		actual, err := ListUsersInBuckets(t.Context(), []objstore.Bucket{newBucket, emptyBucket, oldBucket})
		require.NoError(t, err)
		assert.Equal(t, map[string][]int{
			"user-1": {0, 2},
			"user-2": {2},
			"user-3": {0},
		}, actual)
	})

	t.Run("fails if it can't list one of the buckets", func(t *testing.T) {
		listErr := errors.New("failed to list")

		newBucket := &bucket.ClientMock{}
		newBucket.MockIter("", []string{"user-1/"}, nil)
		oldBucket := &bucket.ClientMock{}
		oldBucket.MockIter("", nil, listErr)

		actual, err := ListUsersInBuckets(t.Context(), []objstore.Bucket{newBucket, oldBucket})
		require.ErrorIs(t, err, listErr)
		assert.Nil(t, actual)
	})
}
