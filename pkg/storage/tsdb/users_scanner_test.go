// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/tsdb/users_scanner_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package tsdb

import (
	"context"
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

	bucketClient.MockAttributes(path.Join("user-1", "bucket-index.json.gz"),
		objstore.ObjectAttributes{LastModified: time.Now().Add(-20 * time.Minute)}, nil,
	)

	s := NewUsersScanner(bucketClient, isOwned, log.NewNopLogger())
	actual, deleted, err := s.ScanUsers(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"user-1"}, actual)
	assert.Equal(t, []string{"user-3"}, deleted)
}

func TestUsersScanner_ScanUsers_ShouldReturnUsersForWhichOwnerCheckOrTenantDeletionCheckFailed(t *testing.T) {
	expected := []string{"user-1", "user-2"}

	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("", expected, nil)
	bucketClient.MockExists(path.Join("user-1", TenantDeletionMarkPath), false, nil)
	bucketClient.MockExists(path.Join("user-2", TenantDeletionMarkPath), false, errors.New("fail"))

	attrs := objstore.ObjectAttributes{LastModified: time.Now().Add(-20 * time.Minute)}
	bucketClient.MockAttributes(path.Join("user-1", "bucket-index.json.gz"), attrs, nil)
	bucketClient.MockAttributes(path.Join("user-2", "bucket-index.json.gz"), attrs, nil)

	isOwned := func(string) (bool, error) {
		return false, errors.New("failed to check if user is owned")
	}

	s := NewUsersScanner(bucketClient, isOwned, log.NewNopLogger())
	actual, deleted, err := s.ScanUsers(context.Background())
	require.NoError(t, err)
	assert.Equal(t, expected, actual)
	assert.Empty(t, deleted)
}

func TestUsersScanner_ScanUsers_ShouldNotReturnPrefixedUsedByMimirInternals(t *testing.T) {
	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("", []string{"user-1", "user-2", bucket.MimirInternalsPrefix}, nil)
	bucketClient.MockExists(path.Join("user-1", TenantDeletionMarkPath), false, nil)
	bucketClient.MockExists(path.Join("user-2", TenantDeletionMarkPath), false, nil)

	attrs := objstore.ObjectAttributes{LastModified: time.Now().Add(-20 * time.Minute)}
	bucketClient.MockAttributes(path.Join("user-1", "bucket-index.json.gz"), attrs, nil)
	bucketClient.MockAttributes(path.Join("user-2", "bucket-index.json.gz"), attrs, nil)

	s := NewUsersScanner(bucketClient, AllUsers, log.NewNopLogger())
	actual, _, err := s.ScanUsers(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"user-1", "user-2"}, actual)
}

func TestUsersScanner_ScanUsers_ShouldReturnConsistentOrder(t *testing.T) {
	expected := []string{"user-2", "user-3", "user-4", "user-1"}

	users := []struct {
		ID         string
		indexAttrs objstore.ObjectAttributes
	}{
		{"user-1", objstore.ObjectAttributes{LastModified: time.Now().Add(-5 * time.Minute)}},
		{"user-2", objstore.ObjectAttributes{LastModified: time.Now().Add(-20 * time.Minute)}},
		{"user-3", objstore.ObjectAttributes{LastModified: time.Now().Add(-15 * time.Minute)}},
		{"user-4", objstore.ObjectAttributes{LastModified: time.Now().Add(-10 * time.Minute)}},
	}

	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("", []string{"user-1", "user-2", "user-3", "user-4"}, nil)
	for i := 0; i < len(users); i++ {
		bucketClient.MockExists(path.Join(users[i].ID, TenantDeletionMarkPath), false, nil)
		bucketClient.MockAttributes(path.Join(users[i].ID, "bucket-index.json.gz"), users[i].indexAttrs, nil)
	}

	isOwned := func(_ string) (bool, error) {
		return true, nil
	}

	s := NewUsersScanner(bucketClient, isOwned, log.NewNopLogger())
	actual, deleted, err := s.ScanUsers(context.Background())
	require.NoError(t, err)
	assert.Equal(t, expected, actual)
	assert.Empty(t, deleted)
}

func TestUsersScanner_ScanUsers_ShouldReturnTenantWithoutBucketIndexLast(t *testing.T) {
	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("", []string{"user-1", "user-2"}, nil)
	bucketClient.MockExists(path.Join("user-1", TenantDeletionMarkPath), false, nil)
	bucketClient.MockExists(path.Join("user-2", TenantDeletionMarkPath), false, nil)

	bucketClient.MockAttributes(
		path.Join("user-1", "bucket-index.json.gz"),
		objstore.ObjectAttributes{}, errors.New("fail"),
	)

	bucketClient.MockAttributes(
		path.Join("user-2", "bucket-index.json.gz"),
		objstore.ObjectAttributes{LastModified: time.Now().Add(-20 * time.Minute)}, nil,
	)

	isOwned := func(_ string) (bool, error) {
		return true, nil
	}

	s := NewUsersScanner(bucketClient, isOwned, log.NewNopLogger())
	actual, deleted, err := s.ScanUsers(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"user-2", "user-1"}, actual)
	assert.Empty(t, deleted)
}
