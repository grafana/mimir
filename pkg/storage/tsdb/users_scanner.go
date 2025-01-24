// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/tsdb/users_scanner.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package tsdb

import (
	"context"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket"
)

// AllUsers returns true to each call and should be used whenever the UsersScanner should not filter out
// any user due to sharding.
func AllUsers(_ string) (bool, error) {
	return true, nil
}

type UsersScanner struct {
	bucketClient objstore.Bucket
	logger       log.Logger
	isOwned      func(userID string) (bool, error)
}

func NewUsersScanner(bucketClient objstore.Bucket, isOwned func(userID string) (bool, error), logger log.Logger) *UsersScanner {
	return &UsersScanner{
		bucketClient: bucketClient,
		logger:       logger,
		isOwned:      isOwned,
	}
}

// ScanUsers returns a fresh list of users found in the storage, that are not marked for deletion,
// and list of users marked for deletion.
//
// If sharding is enabled, returned lists contains only the users owned by this instance.
func (s *UsersScanner) ScanUsers(ctx context.Context) (users, markedForDeletion []string, err error) {
	users, err = ListUsers(ctx, s.bucketClient)
	if err != nil {
		return nil, nil, err
	}

	// Check users for being owned by instance, and split users into non-deleted and deleted.
	// We do these checks after listing all users, to improve cacheability of Iter (result is only cached at the end of Iter call).
	for ix := 0; ix < len(users); {
		userID := users[ix]

		// Check if it's owned by this instance.
		owned, err := s.isOwned(userID)
		if err != nil {
			level.Warn(s.logger).Log("msg", "unable to check if user is owned by this shard", "user", userID, "err", err)
		} else if !owned {
			users = append(users[:ix], users[ix+1:]...)
			continue
		}

		deletionMarkExists, err := TenantDeletionMarkExists(ctx, s.bucketClient, userID)
		if err != nil {
			level.Warn(s.logger).Log("msg", "unable to check if user is marked for deletion", "user", userID, "err", err)
		} else if deletionMarkExists {
			users = append(users[:ix], users[ix+1:]...)
			markedForDeletion = append(markedForDeletion, userID)
			continue
		}

		ix++
	}

	return users, markedForDeletion, nil
}

// ListUsers returns all user IDs found scanning the root of the bucket.
func ListUsers(ctx context.Context, bucketClient objstore.Bucket) (users []string, err error) {
	// Iterate the bucket to find all users in the bucket. Due to how the bucket listing
	// caching works, it's more likely to have a cache hit if there's no delay while
	// iterating the bucket, so we do load all users in memory and later process them.
	err = bucketClient.Iter(ctx, "", func(entry string) error {
		userID := strings.TrimSuffix(entry, "/")
		if isUserIDReserved(userID) {
			return nil
		}

		users = append(users, userID)
		return nil
	})

	return users, err
}

// isUserIDReserved returns whether the provided user ID is reserved and can't be used for storing metrics.
func isUserIDReserved(name string) bool {
	return name == bucket.MimirInternalsPrefix
}
