// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/tsdb/users_scanner.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package tsdb

import (
	"context"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/objstore"
	"golang.org/x/sync/singleflight"

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

	listedUsers           atomic.Pointer[[]string]
	listings              singleflight.Group
	listingsCtx           context.Context
	stopListings          context.CancelFunc
	lastSuccessfulListing prometheus.Gauge
}

func NewUsersScanner(bucketClient objstore.Bucket, isOwned func(userID string) (bool, error), logger log.Logger, reg prometheus.Registerer) *UsersScanner {
	listingsCtx, stopListings := context.WithCancel(context.Background())
	return &UsersScanner{
		bucketClient: bucketClient,
		logger:       logger,
		isOwned:      isOwned,
		listingsCtx:  listingsCtx,
		stopListings: stopListings,
		lastSuccessfulListing: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "last_tenants_list_timestamp_seconds",
			Help: "Unix timestamp of the last successful listing of tenants.",
		}),
	}
}

// ScanUsers returns a list of users found in the storage, that are not marked for deletion,
// and list of users marked for deletion. If listing the storage takes longer than maxWait or fails,
// it uses the users found by the last successful listing, if any.
//
// If sharding is enabled, returned lists contains only the users owned by this instance.
func (s *UsersScanner) ScanUsers(ctx context.Context, maxWait time.Duration) (users, markedForDeletion []string, err error) {
	listed, err := s.listUsers(ctx, maxWait)
	if err != nil {
		return nil, nil, err
	}

	// Check users for being owned by instance, and split users into non-deleted and deleted.
	// We do these checks after listing all users, to improve cacheability of Iter (result is only cached at the end of Iter call).
	for _, userID := range listed {
		// Check if it's owned by this instance.
		owned, err := s.isOwned(userID)
		if err != nil {
			level.Warn(s.logger).Log("msg", "unable to check if user is owned by this shard", "user", userID, "err", err)
		} else if !owned {
			continue
		}

		deletionMarkExists, err := TenantDeletionMarkExists(ctx, s.bucketClient, userID)
		if err != nil {
			level.Warn(s.logger).Log("msg", "unable to check if user is marked for deletion", "user", userID, "err", err)
		} else if deletionMarkExists {
			markedForDeletion = append(markedForDeletion, userID)
			continue
		}

		users = append(users, userID)
	}

	return users, markedForDeletion, nil
}

func (s *UsersScanner) Stop() {
	s.stopListings()
	_, _, _ = s.listings.Do("", func() (any, error) { return nil, nil })
}

func (s *UsersScanner) listUsers(ctx context.Context, maxWait time.Duration) ([]string, error) {
	listing := s.listings.DoChan("", func() (any, error) {
		users, err := ListUsers(s.listingsCtx, s.bucketClient)
		if err != nil {
			return nil, err
		}

		s.listedUsers.Store(&users)
		s.lastSuccessfulListing.SetToCurrentTime()
		return users, nil
	})

	previous := s.listedUsers.Load()
	// maxWait is ignored if we have no previous results to return.
	var maxWaitExpired <-chan time.Time
	if previous != nil {
		maxWaitTimer := time.NewTimer(maxWait)
		defer maxWaitTimer.Stop()
		maxWaitExpired = maxWaitTimer.C
	}

	select {
	case result := <-listing:
		if result.Err == nil {
			return result.Val.([]string), nil
		}
		if previous == nil {
			return nil, result.Err
		}
		level.Warn(s.logger).Log("msg", "failed to list users, using the users found by the last successful listing", "err", result.Err)
	case <-maxWaitExpired:
		// The listing is too slow: fall back to the previous results.
		level.Warn(s.logger).Log("msg", "listing users takes longer than the max wait, using the users found by the last successful listing", "max_wait", maxWait)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return *previous, nil
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
