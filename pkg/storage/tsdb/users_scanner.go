// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/tsdb/users_scanner.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package tsdb

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/concurrency"
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
	// bucketClients are the read buckets, in read order.
	bucketClients []objstore.Bucket
	logger        log.Logger
	isOwned       func(userID string) (bool, error)
	// maxWait is how long ScanUsers waits for a new listing before it uses the users found by the
	// last successful listing. It is ignored if there is no successful listing yet.
	maxWait time.Duration

	listedUsers           atomic.Pointer[usersListing]
	listings              singleflight.Group
	listingsCtx           context.Context
	stopListings          context.CancelCauseFunc
	lastSuccessfulListing prometheus.Gauge
}

// usersListing holds the users found by a listing of the buckets.
type usersListing struct {
	users []string

	// bucketsPerUser holds the indexes of the buckets that contain each user. It is nil if there is only one bucket.
	bucketsPerUser map[string][]int
}

// NewUsersScanner returns a scanner for the users of the buckets. The bucketClients are in read order.
func NewUsersScanner(bucketClients []objstore.Bucket, isOwned func(userID string) (bool, error), maxWait time.Duration, logger log.Logger, reg prometheus.Registerer) *UsersScanner {
	listingsCtx, stopListings := context.WithCancelCause(context.Background())
	return &UsersScanner{
		bucketClients: bucketClients,
		logger:        logger,
		isOwned:       isOwned,
		maxWait:       maxWait,
		listingsCtx:   listingsCtx,
		stopListings:  stopListings,
		lastSuccessfulListing: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "last_tenants_list_timestamp_seconds",
			Help: "Unix timestamp of the last successful listing of tenants.",
		}),
	}
}

// ScanUsers returns a list of users found in the storage, that are not marked for deletion,
// and list of users marked for deletion. A user is marked for deletion if one of its buckets has the
// tenant deletion mark. If listing the storage takes longer than the max wait or fails,
// it uses the users found by the last successful listing, if any.
//
// With more than one bucket, bucketsPerUser holds the indexes of the buckets that contain each user,
// in increasing order. With one bucket, bucketsPerUser is nil.
//
// If sharding is enabled, returned lists contains only the users owned by this instance.
func (s *UsersScanner) ScanUsers(ctx context.Context) (users, markedForDeletion []string, bucketsPerUser map[string][]int, err error) {
	listed, err := s.listUsers(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	// Check users for being owned by instance, and split users into non-deleted and deleted.
	// We do these checks after listing all users, to improve cacheability of Iter (result is only cached at the end of Iter call).
	for _, userID := range listed.users {
		// Check if it's owned by this instance.
		owned, err := s.isOwned(userID)
		if err != nil {
			level.Warn(s.logger).Log("msg", "unable to check if user is owned by this shard", "user", userID, "err", err)
		} else if !owned {
			continue
		}

		deletionMarkExists, err := TenantDeletionMarkExistsInAnyBucket(ctx, s.userBucketClients(listed, userID), userID)
		if err != nil {
			level.Warn(s.logger).Log("msg", "unable to check if user is marked for deletion", "user", userID, "err", err)
		} else if deletionMarkExists {
			markedForDeletion = append(markedForDeletion, userID)
			continue
		}

		users = append(users, userID)
	}

	return users, markedForDeletion, listed.bucketsPerUser, nil
}

// userBucketClients returns the clients of the buckets that contain the user, in read order.
func (s *UsersScanner) userBucketClients(listed *usersListing, userID string) []objstore.Bucket {
	if listed.bucketsPerUser == nil {
		return s.bucketClients
	}

	clients := make([]objstore.Bucket, 0, len(listed.bucketsPerUser[userID]))
	for _, idx := range listed.bucketsPerUser[userID] {
		clients = append(clients, s.bucketClients[idx])
	}
	return clients
}

func (s *UsersScanner) Stop() {
	s.stopListings(errors.New("users scanner stopped"))
	// listings.Do waits until the running listing, if any, returns, ensuring we've stopped.
	// Failing explicitly ensures that a concurrent ScanUsers call that joins it doesn't use its result.
	_, _, _ = s.listings.Do("", func() (any, error) { return nil, context.Canceled })
}

func (s *UsersScanner) listUsers(ctx context.Context) (*usersListing, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	listing := s.listings.DoChan("", func() (any, error) {
		listed, err := s.listUsersInBuckets(s.listingsCtx)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				level.Warn(s.logger).Log("msg", "failed to list users", "err", err)
			}
			return nil, err
		}

		s.listedUsers.Store(listed)
		s.lastSuccessfulListing.SetToCurrentTime()
		return listed, nil
	})

	previous := s.listedUsers.Load()
	// maxWait is ignored if we have no previous results to return.
	var maxWaitExpired <-chan time.Time
	if previous != nil {
		maxWaitTimer := time.NewTimer(s.maxWait)
		defer maxWaitTimer.Stop()
		maxWaitExpired = maxWaitTimer.C
	}

	select {
	case result := <-listing:
		if result.Err == nil {
			return result.Val.(*usersListing), nil
		}
		if previous == nil {
			return nil, result.Err
		}
	case <-maxWaitExpired:
		// The listing is too slow: fall back to the previous results.
		level.Warn(s.logger).Log("msg", "listing users takes longer than the max wait, using the users found by the last successful listing", "max_wait", s.maxWait)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return previous, nil
}

func (s *UsersScanner) listUsersInBuckets(ctx context.Context) (*usersListing, error) {
	if len(s.bucketClients) == 1 {
		users, err := ListUsers(ctx, s.bucketClients[0])
		if err != nil {
			return nil, err
		}
		return &usersListing{users: users}, nil
	}

	bucketsPerUser, err := ListUsersInBuckets(ctx, s.bucketClients)
	if err != nil {
		return nil, err
	}
	return &usersListing{users: slices.Sorted(maps.Keys(bucketsPerUser)), bucketsPerUser: bucketsPerUser}, nil
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

// ListUsersInBuckets lists the users of each bucket. For each user, it returns the indexes of the buckets
// that contain the user, in increasing order. It returns an error if it can't list one of the buckets.
func ListUsersInBuckets(ctx context.Context, bucketClients []objstore.Bucket) (map[string][]int, error) {
	usersPerBucket := make([][]string, len(bucketClients))
	err := concurrency.ForEachJob(ctx, len(bucketClients), len(bucketClients), func(ctx context.Context, idx int) error {
		users, err := ListUsers(ctx, bucketClients[idx])
		if err != nil {
			return err
		}
		usersPerBucket[idx] = users
		return nil
	})
	if err != nil {
		return nil, err
	}

	bucketsPerUser := map[string][]int{}
	for idx, users := range usersPerBucket {
		for _, userID := range users {
			bucketsPerUser[userID] = append(bucketsPerUser[userID], idx)
		}
	}
	return bucketsPerUser, nil
}

// isUserIDReserved returns whether the provided user ID is reserved and can't be used for storing metrics.
func isUserIDReserved(name string) bool {
	return name == bucket.MimirInternalsPrefix
}
