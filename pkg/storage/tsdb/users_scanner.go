// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/tsdb/users_scanner.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package tsdb

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
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

	listedUsers           atomic.Pointer[[]string]
	runningListingMtx     sync.Mutex
	runningListing        *usersListing
	listings              sync.WaitGroup
	listingsFailed        prometheus.Counter
	lastSuccessfulListing prometheus.Gauge
}

func NewUsersScanner(bucketClient objstore.Bucket, isOwned func(userID string) (bool, error), logger log.Logger, reg prometheus.Registerer) *UsersScanner {
	return &UsersScanner{
		bucketClient: bucketClient,
		logger:       logger,
		isOwned:      isOwned,
		listingsFailed: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_compactor_block_cleanup_tenant_discovery_failed_total",
			Help: "Total number of failed tenant discoveries run by the blocks cleaner.",
		}),
		lastSuccessfulListing: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_compactor_block_cleanup_tenant_discovery_last_successful_run_timestamp_seconds",
			Help: "Unix timestamp of the last successful tenant discovery run by the blocks cleaner.",
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

func (s *UsersScanner) Wait() {
	s.listings.Wait()
}

type usersListing struct {
	done  chan struct{}
	users []string
	err   error
}

func (s *UsersScanner) listUsers(ctx context.Context, maxWait time.Duration) ([]string, error) {
	listing := s.startOrJoinListing(ctx)
	previous := s.listedUsers.Load()
	if previous == nil {
		select {
		case <-listing.done:
			return listing.users, listing.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	maxWaitTimer := time.NewTimer(maxWait)
	defer maxWaitTimer.Stop()

	select {
	case <-listing.done:
		if listing.err == nil {
			return listing.users, nil
		}
		level.Warn(s.logger).Log("msg", "failed to list users, using the users found by the last successful listing", "err", listing.err)
	case <-maxWaitTimer.C:
		level.Warn(s.logger).Log("msg", "listing users takes longer than the max wait, using the users found by the last successful listing", "max_wait", maxWait)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return *previous, nil
}

func (s *UsersScanner) startOrJoinListing(ctx context.Context) *usersListing {
	s.runningListingMtx.Lock()
	defer s.runningListingMtx.Unlock()
	if s.runningListing != nil {
		return s.runningListing
	}

	listing := &usersListing{done: make(chan struct{})}
	s.runningListing = listing
	s.listings.Go(func() {
		listing.users, listing.err = ListUsers(ctx, s.bucketClient)
		if listing.err == nil {
			s.listedUsers.Store(&listing.users)
			s.lastSuccessfulListing.SetToCurrentTime()
		} else if !errors.Is(listing.err, context.Canceled) {
			s.listingsFailed.Inc()
		}

		s.runningListingMtx.Lock()
		s.runningListing = nil
		s.runningListingMtx.Unlock()
		close(listing.done)
	})
	return listing
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
