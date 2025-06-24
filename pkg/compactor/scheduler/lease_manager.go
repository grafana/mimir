package scheduler

import (
	"context"
	"sync"
	"time"

	"github.com/grafana/dskit/services"
)

type lease struct {
	creationTime   time.Time
	expirationTime time.Time
	onExpiration   func()
}

func (l *lease) isExpired(currentTime time.Time) bool {
	return currentTime.After(l.expirationTime)
}

type LeaseManager[K comparable] struct {
	services.Service

	leases map[K]*lease
	mtx    *sync.Mutex

	reclaimationInterval time.Duration
}

func NewLeaseManager[K comparable](reclaimationInterval time.Duration) *LeaseManager[K] {
	return &LeaseManager[K]{
		leases:               make(map[K]*lease),
		mtx:                  &sync.Mutex{},
		reclaimationInterval: reclaimationInterval,
	}
}

func (lm *LeaseManager[K]) start(ctx context.Context) error {
	return nil
}

func (lm *LeaseManager[K]) run(ctx context.Context) error {
	for {
		select {
		case <-time.After(lm.reclaimationInterval):
			lm.reclaim()
		case <-ctx.Done():
			return nil
		}
	}
}

func (lm *LeaseManager[K]) stop(err error) error {
	return nil
}

func (lm *LeaseManager[K]) reclaim() {
	lm.mtx.Lock()

	var expirationFunctions []func()
	now := time.Now()

	for k, lease := range lm.leases {
		if lease.isExpired(now) {
			expirationFunctions = append(expirationFunctions, lease.onExpiration)
			delete(lm.leases, k)
		}
	}

	// Drop the lock before calling expiration functions
	lm.mtx.Unlock()

	for _, f := range expirationFunctions {
		f()
	}
}

func (lm *LeaseManager[K]) AddLease(k K, leaseDuration time.Duration, onExpiration func()) bool {
	lm.mtx.Lock()
	defer lm.mtx.Unlock()

	if _, ok := lm.leases[k]; ok {
		// already existed
		return false
	}

	if leaseDuration <= 0 {
		// Non-positive leases are not allowed
		return false
	}

	now := time.Now()

	lm.leases[k] = &lease{
		creationTime:   now,
		expirationTime: now.Add(leaseDuration),
		onExpiration:   onExpiration,
	}

	return true
}

// RenewLease renews the lease identified by k.
// If true is returned then the lease existed and was renewed.
// If false is returned then the lease either did not exist or already expired.
func (lm *LeaseManager[K]) RenewLease(k K, leaseDuration time.Duration, onExpiration func()) bool {
	lm.mtx.Lock()
	defer lm.mtx.Unlock()

	lease, ok := lm.leases[k]
	if !ok {
		// can only renew existing leases
		return false
	}

	if leaseDuration <= 0 {
		// Non-positive leases are not allowed
		return false
	}

	lease.expirationTime = time.Now().Add(leaseDuration)

	return false
}

// CancelLease cancels the lease identified by k.
// If true is returned then the lease existed and was cancelled.
// If false is returned then the lease either did not exist or already expired.
func (lm *LeaseManager[K]) CancelLease(k K) bool {
	lm.mtx.Lock()
	defer lm.mtx.Unlock()

	if _, ok := lm.leases[k]; ok {
		delete(lm.leases, k)
		return true
	}

	return false
}
