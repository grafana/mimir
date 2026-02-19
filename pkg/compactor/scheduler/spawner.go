// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/services"
	"github.com/thanos-io/objstore"

	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util"
)

// Spawner periodically creates plan jobs for tenants.
type Spawner struct {
	services.Service

	logger               log.Logger
	metrics              *schedulerMetrics
	clock                clock.Clock
	allowedTenants       *util.AllowList
	bkt                  objstore.Bucket
	jpm                  JobPersistenceManager
	planningInterval     time.Duration
	userDiscoveryBackoff backoff.Config
	rotator              *Rotator
	maxLeases            int

	planMap map[string]time.Time
}

func NewSpawner(
	cfg Config,
	allowList *util.AllowList,
	rotator *Rotator,
	bkt objstore.Bucket,
	jpm JobPersistenceManager,
	metrics *schedulerMetrics,
	logger log.Logger) *Spawner {
	s := &Spawner{
		logger:               logger,
		metrics:              metrics,
		clock:                clock.New(),
		allowedTenants:       allowList,
		bkt:                  bkt,
		jpm:                  jpm,
		planningInterval:     cfg.planningInterval,
		userDiscoveryBackoff: cfg.userDiscoveryBackoff,
		rotator:              rotator,
		planMap:              make(map[string]time.Time),
		maxLeases:            cfg.maxLeases,
	}
	s.Service = services.NewTimerService(cfg.planningCheckInterval, s.start, s.iter, nil)
	return s
}

func (s *Spawner) start(ctx context.Context) error {
	// The rotator gets prepoluated upon recovery, use that to determine tenants that are already active
	for tenant := range s.rotator.Tenants() {
		s.planMap[tenant] = time.Time{}
	}

	b := backoff.New(ctx, s.userDiscoveryBackoff)
	var err error
	for b.Ongoing() {
		err = s.discoverTenants(ctx)
		if err == nil {
			s.plan()
			return nil
		}
		b.Wait()
	}
	return fmt.Errorf("failed to discover users for the compactor scheduler: %w", err)
}

func (s *Spawner) iter(ctx context.Context) error {
	// Don't care if user discovery fails since we still want to submit plans for users that are known
	_ = s.discoverTenants(ctx)
	s.plan()
	return nil
}

func (s *Spawner) plan() {
	now := s.clock.Now()
	for tenant, lastSubmitted := range s.planMap {
		if now.Sub(lastSubmitted) > s.planningInterval {
			_, err := s.rotator.OfferPlanJob(tenant, NewTrackedPlanJob(now))
			if err != nil {
				level.Warn(s.logger).Log("msg", "failed submitting plan job", "tenant", tenant, "err", err)
				continue
			}
			s.planMap[tenant] = now
		}
	}
}

func (s *Spawner) discoverTenants(ctx context.Context) error {
	tenants, err := mimir_tsdb.ListUsers(ctx, s.bkt)
	if err != nil {
		level.Warn(s.logger).Log("msg", "failed tenant discovery", "err", err)
		return err
	}

	seen := make(map[string]struct{}, len(tenants))

	for _, tenant := range tenants {
		if !s.allowedTenants.IsAllowed(tenant) {
			continue
		}

		seen[tenant] = struct{}{}

		if _, ok := s.planMap[tenant]; !ok {
			// Discovered a new tenant
			persister, err := s.jpm.InitializeTenant(tenant)
			if err != nil {
				level.Warn(s.logger).Log("msg", "failed initializing tenant", "tenant", tenant, "err", err)
				return err
			}
			tracker := NewJobTracker(persister, tenant, s.clock, s.maxLeases, s.metrics.newTrackerMetricsForTenant(tenant))
			s.rotator.AddTenant(tenant, tracker)
			s.planMap[tenant] = time.Time{}
		}
	}

	for tenant := range s.planMap {
		if _, ok := seen[tenant]; !ok {
			tracker, ok := s.rotator.RemoveTenant(tenant)
			if !ok {
				level.Warn(s.logger).Log("msg", "attempted to remove tenant from rotator, but the tenant was unexpectedly missing", "tenant", tenant)
			}
			err = s.jpm.DeleteTenant(tenant)
			if err != nil {
				level.Warn(s.logger).Log("msg", "failed removing tenant bucket from compactor scheduler", "tenant", tenant, "err", err)
				if ok {
					// Preserve 1:1 with rotator and planMap
					s.rotator.AddTenant(tenant, tracker)
				}
				continue
			}
			delete(s.planMap, tenant)
			level.Info(s.logger).Log("msg", "removed empty tenant from compactor scheduler", "tenant", tenant)
		}
	}

	return nil
}
