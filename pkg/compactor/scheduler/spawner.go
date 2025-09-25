// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/services"
	"github.com/thanos-io/objstore"

	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util"
)

type Spawner struct {
	services.Service

	planMap map[string]time.Time

	allowedTenants *util.AllowList
	planTracker    *JobTracker[string, struct{}]
	rotator        *Rotator
	bkt            objstore.Bucket

	planningInterval time.Duration

	userDiscoveryBackoff backoff.Config

	logger log.Logger
}

func NewSpawner(
	cfg Config,
	allowList *util.AllowList,
	rotator *Rotator,
	planTracker *JobTracker[string, struct{}],
	bkt objstore.Bucket,
	logger log.Logger) *Spawner {
	s := &Spawner{
		allowedTenants:       allowList,
		rotator:              rotator,
		planTracker:          planTracker,
		bkt:                  bkt,
		planningInterval:     cfg.planningInterval,
		userDiscoveryBackoff: cfg.userDiscoveryBackoff,
	}
	s.Service = services.NewTimerService(cfg.planningCheckInterval, s.start, s.iter, nil)
	return s
}

func (s *Spawner) start(ctx context.Context) error {
	b := backoff.New(ctx, s.userDiscoveryBackoff)
	var err error
	for b.Ongoing() {
		err = s.discoverTenants(ctx)
		if err == nil {
			s.plan()
			break
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
	jobs := make([]*Job[string, struct{}], 0, len(s.planMap))
	now := time.Now()
	for tenant, lastSubmitted := range s.planMap {
		if now.Sub(lastSubmitted) > s.planningInterval {
			jobs = append(jobs, NewJob(tenant, struct{}{}, now))
			s.planMap[tenant] = now
		}
	}

	// TODO: Can track how many were actually accepted with metrics
	s.planTracker.Offer(
		jobs,
		func(_ struct{}, _ struct{}) bool {
			return false
		},
	)
}

func (s *Spawner) discoverTenants(ctx context.Context) error {
	tenants, err := mimir_tsdb.ListUsers(ctx, s.bkt)
	if err != nil {
		level.Error(s.logger).Log("msg", "failed tenant discovery", "err", err)
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
			s.planMap[tenant] = time.Time{}
		}
	}

	for tenant := range s.planMap {
		if _, ok := seen[tenant]; !ok {
			level.Info(s.logger).Log("msg", "removing empty tenant from compactor scheduler", "tenant", tenant)
			s.planTracker.RemoveForcefully(tenant)
			s.rotator.RemoveTenant(tenant)
			delete(s.planMap, tenant)
		}
	}

	return nil
}
