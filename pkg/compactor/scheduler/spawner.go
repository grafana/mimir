package scheduler

import (
	"context"
	"time"

	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/services"
	"github.com/thanos-io/objstore"

	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util"
)

type SpawnInformation struct {
	lastPlanSubmitted time.Time
	lastPlanCompleted time.Time
}

type Spawner struct {
	services.Service

	planMap map[string]*PlanInformation

	allowedTenants *util.AllowList
	rotator        *Rotator
	bkt            objstore.Bucket

	leaseCheckInterval     time.Duration
	planningInterval  time.Duration
	userDiscoveryBackoff backoff.Config
	logger log.Logger
}

func NewSpawner(
	 cfg Config,
	 allowList *util.AllowList,
	 rotator *Rotator,
	 bkt objstore.Bucket,
	 logger log.Logger) *Spawner {
		s := &Spawner{
			allowedTenants: allowList,
			rotator: rotator,
			bkt: bkt,
			leaseCheckInterval: cfg.leaseCheckInterval,
			planningInterval: cfg.planningInterval,
			userDiscoveryBackoff: cfg.userDiscoveryBackoff,
		}
		s.Service = services.NewBasicService(s.start, s.run, nil)
}

func (s *Spawner) start(ctx context.Context) error {
	b := backoff.New(ctx, s.userDiscoveryBackoff)
	for b.Ongoing() {
		err := s.discoverTenants(ctx)
		if err == nil {
			break
		}
		b.Wait()
	}
}

func (s *Spawner) run(ctx context.Context) error {
	leaseCheckTicker := time.NewTicker(s.leaseCheckInterval)
	planningTicker := time.NewTicker(s.planningInterval)

	// Do not wait to submit the first plan jobs
	s.plan()

	for {
		select {
		case <-leaseCheckTicker.C:

		case <-s.planningInterval.C:
			// Don't care if user discovery fails since we still want to submit plans for users that are known
			_ := s.discoverTenants(ctx)
			s.plan()
		case <-ctx.Done():
			return nil
		}
	}
}

func (s *Spawner) plan() {

	for tenant, info := s.planMap {
		if info.

	}
	
}

func (s *Spawner) discoverTenants(ctx context.Context) error {
	tenants, err := mimir_tsdb.ListUsers(ctx, p.bkt)
	if err != nil {
		level.Error(p.logger).Log("msg", "failed tenant discovery", "err", err)
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
			s.planMap[tenant] = &PlanInformation{}
		}
	}

	for tenant := range p.planMap {
		if _, ok := seen[tenant]; !ok {
			// Tenant is no longer present
			// TODO: Inform rotator?
			delete(s.planMap, tenant)
		}
	}

	level.Info(p.logger).Log("msg", "ran tenant discovery", "tenants", len(p.planMap))

	return nil
}
