package scheduler

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"

	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util"
)

type SpawnInformation struct {
	lastPlanSubmitted time.Time
	lastPlanCompleted time.Time // TODO: Feedback to know this
}

type Spawner struct {
	services.Service

	planMap map[string]*SpawnInformation

	allowedTenants *util.AllowList
	rotator        *Rotator
	bkt            objstore.Bucket

	leaseCheckInterval    time.Duration
	planningCheckInterval time.Duration
	planningInterval      time.Duration

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
		allowedTenants:        allowList,
		rotator:               rotator,
		bkt:                   bkt,
		leaseCheckInterval:    cfg.leaseCheckInterval,
		planningCheckInterval: cfg.planningCheckInterval,
		planningInterval:      cfg.planningInterval,
		userDiscoveryBackoff:  cfg.userDiscoveryBackoff,
	}
	s.Service = services.NewBasicService(s.start, s.run, nil)
	return s
}

func (s *Spawner) start(ctx context.Context) error {
	b := backoff.New(ctx, s.userDiscoveryBackoff)
	var err error
	for b.Ongoing() {
		err = s.discoverTenants(ctx)
		if err == nil {
			break
		}
		b.Wait()
	}
	return errors.Wrap(err, "failed to discover users for the compactor scheduler")
}

func (s *Spawner) run(ctx context.Context) error {
	leaseCheckTicker := time.NewTicker(s.leaseCheckInterval)
	planningCheckTicker := time.NewTicker(s.planningCheckInterval)

	// Do not wait to submit the first plan jobs
	s.plan()

	for {
		select {
		case <-leaseCheckTicker.C:
		case <-planningCheckTicker.C:
			// Don't care if user discovery fails since we still want to submit plans for users that are known
			_ = s.discoverTenants(ctx)
			s.plan()
		case <-ctx.Done():
			return nil
		}
	}
}

func (s *Spawner) plan() {
	for tenant, info := range s.planMap {
		now := time.Now() // declaring inside the loop because locks are used when submitting plans
		if now.Sub(info.lastPlanSubmitted) > s.planningInterval {
			job := NewJob("plan", &CompactionJob{}, now)
			accepted := s.rotator.OfferJobs(tenant, []*Job[string, *CompactionJob]{job}, func(_, _ *CompactionJob) bool {
				return false
			})
			if accepted == 1 {
				info.lastPlanSubmitted = job.creationTime
			}
		}
	}
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
			s.planMap[tenant] = &SpawnInformation{}
		}
	}

	for tenant := range s.planMap {
		if _, ok := seen[tenant]; !ok {
			s.rotator.RemoveTenant(tenant)
			delete(s.planMap, tenant)
		}
	}

	level.Info(s.logger).Log("msg", "ran tenant discovery", "tenants", len(s.planMap))

	return nil
}
