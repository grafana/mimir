package scheduler

import (
	"context"
	"time"

	"github.com/grafana/dskit/services"
	"github.com/thanos-io/objstore"

	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util"
)

type PlanInformation struct {
	lastSubmitted time.Time
	lastCompleted time.Time
	jobs []*Job
}

type PlanSpawner struct {
	services.Service

	planMap map[string]*PlanInformation

	allowedTenants *util.AllowList
	bkt            objstore.Bucket
	jobQueue       *JobQueue

	checkInterval     time.Duration
	planningInterval  time.Duration
	logger log.Logger
}

func (p *PlanSpawner) start(ctx context.Context) error {
}

func (p *PlanSpawner) run(ctx context.Context) error {
	checkTicker := time.NewTicker(p.checkInterval)

	for {
		select {
		case t := <-checkTicker.C:
			// Don't care if user discovery fails since we still want to submit plans for users that are known
			_ := p.discoverTenants(ctx)
			p.plan()
		case <-ctx.Done():
			return nil
		}
	}
}

func (p *PlanSpawner) stop(_ error) error {
	return nil
}

func (p *PlanSpawner) plan() {

	for tenant, info := p.planMap {

	}
	
}

func (p *PlanSpawner) discoverTenants(ctx context.Context) error {
	tenants, err := mimir_tsdb.ListUsers(ctx, p.bkt)
	if err != nil {
		level.Error(p.logger).Log("msg", "failed tenant discovery", "err", err)
		return err
	}

	seen := make(map[string]struct{}, len(tenants))

	for _, tenant := range tenants {
		if !p.allowedTenants.IsAllowed(tenant) {
			continue
		}

		seen[tenant] = struct{}{}

		if _, ok := p.planMap[tenant]; !ok {
			// Discovered a new tenant
			p.planMap[tenant] = &PlanInformation{}
		}
	}

	for tenant := range p.planMap {
		if _, ok := seen[tenant]; !ok {
			// Tenant is no longer present
			delete(p.planMap, tenant)
		}
	}

	level.Info(p.logger).Log("msg", "ran tenant discovery", "tenants", len(p.planMap))

	return nil
}
