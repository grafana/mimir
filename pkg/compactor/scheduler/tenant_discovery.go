// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"
	"fmt"

	"github.com/benbjohnson/clock"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/services"
	"github.com/thanos-io/objstore"

	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util"
)

// TenantDiscoverer periodically scans the bucket for new tenants.
type TenantDiscoverer struct {
	services.Service

	logger                         log.Logger
	metrics                        *schedulerMetrics
	clock                          clock.Clock
	allowedTenants                 *util.AllowList
	bkt                            objstore.Bucket
	jpm                            JobPersistenceManager
	userDiscoveryBackoff           backoff.Config
	rotator                        *Rotator
	maxLeases                      int
	repeatedFailureReportThreshold int
	knownTenants                   map[string]struct{}
}

func NewTenantDiscoverer(
	cfg Config,
	allowList *util.AllowList,
	rotator *Rotator,
	bkt objstore.Bucket,
	jpm JobPersistenceManager,
	metrics *schedulerMetrics,
	logger log.Logger) *TenantDiscoverer {
	s := &TenantDiscoverer{
		logger:                         logger,
		metrics:                        metrics,
		clock:                          clock.New(),
		allowedTenants:                 allowList,
		bkt:                            bkt,
		jpm:                            jpm,
		userDiscoveryBackoff:           cfg.UserDiscoveryBackoff,
		rotator:                        rotator,
		maxLeases:                      cfg.MaxLeases,
		repeatedFailureReportThreshold: cfg.RepeatedFailureReportThreshold,
		knownTenants:                   make(map[string]struct{}),
	}
	s.Service = services.NewTimerService(cfg.TenantDiscoveryInterval, s.start, s.iter, nil)
	return s
}

// RecoverFrom populates the tenant discoverer with known tenants from recovered state.
// Must be called before the service is started.
func (s *TenantDiscoverer) RecoverFrom(jobTrackers map[string]*JobTracker) {
	for tenant := range jobTrackers {
		s.knownTenants[tenant] = struct{}{}
	}
}

func (s *TenantDiscoverer) start(ctx context.Context) error {
	b := backoff.New(ctx, s.userDiscoveryBackoff)
	var err error
	for b.Ongoing() {
		err = s.discoverTenants(ctx)
		if err == nil {
			return nil
		}
		b.Wait()
	}
	return fmt.Errorf("failed to discover users for the compactor scheduler: %w", err)
}

func (s *TenantDiscoverer) iter(ctx context.Context) error {
	_ = s.discoverTenants(ctx)
	return nil
}

func (s *TenantDiscoverer) discoverTenants(ctx context.Context) error {
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

		if _, ok := s.knownTenants[tenant]; !ok {
			// Discovered a new tenant
			persister, err := s.jpm.InitializeTenant(tenant)
			if err != nil {
				level.Warn(s.logger).Log("msg", "failed initializing tenant", "user", tenant, "err", err)
				continue
			}
			tracker := NewJobTracker(persister, tenant, s.clock, s.maxLeases, s.repeatedFailureReportThreshold, s.metrics.newTrackerMetricsForTenant(tenant), s.logger)
			s.rotator.AddTenant(tenant, tracker)
			s.knownTenants[tenant] = struct{}{}
		}
	}

	for tenant := range s.knownTenants {
		if _, ok := seen[tenant]; !ok {
			// A tenant no longer has blocks
			logger := log.With(s.logger, "user", tenant)
			tracker, ok := s.rotator.RemoveTenant(tenant)
			if !ok {
				level.Warn(logger).Log("msg", "attempted to remove tenant from rotator, but the tenant was unexpectedly missing")
			}
			err = s.jpm.DeleteTenant(tenant)
			if err != nil {
				level.Warn(logger).Log("msg", "failed removing tenant bucket from compactor scheduler", "err", err)
				if ok {
					// Preserve 1:1 with rotator and knownTenants
					s.rotator.AddTenant(tenant, tracker)
				}
				continue
			}
			delete(s.knownTenants, tenant)
			s.metrics.deleteTenantMetrics(tenant)
			level.Info(logger).Log("msg", "removed empty tenant from compactor scheduler")
		}
	}

	return nil
}
