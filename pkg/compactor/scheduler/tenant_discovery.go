// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"
	"fmt"
	"strings"

	"github.com/benbjohnson/clock"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/tenant"
	"github.com/thanos-io/objstore"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/storage/bucket"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util"
)

const (
	discoveryPollBlocks    = "blocks"    // pulls the top-level of a blocks bucket
	discoveryPollBackfills = "backfills" // polls the backfill prefix for tenants that have any phase marker
)

// discoverySources is a set of the polls that found a tenant
type discoverySources uint32

const (
	discoveredByBlocks discoverySources = 1 << iota
	discoveredByBackfills
)

type discoveryPoll struct {
	source discoverySources
	list   func(context.Context, objstore.Bucket) ([]string, error)
}

// TenantDiscoverer periodically scans the bucket for new tenants.
type TenantDiscoverer struct {
	services.Service

	logger                         log.Logger
	metrics                        *schedulerMetrics
	clock                          clock.Clock
	lanePolicy                     lanePolicy
	allowedTenants                 *util.AllowList
	bkt                            objstore.Bucket
	jpm                            JobPersistenceManager
	tenantDiscoveryBackoff         backoff.Config
	rotator                        *Rotator
	maxLeases                      int
	repeatedFailureReportThreshold int
	knownTenants                   map[string]*JobTracker
	polls                          []discoveryPoll
}

func NewTenantDiscoverer(
	cfg Config,
	lanePolicy lanePolicy,
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
		lanePolicy:                     lanePolicy,
		allowedTenants:                 allowList,
		bkt:                            bkt,
		jpm:                            jpm,
		tenantDiscoveryBackoff:         cfg.TenantDiscoveryBackoff,
		rotator:                        rotator,
		maxLeases:                      cfg.MaxLeases,
		repeatedFailureReportThreshold: cfg.RepeatedFailureReportThreshold,
		knownTenants:                   make(map[string]*JobTracker),
	}
	pollsByName := map[string]discoveryPoll{
		discoveryPollBlocks:    {source: discoveredByBlocks, list: mimir_tsdb.ListUsers},
		discoveryPollBackfills: {source: discoveredByBackfills, list: listBackfillTenants},
	}
	for _, name := range cfg.DiscoveryPolls {
		s.polls = append(s.polls, pollsByName[name])
	}
	s.Service = services.NewTimerService(cfg.TenantDiscoveryInterval, s.start, s.iter, nil)
	return s
}

// RecoverFrom populates the tenant discoverer with known tenants from recovered state.
func (s *TenantDiscoverer) RecoverFrom(jobTrackers map[string]*JobTracker) {
	for tenant, tracker := range jobTrackers {
		s.knownTenants[tenant] = tracker
	}
}

// start starts the tenant discovery service. It is expected that RecoverFrom is called before starting the service.
func (s *TenantDiscoverer) start(ctx context.Context) error {
	b := backoff.New(ctx, s.tenantDiscoveryBackoff)
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
	tenants, err := s.pollTenants(ctx)
	if err != nil {
		level.Warn(s.logger).Log("msg", "failed tenant discovery", "err", err)
		return err
	}

	seen := make(map[string]struct{}, len(tenants))

	for tenant, sources := range tenants {
		if !s.allowedTenants.IsAllowed(tenant) {
			continue
		}

		seen[tenant] = struct{}{}

		tracker, ok := s.knownTenants[tenant]
		if !ok {
			// Discovered a new tenant
			persister, err := s.jpm.InitializeTenant(tenant)
			if err != nil {
				level.Warn(s.logger).Log("msg", "failed initializing tenant", "user", tenant, "err", err)
				continue
			}
			tracker = NewJobTracker(persister, tenant, sources, s.clock, s.lanePolicy, s.maxLeases, s.repeatedFailureReportThreshold, s.metrics.newTrackerMetricsForTenant(tenant), s.logger)
			s.rotator.AddTenant(tenant, tracker)
			s.knownTenants[tenant] = tracker
		} else if discoverySources(tracker.discoveredBy.Load()) != sources {
			// There's no TOCTOU to worry about here since dsicovery is the only writer after construction
			tracker.discoveredBy.Store(uint32(sources))
		}
	}

	for tenant := range s.knownTenants {
		if _, ok := seen[tenant]; !ok {
			// A tenant is no longer found by any poll
			logger := log.With(s.logger, "user", tenant)
			tracker, ok := s.rotator.RemoveTenant(tenant)
			if !ok {
				level.Warn(logger).Log("msg", "attempted to remove tenant from rotator, but the tenant was unexpectedly missing")
				continue
			}
			err := tracker.persister.Drop()
			if err != nil {
				level.Warn(logger).Log("msg", "failed removing tenant bucket from compactor scheduler", "err", err)
				// Preserve 1:1 with rotator and knownTenants
				s.rotator.AddTenant(tenant, tracker)
				continue
			}
			delete(s.knownTenants, tenant)
			tracker.CleanupMetrics()
			level.Info(logger).Log("msg", "removed empty tenant from compactor scheduler")
		}
	}

	return nil
}

// pollTenants concurrently runs every poll and returns the union of the tenants they found, along with which polls found each.
// An error is returned if any poll fails
func (s *TenantDiscoverer) pollTenants(ctx context.Context) (map[string]discoverySources, error) {
	results := make([][]string, len(s.polls))
	g, gCtx := errgroup.WithContext(ctx)
	for i, poll := range s.polls {
		g.Go(func() (err error) {
			results[i], err = poll.list(gCtx, s.bkt)
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	tenants := make(map[string]discoverySources)
	for i, result := range results {
		for _, tenant := range result {
			tenants[tenant] |= s.polls[i].source
		}
	}
	return tenants, nil
}

const backfillPhasesPrefix = bucket.MimirInternalsPrefix + "/backfill/phases/"

// listBackfillTenants lists the tenants that have backfill phase markers
func listBackfillTenants(ctx context.Context, bkt objstore.Bucket) (tenants []string, err error) {
	err = bkt.Iter(ctx, backfillPhasesPrefix, func(entry string) error {
		tenantID, ok := strings.CutSuffix(strings.TrimPrefix(entry, backfillPhasesPrefix), objstore.DirDelim)
		if !ok || tenant.ValidTenantID(tenantID) != nil {
			return nil
		}
		tenants = append(tenants, tenantID)
		return nil
	})
	return tenants, err
}
