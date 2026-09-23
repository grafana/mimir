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
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/util"
)

/* Bucket structure (may change later)
phases/ <-- indicates "this phase is now active", can be polled with recursive listing
  backfill/ <-- each phase prefix can have a tenant object underneath
  validate/
  compact/
  copy/
  cleanup/
data/
  <backfill ULID>/
    <block ULID>/ <-- same meta swap logic for validation
*/

// phasesPrefix is the prefix holding one object per tenant participating in a phase.
const phasesPrefix = "phases/"

type backfillPhase string

const (
	phaseUnknown  backfillPhase = ""
	phaseBackfill backfillPhase = "backfill"
	phaseValidate backfillPhase = "validate"
	phaseCompact  backfillPhase = "compact"
	phaseCopy     backfillPhase = "copy"
	phaseCleanup  backfillPhase = "cleanup"
)

// order is the position of the phase in a backfill's progression, or 0 if it is unknown
func (b backfillPhase) order() int {
	switch b {
	case phaseBackfill:
		return 1
	case phaseValidate:
		return 2
	case phaseCompact:
		return 3
	case phaseCopy:
		return 4
	case phaseCleanup:
		return 5
	default:
		return 0
	}
}

// isAfter reports whether b comes later than o in a backfill's progression
func (b backfillPhase) isAfter(o backfillPhase) bool { return b.order() > o.order() }

// BackfillDiscoverer periodically scans the bucket for backfill phase transitions
type BackfillDiscoverer struct {
	services.Service

	logger                         log.Logger
	metrics                        *schedulerMetrics
	clock                          clock.Clock
	lanePolicy                     lanePolicy
	allowedTenants                 *util.AllowList
	bkt                            objstore.Bucket
	jpm                            JobPersistenceManager
	backfillDiscoveryBackoff       backoff.Config
	rotator                        *Rotator
	maxLeases                      int
	repeatedFailureReportThreshold int

	// knownBackfills is every backfill already added to the rotator, keyed by tenant
	knownBackfills map[string]*JobTracker
}

func NewBackfillDiscoverer(
	cfg Config,
	lanePolicy lanePolicy,
	allowList *util.AllowList,
	rotator *Rotator,
	bkt objstore.Bucket,
	jpm JobPersistenceManager,
	metrics *schedulerMetrics,
	logger log.Logger) *BackfillDiscoverer {
	s := &BackfillDiscoverer{
		logger:                         logger,
		metrics:                        metrics,
		clock:                          clock.New(),
		lanePolicy:                     lanePolicy,
		allowedTenants:                 allowList,
		bkt:                            bkt,
		jpm:                            jpm,
		backfillDiscoveryBackoff:       cfg.BackfillDiscoveryBackoff,
		rotator:                        rotator,
		maxLeases:                      cfg.MaxLeases,
		repeatedFailureReportThreshold: cfg.RepeatedFailureReportThreshold,
		knownBackfills:                 make(map[string]*JobTracker),
	}
	s.Service = services.NewTimerService(cfg.BackfillDiscoveryInterval, s.start, s.iter, nil)
	return s
}

// RecoverFrom populates the backfill discoverer with the backfills already known from recovered state
func (s *BackfillDiscoverer) RecoverFrom(m map[string]*JobTracker) {
	s.knownBackfills = make(map[string]*JobTracker, len(m))
	for tenant, tracker := range m {
		s.knownBackfills[tenant] = tracker
	}
}

// start starts the backfill discovery service. It is expected that RecoverFrom is called before starting the service.
func (s *BackfillDiscoverer) start(ctx context.Context) error {
	b := backoff.New(ctx, s.backfillDiscoveryBackoff)
	var err error
	for b.Ongoing() {
		err = s.discoverBackfillChanges(ctx)
		if err == nil {
			return nil
		}
		b.Wait()
	}
	return fmt.Errorf("failed to discover backfills for the compactor scheduler: %w", err)
}

func (s *BackfillDiscoverer) iter(ctx context.Context) error {
	_ = s.discoverBackfillChanges(ctx)
	return nil
}

func parsePhase(objectName string) (backfillPhase, string, bool) {
	pt := strings.TrimPrefix(objectName, phasesPrefix)
	phase, tenant, found := strings.Cut(pt, objstore.DirDelim)
	if !found || tenant == "" || strings.Contains(tenant, objstore.DirDelim) {
		return "", "", false
	}
	p := backfillPhase(phase)
	if p.order() == 0 {
		return "", "", false
	}
	return p, tenant, true
}

func (s *BackfillDiscoverer) listPhases(ctx context.Context) (map[string]backfillPhase, error) {
	seen := make(map[string]backfillPhase)
	err := s.bkt.Iter(ctx, phasesPrefix, func(name string) error {
		phase, tenant, ok := parsePhase(name)
		if !ok {
			level.Warn(s.logger).Log("msg", "ignoring unrecognized backfill phase object", "object", name)
			return nil
		}
		if !s.allowedTenants.IsAllowed(tenant) {
			return nil
		}
		// Writers create the next phase object before deleting the previous one, so keep the furthest phase
		if prev, ok := seen[tenant]; ok && prev.isAfter(phase) {
			return nil
		}
		seen[tenant] = phase
		return nil
	}, objstore.WithRecursiveIter())
	if err != nil {
		level.Warn(s.logger).Log("msg", "failed backfill discovery", "err", err)
		return nil, err
	}
	return seen, nil
}

// discoverBackfillChanges scans the bucket for backfill phase objects, tracking phase transitions
func (s *BackfillDiscoverer) discoverBackfillChanges(ctx context.Context) error {
	seen, err := s.listPhases(ctx)
	if err != nil {
		return err
	}

	for tenant, phase := range seen {
		tracker, isKnown := s.knownBackfills[tenant]
		if !isKnown {
			// Discovered a new backfill
			persister, err := s.jpm.InitializeTenant(tenant)
			if err != nil {
				level.Warn(s.logger).Log("msg", "failed initializing tenant", "user", tenant, "phase", phase, "err", err)
				continue
			}
			tracker = NewJobTracker(persister, tenant, s.clock, s.lanePolicy, s.maxLeases, s.repeatedFailureReportThreshold, s.metrics.newTrackerMetricsForTenant(tenant), s.logger)
			tracker.SetBackfillPhase(phase)
			s.knownBackfills[tenant] = tracker
			s.rotator.AddTenant(tenant, tracker)
			level.Info(s.logger).Log("msg", "discovered backfill", "user", tenant, "phase", phase)
			continue
		}

		prev := tracker.BackfillPhase()
		if prev == phase {
			// There was no change so there is nothing to do
			continue
		}
		// When the phase is unknown don't log. This can currently happen during recovery (no job inference) or a failed drop.
		if prev != phaseUnknown {
			level.Info(s.logger).Log("msg", "backfill phase changed", "user", tenant, "previous_phase", prev, "phase", phase)
		}

		// The phase is different, mark it in the tracker
		tracker.SetBackfillPhase(phase)
	}

	for tenant, tracker := range s.knownBackfills {
		if _, ok := seen[tenant]; ok {
			continue
		}

		// Drop tenants that no longer have a phase object
		logger := log.With(s.logger, "user", tenant)
		if _, ok := s.rotator.RemoveTenant(tenant); !ok {
			level.Warn(logger).Log("msg", "attempted to remove backfill tenant from rotator, but the tenant was unexpectedly missing")
			continue
		}
		if err := tracker.persister.Drop(); err != nil {
			level.Warn(logger).Log("msg", "failed removing tenant bucket", "err", err)
			// The phase object is gone, so there is no phase to act on until removal succeeds
			tracker.SetBackfillPhase(phaseUnknown)
			// Preserve 1:1 with rotator and knownBackfills
			s.rotator.AddTenant(tenant, tracker)
			continue
		}
		delete(s.knownBackfills, tenant)
		tracker.CleanupMetrics()
		level.Info(logger).Log("msg", "removed tenant with no remaining phase objects")
	}

	return nil
}
