// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/status"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/multierror"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
	"go.uber.org/atomic"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/compactor"
	"github.com/grafana/mimir/pkg/compactor/scheduler/compactorschedulerpb"
	"github.com/grafana/mimir/pkg/storage/bucket"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util"
)

var (
	errFailedAbandoningJob = status.Error(codes.Internal, "failed to abandon job")
	errFailedCancelLease   = status.Error(codes.Internal, "failed to cancel job lease")
	errFailedCompletingJob = status.Error(codes.Internal, "failed to complete job")
	errFailedLeasingJob    = status.Error(codes.Internal, "failed to lease job")
	errLeaseNotFound       = status.Error(codes.NotFound, "lease was not found")
	errInvalidLanes        = status.Error(codes.InvalidArgument, "the requested lanes were invalid")
	errMissingKey          = status.Error(codes.InvalidArgument, "missing required job key")
	errBackfillModeOff     = status.Error(codes.InvalidArgument, "backfill jobs can only be planned when backfill mode is enabled")
	errNotRunning          = status.Error(codes.Unavailable, "the compactor scheduler is not currently running (starting or shutting down)")
)

type Config struct {
	MaxLeases                                   int              `yaml:"max_leases" category:"experimental"`
	LeaseDuration                               time.Duration    `yaml:"lease_duration" category:"experimental"`
	PlanningInterval                            time.Duration    `yaml:"planning_interval" category:"experimental"`
	MaintenanceInterval                         time.Duration    `yaml:"maintenance_interval" category:"experimental"`
	MaintenanceIntervalsBeforeLeaseExpiration   int              `yaml:"maintenance_intervals_before_lease_expiration" category:"experimental"`
	MaintenanceIntervalsBeforeColdStartPlanning int              `yaml:"maintenance_intervals_before_cold_start_planning" category:"experimental"`
	TenantDiscoveryInterval                     time.Duration    `yaml:"tenant_discovery_interval" category:"experimental"`
	TenantDiscoveryBackoff                      backoff.Config   `yaml:"tenant_discovery_backoff"`
	PersistenceType                             string           `yaml:"persistence_type" category:"experimental"`
	RepeatedFailureReportThreshold              int              `yaml:"repeated_failure_report_threshold" category:"experimental"`
	Bbolt                                       BboltConfig      `yaml:"bbolt"`
	LanePolicy                                  LanePolicyConfig `yaml:"lane_policy"`
	BackfillModeEnabled                         bool             `yaml:"backfill_mode_enabled" category:"experimental"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&cfg.MaxLeases, "compactor-scheduler.max-leases", 3, "The maximum number of times a compaction job can be retried before it is removed. Leases that are reassigned due to an interrupted worker do not count against this limit. 0 for no limit.")
	f.DurationVar(&cfg.LeaseDuration, "compactor-scheduler.lease-duration", 10*time.Minute, "The duration of time without contact until the scheduler is able to lease a work item to another worker.")
	f.DurationVar(&cfg.PlanningInterval, "compactor-scheduler.planning-interval", 30*time.Minute, "The duration of time between when plan jobs are submitted aligned by UTC. Note that -compactor.first-level-compaction-wait-period is accounted for during alignment of this interval.")
	f.DurationVar(&cfg.MaintenanceInterval, "compactor-scheduler.maintenance-interval", 2*time.Minute, "The duration of time between when maintenance tasks are performed on job trackers. This includes lease expiration and plan job submission checks.")
	f.IntVar(&cfg.MaintenanceIntervalsBeforeLeaseExpiration, "compactor-scheduler.maintenance-intervals-before-lease-expiration", 3, "The number of maintenance intervals before lease expiration is enforced. Nonpositive values are all treated as zero.")
	f.IntVar(&cfg.MaintenanceIntervalsBeforeColdStartPlanning, "compactor-scheduler.maintenance-intervals-before-cold-start-planning", 5, "The number of maintenance intervals before planning occurs when starting from no recovered state. Nonpositive values are all treated as zero.")
	f.DurationVar(&cfg.TenantDiscoveryInterval, "compactor-scheduler.tenant-discovery-interval", 10*time.Minute, "The duration of time between bucket listings to discover new tenants.")
	cfg.TenantDiscoveryBackoff.RegisterFlagsWithPrefix("compactor-scheduler.tenant-discovery-backoff", f)
	f.StringVar(&cfg.PersistenceType, "compactor-scheduler.persistence-type", "bbolt", "The type of persistence the compactor scheduler should use. Valid values: none, bbolt")
	f.IntVar(&cfg.RepeatedFailureReportThreshold, "compactor-scheduler.repeated-failure-report-threshold", 2, "The number of times a job can fail before a repeated failure is recorded. Reassignments due to an interrupted worker are not counted as a failure. 0 for no limit.")
	cfg.Bbolt.RegisterFlagsWithPrefix("compactor-scheduler.bbolt", f)
	cfg.LanePolicy.RegisterFlagsWithPrefix("compactor-scheduler.lane-policy", f)
	f.BoolVar(&cfg.BackfillModeEnabled, "compactor-scheduler.backfill-mode-enabled", false, "If enabled, the compactor scheduler schedules work for backfills instead of compaction of the tenants in a cell.")
}

func (cfg *Config) Validate() error {
	isBackfillPolicy := cfg.LanePolicy.Policy == lanePolicyBackfill
	if cfg.BackfillModeEnabled && !isBackfillPolicy {
		return fmt.Errorf("compactor-scheduler.lane-policy.policy must be %s when compactor-scheduler.backfill-mode-enabled is true", lanePolicyBackfill)
	}
	if !cfg.BackfillModeEnabled && isBackfillPolicy {
		return fmt.Errorf("compactor-scheduler.lane-policy.policy can only be %s when compactor-scheduler.backfill-mode-enabled is true", lanePolicyBackfill)
	}
	if cfg.MaxLeases < 0 {
		return errors.New("compactor-scheduler.max-leases must be non-negative")
	}
	if cfg.LeaseDuration <= 0 {
		return errors.New("compactor-scheduler.lease-duration must be positive")
	}
	if cfg.PlanningInterval <= 0 {
		return errors.New("compactor-scheduler.planning-interval must be positive")
	}
	if cfg.MaintenanceInterval <= 0 {
		return errors.New("compactor-scheduler.maintenance-interval must be positive")
	}
	if cfg.TenantDiscoveryInterval <= 0 {
		return errors.New("compactor-scheduler.tenant-discovery-interval must be positive")
	}
	if cfg.RepeatedFailureReportThreshold < 0 {
		return errors.New("compactor-scheduler.repeated-failure-report-threshold must be non-negative")
	}
	if cfg.PersistenceType == "bbolt" {
		if err := cfg.Bbolt.Validate("compactor-scheduler.bbolt"); err != nil {
			return err
		}
	}
	return nil
}

type Scheduler struct {
	services.Service

	running            *atomic.Bool
	cfg                Config
	lanePolicy         lanePolicy
	allowList          *util.AllowList
	jpm                JobPersistenceManager
	rotator            *Rotator
	tenantDiscoverer   *TenantDiscoverer
	subservicesManager *services.Manager
	metrics            *schedulerMetrics
	logger             log.Logger
	clock              clock.Clock
}

func NewCompactorScheduler(
	compactorCfg compactor.Config,
	cfg Config,
	storageCfg mimir_tsdb.BlocksStorageConfig,
	logger log.Logger,
	registerer prometheus.Registerer) (*Scheduler, error) {

	if cfg.BackfillModeEnabled && compactorCfg.SchedulerClientConfig.LastContactTimeout <= 0 {
		// Without it there is no bound on how long workers of preempted jobs keep running
		return nil, errors.New("compactor.scheduler-client.last-contact-timeout must be positive when compactor-scheduler.backfill-mode-enabled is true")
	}

	allowList := util.NewAllowList(compactorCfg.EnabledTenants, compactorCfg.DisabledTenants)

	jpm, err := jobPersistenceManagerFactory(cfg, logger)
	if err != nil {
		return nil, err
	}

	// TODO: use the backfill storage bucket when backfill mode is enabled
	bkt, err := bucket.NewClient(context.Background(), storageCfg.Bucket, "compactor-scheduler", logger, registerer)
	if err != nil {
		return nil, err
	}

	return newCompactorScheduler(compactorCfg, cfg, allowList, bkt, jpm, registerer, logger)
}

func newCompactorScheduler(
	compactorCfg compactor.Config,
	cfg Config,
	allowList *util.AllowList,
	bkt objstore.Bucket,
	jpm JobPersistenceManager,
	registerer prometheus.Registerer,
	logger log.Logger) (*Scheduler, error) {

	lanePolicy, err := newLanePolicy(cfg.LanePolicy)
	if err != nil {
		return nil, err
	}

	metrics := newSchedulerMetrics(registerer, lanePolicy, cfg.BackfillModeEnabled)

	compactionWaitPeriod := compactorCfg.CompactionWaitPeriod
	if cfg.BackfillModeEnabled {
		// Backfilled blocks are complete when uploaded, so there is no need to wait before compacting them
		compactionWaitPeriod = 0
	}

	rotator := NewRotator(
		cfg.LeaseDuration,
		cfg.PlanningInterval,
		compactionWaitPeriod,
		compactorCfg.SchedulerClientConfig.LastContactTimeout,
		cfg.MaintenanceInterval,
		cfg.MaintenanceIntervalsBeforeLeaseExpiration,
		cfg.MaintenanceIntervalsBeforeColdStartPlanning,
		lanePolicy,
		metrics.pendingJobsLastEmpty,
		metrics.lanePendingJobsLastEmpty,
		logger,
	)

	scheduler := &Scheduler{
		running:          atomic.NewBool(false),
		cfg:              cfg,
		lanePolicy:       lanePolicy,
		allowList:        allowList,
		jpm:              jpm,
		rotator:          rotator,
		tenantDiscoverer: NewTenantDiscoverer(cfg, lanePolicy, allowList, rotator, bkt, jpm, metrics, logger),
		metrics:          metrics,
		logger:           logger,
		clock:            clock.New(),
	}

	subservicesManager, err := services.NewManager(scheduler.rotator, scheduler.tenantDiscoverer)
	if err != nil {
		return nil, err
	}
	scheduler.subservicesManager = subservicesManager

	svc := services.NewBasicService(scheduler.start, scheduler.run, scheduler.stop)
	scheduler.Service = svc

	return scheduler, nil
}

func (s *Scheduler) createJobTracker(tenant string, jp JobPersister) *JobTracker {
	return NewJobTracker(jp, tenant, s.clock, s.lanePolicy, s.cfg.MaxLeases, s.cfg.RepeatedFailureReportThreshold, s.cfg.BackfillModeEnabled, s.metrics.newTrackerMetricsForTenant(tenant), s.logger)
}

func (s *Scheduler) start(ctx context.Context) error {
	jobTrackers, err := s.jpm.RecoverAll(s.allowList, s.createJobTracker)
	if err != nil {
		return fmt.Errorf("failed recovering state: %w", err)
	}
	s.rotator.RecoverFrom(jobTrackers, s.jpm.CreationTime())
	s.tenantDiscoverer.RecoverFrom(jobTrackers)

	if err := s.subservicesManager.StartAsync(ctx); err != nil {
		return fmt.Errorf("unable to start compactor scheduler subservices: %w", err)
	}
	if err := s.subservicesManager.AwaitHealthy(ctx); err != nil {
		return fmt.Errorf("compactor scheduler subservices not healthy: %w", err)
	}
	return nil
}

func (s *Scheduler) run(ctx context.Context) error {
	s.running.Store(true)
	<-ctx.Done()
	return nil
}

func (s *Scheduler) stop(_ error) error {
	s.running.Store(false)

	errs := multierror.New()

	// We want the other services to stop first since they may also use the database
	errs.Add(services.StopManagerAndAwaitStopped(context.Background(), s.subservicesManager))

	// Prepare for shutdown by making sure no more persist operations are possible.
	// We know after the rotator is cleared all subsequent calls will see an empty state.
	// The only cases to still handle then are lease update requests, since "not found" is interpreted as the lease being invalid.
	s.rotator.PrepareForShutdown()

	// Since it is now shielded from further persist calls, close the persistence manager
	if err := s.jpm.Close(); err != nil {
		level.Error(s.logger).Log("msg", "failed to close persistence manager", "err", err)
		errs.Add(err)
	}

	return errs.Err()
}

func (s *Scheduler) isRunning() bool {
	return s.running.Load()
}

func (s *Scheduler) LeaseJob(ctx context.Context, req *compactorschedulerpb.LeaseJobRequest) (*compactorschedulerpb.LeaseJobResponse, error) {
	lanes, err := s.lanePolicy.LanesForRequest(req)
	if err != nil {
		level.Error(s.logger).Log("msg", "invalid lanes in request", "err", err)
		return nil, errInvalidLanes
	}
	response, ok, err := s.rotator.LeaseJob(ctx, lanes)
	if err != nil {
		level.Error(s.logger).Log("msg", "failed leasing job", "err", err)
		return nil, errFailedLeasingJob
	}
	if ok {
		level.Info(s.logger).Log("msg", "leased job", "user", response.Spec.Tenant, "job_type", response.Spec.JobType.String(), "id", response.Key.Id, "epoch", response.Key.Epoch, "worker", req.WorkerId)
		return response, nil
	}
	return &compactorschedulerpb.LeaseJobResponse{}, nil
}

func (s *Scheduler) PlannedJobs(ctx context.Context, req *compactorschedulerpb.PlannedJobsRequest) (*compactorschedulerpb.PlannedJobsResponse, error) {
	if req.Key == nil {
		return nil, errMissingKey
	}
	if !s.isRunning() {
		// This check is required to prevent requests from seeing empty state before startup
		return nil, errNotRunning
	}

	if req.Unchanged && len(req.Jobs) > 0 {
		return nil, status.Error(codes.InvalidArgument, "planned jobs can not be provided when planning is unchanged")
	}

	logger := log.With(s.logger, "user", req.Tenant, "epoch", req.Key.Epoch)
	level.Info(logger).Log("msg", "received plan results", "job_count", len(req.Jobs), "unchanged", req.Unchanged)

	now := s.clock.Now()
	jobs := make([]TrackedJob, 0, len(req.Jobs))
	idSet := make(map[string]struct{}, len(req.Jobs))
	for i, job := range req.Jobs {
		// Technically this casting could truncate, but that's an unrealistic case.
		// The +1 is a minor detail that ensures plan jobs (order of 0) can deterministically sort first in ordering upon recovery if they exist.
		order := uint32(i + 1)

		var tj TrackedJob
		switch j := job.Job.(type) {
		case *compactorschedulerpb.PlannedJob_Compaction:
			if len(job.Id) == reservedJobIdLen || isTypedJobKey(job.Id) {
				// This is never expected to actually happen. We reserve these keys for internal use.
				level.Warn(logger).Log("msg", "ignoring planned job with an internally reserved ID", "id", job.Id)
				continue
			}
			tj = NewTrackedCompactionJob(
				job.Id,
				&CompactionJob{
					blocks:  j.Compaction.BlockIds,
					isSplit: j.Compaction.Split,
				},
				order,
				j.Compaction.TotalBlocksBytes,
				now,
			)
		case *compactorschedulerpb.PlannedJob_BackfillValidate:
			if !s.cfg.BackfillModeEnabled {
				return nil, errBackfillModeOff
			}
			if job.Id == "" {
				level.Warn(logger).Log("msg", "ignoring planned backfill validate job with an empty ID")
				continue
			}
			tj = NewTrackedBackfillBlockJob(compactorschedulerpb.JOB_TYPE_BACKFILL_VALIDATE, job.Id, j.BackfillValidate.BackfillId, j.BackfillValidate.BlockId, order, now)
		case *compactorschedulerpb.PlannedJob_BackfillCopy:
			if !s.cfg.BackfillModeEnabled {
				return nil, errBackfillModeOff
			}
			if job.Id == "" {
				level.Warn(logger).Log("msg", "ignoring planned backfill copy job with an empty ID")
				continue
			}
			tj = NewTrackedBackfillBlockJob(compactorschedulerpb.JOB_TYPE_BACKFILL_COPY, job.Id, j.BackfillCopy.BackfillId, j.BackfillCopy.BlockId, order, now)
		case *compactorschedulerpb.PlannedJob_BackfillCleanup:
			if !s.cfg.BackfillModeEnabled {
				return nil, errBackfillModeOff
			}
			if len(req.Jobs) != 1 {
				return nil, status.Error(codes.InvalidArgument, "a backfill cleanup job must be the only planned job")
			}
			// The provided ID is not used since a tenant has at most one cleanup job
			tj = NewTrackedBackfillCleanupJob(j.BackfillCleanup.BackfillId, now)
		default:
			return nil, status.Errorf(codes.InvalidArgument, "planned job %q is missing required job field", job.Id)
		}

		if _, ok := idSet[tj.ID()]; ok {
			// This is never expected to actually happen. It enforces a guarantee for later code.
			level.Warn(logger).Log("msg", "ignoring planned job with a duplicated job ID", "id", tj.ID())
			continue
		}
		idSet[tj.ID()] = struct{}{}
		jobs = append(jobs, tj)
	}

	var found bool
	var err error
	if req.Unchanged {
		found, err = s.rotator.CompletePlanJob(req.Tenant, req.Key.Epoch)
	} else {
		_, found, err = s.rotator.OfferJobs(req.Tenant, jobs, req.Key.Epoch)
	}
	if err != nil {
		level.Error(logger).Log("msg", "failed offering result of plan job", "err", err)
		return nil, errFailedCompletingJob // this error is used because PlannedJobs is the completion of a plan job
	} else if !found {
		if s.isRunning() {
			return nil, errLeaseNotFound
		} else {
			// This request may have erroneously seen empty state. Transform it to an unavailable error to preserve state in the worker.
			// Worst case we are accidentally pushing them to return the same results later
			return nil, errNotRunning
		}
	}
	s.metrics.jobsCompleted.WithLabelValues(jobTypePlan).Inc()
	return &compactorschedulerpb.PlannedJobsResponse{}, nil
}

func (s *Scheduler) UpdatePlanJob(ctx context.Context, req *compactorschedulerpb.UpdatePlanJobRequest) (*compactorschedulerpb.UpdateJobResponse, error) {
	if req.Key == nil {
		return nil, errMissingKey
	}
	if !s.isRunning() {
		// This check is required to prevent requests from seeing empty state before startup, but then running when checking to transform not found errors.
		return nil, errNotRunning
	}

	// Not logging the key ID because plan jobs all have an identical ID
	logger := log.With(s.logger, "user", req.Tenant, "epoch", req.Key.Epoch)

	switch req.Update {
	case compactorschedulerpb.UPDATE_TYPE_IN_PROGRESS:
		if s.rotator.RenewJobLease(req.Tenant, req.Key.Id, req.Key.Epoch) {
			// Lease renewals are only debug logged to prevent noise
			level.Debug(logger).Log("msg", "plan job lease renewed")
			return &compactorschedulerpb.UpdateJobResponse{}, nil
		}
	case compactorschedulerpb.UPDATE_TYPE_ABANDON:
		removed, err := s.rotator.RemoveJob(req.Tenant, req.Key.Id, req.Key.Epoch, false)
		if err != nil {
			level.Error(logger).Log("msg", "failed plan job abandon", "err", err)
			return nil, errFailedAbandoningJob
		}
		if removed {
			level.Info(logger).Log("msg", "plan job abandoned")
			return &compactorschedulerpb.UpdateJobResponse{}, nil
		}
	case compactorschedulerpb.UPDATE_TYPE_REASSIGN, compactorschedulerpb.UPDATE_TYPE_INTERRUPTED_REASSIGN:
		interrupted := req.Update == compactorschedulerpb.UPDATE_TYPE_INTERRUPTED_REASSIGN
		canceled, err := s.rotator.CancelJobLease(req.Tenant, req.Key.Id, req.Key.Epoch, interrupted)
		if err != nil {
			level.Error(logger).Log("msg", "failed plan job cancel", "err", err)
			return nil, errFailedCancelLease
		}
		if canceled {
			level.Info(logger).Log("msg", "plan job canceled", "worker_interrupted", interrupted)
			return &compactorschedulerpb.UpdateJobResponse{}, nil
		}
	case compactorschedulerpb.UPDATE_TYPE_COMPLETE:
		// Plan jobs do not send a complete. They instead call PlannedJobs.
		fallthrough
	default:
		return nil, invalidUpdateTypeError(req.Update)
	}

	if !s.isRunning() {
		// This request may have erroneously seen empty state. Transform it to an unavailable error to preserve state in the worker.
		return nil, errNotRunning
	}

	level.Info(logger).Log("msg", "could not find lease during update for plan job", "update_type", req.Update.String())
	return nil, errLeaseNotFound
}

func (s *Scheduler) UpdateCompactionJob(ctx context.Context, req *compactorschedulerpb.UpdateCompactionJobRequest) (*compactorschedulerpb.UpdateJobResponse, error) {
	if req.Key == nil {
		return nil, errMissingKey
	}
	if !s.isRunning() {
		// This check is required to prevent requests from seeing empty state before startup, but then running when checking to transform not found errors.
		return nil, errNotRunning
	}

	logger := log.With(s.logger, "user", req.Tenant, "id", req.Key.Id, "epoch", req.Key.Epoch)

	switch req.Update {
	case compactorschedulerpb.UPDATE_TYPE_IN_PROGRESS:
		if s.rotator.RenewJobLease(req.Tenant, req.Key.Id, req.Key.Epoch) {
			// Lease renewals are only debug logged to prevent noise
			level.Debug(logger).Log("msg", "compaction job lease renewed")
			return &compactorschedulerpb.UpdateJobResponse{}, nil
		}
	case compactorschedulerpb.UPDATE_TYPE_COMPLETE:
		removed, err := s.rotator.RemoveJob(req.Tenant, req.Key.Id, req.Key.Epoch, true)
		if err != nil {
			level.Error(logger).Log("msg", "failed compaction job completion", "err", err)
			return nil, errFailedCompletingJob
		}
		if removed {
			s.metrics.jobsCompleted.WithLabelValues(jobTypeCompaction).Inc()
			level.Info(logger).Log("msg", "compaction job completed")
			return &compactorschedulerpb.UpdateJobResponse{}, nil
		}
	case compactorschedulerpb.UPDATE_TYPE_ABANDON:
		removed, err := s.rotator.RemoveJob(req.Tenant, req.Key.Id, req.Key.Epoch, false)
		if err != nil {
			level.Error(logger).Log("msg", "failed compaction job abandon", "err", err)
			return nil, errFailedAbandoningJob
		}
		if removed {
			level.Info(logger).Log("msg", "compaction job abandoned")
			return &compactorschedulerpb.UpdateJobResponse{}, nil
		}
	case compactorschedulerpb.UPDATE_TYPE_REASSIGN, compactorschedulerpb.UPDATE_TYPE_INTERRUPTED_REASSIGN:
		interrupted := req.Update == compactorschedulerpb.UPDATE_TYPE_INTERRUPTED_REASSIGN
		canceled, err := s.rotator.CancelJobLease(req.Tenant, req.Key.Id, req.Key.Epoch, interrupted)
		if err != nil {
			level.Error(logger).Log("msg", "failed compaction job cancel", "err", err)
			return nil, errFailedCancelLease
		}
		if canceled {
			level.Info(logger).Log("msg", "compaction job lease canceled", "worker_interrupted", interrupted)
			return &compactorschedulerpb.UpdateJobResponse{}, nil
		}
	default:
		return nil, invalidUpdateTypeError(req.Update)
	}

	if !s.isRunning() {
		// This request may have erroneously seen empty state. Transform it to an unavailable error to preserve state in the worker.
		return nil, errNotRunning
	}

	level.Info(logger).Log("msg", "could not find lease during update for compaction job", "update_type", req.Update.String())
	return nil, errLeaseNotFound
}

// backfillJobTypeLabel returns the metric label for the job type of a backfill job key, if it is one
func backfillJobTypeLabel(id string) (string, bool) {
	if id == backfillCleanupJobId {
		return jobTypeLabel(compactorschedulerpb.JOB_TYPE_BACKFILL_CLEANUP), true
	}
	jobType, ok := blockJobTypeForKey(id)
	if !ok {
		return "", false
	}
	return jobTypeLabel(jobType), true
}

func (s *Scheduler) UpdateBackfillJob(ctx context.Context, req *compactorschedulerpb.UpdateBackfillJobRequest) (*compactorschedulerpb.UpdateJobResponse, error) {
	if req.Key == nil {
		return nil, errMissingKey
	}
	jobType, ok := backfillJobTypeLabel(req.Key.Id)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "job %q is not a backfill job", req.Key.Id)
	}
	if !s.isRunning() {
		// This check is required to prevent requests from seeing empty state before startup, but then running when checking to transform not found errors.
		return nil, errNotRunning
	}

	logger := log.With(s.logger, "user", req.Tenant, "job_type", jobType, "id", req.Key.Id, "epoch", req.Key.Epoch)

	switch req.Update {
	case compactorschedulerpb.UPDATE_TYPE_IN_PROGRESS:
		if s.rotator.RenewJobLease(req.Tenant, req.Key.Id, req.Key.Epoch) {
			// Lease renewals are only debug logged to prevent noise
			level.Debug(logger).Log("msg", "backfill job lease renewed")
			return &compactorschedulerpb.UpdateJobResponse{}, nil
		}
	case compactorschedulerpb.UPDATE_TYPE_COMPLETE:
		removed, err := s.rotator.RemoveJob(req.Tenant, req.Key.Id, req.Key.Epoch, true)
		if err != nil {
			level.Error(logger).Log("msg", "failed backfill job completion", "err", err)
			return nil, errFailedCompletingJob
		}
		if removed {
			s.metrics.jobsCompleted.WithLabelValues(jobType).Inc()
			level.Info(logger).Log("msg", "backfill job completed")
			return &compactorschedulerpb.UpdateJobResponse{}, nil
		}
	case compactorschedulerpb.UPDATE_TYPE_ABANDON:
		removed, err := s.rotator.RemoveJob(req.Tenant, req.Key.Id, req.Key.Epoch, false)
		if err != nil {
			level.Error(logger).Log("msg", "failed backfill job abandon", "err", err)
			return nil, errFailedAbandoningJob
		}
		if removed {
			level.Info(logger).Log("msg", "backfill job abandoned")
			return &compactorschedulerpb.UpdateJobResponse{}, nil
		}
	case compactorschedulerpb.UPDATE_TYPE_REASSIGN, compactorschedulerpb.UPDATE_TYPE_INTERRUPTED_REASSIGN:
		interrupted := req.Update == compactorschedulerpb.UPDATE_TYPE_INTERRUPTED_REASSIGN
		canceled, err := s.rotator.CancelJobLease(req.Tenant, req.Key.Id, req.Key.Epoch, interrupted)
		if err != nil {
			level.Error(logger).Log("msg", "failed backfill job cancel", "err", err)
			return nil, errFailedCancelLease
		}
		if canceled {
			level.Info(logger).Log("msg", "backfill job lease canceled", "worker_interrupted", interrupted)
			return &compactorschedulerpb.UpdateJobResponse{}, nil
		}
	default:
		return nil, invalidUpdateTypeError(req.Update)
	}

	if !s.isRunning() {
		// This request may have erroneously seen empty state. Transform it to an unavailable error to preserve state in the worker.
		return nil, errNotRunning
	}

	level.Info(logger).Log("msg", "could not find lease during update for backfill job", "update_type", req.Update.String())
	return nil, errLeaseNotFound
}

func invalidUpdateTypeError(updateType compactorschedulerpb.UpdateType) error {
	return status.Error(codes.InvalidArgument, fmt.Sprintf("update type was not recognized: %s", updateType.String()))
}
