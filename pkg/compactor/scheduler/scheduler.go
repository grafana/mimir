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
	errMissingKey          = status.Error(codes.InvalidArgument, "missing required job key")
	errNotRunning          = status.Error(codes.Unavailable, "the compactor scheduler is not currently running (starting or shutting down)")
)

type Config struct {
	MaxLeases                                   int            `yaml:"max_leases" category:"experimental"`
	LeaseDuration                               time.Duration  `yaml:"lease_duration" category:"experimental"`
	PlanningInterval                            time.Duration  `yaml:"planning_interval" category:"experimental"`
	MaintenanceInterval                         time.Duration  `yaml:"maintenance_interval" category:"experimental"`
	MaintenanceIntervalsBeforeLeaseExpiration   int            `yaml:"maintenance_intervals_before_lease_expiration" category:"experimental"`
	MaintenanceIntervalsBeforeColdStartPlanning int            `yaml:"maintenance_intervals_before_cold_start_planning" category:"experimental"`
	TenantDiscoveryInterval                     time.Duration  `yaml:"tenant_discovery_interval" category:"experimental"`
	TenantDiscoveryBackoff                      backoff.Config `yaml:"tenant_discovery_backoff" category:"experimental"`
	PersistenceType                             string         `yaml:"persistence_type" category:"experimental"`
	RepeatedFailureReportThreshold              int            `yaml:"repeated_failure_report_threshold" category:"experimental"`
	Bbolt                                       BboltConfig    `yaml:"bbolt" category:"experimental"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&cfg.MaxLeases, "compactor-scheduler.max-leases", 3, "The maximum number of times a job can be retried before it is removed. 0 for no limit.")
	f.DurationVar(&cfg.LeaseDuration, "compactor-scheduler.lease-duration", 10*time.Minute, "The duration of time without contact until the scheduler is able to lease a work item to another worker.")
	f.DurationVar(&cfg.PlanningInterval, "compactor-scheduler.planning-interval", 1*time.Hour, "The duration of time between when plan jobs are submitted aligned by UTC. Note that -compactor.first-level-compaction-wait-period is accounted for during alignment of this interval.")
	f.DurationVar(&cfg.MaintenanceInterval, "compactor-scheduler.maintenance-interval", 2*time.Minute, "The duration of time between when maintenance tasks are performed on job trackers. This includes lease expiration and plan job submission checks.")
	f.IntVar(&cfg.MaintenanceIntervalsBeforeLeaseExpiration, "compactor-scheduler.maintenance-intervals-before-lease-expiration", 3, "The number of maintenance intervals before lease expiration is enforced. Nonpositive values are all treated as zero.")
	f.IntVar(&cfg.MaintenanceIntervalsBeforeColdStartPlanning, "compactor-scheduler.maintenance-intervals-before-cold-start-planning", 4, "The number of maintenance intervals before planning occurs when starting from no recovered state. Nonpositive values are all treated as zero.")
	f.DurationVar(&cfg.TenantDiscoveryInterval, "compactor-scheduler.tenant-discovery-interval", 10*time.Minute, "The duration of time between bucket listings to discover new tenants.")
	cfg.TenantDiscoveryBackoff.RegisterFlagsWithPrefix("compactor-scheduler.tenant-discovery-backoff", f)
	f.StringVar(&cfg.PersistenceType, "compactor-scheduler.persistence-type", "bbolt", "The type of persistence the compactor scheduler should use. Valid values: none, bbolt")
	f.IntVar(&cfg.RepeatedFailureReportThreshold, "compactor-scheduler.repeated-failure-report-threshold", 2, "The number of times a job can fail before a repeated failure is recorded. 0 for no limit.")
	cfg.Bbolt.RegisterFlagsWithPrefix("compactor-scheduler.bbolt", f)
}

func (cfg *Config) Validate() error {
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

	metrics := newSchedulerMetrics(registerer)
	allowList := util.NewAllowList(compactorCfg.EnabledTenants, compactorCfg.DisabledTenants)

	jpm, err := jobPersistenceManagerFactory(cfg, logger)
	if err != nil {
		return nil, err
	}

	bkt, err := bucket.NewClient(context.Background(), storageCfg.Bucket, "compactor-scheduler", logger, registerer)
	if err != nil {
		return nil, err
	}

	return newCompactorScheduler(compactorCfg, cfg, allowList, bkt, jpm, metrics, logger)
}

func newCompactorScheduler(
	compactorCfg compactor.Config,
	cfg Config,
	allowList *util.AllowList,
	bkt objstore.Bucket,
	jpm JobPersistenceManager,
	metrics *schedulerMetrics,
	logger log.Logger) (*Scheduler, error) {

	rotator := NewRotator(
		cfg.LeaseDuration,
		cfg.PlanningInterval,
		compactorCfg.CompactionWaitPeriod,
		cfg.MaintenanceInterval,
		cfg.MaintenanceIntervalsBeforeLeaseExpiration,
		cfg.MaintenanceIntervalsBeforeColdStartPlanning,
		metrics,
		logger,
	)

	scheduler := &Scheduler{
		running:          atomic.NewBool(false),
		cfg:              cfg,
		allowList:        allowList,
		jpm:              jpm,
		rotator:          rotator,
		tenantDiscoverer: NewTenantDiscoverer(cfg, allowList, rotator, bkt, jpm, metrics, logger),
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
	return NewJobTracker(jp, tenant, s.clock, s.cfg.MaxLeases, s.cfg.RepeatedFailureReportThreshold, s.metrics.newTrackerMetricsForTenant(tenant), s.logger)
}

func (s *Scheduler) start(ctx context.Context) error {
	jobTrackers, err := s.jpm.RecoverAll(s.allowList, s.createJobTracker)
	if err != nil {
		return fmt.Errorf("failed recovering state: %w", err)
	}
	s.rotator.RecoverFrom(jobTrackers)
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
	response, ok, err := s.rotator.LeaseJob(ctx)
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

	logger := log.With(s.logger, "user", req.Tenant, "epoch", req.Key.Epoch)
	level.Info(logger).Log("msg", "received plan results", "job_count", len(req.Jobs))

	now := s.clock.Now()
	jobs := make([]*TrackedCompactionJob, 0, len(req.Jobs))
	idSet := make(map[string]struct{}, len(req.Jobs))
	for i, job := range req.Jobs {
		if job.Job == nil {
			return nil, status.Errorf(codes.InvalidArgument, "planned job %q is missing required job field", job.Id)
		}
		if len(job.Id) == reservedJobIdLen {
			// This is never expected to actually happen. We reserve single character keys for internal use.
			level.Warn(logger).Log("msg", "ignoring planned job with an internally reserved ID length", "id", job.Id)
			continue
		}
		if _, ok := idSet[job.Id]; ok {
			// This is never expected to actually happen. It enforces a guarantee for later code.
			level.Warn(logger).Log("msg", "ignoring planned job with a duplicated job ID", "id", job.Id)
			continue
		}

		idSet[job.Id] = struct{}{}

		jobs = append(jobs, NewTrackedCompactionJob(
			job.Id,
			&CompactionJob{
				blocks:  job.Job.BlockIds,
				isSplit: job.Job.Split,
			},
			uint32(i), // technically this casting could truncate, but that's an unrealistic case
			now,
		))
	}

	_, found, err := s.rotator.OfferCompactionJobs(req.Tenant, jobs, req.Key.Epoch)
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
	case compactorschedulerpb.UPDATE_TYPE_REASSIGN:
		canceled, err := s.rotator.CancelJobLease(req.Tenant, req.Key.Id, req.Key.Epoch)
		if err != nil {
			level.Error(logger).Log("msg", "failed plan job cancel", "err", err)
			return nil, errFailedCancelLease
		}
		if canceled {
			level.Info(logger).Log("msg", "plan job canceled")
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
	case compactorschedulerpb.UPDATE_TYPE_REASSIGN:
		canceled, err := s.rotator.CancelJobLease(req.Tenant, req.Key.Id, req.Key.Epoch)
		if err != nil {
			level.Error(logger).Log("msg", "failed compaction job cancel", "err", err)
			return nil, errFailedCancelLease
		}
		if canceled {
			level.Info(logger).Log("msg", "compaction job lease canceled")
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

func invalidUpdateTypeError(updateType compactorschedulerpb.UpdateType) error {
	return status.Error(codes.InvalidArgument, fmt.Sprintf("update type was not recognized: %s", updateType.String()))
}
