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
	"go.uber.org/atomic"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/compactor"
	"github.com/grafana/mimir/pkg/compactor/scheduler/compactorschedulerpb"
	"github.com/grafana/mimir/pkg/storage/bucket"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util"
)

type Config struct {
	maxLeases             int
	leaseDuration         time.Duration
	leaseCheckInterval    time.Duration
	planningCheckInterval time.Duration
	planningInterval      time.Duration
	userDiscoveryBackoff  backoff.Config
	persistenceType       string
	bboltPath             string
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&cfg.maxLeases, "compactor-scheduler.max-leases", 3, "The maximum number of times a job can be retried from the same plan before it is removed. 0 for no limit.")
	f.DurationVar(&cfg.leaseDuration, "compactor-scheduler.lease-duration", 10*time.Minute, "The duration of time without contact until the scheduler is able to lease a work item to another worker.")
	f.DurationVar(&cfg.leaseCheckInterval, "compactor-scheduler.lease-check-interval", 3*time.Minute, "How often the scheduler will check for expired leases to make the work items pending again.")
	f.DurationVar(&cfg.planningCheckInterval, "compactor-scheduler.planning-check-interval", time.Minute, "How often the scheduler will check all tenants to see if a planning job needs to be submitted.")
	f.DurationVar(&cfg.planningInterval, "compactor-scheduler.planning-interval", 30*time.Minute, "How often the plan jobs will be created by the scheduler.")
	f.StringVar(&cfg.persistenceType, "compactor-scheduler.persistence-type", "bbolt", "The type of persistence the compactor scheduler should use. Valid values: none, bbolt")
	f.StringVar(&cfg.bboltPath, "compactor-scheduler.bbolt.db-path", "bbolt_1.db", "The path to the bbolt database file for the compactor scheduler.")
	cfg.userDiscoveryBackoff.RegisterFlagsWithPrefix("compactor-scheduler", f)
}

func (cfg *Config) Validate() error {
	if cfg.maxLeases < 0 {
		return errors.New("compactor-scheduler.max-leases must be non-negative")
	}
	if cfg.leaseDuration <= 0 {
		return errors.New("compactor-scheduler.lease-duration must be positive")
	}
	if cfg.leaseCheckInterval <= 0 {
		return errors.New("compactor-scheduler.lease-check-interval must be positive")
	}
	if cfg.planningCheckInterval <= 0 {
		return errors.New("compactor-scheduler.planning-check-interval must be positive")
	}
	if cfg.planningInterval <= 0 {
		return errors.New("compactor-scheduler.planning-interval must be positive")
	}
	if cfg.persistenceType == "bbolt" {
		if cfg.bboltPath == "" {
			return errors.New("compactor-scheduler.bbolt.db-path must be set")
		}
	}
	return nil
}

type CompactionJob struct {
	blocks  [][]byte // empty indicates a planning job
	isSplit bool
}

type Scheduler struct {
	services.Service

	running            *atomic.Bool
	cfg                Config
	allowList          *util.AllowList
	jpm                JobPersistenceManager
	rotator            *Rotator
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

	scheduler := &Scheduler{
		running:   atomic.NewBool(false),
		cfg:       cfg,
		allowList: allowList,
		jpm:       jpm,
		rotator:   NewRotator(cfg.leaseDuration, cfg.leaseCheckInterval, metrics, logger),
		metrics:   metrics,
		logger:    logger,
		clock:     clock.New(),
	}

	// TODO: This will need to be moved for testing
	bkt, err := bucket.NewClient(context.Background(), storageCfg.Bucket, "compactor-scheduler", logger, registerer)
	if err != nil {
		return nil, err
	}

	planSpawner := NewSpawner(
		cfg,
		allowList,
		scheduler.rotator,
		bkt,
		jpm,
		metrics,
		logger,
	)

	subservicesManager, err := services.NewManager(scheduler.rotator, planSpawner)
	if err != nil {
		return nil, err
	}
	scheduler.subservicesManager = subservicesManager

	svc := services.NewBasicService(scheduler.start, scheduler.run, scheduler.stop)
	scheduler.Service = svc

	return scheduler, nil

}

func (s *Scheduler) createJobTracker(tenant string, jp JobPersister) *JobTracker {
	return NewJobTracker(jp, tenant, s.cfg.maxLeases, s.metrics.trackerMetricsForTenant(tenant))
}

func (s *Scheduler) start(ctx context.Context) error {
	compactionTrackers, err := s.jpm.RecoverAll(s.allowList, s.createJobTracker)
	if err != nil {
		return fmt.Errorf("failed recovering state: %w", err)
	}
	s.rotator.RecoverFrom(compactionTrackers)

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
		return nil, failedTo("lease job")
	}
	if ok {
		level.Info(s.logger).Log("msg", "leasing job", "tenant", response.Spec.Tenant, "id", response.Key.Id, "epoch", response.Key.Epoch, "worker", req.WorkerId)
		return response, nil
	}

	level.Info(s.logger).Log("msg", "no jobs pending to be leased", "worker", req.WorkerId)
	return nil, status.Error(codes.NotFound, "no jobs were pending to be leased")
}

func (s *Scheduler) PlannedJobs(ctx context.Context, req *compactorschedulerpb.PlannedJobsRequest) (*compactorschedulerpb.PlannedJobsResponse, error) {
	if !s.isRunning() {
		// This check is required to prevent requests from seeing empty state before startup
		return nil, notRunning()
	}

	level.Info(s.logger).Log("msg", "received plan results", "tenant", req.Key.Id, "epoch", req.Key.Epoch, "job_count", len(req.Jobs))

	now := s.clock.Now()
	jobs := make([]TrackedJob, 0, len(req.Jobs))
	for i, job := range req.Jobs {
		if len(job.Id) == reservedJobIdLen {
			// This is never expected to actually happen. We reserve single character keys for internal use.
			level.Warn(s.logger).Log("msg", "ignoring planned job with an internally reserved ID length", "id", job.Id)
			continue
		}
		jobs = append(jobs, NewTrackedCompactionJob(
			job.Id,
			&CompactionJob{
				blocks:  job.Job.BlockIds,
				isSplit: job.Job.Split,
			},
			uint32(i), // technically this casting could truncate, but that's an unrealistic case
			now,
			s.clock,
		))
	}

	added, err := s.rotator.OfferJobs(req.Key.Id, jobs, req.Key.Epoch)
	if err != nil {
		level.Error(s.logger).Log("msg", "failed offering result of plan job", "err", err)
		return nil, failedTo("offering results")
	} else if added == 0 && !s.isRunning() {
		// This request may have erroneously seen empty state. Transform it to an unavailable error to preserve state in the worker.
		// Worst case we are accidentally pushing them to return the same results later
		return nil, notRunning()
	}

	return &compactorschedulerpb.PlannedJobsResponse{}, nil
}

func (s *Scheduler) UpdatePlanJob(ctx context.Context, req *compactorschedulerpb.UpdatePlanJobRequest) (*compactorschedulerpb.UpdateJobResponse, error) {
	if !s.isRunning() {
		// This check is required to prevent requests from seeing empty state before startup, but then running when checking to transform not found errors.
		return nil, notRunning()
	}

	level.Info(s.logger).Log("msg", "received plan job lease update request", "update_type", compactorschedulerpb.UpdateType_name[int32(req.Update)], "id", req.Key.Id, "epoch", req.Key.Epoch)
	switch req.Update {
	case compactorschedulerpb.UPDATE_TYPE_IN_PROGRESS:
		if s.rotator.RenewJobLease(req.Key.Id, req.Key.Id, req.Key.Epoch) {
			return ok()
		}
	case compactorschedulerpb.UPDATE_TYPE_ABANDON:
		removed, err := s.rotator.RemoveJob(req.Key.Id, req.Key.Id, req.Key.Epoch, false)
		if err != nil {
			return nil, failedTo("remove job")
		}
		if removed {
			return ok()
		}
	case compactorschedulerpb.UPDATE_TYPE_REASSIGN:
		canceled, err := s.rotator.CancelJobLease(req.Key.Id, req.Key.Id, req.Key.Epoch)
		if err != nil {
			return nil, failedTo("cancel job lease")
		}
		if canceled {
			return ok()
		}
	case compactorschedulerpb.UPDATE_TYPE_COMPLETE:
		// Plan jobs do not send a complete. They instead call PlannedJobs.
		fallthrough
	default:
		return nil, status.Error(codes.InvalidArgument, "unrecognized update type")
	}

	if !s.isRunning() {
		// This request may have erroneously seen empty state. Transform it to an unavailable error to preserve state in the worker.
		return nil, notRunning()
	}

	level.Info(s.logger).Log("msg", "could not find lease during update for plan job", "update_type", compactorschedulerpb.UpdateType_name[int32(req.Update)], "id", req.Key.Id, "epoch", req.Key.Epoch)
	return notFound()
}

func (s *Scheduler) UpdateCompactionJob(ctx context.Context, req *compactorschedulerpb.UpdateCompactionJobRequest) (*compactorschedulerpb.UpdateJobResponse, error) {
	if !s.isRunning() {
		// This check is required to prevent requests from seeing empty state before startup, but then running when checking to transform not found errors.
		return nil, notRunning()
	}

	level.Info(s.logger).Log("msg", "received compaction lease update request", "update_type", compactorschedulerpb.UpdateType_name[int32(req.Update)], "id", req.Key.Id, "epoch", req.Key.Epoch)

	switch req.Update {
	case compactorschedulerpb.UPDATE_TYPE_IN_PROGRESS:
		if s.rotator.RenewJobLease(req.Tenant, req.Key.Id, req.Key.Epoch) {
			return ok()
		}
	case compactorschedulerpb.UPDATE_TYPE_COMPLETE:
		removed, err := s.rotator.RemoveJob(req.Tenant, req.Key.Id, req.Key.Epoch, true)
		if err != nil {
			return nil, failedTo("complete job")
		}
		if removed {
			return ok()
		}
	case compactorschedulerpb.UPDATE_TYPE_ABANDON:
		removed, err := s.rotator.RemoveJob(req.Tenant, req.Key.Id, req.Key.Epoch, false)
		if err != nil {
			return nil, failedTo("remove job")
		}
		if removed {
			return ok()
		}
	case compactorschedulerpb.UPDATE_TYPE_REASSIGN:
		canceled, err := s.rotator.CancelJobLease(req.Tenant, req.Key.Id, req.Key.Epoch)
		if err != nil {
			return nil, failedTo("cancel job lease")
		}
		if canceled {
			return ok()
		}
	default:
		return nil, status.Error(codes.InvalidArgument, "unrecognized update type")
	}

	if !s.isRunning() {
		// This request may have erroneously seen empty state. Transform it to an unavailable error to preserve state in the worker.
		return nil, notRunning()
	}

	level.Info(s.logger).Log("msg", "could not find lease during update for compaction job", "update_type", compactorschedulerpb.UpdateType_name[int32(req.Update)], "id", req.Key.Id, "epoch", req.Key.Epoch)
	return notFound()
}

func ok() (*compactorschedulerpb.UpdateJobResponse, error) {
	return &compactorschedulerpb.UpdateJobResponse{}, nil
}

func failedTo(operation string) error {
	return status.Error(codes.Internal, fmt.Sprintf("failed to %s", operation))
}

func notRunning() error {
	return status.Error(codes.Unavailable, "the compactor scheduler is not currently running (starting or shutting down)")
}

func notFound() (*compactorschedulerpb.UpdateJobResponse, error) {
	return nil, status.Error(codes.NotFound, "lease was not found")
}
