// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"slices"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/status"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
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

	cfg                Config
	allowList          *util.AllowList
	jpm                JobPersistenceManager
	rotator            *Rotator
	planTracker        *JobTracker[struct{}]
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

	jpm, err := jobPersistenceManagerFactory(cfg)
	if err != nil {
		return nil, err
	}

	// TODO: Creates file on init, not ideal
	pjp, err := jpm.InitializePlanning()
	if err != nil {
		return nil, err
	}

	planTracker := NewJobTracker(pjp, InfiniteLeases, "planning", metrics)

	scheduler := &Scheduler{
		cfg:         cfg,
		allowList:   allowList,
		jpm:         jpm,
		rotator:     NewRotator(planTracker, cfg.leaseDuration, cfg.leaseCheckInterval, metrics, logger),
		planTracker: planTracker,
		metrics:     metrics,
		logger:      logger,
		clock:       clock.New(),
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
		scheduler.planTracker,
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

func (s *Scheduler) createCompactionTracker(jp JobPersister[*CompactionJob]) *JobTracker[*CompactionJob] {
	return NewJobTracker(jp, s.cfg.maxLeases, "compaction", s.metrics)
}

func (s *Scheduler) start(ctx context.Context) error {
	compactionTrackers, err := s.jpm.RecoverAll(s.allowList, s.planTracker, s.createCompactionTracker)
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
	<-ctx.Done()
	return nil
}

func (s *Scheduler) stop(err error) error {
	// We want the other services to stop first since they may also use the database
	stopErr := services.StopManagerAndAwaitStopped(context.Background(), s.subservicesManager)

	// Regardless of stopErr value, try to close database
	if err := s.jpm.Close(); err != nil {
		level.Error(s.logger).Log("msg", "failed to close database", "err", err)
		return err
	}

	return stopErr
}

func (s *Scheduler) LeaseJob(ctx context.Context, req *compactorschedulerpb.LeaseJobRequest) (*compactorschedulerpb.LeaseJobResponse, error) {
	// Plan jobs are checked first
	tenant, _, epoch, ok, err := s.planTracker.Lease(func(tenant string, _ struct{}) bool {
		// TODO: Using req.WorkerId, can this worker plan for this tenant?
		// Calculating this under a write lock is likely undesirable, the list of known tenants is in the planSpawner too
		return true
	})
	if err != nil {
		level.Error(s.logger).Log("msg", "failed leasing plan job", "err", err)
		return nil, err
	}
	if ok {
		level.Info(s.logger).Log("msg", "leasing plan job", "tenant", tenant, "epoch", epoch, "worker", req.WorkerId)
		return &compactorschedulerpb.LeaseJobResponse{
			Key: &compactorschedulerpb.JobKey{
				Id:    tenant,
				Epoch: epoch,
			},
			Spec: &compactorschedulerpb.JobSpec{
				JobType: compactorschedulerpb.PLANNING,
			},
		}, nil
	}

	// There were no pending plan jobs, check for compaction jobs
	response, ok, err := s.rotator.LeaseJob(ctx, func(_ string, _ *CompactionJob) bool {
		return true
	})
	if err != nil {
		level.Error(s.logger).Log("msg", "failed leasing compaction job", "err", err)
		return nil, err
	}
	if ok {
		level.Info(s.logger).Log("msg", "leasing compaction job", "tenant", response.Spec.Tenant, "id", response.Key.Id, "epoch", response.Key.Epoch, "worker", req.WorkerId)
		return response, nil
	}

	level.Info(s.logger).Log("msg", "no jobs pending to be leased", "worker", req.WorkerId)
	return nil, status.Error(codes.NotFound, "no jobs were pending to be leased")
}

func (s *Scheduler) PlannedJobs(ctx context.Context, req *compactorschedulerpb.PlannedJobsRequest) (*compactorschedulerpb.PlannedJobsResponse, error) {
	removed, _, err := s.planTracker.Remove(req.Key.Id, req.Key.Epoch)
	if err != nil {
		level.Error(s.logger).Log("msg", "failed removing plan job", "err", err)
		return nil, err
	}
	if removed {
		level.Info(s.logger).Log("msg", "received plan results", "tenant", req.Key.Id, "epoch", req.Key.Epoch, "job_count", len(req.Jobs))
		now := s.clock.Now()
		jobs := make([]*Job[*CompactionJob], 0, len(req.Jobs))
		for _, job := range req.Jobs {
			jobs = append(jobs, NewJob(
				job.Id,
				&CompactionJob{
					blocks:  job.Job.BlockIds,
					isSplit: job.Job.Split,
				},
				now,
				s.clock,
			))
		}
		_, err := s.rotator.OfferJobs(req.Key.Id, jobs, func(previous *CompactionJob, new *CompactionJob) bool {
			// Replace an existing job with the same ID if it has differs in type of compaction or has different blocks
			return previous.isSplit != new.isSplit || !slices.EqualFunc(previous.blocks, new.blocks, slices.Equal)
		})
		if err != nil {
			level.Error(s.logger).Log("msg", "failed offering result of plan job", "err", err)
			return nil, err
		}
		return &compactorschedulerpb.PlannedJobsResponse{}, nil
	}

	level.Error(s.logger).Log("msg", "received results for unknown plan job", "tenant", req.Key.Id, "epoch", req.Key.Epoch, "job_count", len(req.Jobs))
	return nil, status.Error(codes.NotFound, "plan job was not found")
}

func (s *Scheduler) UpdatePlanJob(ctx context.Context, req *compactorschedulerpb.UpdatePlanJobRequest) (*compactorschedulerpb.UpdateJobResponse, error) {
	level.Info(s.logger).Log("msg", "received plan job lease update request", "update_type", compactorschedulerpb.UpdateType_name[int32(req.Update)], "id", req.Key.Id, "epoch", req.Key.Epoch)
	switch req.Update {
	case compactorschedulerpb.IN_PROGRESS:
		if s.planTracker.RenewLease(req.Key.Id, req.Key.Epoch) {
			return ok()
		}
	case compactorschedulerpb.COMPLETE:
		fallthrough
	case compactorschedulerpb.ABANDON:
		removed, _, err := s.planTracker.Remove(req.Key.Id, req.Key.Epoch)
		if err != nil {
			return nil, err
		}
		if removed {
			return ok()
		}
	case compactorschedulerpb.REASSIGN:
		canceled, _, err := s.planTracker.CancelLease(req.Key.Id, req.Key.Epoch)
		if err != nil {
			return nil, err
		}
		if canceled {
			return ok()
		}
	default:
		return nil, status.Error(codes.InvalidArgument, "unrecognized update type")
	}

	level.Info(s.logger).Log("msg", "could not find lease during update for plan job", "update_type", compactorschedulerpb.UpdateType_name[int32(req.Update)], "id", req.Key.Id, "epoch", req.Key.Epoch)
	return notFound()
}

func (s *Scheduler) UpdateCompactionJob(ctx context.Context, req *compactorschedulerpb.UpdateCompactionJobRequest) (*compactorschedulerpb.UpdateJobResponse, error) {
	level.Info(s.logger).Log("msg", "received compaction lease update request", "update_type", compactorschedulerpb.UpdateType_name[int32(req.Update)], "id", req.Key.Id, "epoch", req.Key.Epoch)
	switch req.Update {
	case compactorschedulerpb.IN_PROGRESS:
		if s.rotator.RenewJobLease(req.Tenant, req.Key.Id, req.Key.Epoch) {
			return ok()
		}
	case compactorschedulerpb.COMPLETE:
		fallthrough
	case compactorschedulerpb.ABANDON:
		removed, err := s.rotator.RemoveJob(req.Tenant, req.Key.Id, req.Key.Epoch)
		if err != nil {
			return nil, err
		}
		if removed {
			return ok()
		}
	case compactorschedulerpb.REASSIGN:
		canceled, err := s.rotator.CancelJobLease(req.Tenant, req.Key.Id, req.Key.Epoch)
		if err != nil {
			return nil, err
		}
		if canceled {
			return ok()
		}
	default:
		return nil, status.Error(codes.InvalidArgument, "unrecognized update type")
	}

	level.Info(s.logger).Log("msg", "could not find lease during update for compaction job", "update_type", compactorschedulerpb.UpdateType_name[int32(req.Update)], "id", req.Key.Id, "epoch", req.Key.Epoch)
	return notFound()
}

func ok() (*compactorschedulerpb.UpdateJobResponse, error) {
	return &compactorschedulerpb.UpdateJobResponse{}, nil
}

func notFound() (*compactorschedulerpb.UpdateJobResponse, error) {
	return nil, status.Error(codes.NotFound, "lease was not found")
}
