// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

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
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&cfg.maxLeases, "compactor-scheduler.max-leases", 3, "The maximum number of times a job can be retried from the same plan before it is removed. 0 for no limit.")
	f.DurationVar(&cfg.leaseDuration, "compactor-scheduler.lease-duration", 10*time.Minute, "The duration of time without contact until the scheduler is able to lease a work item to another worker.")
	f.DurationVar(&cfg.leaseCheckInterval, "compactor-scheduler.lease-check-interval", 3*time.Minute, "How often the scheduler will check for expired leases to make the work items available again.")
	f.DurationVar(&cfg.planningCheckInterval, "compactor-scheduler.planning-check-interval", time.Minute, "How often the scheduler will check all tenants to see if a planning job needs to be submitted.")
	f.DurationVar(&cfg.planningInterval, "compactor-scheduler.planning-interval", 30*time.Minute, "How often the plan jobs will be created by the scheduler.")
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
	return nil
}

type CompactionJob struct {
	blocks  [][]byte // empty indicates a planning job
	isSplit bool
}

type Scheduler struct {
	services.Service

	cfg                Config
	rotator            *Rotator
	planTracker        *JobTracker[string, struct{}]
	subservicesManager *services.Manager
	logger             log.Logger
}

func NewCompactorScheduler(
	compactorCfg compactor.Config,
	cfg Config,
	storageCfg mimir_tsdb.BlocksStorageConfig,
	logger log.Logger,
	registerer prometheus.Registerer) (*Scheduler, error) {

	planTracker := NewJobTracker[string, struct{}](INFINITE_LEASES)

	scheduler := &Scheduler{
		cfg:         cfg,
		rotator:     NewRotator(planTracker, cfg.leaseDuration, cfg.leaseCheckInterval, cfg.maxLeases),
		planTracker: planTracker,
		logger:      logger,
	}

	// TODO: This will need to be moved for testing
	bkt, err := bucket.NewClient(context.Background(), storageCfg.Bucket, "compactor-scheduler", logger, registerer)
	if err != nil {
		return nil, err
	}

	planSpawner := NewSpawner(
		cfg,
		util.NewAllowList(compactorCfg.EnabledTenants, compactorCfg.DisabledTenants),
		scheduler.rotator,
		scheduler.planTracker,
		bkt,
		logger,
	)

	subservicesManager, err := services.NewManager(scheduler.rotator, planSpawner)
	if err != nil {
		return nil, err
	}
	scheduler.subservicesManager = subservicesManager

	svc := services.NewBasicService(scheduler.start, nil, scheduler.stop)
	scheduler.Service = svc

	return scheduler, nil

}

func (s *Scheduler) start(ctx context.Context) error {
	if err := s.subservicesManager.StartAsync(ctx); err != nil {
		return fmt.Errorf("unable to start compactor scheduler subservices: %w", err)
	}
	if err := s.subservicesManager.AwaitHealthy(ctx); err != nil {
		return fmt.Errorf("compactor scheduler subservices not healthy: %w", err)
	}
	return nil
}

func (s *Scheduler) stop(err error) error {
	return services.StopManagerAndAwaitStopped(context.Background(), s.subservicesManager)
}

func (s *Scheduler) LeaseJob(ctx context.Context, req *compactorschedulerpb.LeaseJobRequest) (*compactorschedulerpb.LeaseJobResponse, error) {
	// Plan jobs are checked first
	tenant, _, epoch, ok := s.planTracker.Lease(func(tenant string, _ struct{}) bool {
		// TODO: Using req.WorkerId, can this worker plan for this tenant?
		// Calculating this under a write lock is likely undesirable, the list of known tenants is in the planSpawner too
		return true
	})
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

	// There were no available plan jobs, check for compaction jobs
	response, ok := s.rotator.LeaseJob(ctx, func(_ string, _ *CompactionJob) bool {
		return true
	})

	if ok {
		level.Info(s.logger).Log("msg", "leasing compaction job", "tenant", response.Spec.Tenant, "id", response.Key.Id, "epoch", response.Key.Epoch, "worker", req.WorkerId)
		return response, nil
	}

	level.Info(s.logger).Log("msg", "no jobs available to be leased", "worker", req.WorkerId)
	return nil, status.Error(codes.NotFound, "no jobs were available to be leased")
}

func (s *Scheduler) PlannedJobs(ctx context.Context, req *compactorschedulerpb.PlannedJobsRequest) (*compactorschedulerpb.PlannedJobsResponse, error) {
	if removed, _ := s.planTracker.Remove(req.Key.Id, req.Key.Epoch); removed {
		level.Info(s.logger).Log("msg", "received plan results", "tenant", req.Key.Id, "epoch", req.Key.Epoch, "job_count", len(req.Jobs))
		now := time.Now()
		jobs := make([]*Job[string, *CompactionJob], 0, len(req.Jobs))
		for _, job := range req.Jobs {
			jobs = append(jobs, NewJob(
				job.Id,
				&CompactionJob{
					blocks:  job.Job.BlockIds,
					isSplit: job.Job.Split,
				},
				now,
			))
		}
		s.rotator.OfferJobs(req.Key.Id, jobs, func(_ *CompactionJob, _ *CompactionJob) bool {
			return true
		})
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
		if removed, _ := s.planTracker.Remove(req.Key.Id, req.Key.Epoch); removed {
			return ok()
		}
	case compactorschedulerpb.REASSIGN:
		if canceled, _ := s.planTracker.CancelLease(req.Key.Id, req.Key.Epoch); canceled {
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
		if s.rotator.RemoveJob(req.Tenant, req.Key.Id, req.Key.Epoch) {
			return ok()
		}
	case compactorschedulerpb.REASSIGN:
		if s.rotator.CancelJobLease(req.Tenant, req.Key.Id, req.Key.Epoch) {
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
