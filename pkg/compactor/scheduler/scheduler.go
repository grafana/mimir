// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"
	"flag"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/status"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/services"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/compactor"
	"github.com/grafana/mimir/pkg/compactor/scheduler/schedulerpb"
	"github.com/grafana/mimir/pkg/storage/bucket"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util"
)

const planningJobID string = "plan"

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
	f.DurationVar(&cfg.leaseCheckInterval, "compactor-scheduler.lease-check-interval", 2*time.Minute, "How often the scheduler will check for expired leases to make the work items available again.")
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
	blocks  []ulid.ULID // empty indicates a planning job
	isSplit bool
}

func (c *CompactionJob) MarshalBlocks() [][]byte {
	b := make([][]byte, len(c.blocks))
	for i := range c.blocks {
		b[i] = c.blocks[i].Bytes()
	}
	return b
}

type Scheduler struct {
	services.Service

	compactorCfg compactor.Config
	cfg          Config

	rotator            *Rotator
	subservicesManager *services.Manager

	logger log.Logger
}

func NewCompactorScheduler(
	compactorCfg compactor.Config,
	cfg Config,
	storageCfg mimir_tsdb.BlocksStorageConfig,
	logger log.Logger,
	registerer prometheus.Registerer) (*Scheduler, error) {

	scheduler := &Scheduler{
		compactorCfg: compactorCfg,
		cfg:          cfg,
		rotator:      NewRotator(cfg.leaseDuration, cfg.leaseCheckInterval, cfg.maxLeases),
		logger:       logger,
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
		bkt,
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

func (s *Scheduler) start(ctx context.Context) error {
	if err := s.subservicesManager.StartAsync(ctx); err != nil {
		return errors.Wrap(err, "unable to start compactor scheduler subservices")
	}
	if err := s.subservicesManager.AwaitHealthy(ctx); err != nil {
		return errors.Wrap(err, "compactor scheduler subservices not healthy")
	}
	return nil
}

func (s *Scheduler) run(ctx context.Context) error {
	// TODO: Implement server handling
	return nil
}

func (s *Scheduler) stop(err error) error {
	return services.StopManagerAndAwaitStopped(context.Background(), s.subservicesManager)
}

func (s *Scheduler) LeaseJob(ctx context.Context, req *schedulerpb.LeaseJobRequest) (*schedulerpb.LeaseJobResponse, error) {
	response, ok := s.rotator.LeaseJob(ctx, func(_ *CompactionJob) bool {
		// TODO: Is this a plan job and can this worker plan for this tenant?
		// Can use req.WorkerId
		//
		// It might be better to have a separate tracker for plan jobs. Otherwise
		// it might take a long time for a tenant to get planned unless we get lucky
		// in the rotation.
		return true
	})
	if ok {
		return response, nil
	}
	return nil, status.Error(codes.NotFound, "no jobs were available to be leased")
}

func (s *Scheduler) UpdateJob(ctx context.Context, req *schedulerpb.UpdateJobRequest) (*schedulerpb.UpdateJobResponse, error) {
	switch req.Update {
	case schedulerpb.IN_PROGRESS:
		return s.heartbeat(req.Tenant, req.Key)
	case schedulerpb.COMPLETE:
		fallthrough
	case schedulerpb.ABANDON:
		return s.remove(req.Tenant, req.Key)
	case schedulerpb.REASSIGN:
		return s.cancel(req.Tenant, req.Key)
	default:
		return nil, status.Error(codes.InvalidArgument, "unrecognized update type")
	}
}

func (s *Scheduler) heartbeat(tenant string, key *schedulerpb.JobKey) (*schedulerpb.UpdateJobResponse, error) {
	if s.rotator.RenewJobLease(tenant, key.Id, key.Epoch) {
		return ok()
	}
	return notFound()
}

func (s *Scheduler) remove(tenant string, key *schedulerpb.JobKey) (*schedulerpb.UpdateJobResponse, error) {
	if s.rotator.RemoveJob(tenant, key.Id, key.Epoch) {
		return ok()
	}
	return notFound()
}

func (s *Scheduler) cancel(tenant string, key *schedulerpb.JobKey) (*schedulerpb.UpdateJobResponse, error) {
	if s.rotator.CancelJobLease(tenant, key.Id, key.Epoch) {
		return ok()
	}
	return notFound()
}

func ok() (*schedulerpb.UpdateJobResponse, error) {
	return &schedulerpb.UpdateJobResponse{}, nil
}

func notFound() (*schedulerpb.UpdateJobResponse, error) {
	return nil, status.Error(codes.NotFound, "lease was not found")
}
