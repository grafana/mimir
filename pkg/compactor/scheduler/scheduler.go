package scheduler

import (
	"context"
	"flag"
	"slices"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/services"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/compactor"
	"github.com/grafana/mimir/pkg/storage/bucket"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util"
)

type Config struct {
	leaseDuration        time.Duration
	leaseCheckInterval   time.Duration
	planningInterval     time.Duration
	userDiscoveryBackoff backoff.Config
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.DurationVar(&cfg.leaseDuration, "compactor-scheduler.lease-duration", 10*time.Minute, "The duration of time without contact until the scheduler is able to lease a work item to another worker.")
	f.DurationVar(&cfg.leaseCheckInterval, "compactor-scheduler.lease-check-interval", 2*time.Minute, "How often the scheduler will check for expired leases to make the work items available again.")
	f.DurationVar(&cfg.planningInterval, "compactor-scheduler.planning-interval", 30*time.Minute, "How often the scheduler will create a planning job for individual tenants.")
	cfg.userDiscoveryBackoff.RegisterFlagsWithPrefix("compactor-scheduler", f)
}

func (cfg *Config) Validate() error {
	if cfg.leaseDuration <= 0 {
		return errors.New("compactor-scheduler.lease-duration must be positive")
	}
	if cfg.leaseCheckInterval <= 0 {
		return errors.New("compactor-scheduler.lease-check-interval must be positive")
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

type Scheduler struct {
	services.Service

	compactorCfg compactor.Config
	cfg          Config

	subservicesManager *services.Manager

	planSpawner *Spawner
	rotator     *Rotator

	logger log.Logger
}

func NewCompactorScheduler(
	compactorCfg compactor.Config,
	storageCfg mimir_tsdb.BlocksStorageConfig,
	cfg Config,

	logger log.Logger,
	registerer prometheus.Registerer) (*Scheduler, error) {

	scheduler := &Scheduler{
		compactorCfg: compactorCfg,
		cfg:          cfg,
		rotator:      NewRotator(),
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

	subservicesManager, err := services.NewManager(planSpawner)
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

func (s *Scheduler) poll(ctx context.Context) {
	s.rotator.Lease(ctx, func(_ *CompactionJob) bool {
		// TODO: Is this a plan job and can this worker plan for this tenant?
		return true
	})
	// TODO: build response
}

func (s *Scheduler) heartbeat(jobID string) bool {
}

func (s *Scheduler) fail(jobID string) error {

}

func (s *Scheduler) complete(tenant, jobID string, job *CompactionJob) error {
	s.rotator.RemoveIf(tenant, jobID, func(existing *CompactionJob) bool {
		// TODO: Is this sufficient? Would sorting ever change?
		return slices.Equal(job.blocks, existing.blocks)
	})
}
