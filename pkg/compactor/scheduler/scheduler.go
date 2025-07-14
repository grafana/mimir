package scheduler

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/compactor"
	"github.com/grafana/mimir/pkg/util"
)

type Job struct {
	tenant string

	creationTime time.Time
	ids          []ulid.ULID // if empty, this is a plan job
	key          string
}

func (j *Job) isPlanJob() bool {
	return len(j.ids) == 0
}

type Config struct {
	leaseDuration      time.Duration
	leaseCheckInterval time.Duration
}

type Scheduler struct {
	services.Service

	compactorCfg compactor.Config
	cfg          Config

	subservicesManager *services.Manager
	jobQueue           *JobQueue[string, *Job]
	planSpawner        *PlanSpawner
	leaseManager       LeaseManager[string]

	jobTransferMtx *sync.Mutex

	logger log.Logger
}

func NewScheduler(
	compactorCfg compactor.Config,
	cfg Config,
	bkt objstore.Bucket,
	logger log.Logger) (*Scheduler, error) {

	scheduler := &Scheduler{
		compactorCfg: compactorCfg,
		cfg:          cfg,

		jobQueue: NewJobQueue(func(j *Job) string {
			return j.key
		}),

		logger: logger,
	}

	planSpawner := PlanSpawner{
		allowedTenants: util.NewAllowList(compactorCfg.EnabledTenants, compactorCfg.DisabledTenants),
		bkt:            bkt,
	}
	leaseManager := NewLeaseManager[string](cfg.leaseCheckInterval)

	subservicesManager, err := services.NewManager(planSpawner, leaseManager)
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
	return nil
}

func (s *Scheduler) stop(err error) error {
	return services.StopManagerAndAwaitStopped(context.Background(), s.subservicesManager)
}

func (s *Scheduler) poll() {
	job, ok := s.jobQueue.Poll(func(_ *Job) bool {
		// TODO: Is this a plan job and does the requesting worker own the tenant in the ring?
		return true
	})

	if !ok {
		return
	}

	s.leaseManager.AddLease(job.key, s.cfg.leaseDuration, func() {
		// When the lease expires, push the job back to the front so it can be retried
		s.jobQueue.PushFront(job)
	})
}

func (s *Scheduler) heartbeat(jobID string) bool {
	return s.leaseManager.RenewLease(jobID, s.cfg.leaseDuration)
}

func (s *Scheduler) fail(jobID string) error {
	ok := s.leaseManager.ExpireLease(jobID)

}

func (s *Scheduler) complete(jobID string) error {
	if s.leaseManager.CancelLease(jobID) {
		return nil
	}

	// Lease must have expired, may have been resubmitted

}
