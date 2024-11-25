package schedulerclient

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"

	"github.com/grafana/mimir/pkg/blockbuilder/scheduler"
)

type SchedulerClient interface {
}

// SchedulerClient is a client for the scheduler service.
// It encapsulates the communication style expected by the scheduler service.
type schedulerClient struct {
	workerID       string
	updateInterval time.Duration
	maxUpdateAge   time.Duration
	srv            scheduler.BlockBuilderSchedulerServer
	logger         log.Logger

	mu   sync.Mutex
	jobs map[scheduler.JobKey]job
}

type job struct {
	spec       scheduler.JobSpec
	complete   bool
	forgetTime time.Time
}

func NewSchedulerClient(workerID string, srv scheduler.BlockBuilderSchedulerServer, logger log.Logger,
	updateInterval time.Duration, maxUpdateAge time.Duration) SchedulerClient {

	return &schedulerClient{
		workerID:       workerID,
		updateInterval: updateInterval,
		maxUpdateAge:   maxUpdateAge,
		srv:            srv,
		logger:         logger,

		jobs: make(map[scheduler.JobKey]job),
	}
}

func (s *schedulerClient) Run(ctx context.Context) {
	ticker := time.NewTicker(s.updateInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.sendUpdates(ctx)
			s.forgetOldJobs()

		case <-ctx.Done():
			return
		}
	}
}

func (s *schedulerClient) snapshot() map[scheduler.JobKey]job {
	s.mu.Lock()
	defer s.mu.Unlock()

	jobs := make(map[scheduler.JobKey]job, len(s.jobs))
	for k, v := range s.jobs {
		jobs[k] = job{
			spec:       v.spec,
			complete:   v.complete,
			forgetTime: v.forgetTime,
		}
	}
	return jobs
}

func (s *schedulerClient) sendUpdates(ctx context.Context) {
	for key, j := range s.snapshot() {
		_, err := s.srv.UpdateJob(ctx, &scheduler.UpdateJobRequest{
			Key:      &key,
			WorkerId: s.workerID,
			Spec:     &j.spec,
			Complete: j.complete,
		})
		if err != nil {
			level.Error(s.logger).Log("msg", "failed to update job", "key", key, "err", err)
		}
	}
}

func (s *schedulerClient) forgetOldJobs() {
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	for key, j := range s.jobs {
		if !j.forgetTime.IsZero() && now.After(j.forgetTime) {
			delete(s.jobs, key)
		}
	}
}

// GetJob returns the job assigned to the worker with the given ID.
// It will block until a job is available.
func (s *schedulerClient) GetJob(ctx context.Context, workerID string) (scheduler.JobKey, scheduler.JobSpec, error) {
	boff := backoff.New(ctx, backoff.Config{
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: 5 * time.Second,
		MaxRetries: 0, // retry as long as the context is valid
	})
	var lastErr error
	for boff.Ongoing() {
		response, err := s.srv.AssignJob(ctx, &scheduler.AssignJobRequest{
			WorkerId: s.workerID,
		})
		if err != nil {
			lastErr = err
			boff.Wait()
			continue
		}

		// If we get here, we have a newly assigned job. Track it and return it.
		key := *response.GetKey()
		spec := *response.GetSpec()
		level.Info(s.logger).Log("msg", "assigned job", "jobID", key.Id, "epoch", key.Epoch)

		s.mu.Lock()
		s.jobs[key] = job{
			spec:       spec,
			complete:   false,
			forgetTime: time.Time{},
		}
		s.mu.Unlock()

		return key, spec, nil
	}

	return scheduler.JobKey{}, scheduler.JobSpec{}, lastErr
}

func (s *schedulerClient) CompleteJob(ctx context.Context, jobKey scheduler.JobKey) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[jobKey]
	if !ok {
		return fmt.Errorf("job %s (%d) not found", jobKey.GetId(), jobKey.GetEpoch())
	}

	j.complete = true
	j.forgetTime = time.Now().Add(s.maxUpdateAge)
	return nil
}
