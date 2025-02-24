// SPDX-License-Identifier: AGPL-3.0-only

package schedulerpb

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
)

// SchedulerClient is a client for the scheduler service.
// It encapsulates the communication style expected by the scheduler service:
//   - AssignJob is polled repeatedly until a job is available.
//   - UpdateJob is called periodically to update the status of all known jobs.
//
// SchedulerClient maintains a history of locally-known jobs that are expired
// some time after completion.
type SchedulerClient interface {
	Run(context.Context)
	Close()
	GetJob(context.Context) (JobKey, JobSpec, error)
	CompleteJob(JobKey) error
}

type schedulerClient struct {
	workerID       string
	updateInterval time.Duration
	maxUpdateAge   time.Duration
	scheduler      BlockBuilderSchedulerClient
	logger         log.Logger
	runDone        chan struct{}

	mu   sync.Mutex
	jobs map[JobKey]*job
}

type job struct {
	spec     JobSpec
	complete bool
	// The time, if non-zero, when this job entry will become eligible for purging.
	forgetTime time.Time
}

// NewSchedulerClient creates a new SchedulerClient around the given scheduler
// service client. The client will inform the scheduler service about jobs once
// per updateInterval, and will forget about jobs that have been complete for at
// least maxUpdateAge. Thus maxUpdateAge should be at least twice
// updateInterval.
func NewSchedulerClient(workerID string, scheduler BlockBuilderSchedulerClient, logger log.Logger,
	updateInterval time.Duration, maxUpdateAge time.Duration) (SchedulerClient, error) {

	if workerID == "" {
		return nil, fmt.Errorf("workerID cannot be empty")
	}
	if updateInterval <= 0 {
		return nil, fmt.Errorf("updateInterval must be positive")
	}
	if maxUpdateAge <= 0 {
		return nil, fmt.Errorf("maxUpdateAge must be positive")
	}
	if maxUpdateAge < updateInterval*2 {
		return nil, fmt.Errorf("maxUpdateAge must be at least twice the updateInterval")
	}

	return &schedulerClient{
		workerID:       workerID,
		updateInterval: updateInterval,
		maxUpdateAge:   maxUpdateAge,
		scheduler:      scheduler,
		logger:         logger,
		runDone:        make(chan struct{}),

		jobs: make(map[JobKey]*job),
	}, nil
}

// Run periodically sends updates to the scheduler service and performs cleanup of old jobs.
// Run will block and run until the given context is canceled.
func (s *schedulerClient) Run(ctx context.Context) {
	ticker := time.NewTicker(s.updateInterval)
	defer close(s.runDone)
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

func (s *schedulerClient) Close() {
	// Wait for the runloop to exit, then send a final update.
	<-s.runDone

	updateCtx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	s.sendUpdates(updateCtx)
}

func (s *schedulerClient) sendUpdates(ctx context.Context) {
	for key, j := range s.snapshot() {
		_, err := s.scheduler.UpdateJob(ctx, &UpdateJobRequest{
			Key:      &key,
			WorkerId: s.workerID,
			Spec:     &j.spec,
			Complete: j.complete,
		})
		if err != nil {
			level.Error(s.logger).Log("msg", "failed to update job", "job_id", key.Id, "epoch", key.Epoch, "err", err)
		}
	}
}

// snapshot returns a snapshot of the current jobs map.
func (s *schedulerClient) snapshot() map[JobKey]*job {
	s.mu.Lock()
	defer s.mu.Unlock()

	jobs := make(map[JobKey]*job, len(s.jobs))
	for k, v := range s.jobs {
		jobs[k] = &job{
			spec:       v.spec,
			complete:   v.complete,
			forgetTime: v.forgetTime,
		}
	}
	return jobs
}

func (s *schedulerClient) forgetOldJobs() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()

	for key, j := range s.jobs {
		if !j.forgetTime.IsZero() && now.After(j.forgetTime) {
			level.Info(s.logger).Log("msg", "forgetting old job", "job_id", key.Id, "epoch", key.Epoch)
			delete(s.jobs, key)
		}
	}
}

// GetJob returns the job assigned to the worker with the given ID.
// It will block until a job is available.
func (s *schedulerClient) GetJob(ctx context.Context) (JobKey, JobSpec, error) {
	boff := backoff.New(ctx, backoff.Config{
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: 10 * time.Second,
		MaxRetries: 0, // retry as long as the context is valid
	})
	var lastErr error
	for boff.Ongoing() {
		callCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		response, err := s.scheduler.AssignJob(callCtx, &AssignJobRequest{
			WorkerId: s.workerID,
		})
		cancel()
		if err != nil {
			lastErr = err
			boff.Wait()
			continue
		}

		// If we get here, we have a newly assigned job. Track it and return it.
		key := *response.GetKey()
		spec := *response.GetSpec()
		level.Info(s.logger).Log("msg", "assigned job", "job_id", key.Id, "epoch", key.Epoch)

		s.mu.Lock()
		if _, ok := s.jobs[key]; ok {
			// This should never happen; we'd like to see a log if it does.
			level.Warn(s.logger).Log("msg", "job already assigned", "job_id", key.Id, "epoch", key.Epoch)
		}

		s.jobs[key] = &job{
			spec:       spec,
			complete:   false,
			forgetTime: time.Time{},
		}
		s.mu.Unlock()

		return key, spec, nil
	}

	return JobKey{}, JobSpec{}, lastErr
}

func (s *schedulerClient) CompleteJob(jobKey JobKey) error {
	level.Info(s.logger).Log("msg", "marking job as completed", "job_id", jobKey.Id, "epoch", jobKey.Epoch)

	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[jobKey]
	if !ok {
		return fmt.Errorf("job %s (%d) not found", jobKey.GetId(), jobKey.GetEpoch())
	}
	if j.complete {
		return nil
	}

	// Set it as complete and also set a time when it'll become eligible for forgetting.
	j.complete = true
	j.forgetTime = time.Now().Add(s.maxUpdateAge)
	return nil
}

var _ SchedulerClient = (*schedulerClient)(nil)
