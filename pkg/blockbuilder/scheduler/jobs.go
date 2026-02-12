// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"container/list"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/grafana/mimir/pkg/blockbuilder/schedulerpb"
)

var (
	errNoJobAvailable        = errors.New("no job available")
	errJobNotFound           = errors.New("job not found")
	errJobNotAssigned        = errors.New("job not assigned to given worker")
	errBadEpoch              = errors.New("bad epoch")
	errJobAlreadyExists      = errors.New("job already exists")
	errJobCreationDisallowed = errors.New("job creation policy disallowed job")
)

type jobInfo struct {
	ID          string    `json:"id"`
	Epoch       int64     `json:"epoch"`
	Topic       string    `json:"topic"`
	Partition   int32     `json:"partition"`
	StartOffset int64     `json:"start_offset"`
	EndOffset   int64     `json:"end_offset"`
	Assignee    string    `json:"assignee,omitempty"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"created_at"`
	LeaseExpiry time.Time `json:"lease_expiry,omitempty"`
	FailCount   int       `json:"fail_count"`
}

type jobQueue[T any] struct {
	leaseExpiry        time.Duration
	creationPolicy     jobCreationPolicy[T]
	jobFailuresAllowed int
	metrics            schedulerMetrics
	logger             log.Logger

	mu         sync.Mutex
	epoch      int64
	jobs       map[string]*job[T]
	unassigned *list.List
}

func newJobQueue[T any](leaseExpiry time.Duration, jobCreationPolicy jobCreationPolicy[T], jobFailuresAllowed int, metrics schedulerMetrics, logger log.Logger) *jobQueue[T] {
	return &jobQueue[T]{
		leaseExpiry:        leaseExpiry,
		creationPolicy:     jobCreationPolicy,
		jobFailuresAllowed: jobFailuresAllowed,
		metrics:            metrics,
		logger:             logger,

		jobs:       make(map[string]*job[T]),
		unassigned: list.New(),
	}
}

// assign assigns the highest-priority unassigned job to the given worker.
func (s *jobQueue[T]) assign(workerID string) (jobKey, T, error) {
	var empty T
	if workerID == "" {
		return jobKey{}, empty, errors.New("workerID cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	next := s.unassigned.Front()
	if next == nil {
		return jobKey{}, empty, errNoJobAvailable
	}
	j := s.unassigned.Remove(next).(*job[T])

	j.key.epoch = s.epoch
	s.epoch++
	j.assignee = workerID
	j.leaseExpiry = time.Now().Add(s.leaseExpiry)

	level.Info(s.logger).Log("msg", "assigned job", "job_id", j.key.id, "epoch", j.key.epoch, "worker_id", workerID)

	return j.key, j.spec, nil
}

// importJob imports a job with the given ID and spec into the jobQueue. This is
// meant to be used during recovery, when we're reconstructing the jobQueue from
// worker updates.
func (s *jobQueue[T]) importJob(key jobKey, workerID string, spec T) error {
	if key.id == "" {
		return errors.New("jobID cannot be empty")
	}
	if workerID == "" {
		return errors.New("workerID cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// When we start assigning new jobs, the epochs need to be compatible with
	// these "imported" jobs.
	s.epoch = max(s.epoch, key.epoch+1)

	j, ok := s.jobs[key.id]
	if ok {
		if key.epoch < j.key.epoch {
			return errBadEpoch
		} else if key.epoch == j.key.epoch {
			if j.assignee != workerID {
				return errJobNotAssigned
			}
		} else {
			// Otherwise, this caller is the new authority, so we accept the update.
			j.assignee = workerID
			j.key = key
			j.spec = spec
		}
	} else {
		s.jobs[key.id] = &job[T]{
			key:         key,
			assignee:    workerID,
			leaseExpiry: time.Now().Add(s.leaseExpiry),
			failCount:   0,
			spec:        spec,
		}
	}
	return nil
}

// add adds a new job with the given spec.
func (s *jobQueue[T]) add(id string, spec T) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.jobs[id]; ok {
		return errJobAlreadyExists
	}

	// See if the creation policy would allow it.

	existingJobs := make([]*T, 0, len(s.jobs))
	for _, j := range s.jobs {
		existingJobs = append(existingJobs, &j.spec)
	}
	if !s.creationPolicy.canCreateJob(jobKey{id: id}, &spec, existingJobs) {
		return errJobCreationDisallowed
	}

	j := &job[T]{
		key: jobKey{
			id:    id,
			epoch: 0,
		},
		assignee:    "",
		leaseExpiry: time.Now().Add(s.leaseExpiry),
		failCount:   0,
		spec:        spec,
	}
	s.jobs[id] = j

	s.unassigned.PushBack(j)

	level.Info(s.logger).Log("msg", "created job", "job_id", id)
	return nil
}

// renewLease renews the lease of the job with the given ID for the given
// worker.
func (s *jobQueue[T]) renewLease(key jobKey, workerID string) error {
	if key.id == "" {
		return errors.New("jobID cannot be empty")
	}
	if workerID == "" {
		return errors.New("workerID cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[key.id]
	if !ok {
		return errJobNotFound
	}
	if j.key.epoch != key.epoch {
		return errBadEpoch
	}
	if j.assignee != workerID {
		return errJobNotAssigned
	}

	j.leaseExpiry = time.Now().Add(s.leaseExpiry)

	level.Debug(s.logger).Log("msg", "renewed lease", "job_id", key.id, "epoch", key.epoch, "worker_id", workerID)
	return nil
}

// completeJob completes the job with the given ID for the given worker,
// removing it from the jobQueue.
func (s *jobQueue[T]) completeJob(key jobKey, workerID string) error {
	if key.id == "" {
		return errors.New("jobID cannot be empty")
	}
	if workerID == "" {
		return errors.New("workerID cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[key.id]
	if !ok {
		return errJobNotFound
	}
	if j.key.epoch != key.epoch {
		return errBadEpoch
	}
	if j.assignee != workerID {
		return errJobNotAssigned
	}

	delete(s.jobs, key.id)

	level.Info(s.logger).Log("msg", "removed completed job from queue", "job_id", key.id, "epoch", key.epoch, "worker_id", workerID)
	return nil
}

// clearExpiredLeases unassigns jobs whose leases have expired, making them
// eligible for reassignment.
func (s *jobQueue[T]) clearExpiredLeases() {
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, j := range s.jobs {
		if j.assignee != "" && now.After(j.leaseExpiry) {
			priorAssignee := j.assignee
			j.assignee = ""
			j.failCount++

			// An expired job gets to go to the front of the unassigned list.
			s.unassigned.PushFront(j)

			if j.failCount > s.jobFailuresAllowed {
				s.metrics.persistentJobFailures.Inc()
				level.Error(s.logger).Log("msg", "job failed in a persistent manner", "job_id", j.key.id, "epoch", j.key.epoch, "assignee", priorAssignee, "fail_count", j.failCount)
			} else {
				level.Info(s.logger).Log("msg", "unassigned expired lease", "job_id", j.key.id, "epoch", j.key.epoch, "assignee", priorAssignee)
			}
		}
	}
}

// removeJob removes a job from both the jobs map and unassigned list.
func (s *jobQueue[T]) removeJob(key jobKey) {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[key.id]
	if !ok {
		return
	}

	// Remove from jobs map
	delete(s.jobs, key.id)

	// If the job is in the unassigned list, remove it
	if j.assignee == "" {
		// Find and remove the job from the unassigned list
		for e := s.unassigned.Front(); e != nil; e = e.Next() {
			if e.Value.(*job[T]).key.id == key.id {
				s.unassigned.Remove(e)
				break
			}
		}
	}

	level.Debug(s.logger).Log("msg", "removed job", "job_id", key.id, "epoch", key.epoch)
}

// count returns the number of jobs in the jobQueue.
func (s *jobQueue[T]) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.jobs)
}

// assigned returns the number of assigned jobs in the jobQueue.
func (s *jobQueue[T]) assigned() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := 0
	for _, j := range s.jobs {
		if j.assignee != "" {
			count++
		}
	}
	return count
}

func (s *jobQueue[T]) setEpoch(epoch int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.epoch = epoch
}

// getAllJobs returns information about all jobs in the job queue
func (s *jobQueue[T]) getAllJobs() []jobInfo {
	s.mu.Lock()
	defer s.mu.Unlock()

	jobs := make([]jobInfo, 0, len(s.jobs))

	for _, j := range s.jobs {
		status := "pending"
		if j.assignee != "" {
			status = "assigned"
		}
		if j.failCount > 0 {
			status = fmt.Sprintf("%s (failed %d times)", status, j.failCount)
		}

		// Convert the generic spec to JobSpec
		if spec, ok := any(j.spec).(schedulerpb.JobSpec); ok {
			jobs = append(jobs, jobInfo{
				ID:          j.key.id,
				Epoch:       j.key.epoch,
				Topic:       spec.Topic,
				Partition:   spec.Partition,
				StartOffset: spec.StartOffset,
				EndOffset:   spec.EndOffset,
				Assignee:    j.assignee,
				Status:      status,
				CreatedAt:   time.Now(), // We don't track creation time
				LeaseExpiry: j.leaseExpiry,
				FailCount:   j.failCount,
			})
		}
	}

	return jobs
}

type job[T any] struct {
	key jobKey

	assignee    string
	leaseExpiry time.Time
	failCount   int

	// spec contains the job payload details disseminated to the worker.
	spec T
}

type jobKey struct {
	id string
	// The assignment epoch. This is used to break ties when multiple workers
	// have knowledge of the same job.
	epoch int64
}

type jobCreationPolicy[T any] interface {
	canCreateJob(jobKey, *T, []*T) bool
}

type noOpJobCreationPolicy[T any] struct{}

func (p noOpJobCreationPolicy[T]) canCreateJob(_ jobKey, _ *T, _ []*T) bool { // nolint:unused
	return true
}

var _ jobCreationPolicy[any] = (*noOpJobCreationPolicy[any])(nil)
