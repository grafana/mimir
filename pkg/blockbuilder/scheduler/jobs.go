// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"container/heap"
	"errors"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

var (
	errNoJobAvailable = errors.New("no job available")
	errJobNotFound    = errors.New("job not found")
	errJobNotAssigned = errors.New("job not assigned to given worker")
	errBadEpoch       = errors.New("bad epoch")
)

type jobQueue struct {
	leaseExpiry time.Duration
	logger      log.Logger

	mu         sync.Mutex
	epoch      int64
	jobs       map[string]*job
	unassigned jobHeap
}

func newJobQueue(leaseExpiry time.Duration, logger log.Logger) *jobQueue {
	return &jobQueue{
		leaseExpiry: leaseExpiry,
		logger:      logger,

		jobs: make(map[string]*job),
	}
}

// assign assigns the highest-priority unassigned job to the given worker.
func (s *jobQueue) assign(workerID string) (jobKey, jobSpec, error) {
	if workerID == "" {
		return jobKey{}, jobSpec{}, errors.New("workerID cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.unassigned.Len() == 0 {
		return jobKey{}, jobSpec{}, errNoJobAvailable
	}

	j := heap.Pop(&s.unassigned).(*job)
	j.key.epoch = s.epoch
	s.epoch++
	j.assignee = workerID
	j.leaseExpiry = time.Now().Add(s.leaseExpiry)
	return j.key, j.spec, nil
}

// importJob imports a job with the given ID and spec into the jobQueue. This is
// meant to be used during recovery, when we're reconstructing the jobQueue from
// worker updates.
func (s *jobQueue) importJob(key jobKey, workerID string, spec jobSpec) error {
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
		s.jobs[key.id] = &job{
			key:         key,
			assignee:    workerID,
			leaseExpiry: time.Now().Add(s.leaseExpiry),
			failCount:   0,
			spec:        spec,
		}
	}
	return nil
}

// addOrUpdate adds a new job or updates an existing job with the given spec.
func (s *jobQueue) addOrUpdate(id string, spec jobSpec) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if j, ok := s.jobs[id]; ok {
		// We can only update an unassigned job.
		if j.assignee == "" {
			level.Info(s.logger).Log("msg", "updated job", "job_id", id)
			j.spec = spec
		}
		return
	}

	// Otherwise, add a new job.
	j := &job{
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
	heap.Push(&s.unassigned, j)

	level.Info(s.logger).Log("msg", "created job", "job_id", id)
}

// renewLease renews the lease of the job with the given ID for the given
// worker.
func (s *jobQueue) renewLease(key jobKey, workerID string) error {
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
	if j.assignee != workerID {
		return errJobNotAssigned
	}
	if j.key.epoch != key.epoch {
		return errBadEpoch
	}

	j.leaseExpiry = time.Now().Add(s.leaseExpiry)

	level.Info(s.logger).Log("msg", "renewed lease", "job_id", key.id, "epoch", key.epoch, "worker_id", workerID)

	return nil
}

// completeJob completes the job with the given ID for the given worker,
// removing it from the jobQueue.
func (s *jobQueue) completeJob(key jobKey, workerID string) error {
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
	if j.assignee != workerID {
		return errJobNotAssigned
	}
	if j.key.epoch != key.epoch {
		return errBadEpoch
	}

	delete(s.jobs, key.id)

	level.Info(s.logger).Log("msg", "removed completed job from queue", "job_id", key.id, "epoch", key.epoch, "worker_id", workerID)
	return nil
}

// clearExpiredLeases unassigns jobs whose leases have expired, making them
// eligible for reassignment.
func (s *jobQueue) clearExpiredLeases() {
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, j := range s.jobs {
		if j.assignee != "" && now.After(j.leaseExpiry) {
			j.assignee = ""
			j.failCount++
			heap.Push(&s.unassigned, j)

			level.Debug(s.logger).Log("msg", "unassigned expired lease", "job_id", j.key.id, "epoch", j.key.epoch, "assignee", j.assignee)
		}
	}
}

type job struct {
	key jobKey

	assignee    string
	leaseExpiry time.Time
	failCount   int

	// job payload details. We can make this generic later for reuse.
	spec jobSpec
}

type jobKey struct {
	id string
	// The assignment epoch. This is used to break ties when multiple workers
	// have knowledge of the same job.
	epoch int64
}

type jobSpec struct {
	topic          string
	partition      int32
	startOffset    int64
	endOffset      int64
	commitRecTs    time.Time
	lastSeenOffset int64
	lastBlockEndTs time.Time
}

func (a *jobSpec) less(b *jobSpec) bool {
	return a.commitRecTs.Before(b.commitRecTs)
}

type jobHeap []*job

// Implement the heap.Interface for jobHeap.
func (h jobHeap) Len() int           { return len(h) }
func (h jobHeap) Less(i, j int) bool { return h[i].spec.less(&h[j].spec) }
func (h jobHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *jobHeap) Push(x interface{}) {
	*h = append(*h, x.(*job))
}

func (h *jobHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*h = old[0 : n-1]
	return x
}

var _ heap.Interface = (*jobHeap)(nil)
