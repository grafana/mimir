// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"container/heap"
	"errors"
	"sync"
	"time"

	"github.com/go-kit/log"
)

var (
	errNoJobAvailable = errors.New("no job available")
	errJobNotFound    = errors.New("job not found")
	errJobNotAssigned = errors.New("job not assigned to worker")
	errBadEpoch       = errors.New("bad epoch")
)

type jobQueue struct {
	leaseTime time.Duration
	logger    log.Logger

	mu sync.Mutex

	epoch      uint
	jobs       map[string]*job
	unassigned jobHeap
}

func newJobQueue(leaseTime time.Duration, logger log.Logger) *jobQueue {
	return &jobQueue{
		leaseTime: leaseTime,
		logger:    logger,

		jobs: make(map[string]*job),
	}
}

func (s *jobQueue) setEpoch(epoch uint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.epoch = epoch
}

// assign assigns the highest-priority unassigned job to the given worker.
func (s *jobQueue) assign(workerID string) (jobKey, jobSpec, error) {
	if workerID == "" {
		return jobKey{}, jobSpec{}, errors.New("workerID cannot not be empty")
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
	j.leaseExpiry = time.Now().Add(s.leaseTime)
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
		j = &job{
			key:         key,
			assignee:    workerID,
			leaseExpiry: time.Now().Add(s.leaseTime),
			failCount:   0,
			spec:        spec,
		}
		s.jobs[key.id] = j
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
			j.spec = spec
		}
	} else {
		j = &job{
			key: jobKey{
				id:    id,
				epoch: 0,
			},
			assignee:    "",
			leaseExpiry: time.Now().Add(s.leaseTime),
			failCount:   0,
			spec:        spec,
		}
		s.jobs[id] = j
		heap.Push(&s.unassigned, j)
	}
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

	j.leaseExpiry = time.Now().Add(s.leaseTime)
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
	epoch uint
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
func (h *jobHeap) Len() int           { return len(*h) }
func (h *jobHeap) Less(i, j int) bool { return (*h)[i].spec.less(&(*h)[j].spec) }
func (h *jobHeap) Swap(i, j int)      { (*h)[i], (*h)[j] = (*h)[j], (*h)[i] }

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
