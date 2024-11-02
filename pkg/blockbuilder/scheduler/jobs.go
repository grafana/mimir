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
)

type jobQueue struct {
	leaseTime time.Duration
	logger    log.Logger

	mu         sync.Mutex
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

func (s *jobQueue) assign(worker string) (*job, error) {
	if worker == "" {
		return nil, errors.New("worker cannot not be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.unassigned.Len() == 0 {
		return nil, errNoJobAvailable
	}

	j := heap.Pop(&s.unassigned).(*job)
	j.assignee = worker
	j.leaseExpiry = time.Now().Add(s.leaseTime)
	return j, nil
}

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
			id:          id,
			assignee:    "",
			leaseExpiry: time.Now().Add(s.leaseTime),
			failCount:   0,
			spec:        spec,
		}
		s.jobs[id] = j
		heap.Push(&s.unassigned, j)
	}
}

func (s *jobQueue) renewLease(jobID, worker string) error {
	if jobID == "" {
		return errors.New("jobID cannot be empty")
	}
	if worker == "" {
		return errors.New("worker cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[jobID]
	if !ok {
		return errJobNotFound
	}
	if j.assignee != worker {
		return errJobNotAssigned
	}

	j.leaseExpiry = time.Now().Add(s.leaseTime)
	return nil
}

func (s *jobQueue) completeJob(jobID string, worker string) error {
	if jobID == "" {
		return errors.New("jobID cannot be empty")
	}
	if worker == "" {
		return errors.New("worker cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[jobID]
	if !ok {
		return errJobNotFound
	}
	if j.assignee != worker {
		return errJobNotAssigned
	}

	delete(s.jobs, jobID)
	return nil
}

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
	id string

	assignee    string
	leaseExpiry time.Time
	failCount   int

	// job payload details. We can make this generic later for reuse.
	spec jobSpec
}

func (a *job) less(b *job) bool {
	return a.spec.commitRecTs.Before(b.spec.commitRecTs)
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

type jobHeap []*job

// Implement the heap.Interface for jobHeap.
func (h *jobHeap) Len() int           { return len(*h) }
func (h *jobHeap) Less(i, j int) bool { return (*h)[i].less((*h)[j]) }
func (h *jobHeap) Swap(i, j int)      { (*h)[i], (*h)[j] = (*h)[j], (*h)[i] }

func (h *jobHeap) Push(x interface{}) {
	*h = append(*h, x.(*job))
}

func (h *jobHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

var _ heap.Interface = (*jobHeap)(nil)
