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

// assign assigns the highest-priority unassigned job to the given worker.
func (s *jobQueue) assign(workerID string) (string, jobSpec, error) {
	if workerID == "" {
		return "", jobSpec{}, errors.New("workerID cannot not be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.unassigned.Len() == 0 {
		return "", jobSpec{}, errNoJobAvailable
	}

	j := heap.Pop(&s.unassigned).(*job)
	j.assignee = workerID
	j.leaseExpiry = time.Now().Add(s.leaseTime)
	return j.id, j.spec, nil
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

// renewLease renews the lease of the job with the given ID for the given
// worker.
func (s *jobQueue) renewLease(jobID, workerID string) error {
	if jobID == "" {
		return errors.New("jobID cannot be empty")
	}
	if workerID == "" {
		return errors.New("workerID cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[jobID]
	if !ok {
		return errJobNotFound
	}
	if j.assignee != workerID {
		return errJobNotAssigned
	}

	j.leaseExpiry = time.Now().Add(s.leaseTime)
	return nil
}

// completeJob completes the job with the given ID for the given worker,
// removing it from the jobQueue.
func (s *jobQueue) completeJob(jobID, workerID string) error {
	if jobID == "" {
		return errors.New("jobID cannot be empty")
	}
	if workerID == "" {
		return errors.New("workerID cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[jobID]
	if !ok {
		return errJobNotFound
	}
	if j.assignee != workerID {
		return errJobNotAssigned
	}

	delete(s.jobs, jobID)
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
	id string

	assignee    string
	leaseExpiry time.Time
	failCount   int

	// job payload details. We can make this generic later for reuse.
	spec jobSpec
}

type jobSpec struct {
	topic     string
	partition int32

	// Start and end offsets of the scan window, inclusive. (i.e., the offsets we'll Fetch.)
	startOffset int64
	endOffset   int64

	// commitRecTs is the timestamp of the record which was committed (and not the commit time).
	commitRecTs time.Time

	lastSeenOffset int64

	lastBlockEndTs time.Time

	// currBlockEndTs is the timestamp
	currBlockEndTs time.Time
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
