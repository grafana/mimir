package scheduler

import (
	"errors"
	"slices"
	"sync"
	"time"
)

var (
	errJobNotFound    = errors.New("job not found")
	errJobNotAssigned = errors.New("job not assigned to worker")
)

type jobQueue struct {
	leaseTime time.Duration

	mu          sync.Mutex
	jobs        map[string]*job
	outstanding []*job
}

func newJobQueue(leaseTime time.Duration) *jobQueue {
	return &jobQueue{
		leaseTime: leaseTime,
		jobs:      make(map[string]*job),
	}
}

func (s *jobQueue) assign(worker string) (*job, error) {
	if worker == "" {
		return nil, errors.New("worker cannot not be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.outstanding) == 0 {
		return nil, nil
	}

	j := s.outstanding[0]
	s.outstanding = s.outstanding[1:]
	j.assignee = worker
	j.leaseExpiry = time.Now().Add(s.leaseTime)
	return j, nil
}

func (s *jobQueue) addOrUpdate(id string, jobTime time.Time, spec jobSpec) {
	resort := false

	s.mu.Lock()
	defer s.mu.Unlock()

	if j, ok := s.jobs[id]; ok {
		// We can only update an unassigned job.
		if j.assignee == "" {
			resort = j.sortTime != jobTime
			j.sortTime = jobTime
			j.spec = spec
		}
	} else {
		j = &job{
			id:          id,
			sortTime:    jobTime,
			assignee:    "",
			leaseExpiry: time.Now().Add(s.leaseTime),
			failCount:   0,
			spec:        spec,
		}
		s.jobs[id] = j
		s.outstanding = append(s.outstanding, j)
		resort = true
	}

	if resort {
		s.sortOutstanding()
	}
}

// sortOutstanding maintains the sort order of the outstanding list. Caller must
// hold the lock.
func (s *jobQueue) sortOutstanding() {
	slices.SortFunc(s.outstanding, func(i, j *job) int {
		return i.sortTime.Compare(j.sortTime)
	})
}

func (s *jobQueue) renewLease(id string, worker string) error {
	if worker == "" {
		return errors.New("worker cannot not be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[id]
	if !ok {
		return errJobNotFound
	}
	if j.assignee != worker {
		return errJobNotAssigned
	}

	j.leaseExpiry = time.Now().Add(s.leaseTime)
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
			s.outstanding = append(s.outstanding, j)
		}
	}

	s.sortOutstanding()
}

/*
Operations:
completeJob
* assignJob
addJob
updateJobTime
* renewLease

Need a job lease mechanism with an expiry. And a goroutine to do lease expirations.

*/

type job struct {
	id       string
	sortTime time.Time

	assignee    string
	leaseExpiry time.Time
	failCount   int

	// job payload details. We can make this generic later for reuse.
	spec jobSpec
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
