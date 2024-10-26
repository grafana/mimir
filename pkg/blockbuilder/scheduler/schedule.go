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

type schedule struct {
	leaseTime time.Duration

	mu       sync.Mutex
	jobs     map[string]*job
	schedule []*job
}

func newSchedule(leaseTime time.Duration) *schedule {
	return &schedule{
		leaseTime: leaseTime,
		jobs:      make(map[string]*job),
	}
}

func (s *schedule) assign(worker string) (*job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.schedule) == 0 {
		return nil, nil
	}

	j := s.schedule[0]
	s.schedule = s.schedule[1:]
	j.assignee = worker
	j.leaseExpiry = time.Now().Add(s.leaseTime)
	return j, nil
}

func (s *schedule) addOrUpdate(id string, jobTime time.Time, spec jobSpec) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if j, ok := s.jobs[id]; ok {
		j.sortTime = jobTime
		j.spec = spec
	} else {
		j = &job{
			id:          id,
			sortTime:    jobTime,
			assignee:    "",
			leaseExpiry: time.Now().Add(s.leaseTime),
			spec:        spec,
		}
		s.jobs[id] = j
		s.schedule = append(s.schedule, j)
	}

	s.sort()
}

// sort maintains the sort order of the schedule. Caller must hold the lock.
func (s *schedule) sort() {
	slices.SortStableFunc(s.schedule, func(i, j *job) int {
		return i.sortTime.Compare(j.sortTime)
	})
}

func (s *schedule) renewLease(id string, worker string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if j, ok := s.jobs[id]; !ok {
		return errJobNotFound
	} else {
		if j.assignee != worker {
			return errJobNotAssigned
		}
		j.leaseExpiry = time.Now().Add(s.leaseTime)
	}
	return nil
}

func (s *schedule) clearExpiredLeases() {
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, j := range s.jobs {
		if j.assignee != "" && j.leaseExpiry.Before(now) {
			j.assignee = ""
			s.schedule = append(s.schedule, j)
		}
	}

	s.sort()
}

/*
Operations:
completeJob
assignJob
addJob
updateJobTime
renewLease

Need a job lease mechanism with an expiry. And a goroutine to do lease expirations.

*/

type job struct {
	id       string
	sortTime time.Time

	assignee    string
	leaseExpiry time.Time

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
