package scheduler

import (
	"errors"
	"slices"
	"sync"
	"time"
)

const (
	leaseTime = 5 * time.Minute
)

var (
	errJobNotFound    = errors.New("job not found")
	errJobNotAssigned = errors.New("job not assigned to worker")
)

type schedule struct {
	mu       sync.Mutex
	jobs     map[string]*job
	schedule []*job
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
	return j, nil
}

func (s *schedule) addOrUpdate(id string, jobTime time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if j, ok := s.jobs[id]; ok {
		j.time = jobTime
	} else {
		s.jobs[id] = &job{
			id:          id,
			time:        jobTime,
			assignee:    "",
			leaseExpiry: time.Now().Add(leaseTime),
		}
		s.schedule = append(s.schedule, s.jobs[id])
	}

	// Keep the schedule sorted.
	slices.SortStableFunc(s.schedule, func(i, j *job) int {
		return i.time.Compare(j.time)
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
		j.leaseExpiry = time.Now().Add(leaseTime)
	}
	return nil
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
	id   string
	time time.Time

	assignee    string
	leaseExpiry time.Time
}
