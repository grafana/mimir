package scheduler

import (
	"slices"
	"sync"
	"time"
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

func (s *schedule) addOrUpdate(id string, time time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if j, ok := s.jobs[id]; ok {
		j.time = time
		return
	}

	s.jobs[id] = &job{
		id:       id,
		assignee: "",
		time:     time,
	}
	s.schedule = append(s.schedule, s.jobs[id])
	slices.SortFunc(s.schedule, func(i, j *job) int {
		return i.time.Compare(j.time)
	})
}

/*
Operations:
completeJob
assignJob
addJob
updateJobTime

Need a job lease mechanism with an expiry.

*/

type job struct {
	id   string
	time time.Time

	assignee    string
	leaseExpiry time.Time
}
