package scheduler

import (
	"container/list"
	"sync"
)

type JobQueue struct {
	q   *list.List
	mtx *sync.Mutex
}

func NewJobQueue() *JobQueue {
	return &JobQueue{
		q:   list.New(),
		mtx: &sync.Mutex{},
	}
}

func (jq *JobQueue) Poll(canAccept func(j *Job) bool) *Job {
	jq.mtx.Lock()
	defer jq.mtx.Unlock()

	var e *list.Element
	for e = jq.q.Front(); e != nil; e = e.Next() {
		j := e.Value.(*Job)
		if canAccept(j) {
			break
		}
	}
	if e == nil {
		return nil
	}

	j := jq.q.Remove(e).(*Job)

	return j
}

func (jq *JobQueue) PushFront(j *Job) {
	jq.mtx.Lock()
	defer jq.mtx.Unlock()

	jq.q.PushFront(j)
}

func (jq *JobQueue) PushBack(j *Job) {
	jq.mtx.Lock()
	defer jq.mtx.Unlock()

	jq.q.PushBack(j)
}

func (jq *JobQueue) PushBackBatch(jobs []*Job) {
	jq.mtx.Lock()
	defer jq.mtx.Unlock()

	for _, j := range jobs {
		jq.q.PushBack(j)
	}
}