package scheduler

import (
	"container/list"
	"sync"
	"time"
)

// Job is the wrapper type within the JobTracker.
// K identifies the job. If another job is offered with K the conflict must be decided.
// V is the actual type
type Job[K comparable, V any] struct {
	k K
	v V

	creationTime time.Time
	leaseTime    time.Time
	numLeases    int
}

func NewJob[K comparable, V any](k K, v V, creationTime time.Time) *Job[K, V] {
	return &Job[K, V]{
		k:            k,
		v:            v,
		creationTime: creationTime,
	}
}

func (j *Job[K, V]) IsLeased() bool {
	return !j.leaseTime.IsZero()
}

func (j *Job[K, V]) MarkLeased() {
	j.leaseTime = time.Now()
	j.numLeases += 1
}

type JobTracker[K comparable, V any] struct {
	q          *list.List          // available jobs
	leased     *list.List          // leased jobs
	elementMap map[K]*list.Element // all tracked jobs will be in this map, element is in one and only one of q or leased
	mtx        *sync.Mutex
}

func NewJobTracker[K comparable, V any]() *JobTracker[K, V] {
	return &JobTracker[K, V]{
		q:      list.New(),
		leased: list.New(),
		mtx:    &sync.Mutex{},
	}
}

// Lease iterates the job queue for an acceptable job according to the provided canAccept function
// If one is found it is marked as leased and still internally tracked (but is no longer leasable)
func (jq *JobTracker[K, V]) Lease(canAccept func(v V) bool) (V, bool) {
	jq.mtx.Lock()
	defer jq.mtx.Unlock()

	var e *list.Element
	for e = jq.q.Front(); e != nil; e = e.Next() {
		j := e.Value.(Job[K, V])
		if canAccept(j.v) {
			break
		}
	}
	if e == nil {
		// Empty or no acceptable jobs
		// Never hint for removal if a job is not taken
		var v V
		return v, false
	}

	j := jq.q.Remove(e).(*Job[K, V])
	j.MarkLeased()
	jq.elementMap[j.k] = jq.leased.PushBack(j)

	return j.v, jq.isAvailableEmpty()
}

// Does not acquire any lock. It is required that any callers hold an appropriate lock.
func (jq *JobTracker[K, V]) isAvailableEmpty() bool {
	return jq.q.Front() == nil
}

func (jq *JobTracker[K, V]) RemoveIf(k K, predicate func(v V) bool) (bool, bool) {
	jq.mtx.Lock()
	defer jq.mtx.Unlock()

	e, ok := jq.elementMap[k]
	if ok && predicate(e.Value.(Job[K, V]).v) {
		delete(jq.elementMap, k)
		j := e.Value.(*Job[K, V])
		if j.IsLeased() {
			jq.leased.Remove(e)
			return true, false
		} else {
			jq.q.Remove(e)
			return true, jq.isAvailableEmpty()
		}
	}

	return false, false
}

// ExpireLeases iterates through all the jobs known by the JobTracker to find ones that have expired leases.
// If a job has an expired lease and has been leased under maxLeases times, it is returned to the front of the queue
// Otherwise a job with an expired lease will be removed from the tracker.
func (jq *JobTracker[K, V]) ExpireLeases(leaseDuration time.Duration, maxLeases int) bool {
	jq.mtx.Lock()
	defer jq.mtx.Unlock()

	wasEmpty := jq.isAvailableEmpty()
	revivedAny := false

	now := time.Now()
	var e, next *list.Element
	for e = jq.leased.Front(); e != nil; e = next {
		next = e.Next() // get the next element now since it can't be done after removal

		j := e.Value.(*Job[K, V])
		if now.Sub(j.leaseTime) > leaseDuration {
			jq.leased.Remove(e)
			// This lease is expired, can it be returned to the queue?
			if j.numLeases < maxLeases {
				jq.elementMap[j.k] = jq.q.PushFront(j)
				revivedAny = true
			} else {
				delete(jq.elementMap, j.k)
			}
		} else {
			// The leased jobs are pushed in order. If this lease is not expired the others won't be
			break
		}
	}

	return wasEmpty && revivedAny
}

func (jq *JobTracker[K, V]) Offer(jobs []*Job[K, V], shouldReplace func(prev V, new V) bool) (int, bool) {
	jq.mtx.Lock()
	defer jq.mtx.Unlock()

	wasEmpty := jq.isAvailableEmpty()

	// TODO: Count replacements?
	accepted := 0

	for _, j := range jobs {
		if e, ok := jq.elementMap[j.k]; ok {
			prevJ := e.Value.(*Job[K, V])
			// TODO: Set an indicator to do a swap if the lease expires instead of rejecting? That sounds complicated.
			// This case is tricky because we're past a point of no return if a worker is already on it.
			if prevJ.IsLeased() || !shouldReplace(prevJ.v, j.v) {
				// Don't add this job.
				continue
			}

			// Technically we could replace the element's value directly and keep the replaced job's
			// place in the queue. As a choice it seems better to penalize replacement by putting them
			// at the back of the line.
			jq.q.Remove(e)
		}

		jq.elementMap[j.k] = jq.q.PushBack(j)
		accepted += 1
	}

	return accepted, wasEmpty && accepted > 0
}
