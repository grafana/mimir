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

	creationTime      time.Time
	leaseCreationTime time.Time
	lastRenewalTime   time.Time
	numLeases         int
}

func NewJob[K comparable, V any](k K, v V, creationTime time.Time) *Job[K, V] {
	return &Job[K, V]{
		k:            k,
		v:            v,
		creationTime: creationTime,
	}
}

func (j *Job[K, V]) IsLeased() bool {
	return !j.leaseCreationTime.IsZero()
}

func (j *Job[K, V]) MarkLeased() {
	j.leaseCreationTime = time.Now()
	j.lastRenewalTime = j.leaseCreationTime 
	j.numLeases += 1
}

// TODO: Some kind of feedback mechanism so the Spawner can know if a submitted plan job failed or completed
type JobTracker[K comparable, V any] struct {
	q          *list.List          // available jobs
	leased     *list.List          // leased jobs
	elementMap map[K]*list.Element // all tracked jobs will be in this map, element is in one and only one of q or leased
	maxLeases  int
	mtx        *sync.Mutex
}

func NewJobTracker[K comparable, V any](maxLeases int) *JobTracker[K, V] {
	return &JobTracker[K, V]{
		q:         list.New(),
		leased:    list.New(),
		maxLeases: maxLeases,
		mtx:       &sync.Mutex{},
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

// ExpireLeases iterates through all the leased jobs known by the JobTracker to find ones that have expired leases.
// If a job has an expired lease and has been leased under the maximum number of times, it is returned to the front of the queue
// Otherwise a job with an expired lease will be removed from the tracker.
func (jq *JobTracker[K, V]) ExpireLeases(leaseDuration time.Duration) bool {
	jq.mtx.Lock()
	defer jq.mtx.Unlock()

	wasEmpty := jq.isAvailableEmpty()
	revivedAny := false

	now := time.Now()
	var e, next *list.Element
	for e = jq.leased.Front(); e != nil; e = next {
		next = e.Next() // get the next element now since it can't be done after removal

		j := e.Value.(*Job[K, V])
		if now.Sub(j.lastRenewalTime) > leaseDuration {
			if jq.endLease(e, j) {
				revivedAny = true
			}
		}
	}

	return wasEmpty && revivedAny
}

// endLease removes a job from the leased list and returns true if the job was returned to the queue or false otherwise
func (jq *JobTracker[K, V]) endLease(e *list.Element, j *Job[K, V]) bool {
	jq.leased.Remove(e)

	// Can the job be returned to the queue?
	if jq.maxLeases == 0 || j.numLeases < jq.maxLeases {
		j.leaseCreationTime = time.Time{}
		j.lastRenewalTime = time.Time{}
		jq.elementMap[j.k] = jq.q.PushFront(j)
		return true
	}

	delete(jq.elementMap, j.k)
	return false
}

func (jq *JobTracker[K, V]) RenewLease(k K, leaseTime time.Time) bool {
	jq.mtx.Lock()
	defer jq.mtx.Unlock()

	e, ok := jq.elementMap[k]
	if !ok {
		return false
	}

	j := e.Value.(*Job[K, V])
	if j.IsLeased() && j.leaseCreationTime.Equal(leaseTime) {
		j.lastRenewalTime = time.Now()
		return true
	}
	return false
}

func (jq *JobTracker[K, V]) CancelLease(k K, leaseTime time.Time) (bool, bool) {
	jq.mtx.Lock()
	defer jq.mtx.Unlock()

	e, ok := jq.elementMap[k]
	if !ok {
		return false, false
	}

	j := e.Value.(*Job[K, V])
	if j.IsLeased() && j.leaseCreationTime == leaseTime {
		wasEmpty := jq.isAvailableEmpty()
		return true, wasEmpty && jq.endLease(e, j)
	}
	return false, false
}

func (jq *JobTracker[K, V]) Offer(jobs []*Job[K, V], shouldReplace func(prev, new V) bool) (int, bool) {
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
