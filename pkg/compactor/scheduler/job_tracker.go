// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"container/list"
	"math/rand"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
)

const InfiniteLeases = 0

// Job is the wrapper type within the JobTracker.
// V is the contained type
type Job[V any] struct {
	id    string
	value V

	creationTime      time.Time
	leaseCreationTime time.Time
	lastRenewalTime   time.Time
	numLeases         int
	epoch             int64 // used to avoid conflict since a job can be reassigned/replaced
	clock             clock.Clock
}

func NewJob[V any](id string, value V, creationTime time.Time, clock clock.Clock) *Job[V] {
	return &Job[V]{
		id:           id,
		value:        value,
		creationTime: creationTime,
		epoch:        rand.Int63(),
		clock:        clock,
	}
}

func (j *Job[V]) IsLeased() bool {
	return !j.leaseCreationTime.IsZero()
}

func (j *Job[V]) MarkLeased() {
	j.leaseCreationTime = j.clock.Now()
	j.lastRenewalTime = j.leaseCreationTime
	j.numLeases += 1
	j.epoch += int64(1)
}

func (j *Job[V]) ClearLease() {
	j.leaseCreationTime = time.Time{}
	j.lastRenewalTime = time.Time{}
}

// JobTracker tracks pending and active planning and compaction jobs for tenants.
// TODO: Some kind of feedback mechanism so the Spawner can know if a submitted plan job failed or completed
type JobTracker[V any] struct {
	clock     clock.Clock
	maxLeases int
	jobType   string
	metrics   *schedulerMetrics

	mtx     *sync.Mutex
	pending *list.List
	active  *list.List
	allJobs map[string]*list.Element // all tracked jobs will be in this map, element is in one and only one of pending or active
}

func NewJobTracker[V any](maxLeases int, jobType string, metrics *schedulerMetrics) *JobTracker[V] {
	return &JobTracker[V]{
		clock:     clock.New(),
		maxLeases: maxLeases,
		jobType:   jobType,
		metrics:   metrics,
		mtx:       &sync.Mutex{},
		pending:   list.New(),
		active:    list.New(),
		allJobs:   make(map[string]*list.Element),
	}
}

// Lease iterates the job queue for an acceptable job according to the provided canAccept function
// If one is found it is marked as active and still internally tracked (but is no longer leasable)
func (jt *JobTracker[V]) Lease(canAccept func(string, V) bool) (id string, value V, epoch int64, becameEmpty bool) {
	jt.mtx.Lock()
	defer jt.mtx.Unlock()

	var e *list.Element
	for e = jt.pending.Front(); e != nil; e = e.Next() {
		j := e.Value.(*Job[V])
		if canAccept(j.id, j.value) {
			break
		}
	}
	if e == nil {
		// Empty or no acceptable jobs
		// Never hint for removal if a job is not taken
		return "", *new(V), 0, false
	}

	j := jt.pending.Remove(e).(*Job[V])
	j.MarkLeased()
	jt.allJobs[j.id] = jt.active.PushBack(j)

	jt.metrics.pendingJobs.WithLabelValues(jt.jobType).Set(float64(jt.pending.Len()))
	jt.metrics.activeJobs.WithLabelValues(jt.jobType).Set(float64(jt.active.Len()))

	return j.id, j.value, j.epoch, jt.isPendingEmpty()
}

// Does not acquire any lock. It is required that any callers hold an appropriate lock.
func (jt *JobTracker[V]) isPendingEmpty() bool {
	return jt.pending.Front() == nil
}

func (jt *JobTracker[V]) Remove(id string, epoch int64) (removed bool, becameEmpty bool) {
	return jt.remove(id, epoch, true)
}

// Does not check for an epoch match
func (jt *JobTracker[KV]) RemoveForcefully(id string) (removed bool, becameEmpty bool) {
	return jt.remove(id, 0, false)
}

func (jt *JobTracker[V]) remove(id string, epoch int64, checkEpoch bool) (removed bool, becameEmpty bool) {
	jt.mtx.Lock()
	defer jt.mtx.Unlock()

	e, ok := jt.allJobs[id]
	if !ok {
		return false, false
	}

	j := e.Value.(*Job[V])
	if checkEpoch && epoch != j.epoch {
		return false, false
	}

	delete(jt.allJobs, id)
	if j.IsLeased() {
		jt.active.Remove(e)
		jt.metrics.activeJobs.WithLabelValues(jt.jobType).Set(float64(jt.active.Len()))
		return true, false
	}

	jt.pending.Remove(e)
	jt.metrics.pendingJobs.WithLabelValues(jt.jobType).Set(float64(jt.pending.Len()))
	return true, jt.isPendingEmpty()
}

// ExpireLeases iterates through all the active jobs known by the JobTracker to find ones that have expired leases.
// If a job has an expired lease and has been active under the maximum number of times, it is returned to the front of the queue
// Otherwise a job with an expired lease will be removed from the tracker.
func (jt *JobTracker[V]) ExpireLeases(leaseDuration time.Duration) bool {
	jt.mtx.Lock()
	defer jt.mtx.Unlock()

	wasEmpty := jt.isPendingEmpty()
	revivedAny := false

	now := jt.clock.Now()
	var e, next *list.Element
	for e = jt.active.Front(); e != nil; e = next {
		next = e.Next() // get the next element now since it can't be done after removal

		j := e.Value.(*Job[V])
		if now.Sub(j.lastRenewalTime) > leaseDuration {
			if jt.endLease(e, j) {
				revivedAny = true
			}
		} else {
			// No more expirable jobs
			break
		}
	}

	return wasEmpty && revivedAny
}

// endLease removes a job from the active list and returns true if the job was returned to the queue or false otherwise
func (jt *JobTracker[V]) endLease(e *list.Element, j *Job[V]) bool {
	jt.active.Remove(e)
	jt.metrics.activeJobs.WithLabelValues(jt.jobType).Set(float64(jt.active.Len()))

	// Can the job be returned to the queue?
	if jt.maxLeases == InfiniteLeases || j.numLeases < jt.maxLeases {
		j.ClearLease()
		jt.allJobs[j.id] = jt.pending.PushFront(j)
		jt.metrics.pendingJobs.WithLabelValues(jt.jobType).Set(float64(jt.pending.Len()))
		return true
	}

	delete(jt.allJobs, j.id)
	return false
}

func (jt *JobTracker[V]) RenewLease(id string, epoch int64) bool {
	jt.mtx.Lock()
	defer jt.mtx.Unlock()

	e, ok := jt.allJobs[id]
	if !ok {
		return false
	}

	j := e.Value.(*Job[V])
	if j.IsLeased() && j.epoch == epoch {
		j.lastRenewalTime = jt.clock.Now()
		jt.active.MoveToBack(e)
		return true
	}
	return false
}

func (jt *JobTracker[V]) CancelLease(id string, epoch int64) (canceled bool, becamePending bool) {
	jt.mtx.Lock()
	defer jt.mtx.Unlock()

	e, ok := jt.allJobs[id]
	if !ok {
		return false, false
	}

	j := e.Value.(*Job[V])
	if j.IsLeased() && j.epoch == epoch {
		wasEmpty := jt.isPendingEmpty()
		return true, wasEmpty && jt.endLease(e, j)
	}
	return false, false
}

func (jt *JobTracker[V]) Offer(jobs []*Job[V], shouldReplace func(prev, new V) bool) (accepted int, becamePending bool) {
	jt.mtx.Lock()
	defer jt.mtx.Unlock()

	wasEmpty := jt.isPendingEmpty()

	// TODO: Count replacements?

	for _, j := range jobs {
		if e, ok := jt.allJobs[j.id]; ok {
			prevJ := e.Value.(*Job[V])
			// TODO: Set an indicator to do a swap if the lease expires instead of rejecting? That sounds complicated.
			// This case is tricky because we're past a point of no return if a worker is already on it.
			if prevJ.IsLeased() || !shouldReplace(prevJ.value, j.value) {
				// Don't add this job.
				continue
			}

			// Technically we could replace the element's value directly and keep the replaced job's
			// place in the queue. As a choice it seems better to penalize replacement by putting them
			// at the back of the line.
			jt.pending.Remove(e)
		}

		jt.allJobs[j.id] = jt.pending.PushBack(j)
		accepted += 1
	}

	jt.metrics.pendingJobs.WithLabelValues(jt.jobType).Set(float64(jt.pending.Len()))
	return accepted, wasEmpty && accepted > 0
}
