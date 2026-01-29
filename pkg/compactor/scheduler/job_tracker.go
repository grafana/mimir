// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"container/list"
	"fmt"
	"math/rand"
	"slices"
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
	persister JobPersister[V]

	clock     clock.Clock
	maxLeases int
	jobType   string
	metrics   *schedulerMetrics

	mtx     *sync.Mutex
	pending *list.List
	active  *list.List
	allJobs map[string]*list.Element // all tracked jobs will be in this map, element is in one and only one of pending or active
}

func NewJobTracker[V any](jobPersister JobPersister[V], maxLeases int, jobType string, metrics *schedulerMetrics) *JobTracker[V] {
	jt := &JobTracker[V]{
		persister: jobPersister,
		clock:     clock.New(),
		maxLeases: maxLeases,
		jobType:   jobType,
		metrics:   metrics,
		mtx:       &sync.Mutex{},
		pending:   list.New(),
		active:    list.New(),
		allJobs:   make(map[string]*list.Element),
	}
	return jt
}

func (jt *JobTracker[V]) recoverFrom(jobs []*Job[V]) {
	jt.mtx.Lock()
	defer jt.mtx.Unlock()

	leased := make([]*Job[V], 0, len(jobs))
	for _, job := range jobs {
		job.clock = jt.clock
		// TODO: When completed jobs are temporarily remembered there needs to be a third case here
		if job.IsLeased() {
			// Updates from heartbeats are not persisted, but the initial lease time still are so this still requires sorting
			leased = append(leased, job)
		} else {
			// TODO: When job ordering is respected these can be buffered and sorted
			jt.allJobs[job.id] = jt.pending.PushBack(job)
		}
	}
	// Sort to preserve lease renewal time invariant
	slices.SortFunc(leased, func(a *Job[V], b *Job[V]) int {
		return a.lastRenewalTime.Compare(b.lastRenewalTime)
	})
	for _, job := range leased {
		jt.allJobs[job.id] = jt.active.PushBack(job)
	}

	jt.metrics.pendingJobs.WithLabelValues(jt.jobType).Set(float64(jt.pending.Len()))
	jt.metrics.activeJobs.WithLabelValues(jt.jobType).Set(float64(jt.active.Len()))
}

func (jt *JobTracker[V]) PrepareForShutdown() {
	jt.mtx.Lock()
	defer jt.mtx.Unlock()

	jt.allJobs = make(map[string]*list.Element)
	jt.pending = list.New()
	jt.active = list.New()
}

// Lease iterates the job queue for an acceptable job according to the provided canAccept function
// If one is found it is marked as active and still internally tracked (but is no longer leasable)
func (jt *JobTracker[V]) Lease(canAccept func(string, V) bool) (id string, value V, epoch int64, becameEmpty bool, err error) {
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
		return "", *new(V), 0, false, nil
	}

	j := e.Value.(*Job[V])

	// Copy the value, don't want to leave a modification if the write fails
	jv := *j
	jj := &jv

	jj.MarkLeased()
	if err := jt.persister.WriteJob(jj); err != nil {
		return "", *new(V), 0, false, err
	}

	jt.pending.Remove(e)
	jt.allJobs[jj.id] = jt.active.PushBack(jj)

	jt.metrics.pendingJobs.WithLabelValues(jt.jobType).Set(float64(jt.pending.Len()))
	jt.metrics.activeJobs.WithLabelValues(jt.jobType).Set(float64(jt.active.Len()))

	return jj.id, jj.value, jj.epoch, jt.isPendingEmpty(), nil
}

// Does not acquire any lock. It is required that any callers hold an appropriate lock.
func (jt *JobTracker[V]) isPendingEmpty() bool {
	return jt.pending.Front() == nil
}

func (jt *JobTracker[V]) Remove(id string, epoch int64) (removed bool, becameEmpty bool, err error) {
	return jt.remove(id, epoch, true)
}

// Does not check for an epoch match
func (jt *JobTracker[KV]) RemoveForcefully(id string) (removed bool, becameEmpty bool, err error) {
	return jt.remove(id, 0, false)
}

func (jt *JobTracker[V]) remove(id string, epoch int64, checkEpoch bool) (removed bool, becameEmpty bool, err error) {
	jt.mtx.Lock()
	defer jt.mtx.Unlock()

	e, ok := jt.allJobs[id]
	if !ok {
		return false, false, nil
	}

	j := e.Value.(*Job[V])
	if checkEpoch && epoch != j.epoch {
		return false, false, nil
	}

	if err := jt.persister.DeleteJob(j); err != nil {
		return false, false, fmt.Errorf("failed deleting job: %w", err)
	}

	delete(jt.allJobs, id)
	if j.IsLeased() {
		jt.active.Remove(e)
		jt.metrics.activeJobs.WithLabelValues(jt.jobType).Set(float64(jt.active.Len()))
		return true, false, nil
	}

	jt.pending.Remove(e)
	jt.metrics.pendingJobs.WithLabelValues(jt.jobType).Set(float64(jt.pending.Len()))
	return true, jt.isPendingEmpty(), nil
}

// ExpireLeases iterates through all the active jobs known by the JobTracker to find ones that have expired leases.
// If a job has an expired lease and has been active under the maximum number of times, it is returned to the front of the queue
// Otherwise a job with an expired lease will be removed from the tracker.
func (jt *JobTracker[V]) ExpireLeases(leaseDuration time.Duration) (bool, error) {
	jt.mtx.Lock()
	defer jt.mtx.Unlock()

	wasEmpty := jt.isPendingEmpty()

	now := jt.clock.Now()
	var e, next *list.Element

	var deleteJobs []*Job[V]
	var reviveJobs []*Job[V]
	for e = jt.active.Front(); e != nil; e = next {
		next = e.Next() // get the next element now since it can't be done after removal

		j := e.Value.(*Job[V])
		if now.Sub(j.lastRenewalTime) > leaseDuration {
			// Can the job be returned to the queue?
			if jt.maxLeases == InfiniteLeases || j.numLeases < jt.maxLeases {
				// Copy before modifying
				jj := *j
				jj.ClearLease()
				reviveJobs = append(reviveJobs, &jj)
			} else {
				deleteJobs = append(deleteJobs, j)
			}
		} else {
			// No more expirable jobs
			break
		}
	}

	if len(deleteJobs) == 0 && len(reviveJobs) == 0 {
		return false, nil
	}

	err := jt.persister.WriteAndDeleteJobs(reviveJobs, deleteJobs)
	if err != nil {
		return false, fmt.Errorf("failed persisting expiration: %w", err)
	}

	for _, j := range reviveJobs {
		jt.active.Remove(jt.allJobs[j.id])
		jt.allJobs[j.id] = jt.pending.PushFront(j)
	}
	for _, j := range deleteJobs {
		jt.active.Remove(jt.allJobs[j.id])
		delete(jt.allJobs, j.id)
	}

	jt.metrics.activeJobs.WithLabelValues(jt.jobType).Set(float64(jt.active.Len()))
	jt.metrics.pendingJobs.WithLabelValues(jt.jobType).Set(float64(jt.pending.Len()))

	return wasEmpty && len(reviveJobs) > 0, nil
}

// RenewLease renews a lease to prevent it from being expired. This is intentionally not persisted. A buffer time is used across restarts instead.
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

func (jt *JobTracker[V]) CancelLease(id string, epoch int64) (canceled bool, becamePending bool, err error) {
	jt.mtx.Lock()
	defer jt.mtx.Unlock()

	e, ok := jt.allJobs[id]
	if !ok {
		return false, false, nil
	}

	j := e.Value.(*Job[V])
	if !j.IsLeased() || j.epoch != epoch {
		return false, false, nil
	}

	wasEmpty := jt.isPendingEmpty()
	revive := jt.maxLeases == InfiniteLeases || j.numLeases < jt.maxLeases
	if revive {
		// Copy the value, don't want to leave a modification if the write fails
		jv := *j
		jj := &jv
		jj.ClearLease()

		err = jt.persister.WriteJob(jj)
		if err != nil {
			return false, false, err
		}
		jt.active.Remove(jt.allJobs[j.id])
		jt.allJobs[jj.id] = jt.pending.PushFront(jj)
		jt.metrics.pendingJobs.WithLabelValues(jt.jobType).Set(float64(jt.pending.Len()))
	} else {
		err := jt.persister.DeleteJob(j)
		if err != nil {
			return false, false, err
		}
		jt.active.Remove(jt.allJobs[j.id])
		delete(jt.allJobs, j.id)
	}
	jt.metrics.activeJobs.WithLabelValues(jt.jobType).Set(float64(jt.active.Len()))

	return true, wasEmpty && revive, nil
}

func (jt *JobTracker[V]) Offer(jobs []*Job[V], shouldReplace func(prev, new V) bool) (accepted int, becamePending bool, err error) {
	jt.mtx.Lock()
	defer jt.mtx.Unlock()

	wasEmpty := jt.isPendingEmpty()

	// TODO: Count replacements?
	for i, j := range jobs {
		if e, ok := jt.allJobs[j.id]; ok {
			prevJ := e.Value.(*Job[V])
			// TODO: Set an indicator to do a swap if the lease expires instead of rejecting? That sounds complicated.
			// This case is tricky because we're past a point of no return if a worker is already on it.
			if prevJ.IsLeased() || !shouldReplace(prevJ.value, j.value) {
				// Don't add this job.
				continue
			}
		}

		// Reuse the jobs slice to keep track of which jobs were accepted
		// This is safe to do since it would be invalid for a caller to try to read the values after regardless (would require lock)
		jobs[accepted] = jobs[i]
		accepted += 1
	}

	if accepted == 0 {
		return 0, false, nil
	}

	acceptedJobs := jobs[:accepted]
	err = jt.persister.WriteAndDeleteJobs(acceptedJobs, nil)
	if err != nil {
		return 0, false, fmt.Errorf("failed writing offered jobs: %w", err)
	}

	for _, j := range acceptedJobs {
		if e, ok := jt.allJobs[j.id]; ok {
			// Punish replacement, don't replace in-place
			jt.pending.Remove(e)
		}
		jt.allJobs[j.id] = jt.pending.PushBack(j)
	}

	jt.metrics.pendingJobs.WithLabelValues(jt.jobType).Set(float64(jt.pending.Len()))
	return accepted, wasEmpty && accepted > 0, nil
}
