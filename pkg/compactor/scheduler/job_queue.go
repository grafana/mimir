package scheduler

import (
	"container/list"
	"sync"
)

type JobQueue[K comparable, V any] struct {
	q          *list.List
	elementMap map[K]*list.Element
	keyFunc    func(V) K
	mtx        *sync.Mutex
}

func NewJobQueue[K comparable, V any](keyFunc func(V) K) *JobQueue[K, V] {
	return &JobQueue[K, V]{
		q:       list.New(),
		mtx:     &sync.Mutex{},
		keyFunc: keyFunc,
	}
}

// Poll iterates the job queue for an acceptable job according to the provided function
func (jq *JobQueue[K, V]) Poll(canAccept func(v V) bool) (V, bool) {
	jq.mtx.Lock()
	defer jq.mtx.Unlock()

	var e *list.Element
	for e = jq.q.Front(); e != nil; e = e.Next() {
		v := e.Value.(V)
		if canAccept(v) {
			break
		}
	}
	if e == nil {
		// Empty or no acceptable jobs
		var v V
		return v, false
	}

	v := jq.q.Remove(e).(V)
	delete(jq.elementMap, jq.keyFunc(v))

	return v, true
}

// Remove removes the specified job from the job queue if it existed
func (jq *JobQueue[K, V]) Remove(k K) (V, bool) {
	jq.mtx.Lock()
	defer jq.mtx.Unlock()

	if e, ok := jq.elementMap[k]; ok {
		return jq.q.Remove(e).(V), true
	}

	var v V
	return v, false
}

func (jq *JobQueue[K, V]) PushFront(v V) bool {
	jq.mtx.Lock()
	defer jq.mtx.Unlock()

	k := jq.keyFunc(v)
	if _, ok := jq.elementMap[k]; ok {
		return false
	}

	e := jq.q.PushFront(v)
	jq.elementMap[jq.keyFunc(v)] = e

	return true
}

func (jq *JobQueue[K, V]) PushBack(v V) bool {
	jq.mtx.Lock()
	defer jq.mtx.Unlock()

	k := jq.keyFunc(v)
	if _, ok := jq.elementMap[k]; ok {
		return false
	}

	e := jq.q.PushBack(v)
	jq.elementMap[jq.keyFunc(v)] = e

	return true
}

// PushBackBatch pushes several jobs onto the queue while only grabbing the internal lock once
func (jq *JobQueue[K, V]) PushBackBatch(vs []V) int {
	jq.mtx.Lock()
	defer jq.mtx.Unlock()

	for i, v := range vs {
		k := jq.keyFunc(v)
		if _, ok := jq.elementMap[k]; ok {
			// A job already existed, refuse to push more
			return i
		}

		e := jq.q.PushBack(v)
		jq.elementMap[k] = e
	}

	return len(vs)
}
