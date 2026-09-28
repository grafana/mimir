// SPDX-License-Identifier: AGPL-3.0-only

package types

import (
	"github.com/prometheus/prometheus/util/zeropool"

	"github.com/grafana/mimir/pkg/util/limiter"
)

// ObjectPool pools individual objects for reuse across queries.
//
// It is the only sanctioned way to pool objects in this engine; do not use sync.Pool or zeropool
// directly. Put takes the query's memory consumption tracker so that, once a query has panicked and
// been recovered, its objects are abandoned to the garbage collector instead of being handed to the
// next query. An object the aborted query may still reference, or may return twice, must never
// reach another query. See MemoryConsumptionTracker.Poison.
//
// Unlike LimitingBucketedPool, ObjectPool does not track memory consumption.
type ObjectPool[T any] struct {
	inner zeropool.Pool[T]
}

// NewObjectPool returns an ObjectPool that uses newFn to create objects when the pool is empty.
func NewObjectPool[T any](newFn func() T) *ObjectPool[T] {
	return &ObjectPool[T]{inner: zeropool.New(newFn)}
}

// Get returns an object from the pool, creating one if the pool is empty.
func (p *ObjectPool[T]) Get() T {
	return p.inner.Get()
}

// Put returns v to the pool, unless the query that owns tracker has been poisoned, in which case v is
// dropped and left for the garbage collector.
func (p *ObjectPool[T]) Put(v T, tracker *limiter.MemoryConsumptionTracker) {
	if tracker.IsPoisoned() {
		return
	}

	p.inner.Put(v)
}
