// SPDX-License-Identifier: AGPL-3.0-only

package chunk

import "github.com/prometheus/prometheus/util/zeropool"

// SetStartTimestamp records st on batch at the given index, allocating the
// sidecar from pool on first use. Zero start timestamps are ignored so
// batches with only unknown or absent start timestamps do not allocate a
// sidecar.
func SetStartTimestamp(batch *Batch, index int, st int64, pool *zeropool.Pool[*[BatchSize]int64]) {
	if st == 0 {
		return
	}
	if batch.StartTimestamps == nil {
		batch.StartTimestamps = acquireStartTimestampSidecar(pool)
	}
	batch.StartTimestamps[index] = st
}

// ReleaseStartTimestampSidecar detaches the sidecar from batch and returns it
// to pool. Callers must ensure no other consumer still reads through the
// batch's sidecar. When pool is nil, the sidecar is simply dropped and left
// to the garbage collector.
func ReleaseStartTimestampSidecar(batch *Batch, pool *zeropool.Pool[*[BatchSize]int64]) {
	if batch.StartTimestamps == nil {
		return
	}
	st := batch.StartTimestamps
	batch.StartTimestamps = nil
	if pool == nil {
		return
	}
	pool.Put(st)
}

// acquireStartTimestampSidecar returns a zeroed sidecar array. When pool is
// non-nil, the sidecar is drawn from the pool and cleared before being
// returned; otherwise it is heap-allocated.
func acquireStartTimestampSidecar(pool *zeropool.Pool[*[BatchSize]int64]) *[BatchSize]int64 {
	if pool == nil {
		return new([BatchSize]int64)
	}
	st := pool.Get()
	if st == nil {
		return new([BatchSize]int64)
	}
	clear(st[:])
	return st
}
