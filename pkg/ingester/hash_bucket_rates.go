// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"sync/atomic"
	"time"
)

const hashBucketCount = 256

// hashBucketRates tracks ingestion rates bucketed by locality hash.
// The 32-bit hash space is divided into hashBucketCount equal-width buckets.
// Bucket i covers hashes [i * (2^32/hashBucketCount), (i+1) * (2^32/hashBucketCount)).
//
// Callers increment per-bucket sample counts on the hot path via RecordSamples.
// The rebalancer periodically calls Snapshot to get samples-per-second rates
// and reset the counters.
type hashBucketRates struct {
	counts    [hashBucketCount]atomic.Uint64
	lastReset atomic.Int64 // unix nanoseconds
}

func newHashBucketRates() *hashBucketRates {
	h := &hashBucketRates{}
	h.lastReset.Store(time.Now().UnixNano())
	return h
}

// BucketForHash returns the bucket index for a given 32-bit hash.
func BucketForHash(hash uint32) int {
	return int(hash >> (32 - 8)) // 256 buckets = 8 bits
}

// RecordSamples atomically adds count samples to the bucket for the given hash.
func (h *hashBucketRates) RecordSamples(hash uint32, count int) {
	bucket := BucketForHash(hash)
	h.counts[bucket].Add(uint64(count))
}

// HashBucketSnapshot holds the result of a snapshot: per-bucket samples/sec rates.
type HashBucketSnapshot struct {
	NumBuckets      int
	SamplesPerSecond [hashBucketCount]float64
}

// Snapshot returns the current per-bucket rates (samples/sec) and resets
// the counters for the next measurement period.
func (h *hashBucketRates) Snapshot() HashBucketSnapshot {
	now := time.Now().UnixNano()
	lastReset := h.lastReset.Swap(now)
	elapsed := float64(now-lastReset) / float64(time.Second)

	var snap HashBucketSnapshot
	snap.NumBuckets = hashBucketCount

	if elapsed <= 0 {
		return snap
	}

	for i := 0; i < hashBucketCount; i++ {
		count := h.counts[i].Swap(0)
		snap.SamplesPerSecond[i] = float64(count) / elapsed
	}

	return snap
}
