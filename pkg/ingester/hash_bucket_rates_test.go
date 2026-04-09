// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBucketForHash(t *testing.T) {
	assert.Equal(t, 0, BucketForHash(0))
	assert.Equal(t, 0, BucketForHash(1<<24-1))
	assert.Equal(t, 1, BucketForHash(1<<24))
	assert.Equal(t, 255, BucketForHash(math.MaxUint32))
}

func TestHashBucketRates_RecordAndSnapshot(t *testing.T) {
	h := newHashBucketRates()

	// Record some samples in different buckets.
	h.RecordSamples(0, 100)                    // bucket 0
	h.RecordSamples(1<<24, 50)                 // bucket 1
	h.RecordSamples(math.MaxUint32, 200)       // bucket 255
	h.RecordSamples(math.MaxUint32-1000, 300)  // bucket 255

	// Give it a moment so the elapsed time is non-zero.
	time.Sleep(10 * time.Millisecond)

	snap := h.Snapshot()

	require.Equal(t, hashBucketCount, snap.NumBuckets)

	// Bucket 0 should have rate > 0 based on 100 samples.
	assert.Greater(t, snap.SamplesPerSecond[0], 0.0)

	// Bucket 1 should have rate > 0 based on 50 samples.
	assert.Greater(t, snap.SamplesPerSecond[1], 0.0)

	// Bucket 255 should have the highest rate based on 500 samples.
	assert.Greater(t, snap.SamplesPerSecond[255], snap.SamplesPerSecond[0])

	// Buckets 2-254 should be zero.
	for i := 2; i < 255; i++ {
		assert.Equal(t, 0.0, snap.SamplesPerSecond[i], "bucket %d should be zero", i)
	}
}

func TestHashBucketRates_SnapshotResetsCounters(t *testing.T) {
	h := newHashBucketRates()

	h.RecordSamples(0, 100)
	time.Sleep(10 * time.Millisecond)

	snap1 := h.Snapshot()
	assert.Greater(t, snap1.SamplesPerSecond[0], 0.0)

	// After snapshot, counters should be reset.
	time.Sleep(10 * time.Millisecond)
	snap2 := h.Snapshot()

	// All buckets should be zero since no new samples were recorded.
	for i := 0; i < hashBucketCount; i++ {
		assert.Equal(t, 0.0, snap2.SamplesPerSecond[i], "bucket %d should be zero after reset", i)
	}
}

func TestHashBucketRates_ConcurrentAccess(t *testing.T) {
	h := newHashBucketRates()
	done := make(chan struct{})

	go func() {
		for i := 0; i < 10000; i++ {
			h.RecordSamples(uint32(i*1000), 1)
		}
		close(done)
	}()

	// Concurrent snapshot while recording.
	for i := 0; i < 10; i++ {
		snap := h.Snapshot()
		assert.Equal(t, hashBucketCount, snap.NumBuckets)
	}

	<-done
}
