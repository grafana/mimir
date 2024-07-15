// SPDX-License-Identifier: AGPL-3.0-only

package pool

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFastReleasingSlabPool(t *testing.T) {
	t.Run("byte slices do not overlap when fit on the same slab", func(t *testing.T) {
		delegatePool := &TrackedPool{Parent: &sync.Pool{}}
		slabPool := NewFastReleasingSlabPool[byte](delegatePool, 10)

		sliceA, slabIDA := slabPool.Get(5)
		require.Len(t, sliceA, 5)
		require.Equal(t, 5, cap(sliceA))
		require.Equal(t, 1, slabIDA) // Make sure first slabID is 1.
		copy(sliceA, "12345")

		sliceB, slabIDB := slabPool.Get(5)
		require.Len(t, sliceB, 5)
		require.Equal(t, 5, cap(sliceB))
		require.Greater(t, slabIDB, 0)
		copy(sliceB, "67890")

		require.Equal(t, "12345", string(sliceA))
		require.Equal(t, "67890", string(sliceB))

		require.Equal(t, 2, len(slabPool.slabs)) // first slab (id=0) is always nil
		require.Nil(t, slabPool.slabs[0])
		require.Equal(t, 2, slabPool.slabs[1].references)
		require.Equal(t, 10, slabPool.slabs[1].nextFreeIndex)

		slabPool.Release(slabIDA)

		require.Equal(t, 2, len(slabPool.slabs))
		require.Nil(t, slabPool.slabs[0])
		require.Equal(t, 1, slabPool.slabs[1].references)
		require.Equal(t, 10, slabPool.slabs[1].nextFreeIndex)

		slabPool.Release(slabIDB)

		require.Equal(t, 2, len(slabPool.slabs))
		require.Nil(t, slabPool.slabs[0])
		require.Nil(t, slabPool.slabs[1])

		// Allocating another slice needs a new slab.
		sliceC, slabIDC := slabPool.Get(5)
		require.Len(t, sliceC, 5)
		require.Equal(t, 5, cap(sliceC))
		require.Greater(t, slabIDC, 0)

		require.Equal(t, 3, len(slabPool.slabs))
		require.Nil(t, slabPool.slabs[0])
		require.Nil(t, slabPool.slabs[1])
		require.Equal(t, 1, slabPool.slabs[2].references)
		require.Equal(t, 5, slabPool.slabs[2].nextFreeIndex)

		slabPool.Release(slabIDC)

		require.Equal(t, 3, len(slabPool.slabs))
		require.Nil(t, slabPool.slabs[0])
		require.Nil(t, slabPool.slabs[1])
		require.Nil(t, slabPool.slabs[2])

		require.Zero(t, delegatePool.Balance.Load())
		require.Equal(t, 2, int(delegatePool.Gets.Load()))
	})

	t.Run("a new slab is created when the new slice doesn't fit on an existing one", func(t *testing.T) {
		delegatePool := &TrackedPool{Parent: &sync.Pool{}}
		slabPool := NewFastReleasingSlabPool[byte](delegatePool, 10)

		sliceA, slabIDA := slabPool.Get(5)
		assert.Len(t, sliceA, 5)
		assert.Equal(t, 5, cap(sliceA))
		copy(sliceA, "12345")

		require.Equal(t, 2, len(slabPool.slabs))
		require.Equal(t, 10, len(slabPool.slabs[1].slab))
		require.Equal(t, 5, slabPool.slabs[1].nextFreeIndex)
		require.Equal(t, 1, slabPool.slabs[1].references)

		// Size doesn't fit the existing slab, so a new one will be created.
		sliceB, slabIDB := slabPool.Get(6)
		assert.Len(t, sliceB, 6)
		assert.Equal(t, 6, cap(sliceB))
		copy(sliceB, "67890-")

		require.Equal(t, 3, len(slabPool.slabs))
		require.Nil(t, slabPool.slabs[0])
		require.Equal(t, 10, len(slabPool.slabs[1].slab))
		require.Equal(t, 5, slabPool.slabs[1].nextFreeIndex)
		require.Equal(t, 1, slabPool.slabs[1].references)
		require.Equal(t, 10, len(slabPool.slabs[2].slab))
		require.Equal(t, 6, slabPool.slabs[2].nextFreeIndex)
		require.Equal(t, 1, slabPool.slabs[2].references)

		// Size fits in the last slab.
		sliceC, slabIDC := slabPool.Get(3)
		assert.Len(t, sliceC, 3)
		assert.Equal(t, 3, cap(sliceC))
		copy(sliceC, "abc")

		require.Equal(t, 3, len(slabPool.slabs))
		require.Nil(t, slabPool.slabs[0])
		require.Equal(t, 10, len(slabPool.slabs[1].slab))
		require.Equal(t, 5, slabPool.slabs[1].nextFreeIndex)
		require.Equal(t, 1, slabPool.slabs[1].references)
		require.Equal(t, 10, len(slabPool.slabs[2].slab))
		require.Equal(t, 9, slabPool.slabs[2].nextFreeIndex)
		require.Equal(t, 2, slabPool.slabs[2].references)

		// Size fits in the previous last slab.
		sliceD, slabIDD := slabPool.Get(3)
		assert.Len(t, sliceD, 3)
		assert.Equal(t, 3, cap(sliceD))
		copy(sliceD, "def")

		require.Equal(t, 3, len(slabPool.slabs))
		require.Nil(t, slabPool.slabs[0])
		require.Equal(t, 10, len(slabPool.slabs[1].slab))
		require.Equal(t, 8, slabPool.slabs[1].nextFreeIndex)
		require.Equal(t, 2, slabPool.slabs[1].references)
		require.Equal(t, 10, len(slabPool.slabs[2].slab))
		require.Equal(t, 9, slabPool.slabs[2].nextFreeIndex)
		require.Equal(t, 2, slabPool.slabs[2].references)

		assert.Equal(t, "12345", string(sliceA))
		assert.Equal(t, "67890-", string(sliceB))
		assert.Equal(t, "abc", string(sliceC))
		assert.Equal(t, "def", string(sliceD))

		slabPool.Release(slabIDA)
		slabPool.Release(slabIDB)
		slabPool.Release(slabIDC)
		slabPool.Release(slabIDD)

		require.Equal(t, 3, len(slabPool.slabs))
		require.Nil(t, slabPool.slabs[0])
		require.Nil(t, slabPool.slabs[1])
		require.Nil(t, slabPool.slabs[2])

		require.Zero(t, delegatePool.Balance.Load())
		require.Greater(t, int(delegatePool.Gets.Load()), 0)
	})

	t.Run("releasing slabID 0", func(*testing.T) {
		delegatePool := &TrackedPool{Parent: &sync.Pool{}}
		slabPool := NewFastReleasingSlabPool[byte](delegatePool, 10)

		slabPool.Release(0)
	})

	t.Run("releasing slabID 1", func(t *testing.T) {
		delegatePool := &TrackedPool{Parent: &sync.Pool{}}
		slabPool := NewFastReleasingSlabPool[byte](delegatePool, 10)

		defer func() {
			p := recover()
			require.Equal(t, "invalid slab id: 1", p)
		}()

		// Ignored
		slabPool.Release(1)
		require.Fail(t, "Release should panic")
	})

	t.Run("releasing slab too many times", func(t *testing.T) {
		delegatePool := &TrackedPool{Parent: &sync.Pool{}}
		slabPool := NewFastReleasingSlabPool[byte](delegatePool, 10)

		_, slabID := slabPool.Get(10)
		require.Greater(t, slabID, 0)
		slabPool.Release(slabID)

		defer func() {
			p := recover()
			require.Equal(t, "nil slab", p)
		}()
		slabPool.Release(slabID)
		require.Fail(t, "Release of same slab too many times should panic")
	})
}

func TestFastReleasingSlabPool_Fuzzy(t *testing.T) {
	const (
		numRuns           = 100
		numRequestsPerRun = 100
		slabSize          = 100
	)

	type assertion struct {
		expected []byte
		actual   []byte
		slabID   int
	}

	delegatePool := &TrackedPool{Parent: &sync.Pool{}}

	// Randomise the seed but log it in case we need to reproduce the test on failure.
	seed := time.Now().UnixNano()
	rnd := rand.New(rand.NewSource(seed))
	t.Log("random generator seed:", seed)

	for r := 0; r < numRuns; r++ {
		slabPool := NewFastReleasingSlabPool[byte](delegatePool, slabSize)
		var assertions []assertion

		for n := 0; n < numRequestsPerRun; n++ {
			// Get a random size between 1 and (slabSize + 10)
			size := 1 + rnd.Intn(slabSize+9)

			slice, slabID := slabPool.Get(size)

			// Write some data to the slice.
			expected := make([]byte, size)
			_, err := rnd.Read(expected)
			require.NoError(t, err)

			copy(slice, expected)

			// Keep track of it, so we can check no data was overwritten later on.
			assertions = append(assertions, assertion{
				expected: expected,
				actual:   slice,
				slabID:   slabID,
			})
		}

		// Ensure no data was overwritten.
		for _, assertion := range assertions {
			require.Equal(t, assertion.expected, assertion.actual)

			slabPool.Release(assertion.slabID)
		}

		require.Zero(t, delegatePool.Balance.Load())
		require.Greater(t, int(delegatePool.Gets.Load()), 0)
	}
}
