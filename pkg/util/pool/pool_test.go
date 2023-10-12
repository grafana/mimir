// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/pool/pool_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package pool

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestSlabPool(t *testing.T) {
	t.Run("byte slices do not overlap when fit on the same slab", func(t *testing.T) {
		delegatePool := &TrackedPool{Parent: &sync.Pool{}}
		slabPool := NewSlabPool[byte](delegatePool, 10)

		sliceA := slabPool.Get(5)
		require.Len(t, sliceA, 5)
		require.Equal(t, 5, cap(sliceA))
		copy(sliceA, "12345")

		sliceB := slabPool.Get(5)
		require.Len(t, sliceB, 5)
		require.Equal(t, 5, cap(sliceB))
		copy(sliceB, "67890")

		require.Equal(t, "12345", string(sliceA))
		require.Equal(t, "67890", string(sliceB))

		require.Equal(t, 1, len(slabPool.slabs))
		require.Equal(t, 10, len(*(slabPool.slabs[0])))
		require.Equal(t, 10, cap(*(slabPool.slabs[0])))

		slabPool.Release()
		require.Zero(t, delegatePool.Balance.Load())
		require.Greater(t, int(delegatePool.Gets.Load()), 0)
	})

	t.Run("a new slab is created when the new slice doesn't fit on an existing one", func(t *testing.T) {
		delegatePool := &TrackedPool{Parent: &sync.Pool{}}
		slabPool := NewSlabPool[byte](delegatePool, 10)

		sliceA := slabPool.Get(5)
		assert.Len(t, sliceA, 5)
		assert.Equal(t, 5, cap(sliceA))
		copy(sliceA, "12345")

		require.Equal(t, 1, len(slabPool.slabs))
		require.Equal(t, 5, len(*(slabPool.slabs[0])))
		require.Equal(t, 10, cap(*(slabPool.slabs[0])))

		// Size doesn't fit the existing slab, so a new one will be created.
		sliceB := slabPool.Get(6)
		assert.Len(t, sliceB, 6)
		assert.Equal(t, 6, cap(sliceB))
		copy(sliceB, "67890-")

		require.Equal(t, 2, len(slabPool.slabs))
		require.Equal(t, 5, len(*(slabPool.slabs[0])))
		require.Equal(t, 10, cap(*(slabPool.slabs[0])))
		require.Equal(t, 6, len(*(slabPool.slabs[1])))
		require.Equal(t, 10, cap(*(slabPool.slabs[1])))

		// Size fits in the last slab.
		sliceC := slabPool.Get(3)
		assert.Len(t, sliceC, 3)
		assert.Equal(t, 3, cap(sliceC))
		copy(sliceC, "abc")

		require.Equal(t, 2, len(slabPool.slabs))
		require.Equal(t, 5, len(*(slabPool.slabs[0])))
		require.Equal(t, 10, cap(*(slabPool.slabs[0])))
		require.Equal(t, 9, len(*(slabPool.slabs[1])))
		require.Equal(t, 10, cap(*(slabPool.slabs[1])))

		// Size fits in the previous last slab.
		sliceD := slabPool.Get(3)
		assert.Len(t, sliceD, 3)
		assert.Equal(t, 3, cap(sliceD))
		copy(sliceD, "def")

		require.Equal(t, 2, len(slabPool.slabs))
		require.Equal(t, 8, len(*(slabPool.slabs[0])))
		require.Equal(t, 10, cap(*(slabPool.slabs[0])))
		require.Equal(t, 9, len(*(slabPool.slabs[1])))
		require.Equal(t, 10, cap(*(slabPool.slabs[1])))

		assert.Equal(t, "12345", string(sliceA))
		assert.Equal(t, "67890-", string(sliceB))
		assert.Equal(t, "abc", string(sliceC))
		assert.Equal(t, "def", string(sliceD))

		slabPool.Release()
		require.Zero(t, delegatePool.Balance.Load())
		require.Greater(t, int(delegatePool.Gets.Load()), 0)
	})
}

func TestSlabPool_Fuzzy(t *testing.T) {
	const (
		numRuns           = 100
		numRequestsPerRun = 100
		slabSize          = 100
	)

	type assertion struct {
		expected []byte
		actual   []byte
	}

	delegatePool := &TrackedPool{Parent: &sync.Pool{}}

	// Randomise the seed but log it in case we need to reproduce the test on failure.
	seed := time.Now().UnixNano()
	rnd := rand.New(rand.NewSource(seed))
	t.Log("random generator seed:", seed)

	for r := 0; r < numRuns; r++ {
		slabPool := NewSlabPool[byte](delegatePool, slabSize)
		var assertions []assertion

		for n := 0; n < numRequestsPerRun; n++ {
			// Get a random size between 1 and (slabSize + 10)
			size := 1 + rnd.Intn(slabSize+9)

			slice := slabPool.Get(size)

			// Write some data to the slice.
			expected := make([]byte, size)
			_, err := rnd.Read(expected)
			require.NoError(t, err)

			copy(slice, expected)

			// Keep track of it, so we can check no data was overwritten later on.
			assertions = append(assertions, assertion{
				expected: expected,
				actual:   slice,
			})
		}

		// Ensure no data was overwritten.
		for _, assertion := range assertions {
			require.Equal(t, assertion.expected, assertion.actual)
		}

		slabPool.Release()
		require.Zero(t, delegatePool.Balance.Load())
		require.Greater(t, int(delegatePool.Gets.Load()), 0)
	}
}
