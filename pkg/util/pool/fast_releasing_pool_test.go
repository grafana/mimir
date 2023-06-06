package pool

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewFastReleasingSlabPool(t *testing.T) {
	delegatePool := &TrackedPool{Parent: &sync.Pool{}}

	const slabSize = 1024
	const slabsToGet = 100

	s := NewFastReleasingSlabPool[byte](delegatePool, slabSize)

	seed := time.Now().UnixNano()
	r := rand.New(rand.NewSource(seed))

	totalSize := 0
	var slabs []int
	for i := 0; i < slabsToGet; i++ {
		size := r.Intn(256) + 100
		bs, si := s.Get(size)
		slabs = append(slabs, si)

		require.Equal(t, size, len(bs))
		require.Equal(t, size, cap(bs))

		totalSize += size
	}

	// Return all slabs
	for _, si := range slabs {
		s.Release(si)
	}

	fmt.Println("total size:", totalSize, "slabs:", delegatePool.Gets.Load(), "balance:", delegatePool.Balance.Load())
	require.Greater(t, delegatePool.Gets.Load(), int64(0))

	// It is very likely that balance isn't zero at this point, although it could be.
	require.GreaterOrEqual(t, delegatePool.Balance.Load(), int64(0))

	s.ReleaseAll()
	require.Equal(t, int64(0), delegatePool.Balance.Load())
}
