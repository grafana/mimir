// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/pool/pool_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package pool

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestBytesPool(t *testing.T) {
	chunkPool, err := NewBucketedBytes(10, 100, 2, 1000)
	require.NoError(t, err)

	require.Equal(t, []int{10, 20, 40, 80}, chunkPool.sizes)

	for i := 0; i < 10; i++ {
		b, err := chunkPool.Get(40)
		require.NoError(t, err)

		require.Equal(t, uint64(40), chunkPool.usedTotal)

		if i%2 == 0 {
			for j := 0; j < 6; j++ {
				*b = append(*b, []byte{'1', '2', '3', '4', '5'}...)
			}
		}
		chunkPool.Put(b)
	}

	for i := 0; i < 10; i++ {
		b, err := chunkPool.Get(19)
		require.NoError(t, err)
		chunkPool.Put(b)
	}

	// Outside of any bucket.
	b, err := chunkPool.Get(1000)
	require.NoError(t, err)
	chunkPool.Put(b)

	// Check size limitation.
	b1, err := chunkPool.Get(500)
	require.NoError(t, err)

	b2, err := chunkPool.Get(600)
	require.Error(t, err)
	require.Equal(t, ErrPoolExhausted, err)

	chunkPool.Put(b1)
	chunkPool.Put(b2)

	require.Equal(t, uint64(0), chunkPool.usedTotal)
}

func TestRacePutGet(t *testing.T) {
	chunkPool, err := NewBucketedBytes(3, 100, 2, 5000)
	require.NoError(t, err)

	s := sync.WaitGroup{}

	const goroutines = 100

	// Start multiple goroutines: they always Get and Put two byte slices
	// to which they write their contents and check if the data is still
	// there after writing it, before putting it back.
	errs := make(chan error, goroutines)
	stop := make(chan struct{})

	f := func(txt string, grow bool) {
		defer s.Done()
		for {
			select {
			case <-stop:
				return
			default:
				c, err := chunkPool.Get(len(txt))
				if err != nil {
					errs <- errors.Wrapf(err, "goroutine %s", txt)
					return
				}

				*c = append(*c, txt...)
				if string(*c) != txt {
					errs <- errors.New("expected to get the data just written")
					return
				}
				if grow {
					*c = append(*c, txt...)
					*c = append(*c, txt...)
					if string(*c) != txt+txt+txt {
						errs <- errors.New("expected to get the data just written")
						return
					}
				}

				chunkPool.Put(c)
			}
		}
	}

	for i := 0; i < goroutines; i++ {
		s.Add(1)
		// make sure we start multiple goroutines with same len buf requirements, to hit same pools
		s := strings.Repeat(string(byte(i)), i%10)
		// some of the goroutines will append more elements to the provided slice
		grow := i%2 == 0
		go f(s, grow)
	}

	time.Sleep(1 * time.Second)
	close(stop)
	s.Wait()
	select {
	case err := <-errs:
		require.NoError(t, err)
	default:
	}
}

func TestBatchBytes(t *testing.T) {
	t.Run("byte slices do not overlap when fit on the same slab", func(t *testing.T) {
		bytesPool, err := NewBucketedBytes(10, 100, 2, 1000)
		require.NoError(t, err)
		batchBytes := BatchBytes{Delegate: bytesPool}
		bytesA, err := batchBytes.Get(5)
		assert.NoError(t, err)
		assert.Len(t, bytesA, 5)
		assert.Equal(t, 5, cap(bytesA))
		copy(bytesA, "12345")

		bytesB, err := batchBytes.Get(5)
		assert.NoError(t, err)
		assert.Len(t, bytesB, 5)
		assert.Equal(t, 5, cap(bytesB))
		copy(bytesB, "67890")

		assert.Equal(t, 10, int(bytesPool.usedTotal))
		assert.Equal(t, "12345", string(bytesA))
		assert.Equal(t, "67890", string(bytesB))

		batchBytes.Release()
		assert.Zero(t, int(bytesPool.usedTotal))
	})

	t.Run("a new slab is created when the new slice doesn't fit on an existing one", func(t *testing.T) {
		bytesPool, err := NewBucketedBytes(10, 100, 2, 1000)
		require.NoError(t, err)
		batchBytes := BatchBytes{Delegate: bytesPool}
		bytesA, err := batchBytes.Get(5)
		assert.NoError(t, err)
		assert.Len(t, bytesA, 5)
		assert.Equal(t, 5, cap(bytesA))
		copy(bytesA, "12345")

		bytesB, err := batchBytes.Get(6)
		assert.NoError(t, err)
		assert.Len(t, bytesB, 6)
		assert.Equal(t, 6, cap(bytesB))
		copy(bytesB, "67890-")

		assert.Equal(t, 20, int(bytesPool.usedTotal))
		assert.Equal(t, "12345", string(bytesA))
		assert.Equal(t, "67890-", string(bytesB))

		batchBytes.Release()
		assert.Zero(t, int(bytesPool.usedTotal))
	})
}
