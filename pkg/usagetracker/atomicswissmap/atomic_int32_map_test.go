// SPDX-License-Identifier: AGPL-3.0-only
package atomicswissmap

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestAtomicInt32Map_Functionality(t *testing.T) {
	m := New(10)
	_, found := m.GetOrCreate(1, 1)
	require.False(t, found)
	av1, ok := m.Get(1)
	require.True(t, ok)
	require.Equal(t, int32(1), av1.Load())
	require.Equal(t, 1, m.Count())

	// Check that key 2 doesn't exist.
	_, ok = m.Get(2)
	require.False(t, ok)

	// Create key 2.
	_, found = m.GetOrCreate(2, 20)
	require.False(t, found)
	av2, ok := m.Get(2)
	require.True(t, ok)
	require.Equal(t, int32(20), av2.Load())
	require.Equal(t, 2, m.Count())

	// Create key 2 again, should return the existing one.
	av2, found = m.GetOrCreate(2, 20)
	require.True(t, found)
	require.Equal(t, int32(20), av2.Load())

	// need to get it update after the GetOrCreate call.
	av1, ok = m.Get(1)
	require.True(t, ok)
	require.Equal(t, int32(1), av1.Load())
	require.True(t, av1.CompareAndSwap(1, 10))

	// make a clone and check it has the correct length.
	m2 := m.Clone()
	require.Equal(t, m.Count(), m2.Count())

	// It's still valid to update av2.
	require.True(t, av2.CompareAndSwap(20, 200))

	m.Delete(1)
	_, ok = m.Get(1)
	require.False(t, ok)

	// Add 1000 keys, make the map grow.
	for i := 0; i < 1000; i++ {
		_, found = m.GetOrCreate(uint64(i*100), int32(i))
		require.False(t, found)
	}

	// Key 2 should have the same value, but we need to get a new reference.
	av2, ok = m.Get(2)
	require.True(t, ok)
	require.Equal(t, int32(200), av2.Load())

	// Check that clone was not modified.
	require.Equal(t, 2, m2.Count())
	av1, ok = m2.Get(1)
	require.True(t, ok)
	require.Equal(t, int32(10), av1.Load())
	av2, ok = m2.Get(2)
	require.True(t, ok)
	require.Equal(t, int32(20), av2.Load())
}

func TestAtomicInt32Map_Concurrency(t *testing.T) {
	const stopAtKeys = 1_000

	enoughKeys := func(m *Map) bool {
		m.LockKeys()
		if m.Count() > stopAtKeys {
			m.UnlockKeys()
			return true
		}
		m.UnlockKeys()
		return false
	}

	keys := make([]uint64, stopAtKeys/2)
	for i := range keys {
		keys[i] = rand.Uint64()
	}

	// Add half of the keys to the map, let the goroutines fight to add the other half.
	m := New(1e6)
	m.Lock()
	for i := 0; i < len(keys)/2; i++ {
		_, found := m.GetOrCreate(keys[i], 1)
		require.False(t, found)
	}
	m.Unlock()

	updates := atomic.NewInt64(0)
	retries := atomic.NewInt64(0)
	collisions := atomic.NewInt64(0)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		wg.Done()
		for {
			if enoughKeys(m) {
				return
			}

			m.Lock()
			key := rand.Uint64()

			_, found := m.GetOrCreate(key, 1)
			if found {
				collisions.Inc()
			}
			m.Unlock()
			time.Sleep(100 * time.Microsecond)
		}
	}()
	for w := 0; w < max(2, runtime.GOMAXPROCS(0)); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := int32(1); i < 1e6; i++ {
				if enoughKeys(m) {
					return
				}

				key := keys[rand.Intn(len(keys))]
				m.LockKeys()
				gotv, ok := m.Get(key)
				if ok {
					updates.Inc()
					for val := gotv.Load(); val < i && !gotv.CompareAndSwap(val, i); val = gotv.Load() {
						retries.Inc()
					}
					m.UnlockKeys()
					continue
				}
				m.UnlockKeys()

				m.Lock()
				if gotv, found := m.GetOrCreate(key, i); found {
					for val := gotv.Load(); val < i && !gotv.CompareAndSwap(val, i); val = gotv.Load() {
					}
				}
				m.Unlock()
			}
		}()
	}

	wg.Wait()

	t.Logf("updates: %d, retries: %d, collisions: %d", updates.Load(), retries.Load(), collisions.Load())
}

func BenchmarkMap_Clone(b *testing.B) {
	for _, elements := range []int{1e6, 10e6} {
		b.Run(fmt.Sprintf("elements=%d", elements), func(b *testing.B) {
			m := New(uint32(elements))
			for i := 0; i < elements; i++ {
				_, _ = m.GetOrCreate(rand.Uint64(), 1)
			}

			b.ResetTimer()
			total := 0
			for i := 0; i < b.N; i++ {
				cloned := m.Clone()
				total += cloned.Count()
			}
			require.NotZero(b, total)
		})
	}
}
