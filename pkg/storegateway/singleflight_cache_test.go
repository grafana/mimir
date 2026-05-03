// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/singleflight"
)

func TestSingleflightFetchOrCompute_HitBypassesSingleflight(t *testing.T) {
	var sf singleflight.Group
	var computeCalls atomic.Int32

	got, err := singleflightFetchOrCompute[string](
		context.Background(),
		&sf,
		"k",
		func(_ context.Context) (string, bool) { return "cached", true },
		func(_ context.Context) (string, error) {
			computeCalls.Add(1)
			return "computed", nil
		},
	)

	require.NoError(t, err)
	assert.Equal(t, "cached", got)
	assert.Equal(t, int32(0), computeCalls.Load(), "computeAndStore must not be called on a cache hit")
}

func TestSingleflightFetchOrCompute_ConcurrentMissCoalesces(t *testing.T) {
	var sf singleflight.Group
	var computeCalls atomic.Int32

	release := make(chan struct{})
	const n = 10

	type out struct {
		val string
		err error
	}
	results := make(chan out, n)

	for range n {
		go func() {
			val, err := singleflightFetchOrCompute[string](
				context.Background(),
				&sf,
				"k",
				func(_ context.Context) (string, bool) { return "", false },
				func(_ context.Context) (string, error) {
					computeCalls.Add(1)
					<-release
					return "computed", nil
				},
			)
			results <- out{val: val, err: err}
		}()
	}

	// Give all goroutines time to enter the singleflight before the leader returns.
	time.Sleep(50 * time.Millisecond)
	close(release)

	for range n {
		res := <-results
		require.NoError(t, res.err)
		assert.Equal(t, "computed", res.val)
	}
	assert.Equal(t, int32(1), computeCalls.Load(), "computeAndStore should run exactly once across N concurrent callers")
}

func TestSingleflightFetchOrCompute_DifferentKeysIndependent(t *testing.T) {
	var sf singleflight.Group
	var computeCalls atomic.Int32

	const n = 5
	type out struct {
		val string
		err error
	}
	results := make(chan out, n)

	for i := range n {
		key := string(rune('a' + i))
		go func() {
			val, err := singleflightFetchOrCompute[string](
				context.Background(),
				&sf,
				key,
				func(_ context.Context) (string, bool) { return "", false },
				func(_ context.Context) (string, error) {
					computeCalls.Add(1)
					return "v-" + key, nil
				},
			)
			results <- out{val: val, err: err}
		}()
	}

	for range n {
		<-results
	}
	assert.Equal(t, int32(n), computeCalls.Load(), "distinct keys should each invoke computeAndStore once")
}

func TestSingleflightFetchOrCompute_ComputeError(t *testing.T) {
	var sf singleflight.Group

	_, err := singleflightFetchOrCompute[string](
		context.Background(),
		&sf,
		"k",
		func(_ context.Context) (string, bool) { return "", false },
		func(_ context.Context) (string, error) { return "", errors.New("compute failed") },
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "compute failed")

	// Subsequent retry should not be poisoned: the singleflight group does not memoize errors.
	got, err := singleflightFetchOrCompute[string](
		context.Background(),
		&sf,
		"k",
		func(_ context.Context) (string, bool) { return "", false },
		func(_ context.Context) (string, error) { return "ok", nil },
	)
	require.NoError(t, err)
	assert.Equal(t, "ok", got)
}

func TestSingleflightFetchOrCompute_FollowerContextCancelDoesNotKillLeader(t *testing.T) {
	var sf singleflight.Group
	var computeCalls atomic.Int32

	leaderProceed := make(chan struct{})
	leaderDone := make(chan struct{})

	// Leader: call with a never-cancelled context, will run computeAndStore to completion.
	leaderResult := make(chan string, 1)
	go func() {
		val, err := singleflightFetchOrCompute[string](
			context.Background(),
			&sf,
			"k",
			func(_ context.Context) (string, bool) { return "", false },
			func(_ context.Context) (string, error) {
				computeCalls.Add(1)
				<-leaderProceed
				return "computed", nil
			},
		)
		require.NoError(t, err)
		leaderResult <- val
		close(leaderDone)
	}()

	// Allow the leader's compute to start.
	time.Sleep(20 * time.Millisecond)

	// Follower with a context that cancels while waiting.
	followerCtx, cancel := context.WithCancel(context.Background())
	followerDone := make(chan error, 1)
	go func() {
		_, err := singleflightFetchOrCompute[string](
			followerCtx,
			&sf,
			"k",
			func(_ context.Context) (string, bool) { return "", false },
			func(_ context.Context) (string, error) {
				t.Errorf("follower's computeAndStore must not run")
				return "", nil
			},
		)
		followerDone <- err
	}()

	// Cancel the follower while leader is still running.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-followerDone:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("follower did not return after context cancel")
	}

	// Leader should still complete and return its value.
	close(leaderProceed)
	select {
	case got := <-leaderResult:
		assert.Equal(t, "computed", got)
	case <-time.After(time.Second):
		t.Fatal("leader did not finish after follower cancelled")
	}
	<-leaderDone

	assert.Equal(t, int32(1), computeCalls.Load(), "compute ran exactly once for the leader")
}

func TestSingleflightFetchOrCompute_RecheckAfterSingleflight(t *testing.T) {
	var sf singleflight.Group
	var (
		mu          sync.Mutex
		cache       = map[string]string{}
		fetchCalls  int
		computeRuns int
	)

	get := func(k string) (string, bool) {
		mu.Lock()
		defer mu.Unlock()
		fetchCalls++
		v, ok := cache[k]
		return v, ok
	}
	set := func(k, v string) {
		mu.Lock()
		defer mu.Unlock()
		cache[k] = v
	}

	// First caller computes and stores.
	got, err := singleflightFetchOrCompute[string](
		context.Background(),
		&sf,
		"k",
		func(_ context.Context) (string, bool) { return get("k") },
		func(_ context.Context) (string, error) {
			mu.Lock()
			computeRuns++
			mu.Unlock()
			set("k", "computed")
			return "computed", nil
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "computed", got)

	// Second caller hits the cache via the outer fetch.
	got, err = singleflightFetchOrCompute[string](
		context.Background(),
		&sf,
		"k",
		func(_ context.Context) (string, bool) { return get("k") },
		func(_ context.Context) (string, error) {
			t.Errorf("computeAndStore must not run when cache is populated")
			return "", nil
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "computed", got)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, computeRuns, "compute should have run exactly once")
	// First caller: outer fetch (miss) + inner fetch (miss). Second caller: outer fetch (hit). 3 total.
	assert.Equal(t, 3, fetchCalls, "expected outer+inner fetch on slow path, then outer fetch on fast path")
}

func TestSingleflightKey(t *testing.T) {
	t.Run("deterministic", func(t *testing.T) {
		assert.Equal(t,
			singleflightKey("strategy", "matchers"),
			singleflightKey("strategy", "matchers"),
			"same inputs must produce the same key")
	})

	t.Run("no collision across diverse pairs", func(t *testing.T) {
		// Each row is a (prefix, suffix) pair. They are intentionally chosen so that
		// a naive concatenation with any single-byte delimiter (or no delimiter) would
		// produce duplicate keys for at least one pair. The length-prefix encoding in
		// singleflightKey must keep them all distinct.
		cases := []struct{ prefix, suffix string }{
			{"a", "b"},
			{"a|b", "c"}, // legacy '|' collision case: ("a|b","c") vs ("a","b|c")
			{"a", "b|c"},
			{"ab", "c"}, // shifted boundary: ("ab","c") vs ("a","bc") — same chars, different split
			{"a", "bc"},
			{"", "abc"},      // empty prefix
			{"abc", ""},      // empty suffix
			{"a\x00b", "c"},  // prefix contains the NULL separator
			{"a", "\x00bc"},  // suffix starts with NULL
			{"\x00", "\x00"}, // both sides are the separator byte
		}

		seen := map[string]struct{ prefix, suffix string }{}
		for _, c := range cases {
			k := singleflightKey(c.prefix, c.suffix)
			if prev, found := seen[k]; found {
				t.Errorf("collision: (%q,%q) and (%q,%q) both produced key %q",
					prev.prefix, prev.suffix, c.prefix, c.suffix, k)
				continue
			}
			seen[k] = struct{ prefix, suffix string }{c.prefix, c.suffix}
		}
	})

	t.Run("regression: '|' delimiter pre-fix collision", func(t *testing.T) {
		// The pre-fix code was `prefix + "|" + suffix`, which collided on these two
		// distinct inputs. The fix ensures they map to different singleflight keys.
		a := singleflightKey("a|b", "c")
		b := singleflightKey("a", "b|c")
		assert.NotEqual(t, a, b, "previously-colliding inputs must now produce different keys")
	})
}

// BenchmarkSingleflightFetchOrCompute measures the singleflight coalescing benefit
// against an unmediated baseline. Both run with GOMAXPROCS goroutines hammering on
// a single key with a fixed-cost compute. The reported "computes/op" metric is the
// number of computeAndStore invocations divided by b.N — for the singleflight case
// this should be well below 1.0 (followers ride on the leader's compute), while the
// baseline always reports 1.0.
func BenchmarkSingleflightFetchOrCompute(b *testing.B) {
	// computeDelay simulates a cache miss + S3/index-header round-trip. Larger
	// values create more in-flight concurrency, which is what makes followers
	// observable as a fraction of total ops.
	const computeDelay = 100 * time.Microsecond

	b.Run("with-singleflight", func(b *testing.B) {
		var sf singleflight.Group
		var computes atomic.Int64

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, _ = singleflightFetchOrCompute[int](
					context.Background(), &sf, "k",
					func(_ context.Context) (int, bool) { return 0, false },
					func(_ context.Context) (int, error) {
						computes.Add(1)
						time.Sleep(computeDelay)
						return 42, nil
					},
				)
			}
		})
		b.ReportMetric(float64(computes.Load())/float64(b.N), "computes/op")
	})

	b.Run("baseline", func(b *testing.B) {
		var computes atomic.Int64

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				computes.Add(1)
				time.Sleep(computeDelay)
			}
		})
		b.ReportMetric(float64(computes.Load())/float64(b.N), "computes/op")
	})
}
