// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

const testIdleTimeout = 20 * time.Minute

func BenchmarkTrackerStoreTrackSeries(b *testing.B) {
	for _, totalSeries := range []int{10e6, 100e6} {
		b.Run(fmt.Sprintf("totalSeries=%dM", totalSeries/1e6), func(b *testing.B) {
			seriesRefs := make([]uint64, totalSeries)
			for i := range seriesRefs {
				seriesRefs[i] = rand.Uint64()
			}
			for _, seriesPerRequest := range []int{1_000, 10_000} {
				b.Run(fmt.Sprintf("seriesPerRequest=%dK", seriesPerRequest/1e3), func(b *testing.B) {
					for _, tenantsCount := range []int{1, 10, 100, 1000} {
						if totalSeries/tenantsCount <= seriesPerRequest {
							b.Skip()
							// Don't benchmark cases where we just send same series again and again.
							// This skips 10M series for 1K tenants with 10K series per request.
							continue
						}
						b.Run("tenants="+strconv.Itoa(tenantsCount), func(b *testing.B) {
							for _, cleanupsEnabled := range []bool{true, false} {
								b.Run("cleanups="+strconv.FormatBool(cleanupsEnabled), func(b *testing.B) {
									benckmarkTrackerStoreTrackSeries(b, seriesRefs, seriesPerRequest, tenantsCount, cleanupsEnabled)
								})
							}
						})
					}
				})
			}
		})
	}
}

func benckmarkTrackerStoreTrackSeries(b *testing.B, seriesRefs []uint64, seriesPerRequest, tenantsCount int, cleanupsEnabled bool) {
	nowSeconds := atomic.NewInt64(0)
	now := func() time.Time { return time.Unix(nowSeconds.Load(), 0) }
	t := newTrackerStore(testIdleTimeout, 85, log.NewNopLogger(), limiterMock{}, noopEvents{})

	seriesPerTenant := len(seriesRefs) / tenantsCount
	// Warmup each tenant.
	tenantRefs := make([]uint64, seriesPerTenant)
	for tenantID := 0; tenantID < tenantsCount; tenantID++ {
		// Need to copy series refs to tenantRefs because trackSeries modifies the input slice.
		copy(tenantRefs, seriesRefs[tenantID*seriesPerTenant:(tenantID+1)*seriesPerTenant])
		_, err := t.trackSeries(context.Background(), strconv.Itoa(tenantID), tenantRefs, now())
		require.NoError(b, err)
	}
	b.ResetTimer()

	cleanup := make(chan time.Time, 1)
	wg := &sync.WaitGroup{}
	if cleanupsEnabled {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for cleanupTime := range cleanup {
				t.cleanup(cleanupTime)
			}
		}()
	}
	b.RunParallel(func(pb *testing.PB) {
		refs := make([]uint64, seriesPerRequest)
		for pb.Next() {
			tenantID := rand.Intn(tenantsCount)
			tenant := strconv.Itoa(tenantID)
			// Each tenant sends their own series.
			// They start at tenant*seriesPerTenant plus a rand index up to seriesPerTenant-seriesPerRequest (to avoid out of bounds).
			refsStart := tenantID*seriesPerTenant + rand.Intn(seriesPerTenant-seriesPerRequest)
			// Copy refs because trackSeries modifies them.
			copy(refs, seriesRefs[refsStart:refsStart+seriesPerRequest])
			_, err := t.trackSeries(context.Background(), tenant, refs, now())
			require.NoError(b, err)
			if cleanupsEnabled {
				if seconds := nowSeconds.Inc(); seconds%60 == 0 {
					select {
					// Don't block to avoid slowing down the benchmark.
					// We want to measure how long trackSeries() takes with cleanups, not how long cleanups take.
					case cleanup <- time.Unix(seconds, 0):
					default:
					}
				}
			}
		}
	})
	close(cleanup)
	wg.Wait()
}

func BenchmarkTrackerStoreLoadSnapshot(b *testing.B) {
	now := time.Now()
	snapshots, lim := generateSnapshot(b, 1, 10e6, now)
	b.Logf("Snapshots generation took %s", time.Since(now))

	b.Run("mode=sequential", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			t := newTrackerStore(testIdleTimeout, 85, log.NewNopLogger(), lim, noopEvents{})
			for _, d := range snapshots {
				require.NoError(b, t.loadSnapshot(d, now))
			}
		}
	})

	b.Run("mode=parallel", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			t := newTrackerStore(testIdleTimeout, 85, log.NewNopLogger(), lim, noopEvents{})
			require.NoError(b, t.loadSnapshots(snapshots, now))
		}
	})
}

func generateSnapshot(b *testing.B, tenantsCount int, totalSeries int, now time.Time) ([][]byte, limiter) {
	seriesPerTenant := totalSeries / tenantsCount
	tenants := make([]string, tenantsCount)
	lim := limiterMock{}
	for i := 0; i < tenantsCount; i++ {
		tenant := strconv.Itoa(i)
		tenants[i] = tenant
		lim[tenant] = uint64(seriesPerTenant)
	}
	seriesPerTenantPerMinute := seriesPerTenant / int(testIdleTimeout.Minutes())

	t := newTrackerStore(testIdleTimeout, 85, log.NewNopLogger(), lim, noopEvents{})
	deterministicRand := rand.New(rand.NewSource(0))
	for timestamp := now.Add(-testIdleTimeout); timestamp.Before(now); timestamp = timestamp.Add(time.Minute) {
		for i := 0; i < tenantsCount; i++ {
			for _, tenant := range tenants {
				series := make([]uint64, seriesPerTenantPerMinute)
				for j := range series {
					series[j] = deterministicRand.Uint64()
				}
				_, err := t.trackSeries(context.Background(), tenant, series, timestamp)
				require.NoError(b, err)
			}
		}
	}
	snapshots := make([][]byte, shards)
	for shard := 0; shard < shards; shard++ {
		snapshots[shard] = t.snapshot(uint8(shard), now, nil)
	}
	return snapshots, lim
}
