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
	t := newTrackerStore(20*time.Minute, log.NewNopLogger(), limiterMock{}, noopEvents{})
	b.Cleanup(t.shutdownAllTenants)

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
				t.cleanup(cleanupTime, 10*time.Microsecond)
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
