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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/usagetracker/hll"
)

const testIdleTimeout = 20 * time.Minute

func BenchmarkTrackerStoreTrackSeries(b *testing.B) {
	benchmarkTrackerStoreTrackSeriesMatrix(b, false)
}

// BenchmarkTrackerStoreTrackSeriesWithReceivedSeries is BenchmarkTrackerStoreTrackSeries with the received series
// estimation enabled for every tenant.
func BenchmarkTrackerStoreTrackSeriesWithReceivedSeries(b *testing.B) {
	benchmarkTrackerStoreTrackSeriesMatrix(b, true)
}

func benchmarkTrackerStoreTrackSeriesMatrix(b *testing.B, receivedSeries bool) {
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
									benckmarkTrackerStoreTrackSeries(b, seriesRefs, seriesPerRequest, tenantsCount, cleanupsEnabled, receivedSeries)
								})
							}
						})
					}
				})
			}
		})
	}
}

func benckmarkTrackerStoreTrackSeries(b *testing.B, seriesRefs []uint64, seriesPerRequest, tenantsCount int, cleanupsEnabled, receivedSeries bool) {
	nowSeconds := atomic.NewInt64(0)
	now := func() time.Time { return time.Unix(nowSeconds.Load(), 0) }
	t := newTrackerStore(testIdleTimeout, 85, log.NewNopLogger(), limiterMock{}, noopEvents{}, false, 0, newTestShardFactory())
	if receivedSeries {
		t.receivedSeries = newReceivedSeriesEstimation(defaultReceivedSeriesEstimationHLLPrecision, 1, testIdleTimeout)
	}

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
			t := newTrackerStore(testIdleTimeout, 85, log.NewNopLogger(), lim, noopEvents{}, false, 0, newTestShardFactory())
			for _, d := range snapshots {
				require.NoError(b, t.loadSnapshot(d, now))
			}
		}
	})

	b.Run("mode=parallel", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			t := newTrackerStore(testIdleTimeout, 85, log.NewNopLogger(), lim, noopEvents{}, false, 0, newTestShardFactory())
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

	t := newTrackerStore(testIdleTimeout, 85, log.NewNopLogger(), lim, noopEvents{}, false, 0, newTestShardFactory())
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

// BenchmarkTrackerStoreTrackSeriesDuringSnapshot tracks series of a tenant while snapshots of that tenant are
// being taken without pause, and reports the slowest call as max_ms: that is the stall a snapshot adds to the
// tail latency of series tracking.
func BenchmarkTrackerStoreTrackSeriesDuringSnapshot(b *testing.B) {
	for _, totalSeries := range []int{16e6, 64e6} {
		b.Run(fmt.Sprintf("totalSeries=%dM", totalSeries/1e6), func(b *testing.B) {
			const tenant = "tenant"
			const seriesPerRequest = 1_000
			now := time.Now()
			t := newTrackerStore(testIdleTimeout, 85, log.NewNopLogger(), limiterMock{}, noopEvents{}, false, 0, newTestShardFactory())

			// Fill the store, and keep some of the series to track them again: tracking new series would grow
			// the shards, and a rehash would be the slowest call rather than the snapshot.
			r := rand.New(rand.NewSource(0))
			known := make([]uint64, 0, 1e6)
			refs := make([]uint64, 10_000)
			for i := 0; i < totalSeries; i += len(refs) {
				for j := range refs {
					refs[j] = r.Uint64()
				}
				known = append(known, refs[:min(len(refs), cap(known)-len(known))]...)
				_, err := t.trackSeries(context.Background(), tenant, refs, now)
				require.NoError(b, err)
			}

			stop := make(chan struct{})
			done := make(chan struct{})
			go func() {
				defer close(done)
				var buf []byte
				for shard := 0; ; shard = (shard + 1) % shards {
					select {
					case <-stop:
						return
					default:
					}
					buf = t.snapshot(uint8(shard), now, buf[:0])
				}
			}()

			var slowest time.Duration
			refs = refs[:seriesPerRequest]
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// trackSeries reorders its input, so hand it a copy.
				start := r.Intn(len(known) - seriesPerRequest)
				copy(refs, known[start:start+seriesPerRequest])
				t0 := time.Now()
				_, err := t.trackSeries(context.Background(), tenant, refs, now)
				slowest = max(slowest, time.Since(t0))
				require.NoError(b, err)
			}
			b.StopTimer()
			close(stop)
			<-done
			b.ReportMetric(float64(slowest.Microseconds())/1000, "max_ms")
		})
	}
}

// BenchmarkTrackerStoreCollect collects the metrics of a store whose tenants have a sketch in every minute of the
// idle timeout. With estimates=warm, each estimate merges two sketches. With estimates=cold, each Collect is in
// another minute than the previous one, so each estimate merges all the minutes of the window, like the first
// Collect of each minute.
func BenchmarkTrackerStoreCollect(b *testing.B) {
	start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	minutes := int(testIdleTimeout.Minutes())
	// One sketch of 1000 random series per minute, for all the tenants: the timelines keep copies.
	sketches := make([]hll.Sketch, minutes)
	values := make([]uint64, 1000)
	for m := range sketches {
		for i := range values {
			values[i] = rand.Uint64()
		}
		sketches[m] = hll.NewSketch(defaultReceivedSeriesEstimationHLLPrecision)
		sketches[m].InsertAll(values, 1)
	}

	for _, tenantsCount := range []int{100, 1_000, 10_000} {
		for _, estimates := range []string{"disabled", "warm", "cold"} {
			b.Run(fmt.Sprintf("tenants=%d/estimates=%s", tenantsCount, estimates), func(b *testing.B) {
				store := newTrackerStore(testIdleTimeout, 85, log.NewNopLogger(), limiterMock{}, noopEvents{}, false, 0, newTestShardFactory())
				last := start.Add(time.Duration(minutes-1) * time.Minute)
				scrapeTime := last
				if estimates != "disabled" {
					store.receivedSeries = newReceivedSeriesEstimation(defaultReceivedSeriesEstimationHLLPrecision, 1, testIdleTimeout)
					store.receivedSeries.now = func() time.Time { return scrapeTime }
				}
				for tenant := range tenantsCount {
					_, err := store.trackSeries(context.Background(), strconv.Itoa(tenant), nil, start)
					require.NoError(b, err)
				}
				for _, tenant := range store.tenants {
					if timeline := tenant.receivedSeries.Load(); timeline != nil {
						for m, sketch := range sketches {
							minute := unixMinute(start) + int64(m)
							timeline.Merge(minute, minute, sketch)
						}
					}
				}

				// Two metrics per tenant: the active series and the estimate.
				metrics := make(chan prometheus.Metric, 2*tenantsCount+10)
				for b.Loop() {
					if estimates == "cold" {
						if scrapeTime.Equal(last) {
							scrapeTime = last.Add(time.Minute)
						} else {
							scrapeTime = last
						}
					}
					store.Collect(metrics)
					for len(metrics) > 0 {
						<-metrics
					}
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*tenantsCount), "ns/tenant")
			})
		}
	}
}
