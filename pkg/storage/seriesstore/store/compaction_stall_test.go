// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

// Reproduces what a compaction does to the queries and appends of an engine that has out-of-order data: its
// out-of-order pass holds each shard's write lock while it goes over every series of the tenant. Run with
// GO_INGESTER_BENCH=1.

type stallSample struct {
	at       time.Time
	duration time.Duration
}

func percentile(durations []time.Duration, q float64) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	sorted := slices.Clone(durations)
	slices.Sort(sorted)
	return sorted[min(len(sorted)-1, int(float64(len(sorted))*q))]
}

func TestCompactionStall(t *testing.T) {
	requireBench(t)
	const (
		seriesCount = 400_000
		metrics     = 40
		hour        = int64(3_600_000)
	)
	for _, oooPercent := range []int{0, 1, 10} {
		t.Run(fmt.Sprintf("ooo=%d%%", oooPercent), func(t *testing.T) {
			engine, err := OpenEngine(t.TempDir(), "tenant", EngineOptions{Shards: 16, OutOfOrderTimeWindowMs: 2 * hour, SecondaryHashFunction: secondaryHash})
			require.NoError(t, err)
			t.Cleanup(func() { _ = engine.Close() })

			lsets := make([]promlabels.Labels, seriesCount)
			refs := make([]storage.SeriesRef, seriesCount)
			for n := range lsets {
				lsets[n] = promlabels.FromStrings("__name__", fmt.Sprintf("metric_%d", n%metrics), "pod", fmt.Sprintf("pod-%d", n), "job", fmt.Sprintf("job-%d", n%20))
			}
			appendAt := func(ts int64, picks func(n int) bool) {
				app := engine.Appender(context.Background())
				for n := range lsets {
					if !picks(n) {
						continue
					}
					ref, err := app.Append(refs[n], lsets[n], ts, 1)
					require.NoError(t, err)
					refs[n] = ref
				}
				require.NoError(t, app.Commit())
			}
			appendAt(0, func(int) bool { return true })
			appendAt(4*hour, func(int) bool { return true })
			// Out of order for the series that get it: before what they have, inside the window.
			appendAt(3*hour, func(n int) bool { return n%100 < oooPercent })

			var (
				stop    atomic.Bool
				mu      sync.Mutex
				queries []stallSample
				appends []stallSample
				wg      sync.WaitGroup
			)
			wg.Add(2)
			go func() {
				defer wg.Done()
				for i := 0; !stop.Load(); i++ {
					start := time.Now()
					q, err := engine.ChunkQuerier(0, 4*hour)
					if err != nil {
						t.Error(err)
						return
					}
					set := q.Select(context.Background(), true, nil, promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", fmt.Sprintf("metric_%d", i%metrics)))
					for set.Next() {
						it := set.At().Iterator(nil)
						for it.Next() {
						}
					}
					_ = q.Close()
					mu.Lock()
					queries = append(queries, stallSample{start, time.Since(start)})
					mu.Unlock()
					time.Sleep(time.Millisecond)
				}
			}()
			go func() {
				defer wg.Done()
				ts := 4*hour + 1000
				for i := 0; !stop.Load(); i++ {
					start := time.Now()
					app := engine.Appender(context.Background())
					for k := range 200 {
						n := (i*200 + k) % seriesCount
						ref, err := app.Append(refs[n], lsets[n], ts+int64(i), 1)
						if err != nil {
							t.Error(err)
							return
						}
						_ = ref
					}
					if err := app.Commit(); err != nil {
						t.Error(err)
						return
					}
					mu.Lock()
					appends = append(appends, stallSample{start, time.Since(start)})
					mu.Unlock()
					time.Sleep(time.Millisecond)
				}
			}()

			time.Sleep(2 * time.Second)
			compactStart := time.Now()
			require.NoError(t, engine.Compact(context.Background()))
			compactEnd := time.Now()
			time.Sleep(time.Second)
			stop.Store(true)
			wg.Wait()

			report := func(name string, samples []stallSample) {
				var idle, during []time.Duration
				for _, s := range samples {
					if s.at.Before(compactEnd) && s.at.Add(s.duration).After(compactStart) {
						during = append(during, s.duration)
					} else {
						idle = append(idle, s.duration)
					}
				}
				fmt.Printf("ooo=%d%% %-7s idle: n=%d p50=%v p99=%v max=%v | during compaction: n=%d p50=%v p99=%v max=%v\n",
					oooPercent, name, len(idle), percentile(idle, 0.5), percentile(idle, 0.99), percentile(idle, 1),
					len(during), percentile(during, 0.5), percentile(during, 0.99), percentile(during, 1))
			}
			fmt.Printf("ooo=%d%% compaction took %v\n", oooPercent, compactEnd.Sub(compactStart))
			report("queries", queries)
			report("appends", appends)
		})
	}
}
