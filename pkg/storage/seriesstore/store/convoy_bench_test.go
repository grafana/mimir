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

// BenchmarkEngineConvoy times small selects and pushes while slow reads run: a read that holds a shard's lock makes the
// push to it wait, and the selects after the push wait for it, which is what made a few slow reads stall every query of
// an ingester. It reports the selects' and the pushes' p50 and p99.
func BenchmarkEngineConvoy(b *testing.B) {
	for _, slow := range []string{"", "head", "cold"} {
		name := map[string]string{"": "alone", "head": "with-slow-head-reads", "cold": "with-slow-cold-reads"}[slow]
		b.Run(name, func(b *testing.B) { benchmarkConvoy(b, slow) })
	}
}

// benchmarkConvoy runs the selects and pushes with slow reads of the head's series, or of the cold blocks' series that
// left the head, or none.
func benchmarkConvoy(b *testing.B, slowReads string) {
	ctx := context.Background()
	engine, err := OpenEngine(b.TempDir(), "tenant", EngineOptions{Shards: 16, IdleEvictionMs: 3_600_000, SecondaryHashFunction: secondaryHash})
	require.NoError(b, err)
	b.Cleanup(func() { _ = engine.Close() })
	const series = 200_000
	lbls := func(n int) promlabels.Labels {
		return promlabels.FromStrings("__name__", fmt.Sprintf("metric_%d", n%500), "job", fmt.Sprintf("job-%d", n%50), "pod", fmt.Sprintf("pod-%d", n))
	}
	if slowReads == "cold" {
		// Series that stopped hours before the others, which leave the head for cold blocks.
		app := engine.Appender(ctx)
		for n := range series {
			_, err = app.Append(0, promlabels.FromStrings("__name__", fmt.Sprintf("old_%d", n%500), "job", fmt.Sprintf("job-%d", n%50), "pod", fmt.Sprintf("old-pod-%d", n)), 0, 1000, 1, nil, nil, storage.AppendV2Options{})
			require.NoError(b, err)
		}
		require.NoError(b, app.Commit())
	}
	const now = 3 * 3_600_000
	refs := make([]storage.SeriesRef, series)
	app := engine.Appender(ctx)
	for n := range series {
		refs[n], err = app.Append(0, lbls(n), 0, now, 1, nil, nil, storage.AppendV2Options{})
		require.NoError(b, err)
	}
	require.NoError(b, app.Commit())
	if slowReads == "cold" {
		require.NoError(b, engine.Compact(ctx))
		require.Equal(b, uint64(series), engine.NumSeries(), "the old series left the head")
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var pushes []time.Duration
	var pushMu sync.Mutex
	// Pushes of a scrape's worth of samples to series of every shard, as an ingester's ingestion does.
	wg.Add(1)
	go func() {
		defer wg.Done()
		ts := int64(now + 1000)
		for batch := 0; ; batch++ {
			select {
			case <-stop:
				return
			default:
			}
			start := time.Now()
			app := engine.Appender(ctx)
			for i := range 1000 {
				n := (batch*1000 + i) % series
				if _, err := app.Append(refs[n], lbls(n), 0, ts, float64(ts), nil, nil, storage.AppendV2Options{}); err != nil {
					panic(err)
				}
			}
			if err := app.Commit(); err != nil {
				panic(err)
			}
			pushMu.Lock()
			pushes = append(pushes, time.Since(start))
			pushMu.Unlock()
			if batch%(series/1000) == series/1000-1 {
				ts += 15_000
			}
			time.Sleep(time.Millisecond)
		}
	}()
	var slow atomic.Int64
	if slowReads != "" {
		// A label's values under a regex over every series: the lookup visits them all, of the head or of the cold
		// blocks over the hours the old series were in.
		matcher := promlabels.MustNewMatcher(promlabels.MatchRegexp, "pod", ".*pod-.*7")
		start, end := int64(0), int64(1<<40)
		if slowReads == "cold" {
			end = 2000
		}
		for range 2 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					q, err := engine.Querier(start, end)
					if err != nil {
						panic(err)
					}
					if _, _, err := q.LabelValues(ctx, "job", nil, matcher); err != nil {
						panic(err)
					}
					_ = q.Close()
					slow.Add(1)
				}
			}()
		}
	}
	name := promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "metric_7")
	pod := promlabels.MustNewMatcher(promlabels.MatchEqual, "pod", "pod-1507")
	var selects []time.Duration
	b.ResetTimer()
	for b.Loop() {
		start := time.Now()
		q, err := engine.ChunkQuerier(0, 1<<40)
		if err != nil {
			b.Fatal(err)
		}
		set := q.Select(ctx, true, nil, name, pod)
		for set.Next() {
		}
		if err := set.Err(); err != nil {
			b.Fatal(err)
		}
		_ = q.Close()
		selects = append(selects, time.Since(start))
		time.Sleep(100 * time.Microsecond)
	}
	b.StopTimer()
	close(stop)
	wg.Wait()
	quantile := func(durations []time.Duration, q float64) float64 {
		if len(durations) == 0 {
			return 0
		}
		slices.Sort(durations)
		return float64(durations[int(q*float64(len(durations)-1))].Microseconds())
	}
	b.ReportMetric(quantile(selects, 0.5), "select-p50-µs")
	b.ReportMetric(quantile(selects, 0.99), "select-p99-µs")
	b.ReportMetric(quantile(pushes, 0.5), "push-p50-µs")
	b.ReportMetric(quantile(pushes, 0.99), "push-p99-µs")
	b.ReportMetric(float64(slow.Load()), "slow-reads")
}
