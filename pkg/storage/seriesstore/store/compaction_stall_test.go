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

// Reproduces what a compaction does to the queries and appends of an engine: the passes that go over every series of
// the tenant held each shard's write lock all along. Run with GO_INGESTER_BENCH=1.

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

const (
	stallSeries  = 400_000
	stallMetrics = 40
	stallHour    = int64(3_600_000)
)

// stallEngine opens an engine with stallSeries series, each with a sample at 0 and one at 4h.
func stallEngine(t *testing.T, oooPercent int) (*Engine, []promlabels.Labels, []storage.SeriesRef) {
	engine, err := OpenEngine(t.TempDir(), "tenant", EngineOptions{Shards: 16, OutOfOrderTimeWindowMs: 2 * stallHour, SecondaryHashFunction: secondaryHash})
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	lsets := make([]promlabels.Labels, stallSeries)
	refs := make([]storage.SeriesRef, stallSeries)
	for n := range lsets {
		lsets[n] = promlabels.FromStrings("__name__", fmt.Sprintf("metric_%d", n%stallMetrics), "pod", fmt.Sprintf("pod-%d", n), "job", fmt.Sprintf("job-%d", n%20))
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
	appendAt(4*stallHour, func(int) bool { return true })
	// Out of order for the series that get it: before what they have, inside the window.
	appendAt(3*stallHour, func(n int) bool { return n%100 < oooPercent })
	return engine, lsets, refs
}

// measureStall runs action while queries and appends go on against the engine, and reports their latencies during it
// and outside it.
func measureStall(t *testing.T, label string, engine *Engine, lsets []promlabels.Labels, refs []storage.SeriesRef, action func()) {
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
			q, err := engine.ChunkQuerier(0, 4*stallHour)
			if err != nil {
				t.Error(err)
				return
			}
			set := q.Select(context.Background(), true, nil, promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", fmt.Sprintf("metric_%d", i%stallMetrics)))
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
		ts := 4*stallHour + 1000
		for i := 0; !stop.Load(); i++ {
			start := time.Now()
			app := engine.Appender(context.Background())
			for k := range 200 {
				n := (i*200 + k) % stallSeries
				if _, err := app.Append(refs[n], lsets[n], ts+int64(i), 1); err != nil {
					t.Error(err)
					return
				}
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
	actionStart := time.Now()
	action()
	actionEnd := time.Now()
	time.Sleep(time.Second)
	stop.Store(true)
	wg.Wait()

	report := func(name string, samples []stallSample) {
		var idle, during []time.Duration
		for _, s := range samples {
			if s.at.Before(actionEnd) && s.at.Add(s.duration).After(actionStart) {
				during = append(during, s.duration)
			} else {
				idle = append(idle, s.duration)
			}
		}
		fmt.Printf("%s %-7s idle: n=%d p50=%v p99=%v max=%v | during: n=%d p50=%v p99=%v max=%v\n",
			label, name, len(idle), percentile(idle, 0.5), percentile(idle, 0.99), percentile(idle, 1),
			len(during), percentile(during, 0.5), percentile(during, 0.99), percentile(during, 1))
	}
	fmt.Printf("%s took %v\n", label, actionEnd.Sub(actionStart))
	report("queries", queries)
	report("appends", appends)
}

func TestCompactionStall(t *testing.T) {
	requireBench(t)
	for _, oooPercent := range []int{0, 1, 10} {
		t.Run(fmt.Sprintf("ooo=%d%%", oooPercent), func(t *testing.T) {
			engine, lsets, refs := stallEngine(t, oooPercent)
			measureStall(t, fmt.Sprintf("compaction ooo=%d%%", oooPercent), engine, lsets, refs, func() {
				require.NoError(t, engine.Compact(context.Background()))
			})
		})
	}
}

// TestPruneStall is the retention pass, which removes the series whose data is all older than the cutoff: 1% of them.
func TestPruneStall(t *testing.T) {
	requireBench(t)
	engine, lsets, refs := stallEngine(t, 0)
	// The head's compaction moves the first block range out of it, so only the series without a sample past it are
	// left to prune: none, as every series has one at 4h. Prune the cutoff past some of them instead.
	require.NoError(t, engine.Compact(context.Background()))
	for _, cutoff := range []int64{stallHour, 3 * stallHour} {
		measureStall(t, fmt.Sprintf("prune before %dh", cutoff/stallHour), engine, lsets, refs, func() {
			require.NoError(t, engine.prune(cutoff))
		})
	}
}

// TestCompactSelectedStall moves a fifth of the series out of the head, like the eviction of idle series does.
func TestCompactSelectedStall(t *testing.T) {
	requireBench(t)
	engine, lsets, refs := stallEngine(t, 0)
	var selected []storage.SeriesRef
	for n, ref := range refs {
		if n%5 == 0 {
			selected = append(selected, ref)
		}
	}
	measureStall(t, "compact selected", engine, lsets, refs, func() {
		require.NoError(t, engine.CompactSelectedSeries(selected))
	})
}
