// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"testing"

	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

const (
	benchSeries   = 20_000
	benchInterval = 15_000
)

func benchLabels(n int) promlabels.Labels {
	return promlabels.FromStrings(
		"__name__", fmt.Sprintf("metric_%d", n%50),
		"instance", fmt.Sprintf("instance-%d", n%200),
		"job", fmt.Sprintf("job-%d", n%10),
		"pod", fmt.Sprintf("pod-%d", n),
	)
}

var benchEngines = []string{"prometheus", "seriesstore"}

// benchHeads opens the named heads, with every series appended samples times.
func benchHeads(b *testing.B, samples int, names ...string) map[string]headUnderTest {
	heads := map[string]headUnderTest{}
	for _, name := range names {
		if name == "prometheus" {
			heads[name] = openPrometheus(b, b.TempDir(), differentialOptions{}, nil)
		} else {
			heads[name] = openEngine(b, b.TempDir(), differentialOptions{}, nil)
		}
	}
	for _, head := range heads {
		b.Cleanup(func() { _ = head.Close() })
		refs := make([]storage.SeriesRef, benchSeries)
		for sample := range samples {
			app := head.Appender(context.Background())
			for n := range benchSeries {
				ref, err := app.Append(refs[n], benchLabels(n), int64(sample)*benchInterval, float64(sample))
				require.NoError(b, err)
				refs[n] = ref
			}
			require.NoError(b, app.Commit())
		}
	}
	return heads
}

// BenchmarkEngineAppend appends one sample to every series per commit, by cached series refs like
// the ingester's appends of known series.
func BenchmarkEngineAppend(b *testing.B) {
	for _, name := range benchEngines {
		b.Run(name, func(b *testing.B) {
			head := benchHeads(b, 1, name)[name]
			refs := make([]storage.SeriesRef, benchSeries)
			app := head.Appender(context.Background())
			for n := range benchSeries {
				ref, _ := app.GetRef(benchLabels(n), benchLabels(n).Hash())
				refs[n] = ref
			}
			require.NoError(b, app.Rollback())
			lsets := make([]promlabels.Labels, benchSeries)
			for n := range lsets {
				lsets[n] = benchLabels(n)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				app := head.Appender(context.Background())
				ts := int64(i+1) * benchInterval
				for n, ref := range refs {
					if _, err := app.Append(ref, lsets[n], ts, float64(i)); err != nil {
						b.Fatal(err)
					}
				}
				if err := app.Commit(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*benchSeries), "ns/sample")
		})
	}
}

// BenchmarkEngineCreateSeries appends the first sample of new series.
func BenchmarkEngineCreateSeries(b *testing.B) {
	for _, name := range benchEngines {
		b.Run(name, func(b *testing.B) {
			head := benchHeads(b, 0, name)[name]
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				app := head.Appender(context.Background())
				if _, err := app.Append(0, benchLabels(i), benchInterval, 1); err != nil {
					b.Fatal(err)
				}
				if err := app.Commit(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkEngineSelect selects two hours of chunks, like an ingester's streaming query.
func BenchmarkEngineSelect(b *testing.B) {
	heads := benchHeads(b, 480, benchEngines...)
	for _, query := range []struct {
		name     string
		matchers []*promlabels.Matcher
	}{
		{"one pod", []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchEqual, "pod", "pod-77")}},
		{"one metric", []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "metric_7")}},
		{"metric and job regex", []*promlabels.Matcher{
			promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "metric_7"),
			promlabels.MustNewMatcher(promlabels.MatchRegexp, "job", "job-(1|3|7)"),
		}},
	} {
		for _, name := range benchEngines {
			b.Run(query.name+"/"+name, func(b *testing.B) {
				head := heads[name]
				for i := 0; i < b.N; i++ {
					q, err := head.ChunkQuerier(0, 480*benchInterval)
					if err != nil {
						b.Fatal(err)
					}
					set := q.Select(context.Background(), true, nil, query.matchers...)
					for set.Next() {
						it := set.At().Iterator(nil)
						for it.Next() {
							_ = it.At().Chunk.Bytes()
						}
					}
					if err := set.Err(); err != nil {
						b.Fatal(err)
					}
					_ = q.Close()
				}
			})
		}
	}
}

// BenchmarkEngineSeries selects series without their samples, like the series API.
func BenchmarkEngineSeries(b *testing.B) {
	heads := benchHeads(b, 1, benchEngines...)
	for _, query := range []struct {
		name     string
		matchers []*promlabels.Matcher
	}{
		{"one metric", []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "metric_7")}},
		{"every metric", []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchRegexp, "__name__", "metric_.*")}},
	} {
		for _, name := range benchEngines {
			b.Run(query.name+"/"+name, func(b *testing.B) {
				head := heads[name]
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					q, err := head.Querier(0, benchInterval)
					if err != nil {
						b.Fatal(err)
					}
					set := q.Select(context.Background(), true, &storage.SelectHints{Start: 0, End: benchInterval, Func: "series"}, query.matchers...)
					for set.Next() {
						_ = set.At().Labels()
					}
					if err := set.Err(); err != nil {
						b.Fatal(err)
					}
					_ = q.Close()
				}
			})
		}
	}
}

// BenchmarkEngineLabelValues looks up a label's values under a matcher, like the label values API.
func BenchmarkEngineLabelValues(b *testing.B) {
	heads := benchHeads(b, 1, benchEngines...)
	matcher := promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "metric_7")
	for _, name := range benchEngines {
		b.Run(name, func(b *testing.B) {
			head := heads[name]
			for i := 0; i < b.N; i++ {
				q, err := head.Querier(0, benchInterval)
				if err != nil {
					b.Fatal(err)
				}
				if _, _, err := q.LabelValues(context.Background(), "instance", nil, matcher); err != nil {
					b.Fatal(err)
				}
				_ = q.Close()
			}
		})
	}
}

// BenchmarkEngineColdLabels looks up label names and values over a range only compacted blocks
// cover, like the label APIs over the hours before the head.
func BenchmarkEngineColdLabels(b *testing.B) {
	engine := openEngine(b, b.TempDir(), differentialOptions{}, nil)
	b.Cleanup(func() { _ = engine.Close() })
	refs := make([]storage.SeriesRef, benchSeries)
	for sample := range 480 {
		app := engine.Appender(context.Background())
		for n := range benchSeries {
			ref, err := app.Append(refs[n], benchLabels(n), int64(sample)*benchInterval, float64(sample))
			require.NoError(b, err)
			refs[n] = ref
		}
		require.NoError(b, app.Commit())
	}
	// One series keeps the head six hours ahead, so compaction moves every other series out of it.
	app := engine.Appender(context.Background())
	_, err := app.Append(0, promlabels.FromStrings("__name__", "fresh"), 6*3_600_000, 1)
	require.NoError(b, err)
	require.NoError(b, app.Commit())
	require.NoError(b, engine.Compact(context.Background()))
	require.Less(b, engine.NumSeries(), uint64(benchSeries), "series left the head")
	matcher := promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "metric_7")
	for _, lookup := range []struct {
		name string
		run  func(storage.Querier) error
	}{
		{"names", func(q storage.Querier) error { _, _, err := q.LabelNames(context.Background(), nil); return err }},
		{"names with matcher", func(q storage.Querier) error {
			_, _, err := q.LabelNames(context.Background(), nil, matcher)
			return err
		}},
		{"values with matcher", func(q storage.Querier) error {
			_, _, err := q.LabelValues(context.Background(), "pod", nil, matcher)
			return err
		}},
		// A label of few values, which most series repeat: the common label values lookup of a dashboard variable.
		{"few values with matcher", func(q storage.Querier) error {
			_, _, err := q.LabelValues(context.Background(), "job", nil, matcher)
			return err
		}},
		{"few values", func(q storage.Querier) error {
			_, _, err := q.LabelValues(context.Background(), "job", nil)
			return err
		}},
	} {
		b.Run(lookup.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				q, err := engine.Querier(0, 2*3_600_000)
				if err != nil {
					b.Fatal(err)
				}
				if err := lookup.run(q); err != nil {
					b.Fatal(err)
				}
				_ = q.Close()
			}
		})
	}
}

// BenchmarkEngineHeapPerSeries reports the heap each head holds per series, with two hours of
// samples.
func BenchmarkEngineHeapPerSeries(b *testing.B) {
	for _, name := range benchEngines {
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				var before, after runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&before)
				heads := benchHeads(b, 480, name)
				runtime.GC()
				runtime.ReadMemStats(&after)
				b.ReportMetric(float64(after.HeapInuse-before.HeapInuse)/benchSeries, "heap-bytes/series")
				runtime.KeepAlive(heads)
			}
		})
	}
}

// BenchmarkEngineHeapPerRealisticSeries reports the heap each series holds with the labels of a Kubernetes scrape, a
// few samples, and more custom trackers than a series' bits, as production has.
func BenchmarkEngineHeapPerRealisticSeries(b *testing.B) {
	const series = 200_000
	for range b.N {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		engine, err := OpenEngine(b.TempDir(), "tenant", EngineOptions{Shards: 16, SecondaryHashFunction: secondaryHash})
		require.NoError(b, err)
		engine.SetActiveTrackers(ActiveTrackers{Count: 40, Match: func(lset promlabels.Labels) []uint16 {
			// A few trackers by namespace, and one past the bits by job.
			namespace := lset.Get("namespace")
			return []uint16{uint16(len(namespace) % 4), uint16(32 + len(lset.Get("job"))%8)}
		}})
		refs := make([]storage.SeriesRef, series)
		for sample := range 4 {
			app := engine.Appender(context.Background())
			for n := range series {
				ref, err := app.Append(refs[n], rw2Labels(n), int64(sample)*15_000, float64(sample))
				require.NoError(b, err)
				refs[n] = ref
			}
			require.NoError(b, app.Commit())
		}
		engine.ActiveSeries(0)
		runtime.GC()
		runtime.ReadMemStats(&after)
		b.ReportMetric(float64(after.HeapInuse-before.HeapInuse)/series, "unshrunk-heap-bytes/series")
		// What the head's collection gives back of the room the groups grew with.
		engine.shrinkEntries()
		runtime.GC()
		runtime.ReadMemStats(&after)
		b.ReportMetric(float64(after.HeapInuse-before.HeapInuse)/series, "heap-bytes/series")
		if path := os.Getenv("SERIESSTORE_HEAP_PROFILE"); path != "" {
			// What the series hold, which the benchmark's profile, written once the engine is closed, doesn't have.
			file, err := os.Create(path)
			require.NoError(b, err)
			require.NoError(b, pprof.WriteHeapProfile(file))
			require.NoError(b, file.Close())
		}
		runtime.KeepAlive(engine)
		require.NoError(b, engine.Close())
	}
}

// BenchmarkEngineSelectOutOfOrder selects chunks of series whose head has late samples, which the query merges with the
// in-order chunk they overlap.
func BenchmarkEngineSelectOutOfOrder(b *testing.B) {
	heads := map[string]headUnderTest{}
	for _, name := range benchEngines {
		opts := differentialOptions{oooWindowMs: 3_600_000}
		if name == "prometheus" {
			heads[name] = openPrometheus(b, b.TempDir(), opts, nil)
		} else {
			heads[name] = openEngine(b, b.TempDir(), opts, nil)
		}
		head := heads[name]
		b.Cleanup(func() { _ = head.Close() })
		const series, samples = 2000, 240
		for sample := range samples {
			app := head.Appender(context.Background())
			for n := range series {
				_, err := app.Append(0, benchLabels(n), int64(sample)*benchInterval, float64(sample))
				require.NoError(b, err)
			}
			require.NoError(b, app.Commit())
		}
		// A few late samples of each series, between the ones it has.
		app := head.Appender(context.Background())
		for n := range series {
			for late := range 3 {
				_, err := app.Append(0, benchLabels(n), int64(samples-10-late*3)*benchInterval+benchInterval/2, 1)
				require.NoError(b, err)
			}
		}
		require.NoError(b, app.Commit())
	}
	matcher := promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "metric_7")
	for _, name := range benchEngines {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				q, err := heads[name].ChunkQuerier(0, 240*benchInterval)
				require.NoError(b, err)
				set := q.Select(context.Background(), true, nil, matcher)
				for set.Next() {
					it := set.At().Iterator(nil)
					for it.Next() {
						_ = it.At().Chunk.Bytes()
					}
				}
				require.NoError(b, set.Err())
				_ = q.Close()
			}
		})
	}
}
