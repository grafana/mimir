// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"testing"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
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
				ref, err := app.Append(refs[n], benchLabels(n), 0, int64(sample)*benchInterval, float64(sample), nil, nil, storage.AppendV2Options{})
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
					if _, err := app.Append(ref, lsets[n], 0, ts, float64(i), nil, nil, storage.AppendV2Options{}); err != nil {
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

// BenchmarkEngineAppendHistogram appends a native histogram sample to every series of a tenant whose first metric name
// has many series, by cached series refs: Commit finds each series where Append saw it.
func BenchmarkEngineAppendHistogram(b *testing.B) {
	for _, name := range benchEngines {
		b.Run(name, func(b *testing.B) {
			head := benchHeads(b, 1, name)[name]
			lsets := make([]promlabels.Labels, benchSeries)
			for n := range lsets {
				lsets[n] = promlabels.FromStrings("__name__", fmt.Sprintf("histogram_%d", n%50), "pod", fmt.Sprintf("pod-%d", n))
			}
			refs := make([]storage.SeriesRef, benchSeries)
			h := &histogram.Histogram{Count: 3, Sum: 3, Schema: 0, PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []int64{3}}
			app := head.Appender(context.Background())
			for n := range lsets {
				ref, err := app.Append(0, lsets[n], 0, benchInterval, 0, h, nil, storage.AppendV2Options{})
				require.NoError(b, err)
				refs[n] = ref
			}
			require.NoError(b, app.Commit())
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				app := head.Appender(context.Background())
				ts := int64(i+2) * benchInterval
				for n, ref := range refs {
					if _, err := app.Append(ref, lsets[n], 0, ts, 0, h, nil, storage.AppendV2Options{}); err != nil {
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
				if _, err := app.Append(0, benchLabels(i), 0, benchInterval, 1, nil, nil, storage.AppendV2Options{}); err != nil {
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

// BenchmarkEngineLabelValues looks up a label's values under matchers, like the label values API: of one metric, and
// of matchers that select most series, as dashboard variables do.
func BenchmarkEngineLabelValues(b *testing.B) {
	heads := benchHeads(b, 1, benchEngines...)
	for _, lookup := range []struct {
		name     string
		label    string
		matchers []*promlabels.Matcher
	}{
		{"one metric", "instance", []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "metric_7")}},
		{"one job of every metric", "instance", []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchEqual, "job", "job-3")}},
		{"every metric", "job", []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchRegexp, "__name__", "metric_.*")}},
		{"a value per series", "pod", []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchRegexp, "job", ".+")}},
	} {
		for _, name := range benchEngines {
			b.Run(lookup.name+"/"+name, func(b *testing.B) {
				head := heads[name]
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					q, err := head.Querier(0, benchInterval)
					if err != nil {
						b.Fatal(err)
					}
					if _, _, err := q.LabelValues(context.Background(), lookup.label, nil, lookup.matchers...); err != nil {
						b.Fatal(err)
					}
					_ = q.Close()
				}
			})
		}
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
			ref, err := app.Append(refs[n], benchLabels(n), 0, int64(sample)*benchInterval, float64(sample), nil, nil, storage.AppendV2Options{})
			require.NoError(b, err)
			refs[n] = ref
		}
		require.NoError(b, app.Commit())
	}
	// One series keeps the head six hours ahead, so compaction moves every other series out of it.
	app := engine.Appender(context.Background())
	_, err := app.Append(0, promlabels.FromStrings("__name__", "fresh"), 0, 6*3_600_000, 1, nil, nil, storage.AppendV2Options{})
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
				ref, err := app.Append(refs[n], rw2Labels(n), 0, int64(sample)*15_000, float64(sample), nil, nil, storage.AppendV2Options{})
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
				_, err := app.Append(0, benchLabels(n), 0, int64(sample)*benchInterval, float64(sample), nil, nil, storage.AppendV2Options{})
				require.NoError(b, err)
			}
			require.NoError(b, app.Commit())
		}
		// A few late samples of each series, between the ones it has.
		app := head.Appender(context.Background())
		for n := range series {
			for late := range 3 {
				_, err := app.Append(0, benchLabels(n), 0, int64(samples-10-late*3)*benchInterval+benchInterval/2, 1, nil, nil, storage.AppendV2Options{})
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

// BenchmarkEngineSelectBackdated selects chunks of series with samples backdated over the hours their head was
// compacted from, which their out-of-order head spans: Prometheus merges it with the in-order chunks still in its head
// only.
func BenchmarkEngineSelectBackdated(b *testing.B) {
	const (
		series, hours = 2000, 12
		samples       = hours * 3_600_000 / benchInterval
	)
	heads := map[string]headUnderTest{}
	for _, name := range benchEngines {
		opts := differentialOptions{oooWindowMs: 2 * hours * 3_600_000}
		if name == "prometheus" {
			heads[name] = openPrometheus(b, b.TempDir(), opts, nil)
		} else {
			heads[name] = openEngine(b, b.TempDir(), opts, nil)
		}
		head := heads[name]
		b.Cleanup(func() { _ = head.Close() })
		for sample := range samples {
			app := head.Appender(context.Background())
			for n := range series {
				_, err := app.Append(0, benchLabels(n), 0, int64(sample)*benchInterval, float64(sample), nil, nil, storage.AppendV2Options{})
				require.NoError(b, err)
			}
			require.NoError(b, app.Commit())
			if sample%(3_600_000/benchInterval) == 0 {
				require.NoError(b, head.Compact(context.Background()))
			}
		}
		// A few samples of each series backdated over its hours, after their last out-of-order compaction.
		app := head.Appender(context.Background())
		for n := range series {
			for late := range 3 {
				_, err := app.Append(0, benchLabels(n), 0, int64((n*7+late*1999)%samples)*benchInterval+benchInterval/2, 1, nil, nil, storage.AppendV2Options{})
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
				q, err := heads[name].ChunkQuerier(0, samples*benchInterval)
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

// BenchmarkEngineSelectEvicted selects series with late samples, half of which went idle hours ago: the engine evicts
// those, which Prometheus's head keeps, its out-of-order head compacted to blocks.
func BenchmarkEngineSelectEvicted(b *testing.B) {
	const (
		series, hours, idleAfter = 2000, 7, 3
		samples                  = hours * 3_600_000 / benchInterval
	)
	heads := map[string]headUnderTest{}
	for _, name := range benchEngines {
		if name == "prometheus" {
			heads[name] = openPrometheus(b, b.TempDir(), differentialOptions{oooWindowMs: 3_600_000}, nil)
		} else {
			e, err := OpenEngine(b.TempDir(), "tenant", EngineOptions{Shards: 4, OutOfOrderTimeWindowMs: 3_600_000, IdleEvictionMs: 3_600_000, SecondaryHashFunction: secondaryHash})
			require.NoError(b, err)
			heads[name] = e
		}
		head := heads[name]
		b.Cleanup(func() { _ = head.Close() })
		for sample := range samples {
			app := head.Appender(context.Background())
			for n := range series {
				if n%2 == 1 && sample >= idleAfter*3_600_000/benchInterval {
					continue
				}
				_, err := app.Append(0, benchLabels(n), 0, int64(sample)*benchInterval, float64(sample), nil, nil, storage.AppendV2Options{})
				require.NoError(b, err)
				// Every fourth sample has a late one between it and the next.
				if sample >= 4 && sample%4 == 0 {
					_, err := app.Append(0, benchLabels(n), 0, int64(sample-4)*benchInterval+benchInterval/2, 1, nil, nil, storage.AppendV2Options{})
					require.NoError(b, err)
				}
			}
			require.NoError(b, app.Commit())
			if sample%(3_600_000/benchInterval) == 0 {
				require.NoError(b, head.Compact(context.Background()))
			}
		}
		require.NoError(b, head.Compact(context.Background()))
	}
	matcher := promlabels.MustNewMatcher(promlabels.MatchRegexp, "__name__", "metric_[78]")
	for _, name := range benchEngines {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				q, err := heads[name].ChunkQuerier(0, samples*benchInterval)
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

// BenchmarkEngineExemplars selects a metric's exemplars from a tenant whose series have some, like the exemplars API.
func BenchmarkEngineExemplars(b *testing.B) {
	for _, name := range benchEngines {
		b.Run(name, func(b *testing.B) {
			opts := differentialOptions{maxExemplars: 100_000}
			var head headUnderTest
			if name == "prometheus" {
				head = openPrometheus(b, b.TempDir(), opts, nil)
			} else {
				head = openEngine(b, b.TempDir(), opts, nil)
			}
			b.Cleanup(func() { _ = head.Close() })
			for sample := range 20 {
				app := head.Appender(context.Background())
				for n := range benchSeries {
					ref, err := app.Append(0, benchLabels(n), 0, int64(sample)*benchInterval, float64(sample), nil, nil, storage.AppendV2Options{})
					require.NoError(b, err)
					if n%5 == 0 {
						_, err = app.AppendExemplars(ref, benchLabels(n), []exemplar.Exemplar{{Labels: promlabels.FromStrings("trace_id", fmt.Sprintf("%x", n*31+sample)), Value: 1, Ts: int64(sample) * benchInterval, HasTs: true}})
						require.NoError(b, err)
					}
				}
				require.NoError(b, app.Commit())
			}
			matcher := []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "metric_5")}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				q, err := head.ExemplarQuerier(context.Background())
				require.NoError(b, err)
				results, err := q.Select(0, 20*benchInterval, matcher)
				require.NoError(b, err)
				require.NotEmpty(b, results)
			}
		})
	}
}

// BenchmarkEngineCompactOutOfOrder compacts an out-of-order head whose samples were backdated over a month, as a few
// of a tenant's series' are: one block for each of the hundreds of block ranges they are in.
func BenchmarkEngineCompactOutOfOrder(b *testing.B) {
	const (
		series, backdated = 100_000, 2_000
		window            = 720 * 3_600_000
		now               = window + 3_600_000
	)
	e := openEngine(b, b.TempDir(), differentialOptions{oooWindowMs: window}, nil)
	b.Cleanup(func() { _ = e.Close() })
	app := e.Appender(context.Background())
	for n := range series {
		_, err := app.Append(0, benchLabels(n), 0, now, 1, nil, nil, storage.AppendV2Options{})
		require.NoError(b, err)
	}
	require.NoError(b, app.Commit())
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := range b.N {
		b.StopTimer()
		app := e.Appender(context.Background())
		for n := range backdated {
			lset := benchLabels((n*37 + iteration*backdated) % series)
			_, err := app.Append(0, lset, 0, now-1-(int64(n)*window/backdated+int64(iteration)*15_000)%window, 1, nil, nil, storage.AppendV2Options{})
			require.NoError(b, err)
		}
		require.NoError(b, app.Commit())
		b.StartTimer()
		e.compactOOOHead()
	}
}

// BenchmarkEngineLabelValuesAfterEviction looks up a label's values without matchers over the head's last minutes once
// some idle series left it, which the head's cached values don't answer for.
func BenchmarkEngineLabelValuesAfterEviction(b *testing.B) {
	engine, err := OpenEngine(b.TempDir(), "tenant", EngineOptions{Shards: 4, IdleEvictionMs: 3_600_000, SecondaryHashFunction: secondaryHash})
	require.NoError(b, err)
	b.Cleanup(func() { _ = engine.Close() })
	app := engine.Appender(context.Background())
	for n := range 100 {
		_, err := app.Append(0, promlabels.FromStrings("__name__", "idle", "job", fmt.Sprintf("job-%d", n%10), "pod", fmt.Sprintf("idle-%d", n)), 0, 0, 1, nil, nil, storage.AppendV2Options{})
		require.NoError(b, err)
	}
	require.NoError(b, app.Commit())
	const now = 3 * 3_600_000
	refs := make([]storage.SeriesRef, benchSeries)
	for sample := range 8 {
		app := engine.Appender(context.Background())
		for n := range benchSeries {
			ref, err := app.Append(refs[n], benchLabels(n), 0, now+int64(sample)*benchInterval, float64(sample), nil, nil, storage.AppendV2Options{})
			require.NoError(b, err)
			refs[n] = ref
		}
		require.NoError(b, app.Commit())
	}
	require.NoError(b, engine.Compact(context.Background()))
	require.Equal(b, uint64(benchSeries), engine.NumSeries(), "the idle series left the head")
	// An idle series that stays in a shard's index after it left the head, as idle eviction keeps them until they're
	// frozen: the head's cached values then don't answer for that shard.
	for _, shard := range engine.store.shards {
		shard.Lock()
		if t, ok := shard.tenants["tenant"]; ok {
			evicted := false
			t.series.forEach(func(entry *seriesEntry) {
				if !evicted {
					entry.series.headEvicted, evicted = true, true
				}
			})
		}
		shard.Unlock()
	}
	engine.store.evictEpoch.Add(1)
	b.ReportAllocs()
	for b.Loop() {
		q, err := engine.Querier(now, now+8*benchInterval)
		if err != nil {
			b.Fatal(err)
		}
		if _, _, err := q.LabelValues(context.Background(), "job", nil); err != nil {
			b.Fatal(err)
		}
		_ = q.Close()
	}
}

// BenchmarkEngineLabelValuesOverIdleSeries looks up a label's values over the head's last minutes when many series left
// it for being idle: like Prometheus's head, which keeps them, the lookup sees the blocks they went to.
func BenchmarkEngineLabelValuesOverIdleSeries(b *testing.B) {
	engine, err := OpenEngine(b.TempDir(), "tenant", EngineOptions{Shards: 4, IdleEvictionMs: 1_800_000, SecondaryHashFunction: secondaryHash})
	require.NoError(b, err)
	b.Cleanup(func() { _ = engine.Close() })
	app := engine.Appender(context.Background())
	for n := range benchSeries {
		// An hour before the live series: idle for longer than the eviction's half hour, but in the head's range.
		_, err := app.Append(0, promlabels.FromStrings("__name__", fmt.Sprintf("idle_%d", n%50), "job", fmt.Sprintf("job-%d", n%10), "pod", fmt.Sprintf("idle-%d", n)), 0, 2*3_600_000, 1, nil, nil, storage.AppendV2Options{})
		require.NoError(b, err)
	}
	require.NoError(b, app.Commit())
	const now = 3 * 3_600_000
	refs := make([]storage.SeriesRef, benchSeries)
	for sample := range 8 {
		app := engine.Appender(context.Background())
		for n := range benchSeries {
			ref, err := app.Append(refs[n], benchLabels(n), 0, now+int64(sample)*benchInterval, float64(sample), nil, nil, storage.AppendV2Options{})
			require.NoError(b, err)
			refs[n] = ref
		}
		require.NoError(b, app.Commit())
	}
	require.NoError(b, engine.Compact(context.Background()))
	require.Equal(b, uint64(benchSeries), engine.NumSeries(), "the idle series left the head")
	for _, label := range []string{"job", "instance"} {
		b.Run(label, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				q, err := engine.Querier(now, now+8*benchInterval)
				if err != nil {
					b.Fatal(err)
				}
				if _, _, err := q.LabelValues(context.Background(), label, nil); err != nil {
					b.Fatal(err)
				}
				_ = q.Close()
			}
		})
	}
}

// BenchmarkEngineLabelValuesCrawlWhileSeriesLeave looks up every label's values without matchers, as a job listing a
// tenant's labels does, with series leaving the head between the crawls, as idle eviction removes them.
func BenchmarkEngineLabelValuesCrawlWhileSeriesLeave(b *testing.B) {
	engine, err := OpenEngine(b.TempDir(), "tenant", EngineOptions{Shards: 4, IdleEvictionMs: 3_600_000, SecondaryHashFunction: secondaryHash})
	require.NoError(b, err)
	b.Cleanup(func() { _ = engine.Close() })
	const labelNames = 30
	crawlLabels := func(n int) promlabels.Labels {
		builder := promlabels.NewScratchBuilder(labelNames + 2)
		builder.Add("__name__", fmt.Sprintf("metric_%d", n%50))
		builder.Add("pod", fmt.Sprintf("pod-%d", n))
		for l := range labelNames {
			builder.Add(fmt.Sprintf("label_%02d", l), fmt.Sprintf("value-%d", n%(2+l*l)))
		}
		builder.Sort()
		return builder.Labels()
	}
	const now = 3 * 3_600_000
	refs := make([]storage.SeriesRef, benchSeries)
	for sample := range 8 {
		app := engine.Appender(context.Background())
		for n := range benchSeries {
			ref, err := app.Append(refs[n], crawlLabels(n), 0, now+int64(sample)*benchInterval, float64(sample), nil, nil, storage.AppendV2Options{})
			require.NoError(b, err)
			refs[n] = ref
		}
		require.NoError(b, app.Commit())
	}
	// A series of each shard that left the head, as idle eviction keeps them in the index until they're frozen.
	for _, shard := range engine.store.shards {
		shard.Lock()
		if t, ok := shard.tenants["tenant"]; ok {
			evicted := false
			t.series.forEach(func(entry *seriesEntry) {
				if !evicted {
					entry.series.headEvicted, evicted = true, true
				}
			})
		}
		shard.Unlock()
	}
	engine.store.evictEpoch.Add(1)
	names := []string{"pod"}
	for l := range labelNames {
		names = append(names, fmt.Sprintf("label_%02d", l))
	}
	b.ReportAllocs()
	for b.Loop() {
		// Series left every shard since the last crawl.
		for _, shard := range engine.store.shards {
			shard.Lock()
			if t, ok := shard.tenants["tenant"]; ok {
				t.series.removals++
			}
			shard.Unlock()
		}
		q, err := engine.Querier(now, now+8*benchInterval)
		if err != nil {
			b.Fatal(err)
		}
		for _, name := range names {
			if _, _, err := q.LabelValues(context.Background(), name, nil); err != nil {
				b.Fatal(err)
			}
		}
		_ = q.Close()
	}
}
