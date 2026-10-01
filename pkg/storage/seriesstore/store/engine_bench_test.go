// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"fmt"
	"runtime"
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
