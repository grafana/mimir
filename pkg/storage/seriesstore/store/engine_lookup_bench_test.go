// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"fmt"
	"runtime"
	"testing"

	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
)

func BenchmarkMarkIngestedBatches(b *testing.B) {
	e := rw2Engine(b, 20_000)
	app := e.Appender(b.Context())
	refs := make([]IngestedSeries, 2000)
	for n := range refs {
		l := rw2Labels(n * 7)
		ref, _ := app.GetRef(l, l.Hash())
		require.NotZero(b, ref)
		refs[n] = IngestedSeries{Ref: ref, HistogramBuckets: -1}
	}
	require.NoError(b, app.Rollback())
	b.ReportAllocs()
	for b.Loop() {
		e.MarkIngested(refs, 1, false)
	}
}

func BenchmarkStreamedSelection(b *testing.B) {
	e := benchHeads(b, 4, "seriesstore")["seriesstore"].(*Engine)
	for _, tc := range []struct {
		name     string
		matchers []*promlabels.Matcher
	}{
		{"small", []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchEqual, "pod", "pod-77")}},
		{"metric", []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "metric_7"), promlabels.MustNewMatcher(promlabels.MatchEqual, "job", "job-7")}},
		{"broad", []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchRegexp, "__name__", "metric_.*")}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				q, err := e.ChunkQuerier(0, 4*benchInterval)
				if err != nil {
					b.Fatal(err)
				}
				got, err := q.(*engineChunkQuerier).SelectStreamed(nil, tc.matchers...)
				if err != nil {
					b.Fatal(err)
				}
				runtime.KeepAlive(got)
				if err = q.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// A name equality chooses every series; the other predicates need filtering.
func BenchmarkColdSelectionFilters(b *testing.B) {
	e, err := OpenEngine(b.TempDir(), "tenant", EngineOptions{Shards: 16, SecondaryHashFunction: secondaryHash})
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, e.Close()) })
	for generation := range 8 {
		app := e.Appender(b.Context())
		for n := range 2000 {
			_, err := app.Append(0, promlabels.FromStrings("__name__", "old", "generation", fmt.Sprint(generation), "pod", fmt.Sprintf("pod-%d", n), "zone", fmt.Sprintf("z%d", n%4)), 0, int64(generation)*chunkRangeMs, 1, nil, nil, storage.AppendV2Options{})
			require.NoError(b, err)
		}
		require.NoError(b, app.Commit())
		require.NoError(b, e.Compact(b.Context()))
	}
	app := e.Appender(b.Context())
	_, err = app.Append(0, promlabels.FromStrings("__name__", "fresh"), 0, 12*chunkRangeMs, 1, nil, nil, storage.AppendV2Options{})
	require.NoError(b, err)
	require.NoError(b, app.Commit())
	require.NoError(b, e.Compact(b.Context()))
	for _, tc := range []struct {
		name       string
		start, end int64
		matchers   []*promlabels.Matcher
	}{
		{"regex", 0, 12 * chunkRangeMs, []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "old"), promlabels.MustNewMatcher(promlabels.MatchRegexp, "pod", "pod-1.*"), promlabels.MustNewMatcher(promlabels.MatchNotEqual, "zone", "z1")}},
		{"empty", 0, 12 * chunkRangeMs, []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "old"), promlabels.MustNewMatcher(promlabels.MatchRegexp, "pod", "nomatch.*")}},
		{"short", 3 * chunkRangeMs, 3*chunkRangeMs + 1, []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "old"), promlabels.MustNewMatcher(promlabels.MatchRegexp, "pod", "pod-1.*")}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				q, err := e.ChunkQuerier(tc.start, tc.end)
				if err != nil {
					b.Fatal(err)
				}
				got, err := q.(*engineChunkQuerier).SelectStreamed(nil, tc.matchers...)
				if err != nil {
					b.Fatal(err)
				}
				runtime.KeepAlive(got)
				if err = q.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Multiple label names may share one RW2 value symbol, unlike the usual fixture.
func BenchmarkRW2SharedValueSymbols(b *testing.B) {
	for _, names := range []int{1, 2, 3} {
		b.Run(fmt.Sprint(names), func(b *testing.B) {
			e, err := OpenEngine(b.TempDir(), "tenant", EngineOptions{Shards: 16, SecondaryHashFunction: secondaryHash})
			require.NoError(b, err)
			b.Cleanup(func() { require.NoError(b, e.Close()) })
			lsets := make([]promlabels.Labels, 2000)
			for n := range lsets {
				pairs := []string{"__name__", "metric", "pod", fmt.Sprint(n)}
				for k := range names {
					pairs = append(pairs, fmt.Sprintf("shared_%d", k), fmt.Sprintf("shared-value-%d", n%40))
				}
				lsets[n] = promlabels.FromStrings(pairs...)
			}
			app := e.Appender(b.Context())
			for _, l := range lsets {
				_, err := app.Append(0, l, 0, 0, 1, nil, nil, storage.AppendV2Options{})
				require.NoError(b, err)
			}
			require.NoError(b, app.Commit())
			makeRecords := func(first int64) [][]byte {
				var content [][]byte
				for step := range 32 {
					req := &mimirpb.WriteRequest{}
					for _, l := range lsets {
						req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{Labels: mimirpb.FromLabelsToLabelAdapters(l), Samples: []mimirpb.Sample{{TimestampMs: first + int64(step), Value: 1}}}})
					}
					records, _, err := ingest.RecordSerializerFromVersion(2).ToRecords("ingest", 0, "tenant", req, 64<<20)
					require.NoError(b, err)
					require.Len(b, records, 1)
					content = append(content, records[0].Value)
				}
				return content
			}
			first := int64(1)
			records := makeRecords(first)
			sink := &rw2Sink{}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if i > 0 && i%32 == 0 {
					b.StopTimer()
					first += 32
					records = makeRecords(first)
					b.StartTimer()
				}
				left, count := appendRW2(b, e, records[i%32], sink)
				if len(left) != 0 || count != 2000 {
					b.Fatalf("ingested %d; leftovers %d", count, len(left))
				}
			}
			require.Zero(b, sink.errors)
		})
	}
}
