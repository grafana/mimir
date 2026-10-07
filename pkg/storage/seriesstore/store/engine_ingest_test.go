// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"fmt"
	"math"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

type recordingFloatSink struct {
	errors   []string
	ingested []int
}

func (s *recordingFloatSink) Error(series int, err error, timestampMs int64) bool {
	s.errors = append(s.errors, fmt.Sprintf("%d@%d: %v", series, timestampMs, err))
	return true
}

func (s *recordingFloatSink) NeedsLabels() bool { return true }

func (s *recordingFloatSink) Ingested(series int, _ promlabels.Labels, _ storage.SeriesRef) {
	s.ingested = append(s.ingested, series)
}

// TestEngineAppendFloatsMatchesAppender checks the floats ingested by shard against an appender's: the errors, the
// series that got samples, and what a query returns.
func TestEngineAppendFloatsMatchesAppender(t *testing.T) {
	ctx := context.Background()
	options := differentialOptions{oooWindowMs: 30 * 60_000}
	viaAppender, viaFloats := openEngine(t, t.TempDir(), options, nil), openEngine(t, t.TempDir(), options, nil)
	t.Cleanup(func() {
		require.NoError(t, viaAppender.Close())
		require.NoError(t, viaFloats.Close())
	})

	lsets := make([]promlabels.Labels, 40)
	for n := range lsets {
		lsets[n] = promlabels.FromStrings("__name__", fmt.Sprintf("metric_%d", n%4), "pod", fmt.Sprintf("pod-%d", n))
	}
	adapters := func(lset promlabels.Labels) []mimirpb.LabelAdapter {
		var out []mimirpb.LabelAdapter
		lset.Range(func(l promlabels.Label) { out = append(out, mimirpb.LabelAdapter{Name: l.Name, Value: l.Value}) })
		return out
	}
	// The first samples create the series, with an appender in both.
	for _, engine := range []*Engine{viaAppender, viaFloats} {
		app := engine.Appender(ctx)
		for _, lset := range lsets[:30] {
			_, err := app.Append(0, lset, 10_000, 1)
			require.NoError(t, err)
		}
		require.NoError(t, app.Commit())
	}

	// Series 0-29 exist, 30-39 don't. Samples are in order, out of order within the window, too old, duplicated with
	// a different value, and too far in the future.
	request := make([]mimirpb.PreallocTimeseries, len(lsets))
	for n, lset := range lsets {
		samples := []mimirpb.Sample{{TimestampMs: 20_000 + int64(n), Value: float64(n)}}
		switch n % 5 {
		case 1:
			samples = append(samples, mimirpb.Sample{TimestampMs: 5_000, Value: 2})
		case 2:
			samples = append(samples, mimirpb.Sample{TimestampMs: 10_000, Value: 7})
		case 3:
			samples = append(samples, mimirpb.Sample{TimestampMs: 100_000_000, Value: 3})
		case 4:
			samples = append(samples, mimirpb.Sample{TimestampMs: 20_000 + int64(n), Value: float64(n)})
		}
		request[n] = mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{Labels: adapters(lset), Samples: samples}}
	}

	// The appender's way, which the ingester does series by series.
	appenderSink := &recordingFloatSink{}
	app := viaAppender.Appender(ctx)
	for n, lset := range lsets {
		ref, _ := app.GetRef(lset, lset.Hash())
		touched := false
		for _, sample := range request[n].Samples {
			if sample.TimestampMs > 50_000_000 {
				appenderSink.Error(n, fmt.Errorf("too-far-in-future"), sample.TimestampMs)
				continue
			}
			var err error
			ref, err = app.Append(ref, lset, sample.TimestampMs, sample.Value)
			if err != nil {
				appenderSink.Error(n, err, sample.TimestampMs)
				continue
			}
			touched = true
		}
		if touched {
			appenderSink.Ingested(n, lset, ref)
		}
	}
	require.NoError(t, app.Commit())

	floatsSink := &recordingFloatSink{}
	indices := make([]int, len(lsets))
	for n := range indices {
		indices[n] = n
	}
	leftover, ingested, err := viaFloats.AppendFloats(request, indices, 0, 50_000_000, 123, true, floatsSink)
	require.NoError(t, err)
	require.Equal(t, indices[30:], leftover, "the series the engine doesn't have are left over")
	require.Greater(t, ingested, 0)

	// What the leftover series do through the appender.
	app = viaFloats.Appender(ctx)
	for _, n := range leftover {
		ref, err := app.Append(0, lsets[n], request[n].Samples[0].TimestampMs, request[n].Samples[0].Value)
		require.NoError(t, err)
		_ = ref
		for _, sample := range request[n].Samples[1:] {
			if sample.TimestampMs > 50_000_000 {
				floatsSink.Error(n, fmt.Errorf("too-far-in-future"), sample.TimestampMs)
				continue
			}
			if _, err := app.Append(ref, lsets[n], sample.TimestampMs, sample.Value); err != nil {
				floatsSink.Error(n, err, sample.TimestampMs)
			}
		}
	}
	require.NoError(t, app.Commit())

	// The errors the fast part reports are the series 0-29's.
	var appenderErrors []string
	for _, e := range appenderSink.errors {
		var series int
		_, scanErr := fmt.Sscanf(e, "%d@", &series)
		require.NoError(t, scanErr)
		if series < 30 {
			appenderErrors = append(appenderErrors, e)
		}
	}
	var floatErrors []string
	for _, e := range floatsSink.errors {
		var series int
		_, scanErr := fmt.Sscanf(e, "%d@", &series)
		require.NoError(t, scanErr)
		if series < 30 {
			floatErrors = append(floatErrors, e)
		}
	}
	require.NotEmpty(t, appenderErrors)
	require.Equal(t, appenderErrors, floatErrors)
	var appenderTouched []int
	for _, n := range appenderSink.ingested {
		if n < 30 {
			appenderTouched = append(appenderTouched, n)
		}
	}
	require.Equal(t, appenderTouched, floatsSink.ingested)

	query := func(engine *Engine) map[string][]decodedSample {
		q, err := engine.ChunkQuerier(0, 200_000_000)
		require.NoError(t, err)
		defer q.Close()
		set := q.Select(ctx, true, nil, promlabels.MustNewMatcher(promlabels.MatchRegexp, "__name__", "metric_.*"))
		out := map[string][]decodedSample{}
		for set.Next() {
			out[set.At().Labels().String()] = mergedSamples(t, set.At())
		}
		require.NoError(t, set.Err())
		return out
	}
	require.Equal(t, query(viaAppender), query(viaFloats))
	require.Equal(t, viaAppender.NumSeries(), viaFloats.NumSeries())
	require.Equal(t, viaAppender.MaxTime(), viaFloats.MaxTime())
	require.Equal(t, viaAppender.MinOOOTime(), viaFloats.MinOOOTime())
}

// BenchmarkEngineIngestBatch ingests a batch of known series, one sample each, the way the ingester's push path
// hands them over: with an appender, series by series, and with AppendFloats, by shard. Run with -tags stringlabels.
func BenchmarkEngineIngestBatch(b *testing.B) {
	const batchSize = 2000
	ctx := context.Background()
	for _, shards := range []int{16, 64} {
		for _, mode := range []string{"appender", "floats"} {
			b.Run(fmt.Sprintf("shards=%d/%s", shards, mode), func(b *testing.B) {
				engine, err := OpenEngine("", "tenant", EngineOptions{Shards: shards, SecondaryHashFunction: secondaryHash})
				require.NoError(b, err)
				b.Cleanup(func() { _ = engine.Close() })
				const series = 100_000
				lsets := make([]promlabels.Labels, series)
				request := make([]mimirpb.PreallocTimeseries, series)
				app := engine.Appender(ctx)
				for n := range lsets {
					lsets[n] = promlabels.FromStrings("__name__", fmt.Sprintf("metric_%d", n%500), "job", fmt.Sprintf("job-%d", n%20), "instance", fmt.Sprintf("instance-%d", n%700), "pod", fmt.Sprintf("pod-%d", n))
					var adapters []mimirpb.LabelAdapter
					lsets[n].Range(func(l promlabels.Label) {
						adapters = append(adapters, mimirpb.LabelAdapter{Name: l.Name, Value: l.Value})
					})
					request[n] = mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{Labels: adapters, Samples: make([]mimirpb.Sample, 1)}}
					_, err := app.Append(0, lsets[n], 1_000, 1)
					require.NoError(b, err)
				}
				require.NoError(b, app.Commit())

				indices := make([]int, batchSize)
				for n := range indices {
					indices[n] = n
				}
				var worker, clock atomic.Int32
				b.ReportAllocs()
				b.ResetTimer()
				// Workers own disjoint slices of the series, like the pusher's parallel shards.
				b.RunParallel(func(pb *testing.PB) {
					id := int(worker.Add(1) - 1)
					workers := runtime.GOMAXPROCS(0)
					per := series / workers
					own := request[id%workers*per : (id%workers+1)*per]
					for i := 0; pb.Next(); i++ {
						// Time moves on for every batch of every worker: a series only sees later timestamps.
						ts := int64(clock.Add(1))*100 + 10_000
						first := (i * batchSize) % (per - batchSize)
						batch := own[first : first+batchSize]
						for n := range batch {
							batch[n].Samples[0] = mimirpb.Sample{TimestampMs: ts, Value: float64(i)}
						}
						switch mode {
						case "appender":
							app := engine.Appender(ctx)
							var scratch promlabels.ScratchBuilder
							var lbls promlabels.Labels
							for n := range batch {
								mimirpb.FromLabelAdaptersOverwriteLabels(&scratch, batch[n].Labels, &lbls)
								ref, _ := app.GetRef(lbls, lbls.Hash())
								if _, err := app.Append(ref, lbls, ts, float64(i)); err != nil {
									b.Error(err)
									return
								}
							}
							if err := app.Commit(); err != nil {
								b.Error(err)
								return
							}
						default:
							sink := &countingFloatSink{}
							leftover, _, err := engine.AppendFloats(batch, indices, 0, math.MaxInt64, 0, false, sink)
							if err != nil || len(leftover) > 0 || sink.errors > 0 {
								b.Error(err, len(leftover), sink.errors)
								return
							}
						}
					}
				})
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*batchSize), "ns/series")
			})
		}
	}
}

type countingFloatSink struct{ errors int }

func (s *countingFloatSink) Error(int, error, int64) bool                     { s.errors++; return true }
func (*countingFloatSink) NeedsLabels() bool                                  { return false }
func (*countingFloatSink) Ingested(int, promlabels.Labels, storage.SeriesRef) {}

// BenchmarkEngineSelectBigGroup selects nothing from a name group of 50k series by an equality matcher on a label
// that more series than the group have, so the engine reads the label of each series of the group to find out.
func BenchmarkEngineSelectBigGroup(b *testing.B) {
	for _, shards := range []int{16, 64} {
		b.Run(fmt.Sprintf("shards=%d", shards), func(b *testing.B) { benchmarkSelectBigGroup(b, shards) })
	}
}

func benchmarkSelectBigGroup(b *testing.B, shards int) {
	ctx := context.Background()
	engine, err := OpenEngine("", "tenant", EngineOptions{Shards: shards, SecondaryHashFunction: secondaryHash})
	require.NoError(b, err)
	b.Cleanup(func() { _ = engine.Close() })
	app := engine.Appender(ctx)
	const series = 400_000
	for n := range series {
		name := "other"
		if n%8 == 0 {
			name = "big"
		}
		// Every series of the group has one of four clusters; the others have one of three others, which are
		// more series than the group has.
		cluster := 6 + n%3
		if name == "big" {
			cluster = n / 8 % 4
		}
		_, err := app.Append(0, promlabels.FromStrings("__name__", name, "cluster", fmt.Sprintf("cluster-%d", cluster), "pod", fmt.Sprintf("pod-%d", n)), 1_000, 1)
		require.NoError(b, err)
	}
	require.NoError(b, app.Commit())
	matchers := []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "big"), promlabels.MustNewMatcher(promlabels.MatchEqual, "cluster", "cluster-7")}
	b.ResetTimer()
	for range b.N {
		if os.Getenv("MIMIR_REALISTIC_COLD") != "" {
			b.StopTimer()
			evictCaches()
			b.StartTimer()
		}
		q, err := engine.ChunkQuerier(0, 10_000)
		require.NoError(b, err)
		set := q.Select(ctx, true, nil, matchers...)
		for set.Next() {
			b.Fatal("selected a series")
		}
		require.NoError(b, set.Err())
		_ = q.Close()
	}
}

// BenchmarkEngineSelectColdBlocks selects one series of a tenant whose series left the head in many small blocks,
// as a head compaction every minute writes them, which every lookup goes through block by block.
func BenchmarkEngineSelectColdBlocks(b *testing.B) {
	ctx := context.Background()
	engine, err := OpenEngine("", "tenant", EngineOptions{Shards: 16, SecondaryHashFunction: secondaryHash})
	require.NoError(b, err)
	b.Cleanup(func() { _ = engine.Close() })
	const blocks = 60
	for round := range blocks {
		app := engine.Appender(ctx)
		// A few series each round, which only stay in the head for it.
		for n := range 64 {
			_, err := app.Append(0, promlabels.FromStrings("__name__", "old", "round", fmt.Sprint(round), "pod", fmt.Sprint(n)), int64(round)*chunkRangeMs, 1)
			require.NoError(b, err)
		}
		require.NoError(b, app.Commit())
		// A series far ahead moves the head past the others, which compaction takes out of it.
		_, err := engine.Appender(ctx).Append(0, promlabels.FromStrings("__name__", "live"), int64(round+1)*chunkRangeMs*2, 1)
		require.NoError(b, err)
		require.NoError(b, engine.Compact(ctx))
	}
	matchers := []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "old"), promlabels.MustNewMatcher(promlabels.MatchEqual, "round", "3"), promlabels.MustNewMatcher(promlabels.MatchEqual, "pod", "5")}
	b.ResetTimer()
	for range b.N {
		if os.Getenv("MIMIR_REALISTIC_COLD") != "" {
			b.StopTimer()
			evictCaches()
			b.StartTimer()
		}
		q, err := engine.ChunkQuerier(0, blocks*chunkRangeMs*3)
		require.NoError(b, err)
		set := q.Select(ctx, true, nil, matchers...)
		for set.Next() {
		}
		require.NoError(b, set.Err())
		_ = q.Close()
	}
}

func TestEngineActiveSeries(t *testing.T) {
	ctx := context.Background()
	engine := openEngine(t, t.TempDir(), differentialOptions{}, nil)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })

	lsets := make([]promlabels.Labels, 10)
	for n := range lsets {
		lsets[n] = promlabels.FromStrings("__name__", fmt.Sprintf("metric_%d", n%2), "pod", fmt.Sprintf("pod-%d", n))
	}
	app := engine.Appender(ctx)
	refs := make([]storage.SeriesRef, len(lsets))
	for n, lset := range lsets {
		ref, err := app.Append(0, lset, 10_000, 1)
		require.NoError(t, err)
		refs[n] = ref
	}
	require.NoError(t, app.Commit())
	// No series was marked ingested yet: they are all at time zero.
	require.Zero(t, engine.ActiveSeries(1).Total)

	// Series 0-5 are ingested by floats at 100 over OTLP, and 6-7 at 200 through the appender's path, with histograms.
	request := make([]mimirpb.PreallocTimeseries, 6)
	indices := make([]int, 6)
	for n := range request {
		var adapters []mimirpb.LabelAdapter
		lsets[n].Range(func(l promlabels.Label) {
			adapters = append(adapters, mimirpb.LabelAdapter{Name: l.Name, Value: l.Value})
		})
		request[n] = mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{Labels: adapters, Samples: []mimirpb.Sample{{TimestampMs: 20_000, Value: 2}}}}
		indices[n] = n
	}
	leftover, _, err := engine.AppendFloats(request, indices, 0, math.MaxInt64, 100, true, &recordingFloatSink{})
	require.NoError(t, err)
	require.Empty(t, leftover)
	engine.MarkIngested([]IngestedSeries{{Ref: refs[6], HistogramBuckets: 7}, {Ref: refs[7], HistogramBuckets: -1}}, 200, false)

	counts := engine.ActiveSeries(50)
	require.Equal(t, uint64(8), counts.Total)
	require.Equal(t, uint64(6), counts.OTLP)
	require.Equal(t, uint64(1), counts.NativeHistograms)
	require.Equal(t, uint64(7), counts.NativeHistogramBuckets)
	// Only the later ones are active in a later window.
	require.Equal(t, uint64(2), engine.ActiveSeries(150).Total)

	// A custom tracker matches the series of metric_0, and follows a change of trackers.
	engine.SetActiveTrackers(ActiveTrackers{Count: 1, Match: func(lset promlabels.Labels) []uint16 {
		if lset.Get("__name__") == "metric_0" {
			return []uint16{0}
		}
		return nil
	}})
	counts = engine.ActiveSeries(50)
	require.Equal(t, []TrackerCounts{{Total: 4, NativeHistograms: 1, NativeHistogramBuckets: 7}}, counts.Trackers)
	require.Equal(t, counts.Trackers, engine.ActiveSeries(50).Trackers, "from the cached matches")
	engine.SetActiveTrackers(ActiveTrackers{Count: 2, Match: func(lset promlabels.Labels) []uint16 {
		if lset.Get("pod") == "pod-1" {
			return []uint16{1}
		}
		return nil
	}})
	require.Equal(t, []TrackerCounts{{}, {Total: 1}}, engine.ActiveSeries(50).Trackers)

	active, buckets, histogram := engine.IsActive(refs[6], 50)
	require.True(t, active)
	require.True(t, histogram)
	require.Equal(t, 7, buckets)
	active, _, _ = engine.IsActive(refs[9], 50)
	require.False(t, active)

	engine.DeactivateSeries([]storage.SeriesRef{refs[0], refs[6]})
	require.Equal(t, uint64(6), engine.ActiveSeries(50).Total)
	// A later ingest brings a series back.
	engine.MarkIngested([]IngestedSeries{{Ref: refs[0], HistogramBuckets: -1}}, 300, false)
	require.Equal(t, uint64(7), engine.ActiveSeries(50).Total)
	engine.DeactivateAll()
	require.Zero(t, engine.ActiveSeries(0).Total)
}

// activeSink keeps which series the cost attribution counts, and fails on a report that doesn't follow the last.
type activeSink struct {
	t       *testing.T
	counted map[string]int
}

func (s *activeSink) Increment(lbls promlabels.Labels, _ time.Time, buckets int) {
	_, found := s.counted[lbls.String()]
	require.False(s.t, found, "incremented twice: %s", lbls)
	s.counted[lbls.String()] = buckets
}

func (s *activeSink) Decrement(lbls promlabels.Labels, buckets int) {
	was, found := s.counted[lbls.String()]
	require.True(s.t, found, "decremented without increment: %s", lbls)
	require.Equal(s.t, was, buckets, "decremented with other buckets: %s", lbls)
	delete(s.counted, lbls.String())
}

func TestEngineCostAttribution(t *testing.T) {
	ctx := context.Background()
	engine := openEngine(t, t.TempDir(), differentialOptions{}, nil)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })

	lsets := make([]promlabels.Labels, 6)
	refs := make([]storage.SeriesRef, len(lsets))
	app := engine.Appender(ctx)
	for n := range lsets {
		lsets[n] = promlabels.FromStrings("__name__", "metric", "pod", fmt.Sprintf("pod-%d", n))
		ref, err := app.Append(0, lsets[n], 10_000, 1)
		require.NoError(t, err)
		refs[n] = ref
	}
	require.NoError(t, app.Commit())
	sink := &activeSink{t: t, counted: map[string]int{}}
	engine.SetCostAttribution(sink)
	require.Zero(t, engine.ActiveSeries(1).Total)
	require.Empty(t, sink.counted)

	engine.MarkIngested([]IngestedSeries{{Ref: refs[0], HistogramBuckets: -1}, {Ref: refs[1], HistogramBuckets: 4}, {Ref: refs[2], HistogramBuckets: -1}, {Ref: refs[3], HistogramBuckets: -1}}, 100, false)
	require.Equal(t, uint64(4), engine.ActiveSeries(50).Total)
	require.Equal(t, map[string]int{lsets[0].String(): -1, lsets[1].String(): 4, lsets[2].String(): -1, lsets[3].String(): -1}, sink.counted)
	// Nothing changed: nothing is reported again.
	engine.ActiveSeries(50)
	require.Len(t, sink.counted, 4)

	// A histogram that grew, one that went idle, and one that was deactivated.
	engine.MarkIngested([]IngestedSeries{{Ref: refs[1], HistogramBuckets: 9}}, 200, false)
	engine.MarkIngested([]IngestedSeries{{Ref: refs[0], HistogramBuckets: -1}, {Ref: refs[3], HistogramBuckets: -1}}, 200, false)
	engine.DeactivateSeries([]storage.SeriesRef{refs[3]})
	engine.ActiveSeries(150)
	require.Equal(t, map[string]int{lsets[0].String(): -1, lsets[1].String(): 9}, sink.counted)
	// The one that came back is counted again.
	engine.MarkIngested([]IngestedSeries{{Ref: refs[3], HistogramBuckets: -1}}, 300, false)
	engine.ActiveSeries(150)
	require.Contains(t, sink.counted, lsets[3].String())

	// A clear leaves the counts in place, and they aren't doubled when the series are ingested again.
	engine.DeactivateAll()
	engine.ActiveSeries(150)
	require.Len(t, sink.counted, 3)
	engine.MarkIngested([]IngestedSeries{{Ref: refs[0], HistogramBuckets: -1}}, 400, false)
	engine.ActiveSeries(150)
	require.Len(t, sink.counted, 3)

	// A new sink hears of all the active series again.
	next := &activeSink{t: t, counted: map[string]int{}}
	engine.SetCostAttribution(next)
	engine.ActiveSeries(150)
	require.Equal(t, map[string]int{lsets[0].String(): -1}, next.counted)
	engine.SetCostAttribution(nil)
	engine.MarkIngested([]IngestedSeries{{Ref: refs[4], HistogramBuckets: -1}}, 500, false)
	engine.ActiveSeries(150)
	require.Len(t, next.counted, 1)

	// Series that leave the index aren't counted any more.
	last := &activeSink{t: t, counted: map[string]int{}}
	engine.SetCostAttribution(last)
	engine.ActiveSeries(150)
	require.Len(t, last.counted, 2)
	require.NoError(t, engine.prune(1_000_000))
	require.Empty(t, last.counted)
}
