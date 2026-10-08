// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
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
	name := promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "big")
	// None of the series match, so what the lookup costs is how it finds that out.
	for label, matcher := range map[string]*promlabels.Matcher{
		"equal":     promlabels.MustNewMatcher(promlabels.MatchEqual, "cluster", "cluster-7"),
		"regex":     promlabels.MustNewMatcher(promlabels.MatchRegexp, "cluster", "cluster-(7|8)"),
		"not-regex": promlabels.MustNewMatcher(promlabels.MatchNotRegexp, "cluster", "cluster-[0-3]"),
		"not-equal": promlabels.MustNewMatcher(promlabels.MatchNotEqual, "pod", ""),
	} {
		if label == "not-equal" {
			// Every series has a pod: this one selects them all, which is the cost of reading each.
			continue
		}
		matchers := []*promlabels.Matcher{name, matcher}
		b.Run(label, func(b *testing.B) {
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
		})
	}
	// A quarter of the group matches, which the lookup finds and the query reads.
	for label, matchers := range map[string][]*promlabels.Matcher{
		"equal-many":   {name, promlabels.MustNewMatcher(promlabels.MatchEqual, "cluster", "cluster-0")},
		"regex-many":   {name, promlabels.MustNewMatcher(promlabels.MatchRegexp, "cluster", "cluster-(0|1)")},
		"two-equal":    {name, promlabels.MustNewMatcher(promlabels.MatchEqual, "cluster", "cluster-0"), promlabels.MustNewMatcher(promlabels.MatchNotEqual, "cluster", "cluster-3")},
		"not-equal-eq": {name, promlabels.MustNewMatcher(promlabels.MatchNotEqual, "cluster", "cluster-0")},
	} {
		b.Run(label, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				q, err := engine.ChunkQuerier(0, 10_000)
				require.NoError(b, err)
				set := q.Select(ctx, true, nil, matchers...)
				selected := 0
				for set.Next() {
					selected++
				}
				require.NoError(b, set.Err())
				require.NotZero(b, selected)
				_ = q.Close()
			}
		})
	}
}

// BenchmarkEngineSelectColdBlocks selects one series of a tenant whose series left the head in many small blocks,
// as a head compaction every minute writes them, which every lookup goes through block by block.
func BenchmarkEngineSelectColdBlocks(b *testing.B) {
	// A block every minute of a few series is what a busy head leaves, and the other is what a lookup of big ones costs.
	for _, shape := range []struct {
		name             string
		blocks, perBlock int
	}{{"60-blocks-of-1000", 60, 1000}, {"300-blocks-of-20", 300, 20}} {
		b.Run(shape.name, func(b *testing.B) { benchmarkSelectColdBlocks(b, shape.blocks, shape.perBlock) })
	}
}

func benchmarkSelectColdBlocks(b *testing.B, blocks, perBlock int) {
	ctx := context.Background()
	engine, err := OpenEngine("", "tenant", EngineOptions{Shards: 16, SecondaryHashFunction: secondaryHash})
	require.NoError(b, err)
	b.Cleanup(func() { _ = engine.Close() })
	for round := range blocks {
		app := engine.Appender(ctx)
		// A few series each round, which only stay in the head for it.
		for n := range perBlock {
			// The cluster is on all the series, so its posting list is the whole block's.
			_, err := app.Append(0, promlabels.FromStrings("__name__", "old", "cluster", "c1", "round", fmt.Sprint(round), "pod", fmt.Sprint(n)), int64(round)*chunkRangeMs, 1)
			require.NoError(b, err)
		}
		require.NoError(b, app.Commit())
		// A series far ahead moves the head past the others, which compaction takes out of it.
		_, err := engine.Appender(ctx).Append(0, promlabels.FromStrings("__name__", "live"), int64(round+1)*chunkRangeMs*2, 1)
		require.NoError(b, err)
		require.NoError(b, engine.Compact(ctx))
	}
	eq := func(name, value string) *promlabels.Matcher {
		return promlabels.MustNewMatcher(promlabels.MatchEqual, name, value)
	}
	// What a dashboard's variables ask for: the label names and values of a metric, over the whole range.
	b.Run("label-names", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			q, err := engine.Querier(0, int64(blocks)*chunkRangeMs*3)
			require.NoError(b, err)
			_, _, err = q.LabelNames(ctx, nil, eq("__name__", "old"))
			require.NoError(b, err)
			_ = q.Close()
		}
	})
	b.Run("label-values", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			q, err := engine.Querier(0, int64(blocks)*chunkRangeMs*3)
			require.NoError(b, err)
			_, _, err = q.LabelValues(ctx, "pod", nil, eq("__name__", "old"))
			require.NoError(b, err)
			_ = q.Close()
		}
	})
	for name, matchers := range map[string][]*promlabels.Matcher{
		// The lists of the cluster and the name are the whole block's: only the pod's is read.
		"one-series": {eq("__name__", "old"), eq("cluster", "c1"), eq("round", "3"), eq("pod", "5")},
		// Every series of a block, each of which has its labels copied out.
		"one-block": {eq("__name__", "old"), eq("cluster", "c1"), eq("round", "3")},
		// A series in every block, which a lookup of its pod over the whole range finds block by block.
		"every-block": {eq("__name__", "old"), eq("pod", "5")},
	} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				q, err := engine.ChunkQuerier(0, int64(blocks)*chunkRangeMs*3)
				require.NoError(b, err)
				set := q.Select(ctx, true, nil, matchers...)
				for set.Next() {
				}
				require.NoError(b, set.Err())
				_ = q.Close()
			}
		})
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

// Merging the small cold blocks that head compactions write leaves what a query sees as it was, including for a series
// that left the head in several of them.
func TestEngineMergesSmallColdBlocks(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	engine := openEngine(t, dir, differentialOptions{}, nil)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	const rounds = 40
	for round := range rounds {
		app := engine.Appender(ctx)
		for n := range 6 {
			_, err := app.Append(0, promlabels.FromStrings("__name__", "old", "round", fmt.Sprint(round), "pod", fmt.Sprint(n)), int64(round)*chunkRangeMs, float64(round))
			require.NoError(t, err)
		}
		// One series in every block.
		_, err := app.Append(0, promlabels.FromStrings("__name__", "old", "round", "all", "pod", "stable"), int64(round)*chunkRangeMs, float64(round))
		require.NoError(t, err)
		require.NoError(t, app.Commit())
		_, err = engine.Appender(ctx).Append(0, promlabels.FromStrings("__name__", "live"), int64(round+1)*chunkRangeMs*2, 1)
		require.NoError(t, err)
		require.NoError(t, engine.Compact(ctx))
	}
	blocks := 0
	for _, shard := range engine.store.shards {
		blocks += len(shard.cold.blocks)
	}
	require.Less(t, blocks, rounds*len(engine.store.shards)/2, "the small blocks were merged")

	read := func(matchers ...*promlabels.Matcher) map[string][]float64 {
		q, err := engine.ChunkQuerier(0, rounds*chunkRangeMs*3)
		require.NoError(t, err)
		defer q.Close()
		out := map[string][]float64{}
		set := q.Select(ctx, true, nil, matchers...)
		for set.Next() {
			var values []float64
			for _, sample := range mergedSamples(t, set.At()) {
				values = append(values, math.Float64frombits(sample.f))
			}
			out[set.At().Labels().String()] = values
		}
		require.NoError(t, set.Err())
		return out
	}
	eq := func(name, value string) *promlabels.Matcher {
		return promlabels.MustNewMatcher(promlabels.MatchEqual, name, value)
	}
	stable := read(eq("pod", "stable"))
	require.Len(t, stable, 1)
	var want []float64
	for round := range rounds {
		want = append(want, float64(round))
	}
	for _, values := range stable {
		require.Equal(t, want, values, "the chunks of every block, once, in order")
	}
	require.Len(t, read(eq("pod", "3")), rounds)
	byRound := read(eq("__name__", "old"), eq("round", "7"))
	require.Len(t, byRound, 6)
	for _, values := range byRound {
		require.Equal(t, []float64{7}, values)
	}

	// The merged blocks are what a restart finds.
	everything := read(eq("__name__", "old"))
	require.NoError(t, engine.Close())
	engine = openEngine(t, dir, differentialOptions{}, nil)
	require.Equal(t, everything, read(eq("__name__", "old")))
}

// BenchmarkEngineIngestScattered is AppendFloats on a tenant of a million series that a request picks out of at random,
// as the requests of many targets do: what a lookup reads isn't in the processor's caches the way the series of the
// batches of BenchmarkEngineIngestBatch, which come one after the other, are.
func BenchmarkEngineIngestScattered(b *testing.B) {
	const (
		series    = 1_000_000
		batchSize = 2000
	)
	ctx := context.Background()
	engine, err := OpenEngine("", "tenant", EngineOptions{Shards: 64, SecondaryHashFunction: secondaryHash})
	require.NoError(b, err)
	b.Cleanup(func() { _ = engine.Close() })
	request := make([]mimirpb.PreallocTimeseries, series)
	app := engine.Appender(ctx)
	for n := range request {
		lset := promlabels.FromStrings("__name__", fmt.Sprintf("metric_%d", n%5000), "job", fmt.Sprintf("job-%d", n%20), "instance", fmt.Sprintf("instance-%d", n%700), "pod", fmt.Sprintf("pod-%d", n))
		var adapters []mimirpb.LabelAdapter
		lset.Range(func(l promlabels.Label) {
			adapters = append(adapters, mimirpb.LabelAdapter{Name: l.Name, Value: l.Value})
		})
		request[n] = mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{Labels: adapters, Samples: make([]mimirpb.Sample, 1)}}
		_, err := app.Append(0, lset, 1_000, 1)
		require.NoError(b, err)
		if n%100_000 == 99_999 {
			require.NoError(b, app.Commit())
			app = engine.Appender(ctx)
		}
	}
	require.NoError(b, app.Commit())

	indices := make([]int, batchSize)
	for n := range indices {
		indices[n] = n
	}
	var worker, clock atomic.Int32
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		random := rand.New(rand.NewPCG(uint64(worker.Add(1)), 7))
		// The workers' batches share the labels of the series, and nothing else.
		batch := make([]mimirpb.PreallocTimeseries, batchSize)
		for n := range batch {
			batch[n] = mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{Samples: make([]mimirpb.Sample, 1)}}
		}
		for i := 0; pb.Next(); i++ {
			ts := int64(clock.Add(1))*100 + 10_000
			for n := range batch {
				batch[n].Labels = request[random.IntN(series)].Labels
				batch[n].Samples[0] = mimirpb.Sample{TimestampMs: ts, Value: float64(i)}
			}
			sink := &countingFloatSink{}
			if _, _, err := engine.AppendFloats(batch, indices, 0, math.MaxInt64, 0, false, sink); err != nil {
				b.Error(err)
				return
			}
		}
	})
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*batchSize), "ns/series")
}

// BenchmarkEngineSelectRepeated is a selector that rulers and dashboards send again and again: the same name and
// matchers, over a name with many series of which few match, which a lookup finds by scanning the name's series of each
// shard.
func BenchmarkEngineSelectRepeated(b *testing.B) {
	// Names with many series, and names with a few in each shard, which is most of what a tenant has.
	b.Run("big-names", func(b *testing.B) { benchmarkSelectRepeated(b, 20, 100_000) })
	b.Run("small-names", func(b *testing.B) { benchmarkSelectRepeated(b, 500, 2048) })
}

func benchmarkSelectRepeated(b *testing.B, names, perName int) {
	ctx := context.Background()
	engine, err := OpenEngine("", "tenant", EngineOptions{Shards: 64, SecondaryHashFunction: secondaryHash})
	require.NoError(b, err)
	b.Cleanup(func() { _ = engine.Close() })
	app := engine.Appender(ctx)
	for n := range names * perName {
		_, err := app.Append(0, promlabels.FromStrings("__name__", fmt.Sprintf("metric_%d", n%names), "cluster", fmt.Sprintf("c%d", n/names%12), "job", fmt.Sprintf("job-%d", n/names%500), "namespace", fmt.Sprintf("ns-%d", n/names%300), "pod", fmt.Sprintf("pod-%d", n/names)), 1_000, 1)
		require.NoError(b, err)
		if n%100_000 == 99_999 {
			require.NoError(b, app.Commit())
			app = engine.Appender(ctx)
		}
	}
	require.NoError(b, app.Commit())
	name := promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "metric_7")
	for label, matchers := range map[string][]*promlabels.Matcher{
		"regex":        {name, promlabels.MustNewMatcher(promlabels.MatchRegexp, "job", "job-(1|2|3)"), promlabels.MustNewMatcher(promlabels.MatchNotEqual, "cluster", "c5")},
		"two-equal":    {name, promlabels.MustNewMatcher(promlabels.MatchEqual, "cluster", "c5"), promlabels.MustNewMatcher(promlabels.MatchEqual, "namespace", "ns-17")},
		"not-regex":    {name, promlabels.MustNewMatcher(promlabels.MatchNotRegexp, "namespace", "ns-([1-9]|[0-9][0-9]|.*[0-8])")},
		"selective-eq": {name, promlabels.MustNewMatcher(promlabels.MatchEqual, "pod", "pod-77")},
	} {
		b.Run(label, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				q, err := engine.ChunkQuerier(0, 10_000)
				require.NoError(b, err)
				set := q.Select(ctx, true, nil, matchers...)
				selected := 0
				for set.Next() {
					selected++
				}
				require.NoError(b, set.Err())
				require.NotZero(b, selected)
				_ = q.Close()
			}
		})
	}
}

// A series that stopped leaves the head at a compaction once it is idle long enough, with its data still queryable, and
// the series that left don't make a block of label names each time.
func TestEngineEvictsIdleSeriesAtCompaction(t *testing.T) {
	ctx := context.Background()
	engine, err := OpenEngine(t.TempDir(), "tenant", EngineOptions{Shards: 4, IdleEvictionMs: 30 * 60_000, SecondaryHashFunction: secondaryHash})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })

	const minute = int64(60_000)
	base := int64(10) * 60 * minute
	// Pods of ten series that live ten minutes each, one after the other: the head ends an hour in.
	for pod := range 6 {
		app := engine.Appender(ctx)
		for n := range 10 {
			for at := int64(0); at <= 10; at += 5 {
				_, err := app.Append(0, promlabels.FromStrings("__name__", "metric", "pod", fmt.Sprintf("pod-%d-%d", pod, n)), base+(int64(pod)*10+at)*minute, float64(at))
				require.NoError(t, err)
			}
		}
		require.NoError(t, app.Commit())
	}
	require.Equal(t, uint64(60), engine.NumSeries())
	// The pods that stopped more than 30 minutes before the head's end leave: the first two.
	require.NoError(t, engine.Compact(ctx))
	require.Equal(t, uint64(40), engine.NumSeries(), "only the pods that had samples within the last 30 minutes stay")
	blocks := len(engine.home2().blocks)
	require.NoError(t, engine.Compact(ctx))
	require.NoError(t, engine.Compact(ctx))
	require.Equal(t, blocks, len(engine.home2().blocks), "compactions that find nothing to take add no blocks")

	query := func() map[string]int {
		q, err := engine.ChunkQuerier(0, base+200*minute)
		require.NoError(t, err)
		defer q.Close()
		out := map[string]int{}
		set := q.Select(ctx, true, nil, promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "metric"))
		for set.Next() {
			out[set.At().Labels().Get("pod")] = len(mergedSamples(t, set.At()))
		}
		require.NoError(t, set.Err())
		return out
	}
	before := query()
	require.Len(t, before, 60, "what left the head is still queried")
	require.Equal(t, 3, before["pod-0-0"])
	require.Equal(t, 3, before["pod-5-0"])

	// A series that had left takes samples again: it is in the head, with all its samples.
	app := engine.Appender(ctx)
	_, err = app.Append(0, promlabels.FromStrings("__name__", "metric", "pod", "pod-0-0"), base+65*minute, 99)
	require.NoError(t, err)
	require.NoError(t, app.Commit())
	require.Equal(t, uint64(41), engine.NumSeries())
	after := query()
	require.Len(t, after, 60)
	require.Equal(t, 4, after["pod-0-0"])
}

// home2 is the tenant of the home shard, for tests.
func (e *Engine) home2() *tenant {
	_, t := e.home()
	return t
}
