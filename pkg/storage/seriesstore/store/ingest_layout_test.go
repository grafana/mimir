// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
)

func layoutRecord(t testing.TB, lsets []promlabels.Labels, at int64) *RW2Record {
	t.Helper()
	req := &mimirpb.WriteRequest{}
	for i, lset := range lsets {
		req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
			Labels: mimirpb.FromLabelsToLabelAdapters(lset), Samples: []mimirpb.Sample{{TimestampMs: at, Value: float64(i + 1)}},
		}})
	}
	serializer := ingest.RecordSerializerFromVersion(2)
	records, _, err := serializer.ToRecords("ingest", 0, "tenant", req, 64<<20)
	require.NoError(t, err)
	require.Len(t, records, 1)
	record, err := ReadRW2(records[0].Value, ingest.V2CommonSymbols.GetSlice(), ingest.V2RecordSymbolOffset)
	require.NoError(t, err)
	return record
}

func TestRW2PreparedIdentityWithLongLabels(t *testing.T) {
	for _, size := range []int{1, 254, 255, 256, 65536} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			pairs := [][2]string{{"__name__", "metric"}, {strings.Repeat("z", size), strings.Repeat("v", size)}}
			encoded := labels.FromSorted(pairs)
			lset := promlabels.FromStrings(pairs[0][0], pairs[0][1], pairs[1][0], pairs[1][1])
			record := layoutRecord(t, []promlabels.Labels{lset}, 1000)
			defer record.Release()
			key, _, name, hash, known := record.source.prepare(0, nil, nil)
			require.True(t, known)
			require.Equal(t, string(encoded), string(key))
			require.Equal(t, labels.HashPairs(pairs), hash)
			if adapterHashMatches() {
				require.Equal(t, lset.Hash(), hash)
			}
			require.Equal(t, "metric", name)
		})
	}
}

type layoutLabelSink struct {
	rw2Sink
	labels map[int]string
}

func (s *layoutLabelSink) NeedsLabels() bool { return true }
func (s *layoutLabelSink) Ingested(index int, lset promlabels.Labels, _ storage.SeriesRef) {
	s.labels[index] = lset.String()
}

type layoutRefSink struct{ layoutLabelSink }

func (s *layoutRefSink) NeedsLabels() bool { return false }
func (s *layoutRefSink) IngestedRef(index int, _ storage.SeriesRef) bool {
	return index == 0
}

func TestRW2PreparedLabelsWithFallbackSeries(t *testing.T) {
	engine := rw2Engine(t, 2)
	lsets := []promlabels.Labels{rw2Labels(0), promlabels.FromStrings("__name__", "new", "unknown_layout_label", "value"), rw2Labels(1)}
	record := layoutRecord(t, lsets, 1000)
	defer record.Release()
	sink := &layoutLabelSink{labels: map[int]string{}}
	leftover, ingested, err := engine.AppendRW2(record, 0, 1<<62, 1000, false, sink)
	require.NoError(t, err)
	require.Equal(t, []int{1}, leftover)
	require.Equal(t, 2, ingested)
	require.Zero(t, sink.errors)
	require.Equal(t, map[int]string{0: lsets[0].String(), 2: lsets[2].String()}, sink.labels)
	refSink := &layoutRefSink{layoutLabelSink: layoutLabelSink{labels: map[int]string{}}}
	leftover, ingested, err = engine.AppendRW2(record, 0, 1<<62, 1000, false, refSink)
	require.NoError(t, err)
	require.Equal(t, []int{1}, leftover)
	require.Equal(t, 2, ingested)
	require.Zero(t, refSink.errors)
	require.Equal(t, map[int]string{2: lsets[2].String()}, refSink.labels)
}

func layoutEngine(t *testing.T) *Engine {
	t.Helper()
	engine, err := OpenEngine(t.TempDir(), "tenant", EngineOptions{Shards: 1})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	app := engine.Appender(t.Context())
	for n := range 4096 {
		_, err := app.Append(0, rw2Labels(n), 0, 0, 0, nil, nil, storage.AppendV2Options{})
		require.NoError(t, err)
	}
	require.NoError(t, app.Commit())
	return engine
}

func TestIngestLayoutsRejectStaleAndCollidingPositions(t *testing.T) {
	engine := layoutEngine(t)
	lsets := make([]promlabels.Labels, 20)
	for n := range lsets {
		lsets[n] = rw2Labels(n * 17)
	}
	sink := &rw2Sink{}
	appendRecord := func(at int64) {
		record := layoutRecord(t, lsets, at)
		defer record.Release()
		leftover, ingested, err := engine.AppendRW2(record, 0, 1<<62, at, false, sink)
		require.NoError(t, err)
		require.Empty(t, leftover)
		require.Equal(t, len(lsets), ingested)
		require.Zero(t, sink.errors)
	}
	for at := int64(1000); at <= 3000; at += 1000 {
		appendRecord(at)
	}
	tenant := engine.store.shards[0].tenant.Load()
	require.NotNil(t, tenant.ingestLayouts)
	populated := false
	for i := range tenant.ingestLayouts.slots {
		positions := tenant.ingestLayouts.slots[i].positions
		if len(positions) == 0 {
			continue
		}
		populated = true
		for j := range positions {
			// Compaction may move a position to another series, or remove its group entirely.
			positions[j] = refLocation{group: uint32(len(tenant.series.groups) + 1), index: 0}
			if j%2 == 0 {
				positions[j] = refLocation{group: 1, index: 0}
			}
		}
	}
	require.True(t, populated)
	appendRecord(4000)
	group := tenant.series.names[lsets[0].Get(metricNameLabel)]
	entries := tenant.series.groups[group].entries
	wrongHash := entries[1].hash
	entries[1].hash = entries[0].hash
	for i := range tenant.ingestLayouts.slots {
		if positions := tenant.ingestLayouts.slots[i].positions; len(positions) != 0 {
			positions[0] = refLocation{group: group, index: 1}
		}
	}
	// Equal hashes must still compare labels, even for a remembered position.
	appendRecord(5000)
	entries[1].hash = wrongHash
	got := map[string][]float64{}
	for _, series := range querySamples(t, engine, 0, 5000, nil, promlabels.MustNewMatcher(promlabels.MatchRegexp, "__name__", ".+")) {
		for _, sample := range series.samples {
			got[series.labels] = append(got[series.labels], math.Float64frombits(sample.f))
		}
	}
	for n, lset := range lsets {
		require.Equal(t, []float64{0, float64(n + 1), float64(n + 1), float64(n + 1), float64(n + 1), float64(n + 1)}, got[lset.String()])
	}
	require.Equal(t, []float64{0}, got[rw2Labels(300).String()])
}

func TestIngestLayoutsConcurrentRequests(t *testing.T) {
	const workers, rounds, batch = 4, 8, 20
	engine := layoutEngine(t)
	records := make([][]*RW2Record, workers)
	for worker := range workers {
		lsets := make([]promlabels.Labels, batch)
		for i := range lsets {
			lsets[i] = rw2Labels(worker*100 + i)
		}
		for round := range rounds {
			record := layoutRecord(t, lsets, int64(round+1)*1000)
			t.Cleanup(record.Release)
			records[worker] = append(records[worker], record)
		}
	}
	done := make(chan error, workers)
	for worker := range workers {
		go func() {
			sink := &rw2Sink{}
			for _, record := range records[worker] {
				leftover, ingested, err := engine.AppendRW2(record, 0, 1<<62, 1000, false, sink)
				if err != nil || len(leftover) != 0 || ingested != batch || sink.errors != 0 {
					done <- fmt.Errorf("ingested %d of %d series, %d leftovers, %d rejections: %w", ingested, batch, len(leftover), sink.errors, err)
					return
				}
			}
			done <- nil
		}()
	}
	var errs []error
	for range workers {
		errs = append(errs, <-done)
	}
	require.NoError(t, errors.Join(errs...))
	count := 0
	for _, series := range querySamples(t, engine, 0, rounds*1000, nil, promlabels.MustNewMatcher(promlabels.MatchRegexp, "__name__", ".+")) {
		count += len(series.samples)
	}
	require.Equal(t, 4096+workers*rounds*batch, count)
}

func TestRW2PreparedIdentityCachesValueSymbolsByName(t *testing.T) {
	const shared = "same-value-in-two-label-names"
	labelsFromStrings("dictionary_cache_left", "left-first-value")
	labelsFromStrings("dictionary_cache_right", "right-first-value")
	labelsFromStrings("dictionary_cache_right", "right-second-value")
	var sets []promlabels.Labels
	for i := range 8 {
		name := "dictionary_cache_left"
		if i%2 != 0 {
			name = "dictionary_cache_right"
		}
		lset := promlabels.FromStrings("__name__", "cached_symbol_metric", name, shared)
		labels.FromSorted(testDictionaryPairs(lset))
		sets = append(sets, lset)
	}
	record := layoutRecord(t, sets, 1000)
	defer record.Release()
	for range 2 {
		for i, lset := range sets {
			key, _, _, hash, known := record.source.prepare(i, nil, nil)
			want := labels.FromSorted(testDictionaryPairs(lset))
			require.True(t, known)
			require.Equal(t, string(want), string(key))
			require.Equal(t, want.Hash(), hash)
		}
	}
}

func testDictionaryPairs(lset promlabels.Labels) [][2]string {
	var pairs [][2]string
	lset.Range(func(label promlabels.Label) { pairs = append(pairs, [2]string{label.Name, label.Value}) })
	return pairs
}
