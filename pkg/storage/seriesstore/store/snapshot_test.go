// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/chunks"
	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/labels"
	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/limits"
	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/record"
	"github.com/grafana/mimir/pkg/mimirpb"
)

type chunkRead struct {
	start, end int64
	wire       []byte
}

func queryEverything(t testing.TB, s *Store) []chunkRead {
	t.Helper()
	views, err := s.SelectChunks("tenant", math.MinInt64, math.MaxInt64, nil)
	require.NoError(t, err)
	var out []chunkRead
	for _, view := range views {
		for _, chunk := range view.Chunks {
			out = append(out, chunkRead{chunk.StartTimestampMs, chunk.EndTimestampMs, chunk.Wire})
		}
	}
	return out
}

func snapshotLimits() *limits.Overrides {
	l := limits.DefaultLimits()
	l.OutOfOrderTimeWindowMs = 3_600_000
	l.MaxGlobalExemplarsPerUser = 100
	return limits.NewOverrides(l)
}

func TestRestoresSeriesHeadsAndChunksAndRemovesTheSnapshot(t *testing.T) {
	directory := t.TempDir()
	overrides := snapshotLimits()
	s, err := New(20*60*1000, Retention{}, directory)
	require.NoError(t, err)
	s.WithOverrides(overrides)
	request := func(samples []sample, histograms []int64) record.DecodedRequest {
		series := record.DecodedSeries{
			Labels:    [][2]string{{"__name__", "metric"}, {"job", "api"}},
			Exemplars: []mimirpb.Exemplar{{Value: 1, TimestampMs: 5}},
		}
		for _, s := range samples {
			series.Samples = append(series.Samples, mimirpb.Sample{TimestampMs: s.t, Value: s.v})
		}
		for _, timestamp := range histograms {
			series.Histograms = append(series.Histograms, mimirpb.Histogram{
				Timestamp:      timestamp,
				Count:          &mimirpb.Histogram_CountInt{CountInt: 1},
				PositiveSpans:  []mimirpb.BucketSpan{{Offset: 0, Length: 1}},
				PositiveDeltas: []int64{1},
			})
		}
		return record.DecodedRequest{Series: []record.DecodedSeries{series}, Metadata: []mimirpb.MetricMetadata{{Type: 1, MetricFamilyName: "metric", Help: "help"}}}
	}
	require.NoError(t, s.Ingest("tenant", request(samplesRange(0, 500, func(t int64) sample { return sample{t * 15_000, float64(t)} }), nil)))
	var histograms []int64
	for t := int64(1); t < 300; t++ {
		histograms = append(histograms, t*7_000+3)
	}
	require.NoError(t, s.Ingest("tenant", request([]sample{{7, 7}}, histograms)))
	before := queryEverything(t, s)
	exemplarsBefore, err := s.SelectExemplars("tenant", math.MinInt64, math.MaxInt64, nil)
	require.NoError(t, err)
	require.Len(t, exemplarsBefore[0].Exemplars, 1)
	offsets := []SnapshotOffset{{Offset: 41, HasOffset: true, TimestampMs: 99}}
	require.NoError(t, s.WriteSnapshot(offsets))
	require.NoError(t, s.Close())

	restored, err := Restore(20*60*1000, Retention{}, directory, 2)
	require.NoError(t, err)
	require.NotNil(t, restored)
	rs := restored.Store.WithOverrides(overrides)
	require.Equal(t, offsets, restored.Offsets)
	_, err = os.Stat(filepath.Join(directory, snapshotFileName))
	require.True(t, os.IsNotExist(err))
	require.Equal(t, before, queryEverything(t, rs))
	require.Len(t, rs.Metadata("tenant"), 1)
	exemplars, err := rs.SelectExemplars("tenant", math.MinInt64, math.MaxInt64, nil)
	require.NoError(t, err)
	require.Equal(t, exemplarsBefore, exemplars)
	// The head's max time survives: a sample over an hour older than it is too old.
	old := request([]sample{{0, 1}}, nil)
	old.Series[0].Labels[1][1] = "other"
	old.Series[0].Exemplars = nil
	require.NoError(t, rs.Ingest("tenant", old))
	require.Equal(t, uint64(2), rs.NumSeries("tenant"))
	selected, err := rs.SelectLabels("tenant", math.MinInt64, math.MaxInt64, []LabelMatcher{matcher(0, "job", "other")})
	require.NoError(t, err)
	require.Empty(t, selected)
	require.NoError(t, rs.Ingest("tenant", request([]sample{{500 * 15_000, 1}}, nil)))
	require.GreaterOrEqual(t, len(queryEverything(t, rs)), len(before))
	require.NoError(t, rs.Close())

	again, err := Restore(20*60*1000, Retention{}, directory, 2)
	require.NoError(t, err)
	require.Nil(t, again)
}

// writeLegacySnapshot writes the MIMIRHS2 layout: no tenant max time or metadata timestamps,
// exemplars after each series' out-of-order samples, and no exemplar section.
func writeLegacySnapshot(t testing.TB, s *Store, offsets []SnapshotOffset) {
	t.Helper()
	directory := filepath.Dir(s.shards[0].disk.Directory())
	for _, shard := range s.shards {
		require.NoError(t, shard.disk.Sync())
	}
	file, err := os.Create(filepath.Join(directory, snapshotFileName))
	require.NoError(t, err)
	w := &snapshotWriter{w: bufio.NewWriter(file), hasher: crc32.NewIEEE()}
	_, err = w.w.Write(snapshotLegacyMagic)
	require.NoError(t, err)
	must := func(err error) { require.NoError(t, err) }
	must(w.length(len(offsets)))
	for _, offset := range offsets {
		value := int64(math.MinInt64)
		if offset.HasOffset {
			value = offset.Offset
		}
		must(w.i64(value))
		must(w.i64(offset.TimestampMs))
	}
	must(w.length(len(s.shards)))
	for _, shard := range s.shards {
		files, nextSequence := shard.disk.State()
		must(w.u32(nextSequence))
		must(w.length(len(files)))
		for _, file := range files {
			must(w.u32(file.Sequence))
			must(w.u64(file.Written))
			must(w.i64(file.MaxTime))
		}
		must(w.length(len(shard.tenants)))
		for tenantID, tn := range shard.tenants {
			must(w.str(tenantID))
			metadata := sortedMetadata(tn)
			must(w.length(len(metadata)))
			for index := range metadata {
				encoded, err := metadata[index].Marshal()
				require.NoError(t, err)
				must(w.bytes(encoded))
			}
			var seriesExemplars []struct {
				id       uint64
				exemplar mimirpb.Exemplar
			}
			if storage, ok := s.exemplars[tenantID]; ok {
				for _, entry := range storage.InInsertionOrder() {
					seriesExemplars = append(seriesExemplars, struct {
						id       uint64
						exemplar mimirpb.Exemplar
					}{entry.SeriesID, fromExemplar(&entry.Exemplar)})
				}
			}
			must(w.length(tn.series.len))
			tn.series.forEach(func(entry *seriesEntry) {
				series := &entry.series
				must(w.labels(entry.labels))
				must(w.i64(series.lastIngestedMs))
				must(w.u32(series.lastBucketCount))
				metas := series.chunks.toSlice()
				must(w.length(len(metas)))
				for _, chunk := range metas {
					must(w.u64(chunk.Ref))
					must(w.i64(chunk.MinTime))
					must(w.i64(chunk.MaxTime))
					must(w.u32(chunk.Len))
					must(w.u8(chunk.Encoding))
				}
				if fh := series.floatHead; fh != nil {
					must(w.u8(1))
					must(w.i64(fh.minTime))
					must(w.i64(fh.nextAt))
					must(fh.appender.WriteState(hashedWriter{w}))
				} else {
					must(w.u8(0))
				}
				histogramHead, err := series.histogramSamples()
				require.NoError(t, err)
				must(w.length(len(histogramHead)))
				for index := range histogramHead {
					encoded, err := histogramHead[index].Marshal()
					require.NoError(t, err)
					must(w.bytes(encoded))
				}
				must(w.i64(series.histogramNextAt))
				must(w.length(len(series.outOfOrder)))
				for _, sample := range series.outOfOrder {
					require.Nil(t, sample.H, "legacy snapshots hold only float out-of-order samples")
					must(w.i64(sample.T))
					must(w.u64(math.Float64bits(sample.F)))
				}
				var own []mimirpb.Exemplar
				for _, e := range seriesExemplars {
					if e.id == entry.hash {
						own = append(own, e.exemplar)
					}
				}
				must(w.length(len(own)))
				for index := range own {
					encoded, err := own[index].Marshal()
					require.NoError(t, err)
					must(w.bytes(encoded))
				}
			})
		}
	}
	var checksum [4]byte
	binary.LittleEndian.PutUint32(checksum[:], w.hasher.Sum32())
	_, err = w.w.Write(checksum[:])
	require.NoError(t, err)
	require.NoError(t, w.w.Flush())
	require.NoError(t, file.Close())
}

func TestRestoresLegacySnapshotsWithTheirExemplarsAndHeadMaxTime(t *testing.T) {
	directory := t.TempDir()
	l := limits.DefaultLimits()
	l.MaxGlobalExemplarsPerUser = 100
	overrides := limits.NewOverrides(l)
	s, err := New(20*60*1000, Retention{}, directory)
	require.NoError(t, err)
	s.WithOverrides(overrides)
	request := func(job string, timestamp int64) record.DecodedRequest {
		return record.DecodedRequest{
			Series: []record.DecodedSeries{{
				Labels:    [][2]string{{"__name__", "metric"}, {"job", job}},
				Samples:   []mimirpb.Sample{{TimestampMs: timestamp, Value: 1}},
				Exemplars: []mimirpb.Exemplar{{Value: 1, TimestampMs: timestamp}},
			}},
			Metadata: []mimirpb.MetricMetadata{{Type: 1, MetricFamilyName: "metric", Help: "help"}},
		}
	}
	require.NoError(t, s.Ingest("tenant", request("a", 10*hour)))
	require.NoError(t, s.Ingest("tenant", request("b", 10*hour-5)))
	before := queryEverything(t, s)
	exemplarsBefore, err := s.SelectExemplars("tenant", math.MinInt64, math.MaxInt64, nil)
	require.NoError(t, err)
	writeLegacySnapshot(t, s, []SnapshotOffset{{Offset: 3, HasOffset: true, TimestampMs: 1}})
	require.NoError(t, s.Close())

	restored, err := Restore(20*60*1000, Retention{}, directory, 2)
	require.NoError(t, err)
	require.NotNil(t, restored, "legacy snapshot restores")
	rs := restored.Store.WithOverrides(overrides)
	defer rs.Close()
	require.Equal(t, before, queryEverything(t, rs))
	require.Len(t, rs.Metadata("tenant"), 1)
	exemplars, err := rs.SelectExemplars("tenant", math.MinInt64, math.MaxInt64, nil)
	require.NoError(t, err)
	require.Equal(t, exemplarsBefore, exemplars)
	// The head max time is rebuilt from the series: a new series two hours back is rejected.
	require.NoError(t, rs.Ingest("tenant", request("c", 8*hour)))
	old, err := rs.SelectLabels("tenant", math.MinInt64, math.MaxInt64, []LabelMatcher{matcher(0, "job", "c")})
	require.NoError(t, err)
	require.Empty(t, old)
}

// v3FixtureRequests are the requests the Rust ingester's `testdata/head-snapshot-v*` fixtures were
// written from, by the ingesters that wrote those versions.
func v3FixtureRequests() [2]record.DecodedRequest {
	series := func(job string, samples []sample, histograms [][2]int64) record.DecodedSeries {
		s := record.DecodedSeries{
			Labels:    [][2]string{{"__name__", "metric"}, {"job", job}},
			Exemplars: []mimirpb.Exemplar{{Value: 1, TimestampMs: 5}},
		}
		for _, sample := range samples {
			s.Samples = append(s.Samples, mimirpb.Sample{TimestampMs: sample.t, Value: sample.v})
		}
		for _, h := range histograms {
			timestamp, buckets := h[0], uint32(h[1])
			deltas := make([]int64, buckets)
			deltas[0] = 1
			s.Histograms = append(s.Histograms, mimirpb.Histogram{
				Timestamp:      timestamp,
				Count:          &mimirpb.Histogram_CountInt{CountInt: uint64(buckets)},
				Sum:            float64(timestamp % 997),
				PositiveSpans:  []mimirpb.BucketSpan{{Offset: 0, Length: buckets}},
				PositiveDeltas: deltas,
			})
		}
		return s
	}
	request := func(series ...record.DecodedSeries) record.DecodedRequest {
		return record.DecodedRequest{Series: series, Metadata: []mimirpb.MetricMetadata{{Type: 1, MetricFamilyName: "metric", Help: "help"}}}
	}
	var histograms [][2]int64
	for t := int64(0); t < 400; t++ {
		histograms = append(histograms, [2]int64{t*15_000 + 3, 3 + t/100})
	}
	return [2]record.DecodedRequest{
		request(
			series("floats", samplesRange(0, 500, func(t int64) sample { return sample{t * 15_000, float64(t)} }), nil),
			series("histograms", nil, histograms),
		),
		request(
			series("floats", []sample{{7_000_001, 1.5}, {7_000_002, 2.5}}, nil),
			series("histograms", nil, [][2]int64{{5_900_001, 2}}),
		),
	}
}

// normalizedSample is a sample with histogram buckets as absolute counts and without its counter
// reset hint, since recoding a chunk for new buckets or cutting a new one store the same histogram
// with different spans, and a new chunk's first sample has an unknown counter reset.
func normalizedSample(s chunks.Sample) string {
	if s.H == nil {
		return fmt.Sprintf("%d F %v", s.T, math.Float64bits(s.F))
	}
	h := s.H
	var buckets [][2]int64
	index, count, delta := int32(0), int64(0), 0
	for _, span := range h.PositiveSpans {
		index += span.Offset
		for range span.Length {
			count += h.PositiveDeltas[delta]
			delta++
			if count != 0 {
				buckets = append(buckets, [2]int64{int64(index), count})
			}
			index++
		}
	}
	return fmt.Sprintf("%d %v %v %v %v", s.T, h.Sum, h.GetCountInt(), h.GetZeroCountInt(), buckets)
}

func samplesEverything(t testing.TB, s *Store) []string {
	t.Helper()
	views, err := s.SelectChunks("tenant", math.MinInt64, math.MaxInt64, nil)
	require.NoError(t, err)
	var out []string
	for _, view := range views {
		labels := fmt.Sprint(decodeLabels(t, view.EncodedLabels))
		for _, chunk := range decodeChunks(t, []QuerySeriesView{view}) {
			samples, err := chunks.Decode(chunk.Encoding, chunk.Data)
			require.NoError(t, err)
			for _, sample := range samples {
				out = append(out, labels+" "+normalizedSample(sample))
			}
		}
	}
	return out
}

// restoresRustFixture restores a head snapshot the deployed Rust ingester wrote from
// v3FixtureRequests, which the Rust ingester's own tests restore too.
func restoresRustFixture(t *testing.T, name string) {
	directory := filepath.Join(t.TempDir(), "store")
	copyFixture(t, filepath.Join("..", "..", "..", "rust-kafka-ingester", "testdata", name), directory)
	for shard := range DefaultShards {
		require.NoError(t, os.MkdirAll(filepath.Join(directory, fmt.Sprintf("shard-%03d", shard)), 0o755))
	}
	l := limits.DefaultLimits()
	l.OutOfOrderTimeWindowMs = 3_600_000
	l.MaxGlobalExemplarsPerUser = 100
	l.NativeHistogramsIngestionEnabled = true
	overrides := limits.NewOverrides(l)
	restored, err := Restore(20*60*1000, Retention{}, directory, 2)
	require.NoError(t, err)
	require.NotNil(t, restored, "fixture snapshot restores")
	rs := restored.Store.WithOverrides(overrides)
	defer rs.Close()
	fresh, err := New(20*60*1000, Retention{}, filepath.Join(directory, "fresh"))
	require.NoError(t, err)
	fresh.WithOverrides(overrides)
	defer fresh.Close()
	for _, request := range v3FixtureRequests() {
		require.NoError(t, fresh.Ingest("tenant", request))
	}
	require.Equal(t, []SnapshotOffset{{Offset: 41, HasOffset: true, TimestampMs: 99}}, restored.Offsets)
	require.Equal(t, samplesEverything(t, fresh), samplesEverything(t, rs))
	exemplars, err := rs.SelectExemplars("tenant", math.MinInt64, math.MaxInt64, nil)
	require.NoError(t, err)
	require.Len(t, exemplars, 2)
	// Both keep appending to the restored heads alike.
	more := v3FixtureRequests()[0]
	more.Series[0].Samples = []mimirpb.Sample{{TimestampMs: 7_500_000, Value: 9}}
	more.Series[1].Histograms = more.Series[1].Histograms[:1]
	more.Series[1].Histograms[0].Timestamp = 7_500_003
	again := v3FixtureRequests()[0]
	again.Series[0].Samples = slices.Clone(more.Series[0].Samples)
	again.Series[1].Histograms = slices.Clone(more.Series[1].Histograms)
	require.NoError(t, rs.Ingest("tenant", more))
	require.NoError(t, fresh.Ingest("tenant", again))
	require.Equal(t, samplesEverything(t, fresh), samplesEverything(t, rs))
}

func TestRestoresV3SnapshotsWrittenByTheDeployedRustIngester(t *testing.T) {
	restoresRustFixture(t, "head-snapshot-v3")
}

// Written by Mimir `1fbdfb2100` from the same requests.
func TestRestoresV4SnapshotsWrittenByTheDeployedRustIngester(t *testing.T) {
	restoresRustFixture(t, "head-snapshot-v4")
}

func TestRestoresTheEmulatedHeadLikeAGoHeadReplayedFromItsWAL(t *testing.T) {
	directory := t.TempDir()
	l := limits.DefaultLimits()
	l.EarlyHeadCompactionOwnedSeriesThreshold = 1
	overrides := limits.NewOverrides(l)
	overrides.SetActivePartitions(1)
	eviction := &NonOwnedEviction{}
	s, err := New(20*60*1000, Retention{}, directory)
	require.NoError(t, err)
	s.WithOverrides(overrides).WithNonOwnedEviction(eviction)
	now := nowMs()
	for _, name := range []string{"a", "b"} {
		request := record.DecodedRequest{Series: []record.DecodedSeries{{Labels: [][2]string{{"__name__", name}}}}}
		for minute := int64(0); minute <= 4*60; minute++ {
			request.Series[0].Samples = append(request.Series[0].Samples, mimirpb.Sample{TimestampMs: now - 4*hour + minute*60_000, Value: 1})
		}
		require.NoError(t, s.Ingest("tenant", request))
	}
	// Own nothing: both series are pending eviction; only the compaction evicts them.
	s.SetOwnedRanges(map[string]TenantRanges{"tenant": {InShard: true, Ranges: []uint32{0, 0}}})
	s.HeadTick(false, true)
	before := s.HeadTick(true, true)[0]
	require.Equal(t, [2]uint64{0, 2}, [2]uint64{before.MemorySeries, before.NonOwnedEvicted})
	require.NoError(t, s.WriteSnapshot([]SnapshotOffset{{Offset: 1, HasOffset: true, TimestampMs: 1}}))
	require.NoError(t, s.Close())
	restored, err := Restore(20*60*1000, Retention{}, directory, 2)
	require.NoError(t, err)
	rs := restored.Store.WithOverrides(overrides).WithNonOwnedEviction(eviction)
	defer rs.Close()
	rs.SetOwnedRanges(map[string]TenantRanges{"tenant": {InShard: true, Ranges: []uint32{0, 0}}})
	after := rs.HeadTick(false, true)[0]
	require.Equal(t, uint64(0), after.MemorySeries, "evicted series stay out of the head")
	require.Equal(t, before.HeadMinTime, after.HeadMinTime)
	require.True(t, after.Truncated)
}

func TestRejectsCorruptSnapshotAndRemovesIt(t *testing.T) {
	directory := t.TempDir()
	s, err := New(20*60*1000, Retention{}, directory)
	require.NoError(t, err)
	require.NoError(t, s.WriteSnapshot(nil))
	require.NoError(t, s.Close())
	path := filepath.Join(directory, snapshotFileName)
	bytes, err := os.ReadFile(path)
	require.NoError(t, err)
	bytes[len(bytes)-1] ^= 0xff
	require.NoError(t, os.WriteFile(path, bytes, 0o644))
	restored, err := Restore(20*60*1000, Retention{}, directory, 2)
	require.NoError(t, err)
	require.Nil(t, restored)
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err))
}

func frozen(tenant string, pairs [][2]string, bounds ...[2]int64) frozenSeries {
	metas := make([]ChunkMeta, len(bounds))
	for index, b := range bounds {
		metas[index] = ChunkMeta{Ref: uint64(index) * 100, MinTime: b[0], MaxTime: b[1], Len: 10, Encoding: xorEncoding}
	}
	return frozenSeries{tenant: tenant, labels: labels.FromSorted(pairs), chunks: chunkListFromMetas(metas)}
}

func TestColdBlocksFindSeriesByPostingsAndSurviveReopening(t *testing.T) {
	directory := t.TempDir()
	fixture := func() []frozenSeries {
		return []frozenSeries{
			frozen("b", [][2]string{{"__name__", "up"}, {"job", "api"}}, [2]int64{10, 20}),
			frozen("a", [][2]string{{"__name__", "up"}, {"job", "db"}}, [2]int64{5, 8}, [2]int64{30, 40}),
			frozen("a", [][2]string{{"__name__", "up"}, {"job", "api"}}, [2]int64{1, 2}),
			frozen("a", [][2]string{{"__name__", "down"}, {"job", "api"}}, [2]int64{50, 60}),
		}
	}
	inMemory, err := buildColdBlock("", 1, fixture())
	require.NoError(t, err)
	_, err = buildColdBlock(directory, 2, fixture())
	require.NoError(t, err)
	reopened, err := openColdBlock(coldBlockPath(directory, 2), 2)
	require.NoError(t, err)
	for _, block := range []*coldBlock{inMemory, reopened} {
		require.Equal(t, [2]int64{1, 60}, [2]int64{block.minTime, block.maxTime})
		require.Equal(t, 3, block.seriesCount("a"))
		require.True(t, block.overlaps("a", 35, 45) && !block.overlaps("b", 35, 45))
		table := block.tenant("a")
		names := func(indexes []uint32) []string {
			var out []string
			for _, index := range indexes {
				series := block.seriesIn(table, int(index))
				out = append(out, series.labels().String())
			}
			return out
		}
		// Sorted by labels.
		require.Equal(t, []string{
			`[("__name__", "down"), ("job", "api")]`,
			`[("__name__", "up"), ("job", "api")]`,
			`[("__name__", "up"), ("job", "db")]`,
		}, names([]uint32{0, 1, 2}))
		equal := func(name, value string) compiledMatcher {
			return compiledMatcher{kind: kindEqual, name: name, value: value}
		}
		require.Equal(t, []string{`[("__name__", "up"), ("job", "db")]`}, names(block.candidates("a", []compiledMatcher{equal("__name__", "up"), equal("job", "db")})))
		require.Empty(t, block.candidates("a", []compiledMatcher{equal("job", "missing")}))
		require.Empty(t, block.candidates("a", []compiledMatcher{equal("unknown", "x")}))
		db := block.seriesIn(table, 2)
		var bounds [][2]int64
		it := db.chunkIter()
		for chunk, more := it.next(); more; chunk, more = it.next() {
			bounds = append(bounds, [2]int64{chunk.MinTime, chunk.MaxTime})
		}
		require.Equal(t, [][2]int64{{5, 8}, {30, 40}}, bounds)
		require.True(t, db.hasData(9, 31) && !db.hasData(9, 29))
	}
	require.NoError(t, reopened.close())
	path := coldBlockPath(directory, 2)
	corrupt, err := os.ReadFile(path)
	require.NoError(t, err)
	corrupt[20] ^= 1
	require.NoError(t, os.WriteFile(path, corrupt, 0o644))
	_, err = openColdBlock(path, 2)
	require.Error(t, err)
}

func TestParsesRangesAndTenantsOutsideTheShard(t *testing.T) {
	ranges, err := ParseOwnedRanges([]byte(`{"a":[0,10,20,4294967295],"b":null}`))
	require.NoError(t, err)
	require.Equal(t, TenantRanges{InShard: true, Ranges: []uint32{0, 10, 20, math.MaxUint32}}, ranges["a"])
	require.Equal(t, TenantRanges{}, ranges["b"])
	_, err = ParseOwnedRanges([]byte(`[]`))
	require.Error(t, err)
}
