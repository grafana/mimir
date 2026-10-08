// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"fmt"
	"math"
	"os"
	"slices"
	"strconv"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/seriesstore/chunks"
	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
	"github.com/grafana/mimir/pkg/storage/seriesstore/limits"
	"github.com/grafana/mimir/pkg/storage/seriesstore/metrics"
	"github.com/grafana/mimir/pkg/storage/seriesstore/record"
	"github.com/grafana/mimir/pkg/storage/seriesstore/trackers"
)

func TestShardsLikeTheGoIngester(t *testing.T) {
	// labels.StableHash values from Go.
	require.Equal(t, uint64(2_852_606_813_363_628_783), labels.StableHashPairs([][2]string{{"__name__", "up"}, {"job", "api"}}))
	require.Equal(t, uint64(609_427_224_681_409_917), labels.StableHashPairs([][2]string{{"__name__", "sharded"}, {"n", "1"}}))
	s := Default()
	request := seriesRequest("sharded", sample{1_000, 1})
	request.Series[0].Labels = append(request.Series[0].Labels, [2]string{"n", "1"})
	require.NoError(t, s.Ingest("tenant", request))
	shard := func(value string) int {
		views, err := s.SelectChunks("tenant", math.MinInt64, math.MaxInt64, []LabelMatcher{matcher(0, "__query_shard__", value)})
		require.NoError(t, err)
		return len(views)
	}
	index := uint64(609_427_224_681_409_917) % 3
	require.Equal(t, 1, shard(fmt.Sprintf("%d_of_3", index+1)))
	require.Equal(t, 0, shard(fmt.Sprintf("%d_of_3", (index+1)%3+1)))
	// mimirpb.ShardByAllLabels from Go, which owned series are counted by.
	require.Equal(t, uint32(4_096_482_777), ShardByAllLabels("18657", labels.FromSorted([][2]string{{"__name__", "up"}, {"job", "api"}})))
	// Mimir's idealShardsFor.
	pusher := DefaultPusherShards()
	pusher.Max = 2
	require.Equal(t, 1, pusher.count(2*200*150*40-1, 150))
	require.Equal(t, 2, pusher.count(2*200*150*40, 150))
	require.Equal(t, 2, pusher.count(math.MaxInt, 150))
	require.Equal(t, 1, pusher.count(math.MaxInt, 0))
}

func coldShardQueries() [][]LabelMatcher {
	shard := func(index, count int) LabelMatcher {
		return matcher(0, "__query_shard__", fmt.Sprintf("%d_of_%d", index, count))
	}
	queries := [][]LabelMatcher{{matcher(0, "__name__", "old")}, {}}
	for index := 1; index <= 4; index++ {
		queries = append(queries,
			[]LabelMatcher{matcher(0, "__name__", "old"), shard(index, 4)},
			[]LabelMatcher{matcher(2, "n", "1.*"), shard(index, 4)},
			[]LabelMatcher{matcher(0, "__name__", "middle"), matcher(0, "n", "3"), shard(index, 4)})
	}
	for index := 1; index <= 3; index++ {
		queries = append(queries, []LabelMatcher{shard(index, 3)})
	}
	return queries
}

func TestShardedReadsOfColdSeriesMatchTheHead(t *testing.T) {
	directory := t.TempDir()
	s, err := New(20*60*1000, Retention{}, directory)
	require.NoError(t, err)
	start := 10 * hour
	request := seriesRequest("long", samplesRange(0, 301, func(minute int64) sample { return sample{start + minute*60_000, float64(minute)} })...)
	// Every 15 s, so old series have several chunks.
	for _, group := range []struct {
		name     string
		count    int
		from, to int64
	}{{"old", 60, 0, 400}, {"middle", 20, 600, 640}} {
		for n := range group.count {
			series := seriesOf(group.name, samplesRange(group.from, group.to, func(quarter int64) sample { return sample{start + quarter*15_000, float64(n)} })...)
			series.Labels = append(series.Labels, [2]string{"n", strconv.Itoa(n)})
			request.Series = append(request.Series, series)
		}
	}
	require.NoError(t, s.Ingest("tenant", request))
	queries := coldShardQueries()
	golden := rustResults(t)
	reads := func(s *Store) [][]queryResult {
		var out [][]queryResult
		for index, matchers := range queries {
			// Everything, part of old's chunks, a block range after old's samples, the head.
			for rangeIndex, bounds := range [][2]int64{
				{math.MinInt64, math.MaxInt64},
				{start + 30*60_000, start + 40*60_000},
				{start + 105*60_000, start + 115*60_000},
				{start + 2*hour, start + 5*hour},
			} {
				views, err := s.SelectChunks("tenant", bounds[0], bounds[1], matchers)
				require.NoError(t, err)
				results := resultsOf(views)
				name := fmt.Sprintf("coldshards/%d/%d", index, rangeIndex)
				require.Equal(t, golden[name], results, name)
				out = append(out, results)
			}
		}
		return out
	}
	s.HeadTick(true, false)
	before := reads(s)
	require.Equal(t, uint64(81), s.NumSeries("tenant"))
	// The first range of the first sharded query: old with shard 1 of 4.
	shardedOld := before[8]
	for _, series := range shardedOld {
		require.Greater(t, len(series.chunks), 1)
	}
	require.True(t, len(shardedOld) > 0 && len(shardedOld) < 60, "a shard of old")
	s.HeadTick(false, false)
	require.Equal(t, uint64(21), s.NumSeries("tenant"), "old left memory")
	hashed := coldShardHashings.Load()
	require.Equal(t, before, reads(s))
	// Each block's shard hashes are computed once, not for every sharded query.
	require.Greater(t, coldShardHashings.Load(), hashed)
	hashed = coldShardHashings.Load()
	require.Equal(t, before, reads(s))
	require.Equal(t, hashed, coldShardHashings.Load())
	require.NoError(t, s.WriteSnapshot([]SnapshotOffset{{Offset: 1, HasOffset: true, TimestampMs: 1}}))
	require.NoError(t, s.Close())
	restored, err := Restore(20*60*1000, Retention{}, directory, 2)
	require.NoError(t, err)
	defer restored.Store.Close()
	require.Equal(t, before, reads(restored.Store))
}

type freezeReads struct {
	samples     []sample
	values      []string
	names       []string
	seriesNames [][][2]string
}

func TestSeriesThatLeaveTheHeadMoveToColdBlocksWithoutChangingReads(t *testing.T) {
	directory := t.TempDir()
	s, err := New(20*60*1000, Retention{}, directory)
	require.NoError(t, err)
	start := 10 * hour
	request := seriesRequest("long", samplesRange(0, 301, func(minute int64) sample { return sample{start + minute*60_000, float64(minute)} })...)
	// The head keeps the last block range of the 5 hours: 12h to 15h.
	request.Series = append(request.Series,
		seriesOf("old", samplesRange(0, 100, func(minute int64) sample { return sample{start + minute*60_000, 1} })...),
		seriesOf("middle", samplesRange(150, 160, func(minute int64) sample { return sample{start + minute*60_000, 1} })...))
	require.NoError(t, s.Ingest("tenant", request))
	reads := func(s *Store) freezeReads {
		values, err := s.LabelValues("tenant", "__name__", start, start+hour, nil)
		require.NoError(t, err)
		names, err := s.LabelNames("tenant", start, start+hour, nil)
		require.NoError(t, err)
		series, err := s.SelectLabels("tenant", start, start+3*hour, nil)
		require.NoError(t, err)
		slices.SortFunc(series, comparePairLists)
		return freezeReads{floatSamples(t, s, math.MinInt64, math.MaxInt64), values, names, series}
	}
	// Compacting moves the head past "old", and the next tick moves it to a cold block.
	s.HeadTick(true, false)
	before := reads(s)
	require.Equal(t, uint64(3), s.NumSeries("tenant"))
	s.HeadTick(false, false)
	require.Equal(t, uint64(2), s.NumSeries("tenant"), "old left memory")
	require.Equal(t, before, reads(s))
	require.Equal(t, []string{"long", "old"}, before.values)
	// Snapshots keep the cold blocks.
	require.NoError(t, s.WriteSnapshot([]SnapshotOffset{{Offset: 1, HasOffset: true, TimestampMs: 1}}))
	require.NoError(t, s.Close())
	restored, err := Restore(20*60*1000, Retention{}, directory, 2)
	require.NoError(t, err)
	s = restored.Store
	defer s.Close()
	require.Equal(t, before, reads(s))
	// A series written to again is back in memory, and reads include what it had.
	require.NoError(t, s.Ingest("tenant", seriesRequest("old", sample{start + 299*60_000, 7})))
	old, err := s.SelectChunks("tenant", math.MinInt64, math.MaxInt64, []LabelMatcher{matcher(0, "__name__", "old")})
	require.NoError(t, err)
	require.Len(t, old, 1, "one series across the cold block and memory")
	var oldSamples int
	for _, chunk := range decodeChunks(t, old) {
		decoded, err := chunks.DecodeXOR(chunk.Data)
		require.NoError(t, err)
		oldSamples += len(decoded)
	}
	require.Equal(t, 101, oldSamples)
	coldFiles := func() int {
		count := 0
		for shard := range DefaultShards {
			entries, err := os.ReadDir(coldDir(directory, shard))
			if err == nil {
				count += len(entries)
			}
		}
		return count
	}
	require.Equal(t, 1, coldFiles())
	// Retention removes a cold block once all of it is older.
	require.NoError(t, s.pruneBefore(start+200*60_000))
	require.Equal(t, 0, coldFiles())
	// Chunks straddling the cutoff stay, like head truncation: those from 12h on.
	require.Empty(t, floatSamples(t, s, math.MinInt64, start+119*60_000))
}

func TestLookupsOverTheWholeTimeRangeSeeCompactedBlocks(t *testing.T) {
	s := Default()
	start := 10 * hour
	request := seriesRequest("long", samplesRange(0, 301, func(minute int64) sample { return sample{start + minute*60_000, 1} })...)
	request.Series = append(request.Series, seriesOf("old", sample{start, 1}))
	require.NoError(t, s.Ingest("tenant", request))
	s.HeadTick(true, false)
	names := func(start, end int64) []string {
		values, err := s.LabelValues("tenant", "__name__", start, end, nil)
		require.NoError(t, err)
		return values
	}
	require.Equal(t, []string{"long"}, names(12*hour, 15*hour))
	require.Equal(t, []string{"long", "old"}, names(0, math.MaxInt64))
	require.Equal(t, []string{"long", "old"}, names(math.MinInt64, math.MaxInt64))
}

func TestKeepsDenseHistogramsInOneChunkLikeThePrometheusHead(t *testing.T) {
	s := storeWith(t, outOfOrderLimits())
	base := int64(1_790_568_000_000)
	request := seriesRequest("dense")
	for index := range int64(300) {
		timestamp := base + index*10
		h := histogramAt(timestamp, 5)
		h.Sum = float64(timestamp % 997)
		request.Series[0].Histograms = append(request.Series[0].Histograms, h)
	}
	require.NoError(t, s.Ingest("tenant", request))
	// A Prometheus head, given the same samples, keeps them in one 918-byte chunk.
	var got [][3]int64
	for _, chunk := range query(t, s, math.MinInt64, math.MaxInt64) {
		got = append(got, [3]int64{chunk.StartTimestampMs - base, chunk.EndTimestampMs - base, int64(len(chunk.Data))})
	}
	require.Equal(t, [][3]int64{{0, 2_990, 918}}, got)
}

func TestAppendsHistogramsLikeThePrometheusHead(t *testing.T) {
	s := storeWith(t, outOfOrderLimits())
	request := func(histograms ...mimirpb.Histogram) record.DecodedRequest {
		return record.DecodedRequest{Series: []record.DecodedSeries{{Labels: [][2]string{{"__name__", "metric"}}, Histograms: histograms}}}
	}
	var first []mimirpb.Histogram
	for index := range int64(20) {
		first = append(first, histogramAt(1_000+index*10, 5))
	}
	require.NoError(t, s.Ingest("tenant", request(first...)))
	// More buckets recode the open chunk instead of cutting it; the older sample waits in the
	// out-of-order chunk.
	require.NoError(t, s.Ingest("tenant", request(histogramAt(10_000, 7), histogramAt(500, 5))))
	withSeries(t, s, func(series *Series) {
		require.True(t, series.chunks.isEmpty())
		require.Len(t, series.ooo(), 1)
		require.Equal(t, 21, series.histogram().Len())
		require.Equal(t, uint32(5), series.lastBucketCount)
	})
	var bounds [][2]int64
	for _, chunk := range query(t, s, 3_500, 10_000) {
		bounds = append(bounds, [2]int64{chunk.StartTimestampMs, chunk.EndTimestampMs})
	}
	require.Equal(t, [][2]int64{{1_000, 10_000}}, bounds)
	// A counter reset starts a new chunk.
	reset := histogramAt(11_000, 7)
	reset.PositiveDeltas = make([]int64, 7)
	reset.Count = &mimirpb.Histogram_CountInt{CountInt: 0}
	require.NoError(t, s.Ingest("tenant", request(reset)))
	withSeries(t, s, func(series *Series) {
		var metas [][2]int64
		for _, chunk := range series.chunks.toSlice() {
			metas = append(metas, [2]int64{chunk.MinTime, chunk.MaxTime})
		}
		require.Equal(t, [][2]int64{{1_000, 10_000}}, metas)
		require.Equal(t, chunks.HeaderCounterReset, series.histogram().Header())
	})
}

func TestSelectsFloatChunkSpanningHistogramChunks(t *testing.T) {
	s := Default()
	stale := math.Float64frombits(0x7ff0_0000_0000_0002)
	series := record.DecodedSeries{Labels: [][2]string{{"__name__", "metric"}}}
	for _, timestamp := range []int64{1005, 3995} {
		series.Samples = append(series.Samples, mimirpb.Sample{TimestampMs: timestamp, Value: stale})
	}
	for timestamp := int64(1000); timestamp < 4000; timestamp += 10 {
		series.Histograms = append(series.Histograms, histogramAt(timestamp, 1))
	}
	require.NoError(t, s.Ingest("tenant", record.DecodedRequest{Series: []record.DecodedSeries{series}}))
	for _, window := range [][2]int64{{3500, 3999}, {2500, 3999}, {1100, 1200}, {3995, 3995}} {
		found := false
		for _, s := range floatSamples(t, s, window[0], window[1]) {
			found = found || s.t == 3995
		}
		require.True(t, found, "window %v missed the stale marker", window)
	}
}

func TestSelectsSeriesThroughMetricNameGroups(t *testing.T) {
	s := Default()
	now := nowMs()
	series := func(name, job string, timestamp int64) record.DecodedSeries {
		var pairs [][2]string
		if name != "" {
			pairs = append(pairs, [2]string{"__name__", name})
		}
		pairs = append(pairs, [2]string{"job", job})
		return record.DecodedSeries{Labels: pairs, Samples: []mimirpb.Sample{{TimestampMs: timestamp, Value: 1}}}
	}
	require.NoError(t, s.Ingest("tenant", record.DecodedRequest{Series: []record.DecodedSeries{
		series("up", "a", now), series("up", "b", now), series("down", "a", now-1_000), series("", "a", now),
	}}))
	count := func(matchers ...LabelMatcher) int {
		selected, err := s.SelectLabels("tenant", math.MinInt64, math.MaxInt64, matchers)
		require.NoError(t, err)
		return len(selected)
	}
	require.Equal(t, 2, count(matcher(0, "__name__", "up")))
	require.Equal(t, 1, count(matcher(0, "__name__", "up"), matcher(0, "job", "b")))
	require.Equal(t, 3, count(matcher(2, "__name__", "up|down")))
	require.Equal(t, 2, count(matcher(1, "__name__", "up")))
	require.Equal(t, 1, count(matcher(0, "__name__", "")))
	require.Equal(t, 3, count(matcher(0, "job", "a")))
	require.Equal(t, 0, count(matcher(0, "__name__", "missing")))
	require.Equal(t, uint64(4), s.NumSeries("tenant"))
	require.NoError(t, s.pruneBefore(now-1))
	require.Equal(t, uint64(3), s.NumSeries("tenant"))
	require.Equal(t, 0, count(matcher(0, "__name__", "down")))
}

func TestReturnsQuerySeriesInLabelOrder(t *testing.T) {
	s := Default()
	request := record.DecodedRequest{}
	for index := range 200 {
		request.Series = append(request.Series, record.DecodedSeries{
			Labels: [][2]string{
				{"__name__", fmt.Sprintf("metric_%d", index%7)},
				{"instance", fmt.Sprintf("%03d", (index*37)%200)},
				{"Zone", strconv.Itoa(index % 3)},
			},
			Samples: []mimirpb.Sample{{TimestampMs: 1, Value: 1}},
		})
	}
	require.NoError(t, s.Ingest("tenant", request))
	all := everySeries(t, s, "tenant")
	require.Len(t, all, 200)
	for index := 1; index < len(all); index++ {
		require.Negative(t, comparePairLists(all[index-1], all[index]))
	}
}

func mixedRecords(records int64) []IngestRecord {
	out := make([]IngestRecord, 0, records)
	for rec := range records {
		request := record.DecodedRequest{}
		for series := range int64(50) {
			timestamp := rec
			// Every seventh record repeats an older timestamp to exercise out-of-order data.
			if rec%7 == 6 {
				timestamp = rec - 5
			}
			s := record.DecodedSeries{
				Labels:  [][2]string{{"__name__", fmt.Sprintf("metric_%d", series%5)}, {"id", strconv.FormatInt(series, 10)}},
				Samples: []mimirpb.Sample{{TimestampMs: timestamp * 1000, Value: float64(rec * series)}},
			}
			if series%10 == 0 {
				s.Histograms = []mimirpb.Histogram{histogramAt(rec*1000+1, 3)}
			}
			request.Series = append(request.Series, s)
		}
		out = append(out, IngestRecord{Tenant: fmt.Sprintf("tenant-%d", rec%3), Request: request})
	}
	return out
}

func chunksOf(t testing.TB, s *Store, tenant string) []queryResult {
	t.Helper()
	views, err := s.SelectChunks(tenant, math.MinInt64, math.MaxInt64, nil)
	require.NoError(t, err)
	return resultsOf(views)
}

func TestSeriesHashedWhileDecodingIngestLikeTheOthers(t *testing.T) {
	computed, err := NewWithShards(20*60*1000, Retention{}, "", 8, 4)
	require.NoError(t, err)
	precomputed, err := NewWithShards(20*60*1000, Retention{}, "", 8, 4)
	require.NoError(t, err)
	records := mixedRecords(40)
	// A repeated label name is rejected either way.
	records[0].Request.Series[1].Labels = append(records[0].Request.Series[1].Labels, [2]string{"id", "again"})
	hashed := mixedRecords(40)
	hashed[0].Request.Series[1].Labels = append(hashed[0].Request.Series[1].Labels, [2]string{"id", "again"})
	for index := range hashed {
		hashed[index].SeriesHashes = SeriesHashes(&hashed[index].Request)
	}
	require.False(t, hashed[0].SeriesHashes[1].OK)
	require.NoError(t, computed.IngestBatch(records))
	require.NoError(t, precomputed.IngestBatch(hashed))
	for _, tenant := range []string{"tenant-0", "tenant-1", "tenant-2"} {
		require.Equal(t, chunksOf(t, computed, tenant), chunksOf(t, precomputed, tenant))
		require.Equal(t, computed.NumSeries(tenant), precomputed.NumSeries(tenant))
	}
}

func TestShardedStoreMatchesASingleShard(t *testing.T) {
	// 37 records of 50 series run in parallel, 3 on the calling goroutine.
	require.True(t, 37*50 >= parallelMinSeries && 3*50 < parallelMinSeries)
	golden := rustResults(t)
	for _, batch := range []int{37, 3} {
		single, err := NewWithShards(20*60*1000, Retention{}, "", 1, 1)
		require.NoError(t, err)
		sharded, err := NewWithShards(20*60*1000, Retention{}, "", 8, 4)
		require.NoError(t, err)
		records, again := mixedRecords(400), mixedRecords(400)
		for start := 0; start < len(records); start += batch {
			end := min(start+batch, len(records))
			require.NoError(t, single.IngestBatch(records[start:end]))
			require.NoError(t, sharded.IngestBatch(again[start:end]))
		}
		for _, tenant := range []string{"tenant-0", "tenant-1", "tenant-2"} {
			expected := chunksOf(t, single, tenant)
			require.Len(t, expected, 50)
			require.Equal(t, expected, chunksOf(t, sharded, tenant))
			if batch == 37 {
				require.Equal(t, golden["mixed/"+tenant], expected, "like the Rust store")
			}
			require.Equal(t, single.NumSeries(tenant), sharded.NumSeries(tenant))
			singleValues, err := single.LabelValues(tenant, "id", math.MinInt64, math.MaxInt64, nil)
			require.NoError(t, err)
			shardedValues, err := sharded.LabelValues(tenant, "id", math.MinInt64, math.MaxInt64, nil)
			require.NoError(t, err)
			require.Equal(t, singleValues, shardedValues)
		}
		numSeries := func(s *Store) map[string]uint64 {
			out := map[string]uint64{}
			for _, stats := range s.AllUserStats(false) {
				out[stats.Tenant] = stats.Stats.NumSeries
			}
			return out
		}
		require.Equal(t, numSeries(single), numSeries(sharded))
	}
}

func TestParallelBatchAppliesEachSeriesInRecordOrder(t *testing.T) {
	s, err := NewWithShards(20*60*1000, Retention{}, "", 8, 4)
	require.NoError(t, err)
	var batch []IngestRecord
	for timestamp := range int64(200) {
		batch = append(batch, IngestRecord{Tenant: "tenant", Request: floatRequest(sample{timestamp, float64(timestamp)})})
	}
	require.NoError(t, s.IngestBatch(batch))
	withSeries(t, s, func(series *Series) {
		require.Empty(t, series.ooo())
		for _, chunk := range series.chunks.toSlice() {
			require.LessOrEqual(t, chunk.MinTime, chunk.MaxTime)
		}
	})
	require.Equal(t, samplesRange(0, 200, func(t int64) sample { return sample{t, float64(t)} }), floatSamples(t, s, math.MinInt64, math.MaxInt64))
}

func TestPrunesWholeExpiredChunksAndSeries(t *testing.T) {
	s := Default()
	now := nowMs()
	require.NoError(t, s.Ingest("tenant", floatRequest(samplesRange(0, 300, func(index int64) sample { return sample{now - 3*chunkRangeMs + index*15_000, 1} })...)))
	require.NoError(t, s.Ingest("tenant", floatRequest(sample{now, 2})))
	withSeries(t, s, func(series *Series) { require.False(t, series.chunks.isEmpty()) })
	require.NoError(t, s.pruneBefore(now-1_000))
	withSeries(t, s, func(series *Series) {
		require.True(t, series.chunks.isEmpty())
		require.NotNil(t, series.floatHead)
	})
	require.Equal(t, []sample{{now, 2}}, floatSamples(t, s, math.MinInt64, math.MaxInt64))
	require.NoError(t, s.pruneBefore(now+1))
	require.Equal(t, uint64(0), s.NumSeries("tenant"))
}

func TestWithoutAnOutOfOrderWindowRejectsLikeTheHeadAppender(t *testing.T) {
	tenant := "ooo-disabled"
	s := storeWith(t, limits.DefaultLimits())
	require.NoError(t, s.Ingest(tenant, seriesRequest("a", sample{10 * hour, 1})))
	// Older than the series but within an hour of the head: out of order.
	require.NoError(t, s.Ingest(tenant, seriesRequest("a", sample{10*hour - 1, 2})))
	// More than an hour behind the head: out of bounds, even for a new series.
	require.NoError(t, s.Ingest(tenant, seriesRequest("a", sample{9*hour - 1, 3})))
	require.NoError(t, s.Ingest(tenant, seriesRequest("b", sample{9*hour - 1, 3})))
	// A new series within the hour is in order.
	require.NoError(t, s.Ingest(tenant, seriesRequest("c", sample{9*hour + 1, 4})))
	// Same timestamp: the same value is a no-op, another value is rejected.
	require.NoError(t, s.Ingest(tenant, seriesRequest("a", sample{10 * hour, 1}, sample{10 * hour, 5})))
	require.Equal(t, []sample{{10 * hour, 1}}, samplesOf(t, s, tenant, "a"))
	require.Empty(t, samplesOf(t, s, tenant, "b"))
	require.Equal(t, []sample{{9*hour + 1, 4}}, samplesOf(t, s, tenant, "c"))
	require.Equal(t, uint64(1), discarded(DiscardOutOfOrder, tenant))
	require.Equal(t, uint64(2), discarded(DiscardOutOfBounds, tenant))
	require.Equal(t, uint64(1), discarded(DiscardNewValueForTimestamp, tenant))
	require.Equal(t, 3.0, testutil.ToFloat64(metrics.IngestedSamples.WithLabelValues(tenant)))
}

func TestOutOfOrderWindowAcceptsRecentSamplesAndRejectsOlderOnes(t *testing.T) {
	tenant := "ooo-enabled"
	s := storeWith(t, outOfOrderLimits())
	require.NoError(t, s.Ingest(tenant, seriesRequest("a", sample{10 * hour, 1})))
	require.NoError(t, s.Ingest(tenant, seriesRequest("a", sample{8*hour + 1, 2}, sample{8*hour - 1, 3})))
	require.NoError(t, s.Ingest(tenant, seriesRequest("b", sample{8*hour + 2, 4})))
	require.Equal(t, []sample{{8*hour + 1, 2}, {10 * hour, 1}}, samplesOf(t, s, tenant, "a"))
	require.Equal(t, []sample{{8*hour + 2, 4}}, samplesOf(t, s, tenant, "b"))
	require.Equal(t, uint64(1), discarded(DiscardTooOld, tenant))
}

func TestHeadMaxTimeIsPerTenantAndTakenPerFlush(t *testing.T) {
	s := storeWith(t, limits.DefaultLimits())
	rec := func(tenant, name string, timestamp int64) IngestRecord {
		return IngestRecord{Tenant: tenant, Request: seriesRequest(name, sample{timestamp, 1})}
	}
	// An empty head takes its max time from the flush's first sample, like the head's
	// initAppender.
	require.NoError(t, s.IngestBatch([]IngestRecord{rec("per-record", "a", 10*hour), rec("per-record", "b", 8*hour), rec("per-record-other", "b", 8*hour)}))
	require.Empty(t, samplesOf(t, s, "per-record", "b"))
	require.Equal(t, []sample{{8 * hour, 1}}, samplesOf(t, s, "per-record-other", "b"))
	// A head with samples keeps the max time it had when the flush's appender was created.
	require.NoError(t, s.IngestBatch([]IngestRecord{rec("same-record", "a", 8*hour)}))
	require.NoError(t, s.IngestBatch([]IngestRecord{{Tenant: "same-record", Request: record.DecodedRequest{Series: []record.DecodedSeries{
		seriesOf("a", sample{10 * hour, 1}), seriesOf("b", sample{7*hour + 1, 1}),
	}}}}))
	require.Equal(t, []sample{{7*hour + 1, 1}}, samplesOf(t, s, "same-record", "b"))
}

func TestRejectsSamplesOutsideTheGracePeriods(t *testing.T) {
	tenant := "grace"
	l := limits.DefaultLimits()
	l.PastGracePeriodMs = hour
	s := storeWith(t, l)
	now := nowMs()
	require.NoError(t, s.Ingest(tenant, seriesRequest("a", sample{now - 2*hour, 1}, sample{now, 2}, sample{now + 11*60_000, 3})))
	require.Equal(t, []sample{{now, 2}}, samplesOf(t, s, tenant, "a"))
	require.Equal(t, uint64(1), discarded(DiscardTooFarInFuture, tenant))
	// Mimir's ingest pusher drops samples past the grace period without counting them.
	require.Equal(t, uint64(0), discarded(DiscardTooFarInPast, tenant))
}

func TestDropsNativeHistogramsWhenDisabled(t *testing.T) {
	l := limits.DefaultLimits()
	l.NativeHistogramsIngestionEnabled = false
	s := storeWith(t, l)
	request := seriesRequest("h")
	request.Series[0].Histograms = []mimirpb.Histogram{histogramAt(1_000, 2)}
	require.NoError(t, s.Ingest("no-histograms", request))
	require.Equal(t, uint64(1), s.NumSeries("no-histograms"))
	views, err := s.SelectChunks("no-histograms", math.MinInt64, math.MaxInt64, nil)
	require.NoError(t, err)
	require.Empty(t, views)
}

func metadataOf(name, help string) mimirpb.MetricMetadata {
	return mimirpb.MetricMetadata{Type: 1, MetricFamilyName: name, Help: help}
}

func TestMetadataLimitsAndRetentionFollowMimir(t *testing.T) {
	tenant := "metadata-limits"
	l := limits.DefaultLimits()
	l.MaxGlobalMetadataPerUser = 2
	l.MaxGlobalMetadataPerMetric = 2
	s := storeWith(t, l)
	ingest := func(entries ...mimirpb.MetricMetadata) {
		require.NoError(t, s.Ingest(tenant, record.DecodedRequest{Metadata: entries}))
	}
	ingest(metadataOf("a", "1"), metadataOf("a", "2"), metadataOf("a", "3"))
	ingest(metadataOf("b", "1"), metadataOf("c", "1"))
	// A known entry of a full metric is rejected too, as Mimir checks the set size first.
	ingest(metadataOf("a", "1"))
	var names []string
	for _, entry := range s.Metadata(tenant) {
		names = append(names, entry.MetricFamilyName+":"+entry.Help)
	}
	slices.Sort(names)
	require.Equal(t, []string{"a:1", "a:2", "b:1"}, names)
	discardedMetadata := func(reason string) float64 {
		return testutil.ToFloat64(metrics.DiscardedMetadata.WithLabelValues(reason, tenant))
	}
	require.Equal(t, 2.0, discardedMetadata("per_metric_metadata_limit"))
	require.Equal(t, 1.0, discardedMetadata("per_user_metadata_limit"))
	s.PurgeMetadata(60_000)
	require.Len(t, s.Metadata(tenant), 3)
	s.PurgeMetadata(-60_000)
	require.Empty(t, s.Metadata(tenant))
}

func exemplarRequest(name string, sampleAt *int64, timestamps ...int64) record.DecodedRequest {
	var samples []sample
	if sampleAt != nil {
		samples = append(samples, sample{*sampleAt, 1})
	}
	request := seriesRequest(name, samples...)
	for _, timestamp := range timestamps {
		request.Series[0].Exemplars = append(request.Series[0].Exemplars, mimirpb.Exemplar{
			Labels:      []mimirpb.LabelAdapter{{Name: "trace_id", Value: strconv.FormatInt(timestamp, 10)}},
			Value:       1,
			TimestampMs: timestamp,
		})
	}
	return request
}

type exemplarSeries struct {
	name       string
	timestamps []int64
}

func exemplarTimestamps(t testing.TB, s *Store, tenant string) []exemplarSeries {
	t.Helper()
	views, err := s.SelectExemplars(tenant, math.MinInt64, math.MaxInt64, nil)
	require.NoError(t, err)
	out := []exemplarSeries{}
	for _, view := range views {
		series := exemplarSeries{name: view.Labels[0][1]}
		for _, e := range view.Exemplars {
			series.timestamps = append(series.timestamps, e.TimestampMs)
		}
		out = append(out, series)
	}
	return out
}

func TestExemplarsAreBoundedPerTenantAndNeedASeries(t *testing.T) {
	tenant := "exemplar-limits"
	l := limits.DefaultLimits()
	l.MaxGlobalExemplarsPerUser = 6
	overrides := limits.NewOverrides(l)
	overrides.SetActivePartitions(2)
	s := Default().WithOverrides(overrides)
	at := func(timestamp int64) *int64 { return &timestamp }
	// No sample and no existing series: dropped.
	require.NoError(t, s.Ingest(tenant, exemplarRequest("a", nil, 1)))
	require.Empty(t, exemplarTimestamps(t, s, tenant))
	require.NoError(t, s.Ingest(tenant, exemplarRequest("a", at(10), 1, 2)))
	require.NoError(t, s.Ingest(tenant, exemplarRequest("b", at(10), 3)))
	// An existing series takes exemplars without samples; the oldest are evicted at 6 / 2 = 3.
	require.NoError(t, s.Ingest(tenant, exemplarRequest("a", nil, 4)))
	require.Equal(t, []exemplarSeries{{"a", []int64{2, 4}}, {"b", []int64{3}}}, exemplarTimestamps(t, s, tenant))
	// Disabling exemplars stops storing new ones.
	require.NoError(t, overrides.ApplyRuntimeConfig(map[string]any{"overrides": map[string]any{tenant: map[string]any{"max_global_exemplars_per_user": 0}}}))
	require.NoError(t, s.Ingest(tenant, exemplarRequest("b", at(20), 5)))
	require.Equal(t, exemplarSeries{"b", []int64{3}}, exemplarTimestamps(t, s, tenant)[1])
}

func TestTrackerMatchesSurviveReloadsThatKeepTheTrackers(t *testing.T) {
	tenant := "trackers"
	overrides := limits.NewOverrides(limits.DefaultLimits())
	overrides.SetActivePartitions(1)
	s := Default().WithOverrides(overrides)
	reload := func(tracker string, exemplars int) {
		require.NoError(t, overrides.ApplyRuntimeConfig(map[string]any{"overrides": map[string]any{
			tenant:  map[string]any{"active_series_custom_trackers": map[string]any{"t": tracker}},
			"other": map[string]any{"max_global_exemplars_per_user": exemplars},
		}}))
	}
	reload(`{__name__="a"}`, 1)
	for _, name := range []string{"a", "b"} {
		require.NoError(t, s.Ingest(tenant, seriesRequest(name, sample{nowMs(), 1})))
	}
	tracked := func() uint64 {
		for _, report := range s.ActiveSeriesReport() {
			if report.Tenant == tenant {
				return report.CustomTrackers[0].Counts[0]
			}
		}
		t.Fatal("no report")
		return 0
	}
	generations := func() []uint64 {
		var out []uint64
		for _, shard := range s.shards {
			shard.RLock()
			if t, ok := shard.tenants[tenant]; ok {
				t.series.forEach(func(entry *seriesEntry) { out = append(out, uint64(entry.series.trackerGeneration)) })
			}
			shard.RUnlock()
		}
		return out
	}
	require.Equal(t, uint64(1), tracked())
	matched := generations()
	// Another tenant's change reloads the runtime config, but not this tenant's trackers.
	reload(`{__name__="a"}`, 2)
	require.Equal(t, uint64(1), tracked())
	require.Equal(t, matched, generations(), "matches kept")
	reload(`{__name__=~"a|b"}`, 2)
	require.Equal(t, uint64(2), tracked())
	require.NotEqual(t, matched, generations())
}

func TestOwnedSeriesRecomputeRemovesNonOwnedActiveSeriesLikeMimir(t *testing.T) {
	tenant := "owned"
	l := limits.DefaultLimits()
	cost, err := trackers.CostAttributionTrackersFromValue(map[string]any{"by-name": map[string]any{"labels": []any{map[string]any{"input": "__name__"}}}})
	require.NoError(t, err)
	l.CostAttributionTrackers = cost
	l.MaxCostAttributionCardinality = 10
	s := storeWith(t, l)
	ingest := func(name string) {
		require.NoError(t, s.Ingest(tenant, seriesRequest(name, sample{nowMs(), 1})))
	}
	ingest("a")
	ingest("b")
	report := func() [2]uint64 {
		for _, report := range s.ActiveSeriesReport() {
			if report.Tenant == tenant {
				var attributed uint64
				for _, value := range report.CostAttribution[0].Values {
					attributed += value.Counts[0]
				}
				return [2]uint64{report.Active, attributed}
			}
		}
		t.Fatal("no report")
		return [2]uint64{}
	}
	owned := func(ranges TenantRanges) uint64 {
		s.SetOwnedRanges(map[string]TenantRanges{tenant: ranges})
		return s.HeadTick(false, true)[0].OwnedSeries
	}
	// Without tracking, owned series never change the active series.
	s.SetOwnedRanges(map[string]TenantRanges{tenant: {}})
	s.HeadTick(false, false)
	require.Equal(t, [2]uint64{2, 2}, report())
	// Owning no ranges clears the active series but keeps cost attribution counting them.
	require.Equal(t, uint64(0), owned(TenantRanges{}))
	require.Equal(t, [2]uint64{0, 2}, report())
	ingest("a")
	require.Equal(t, [2]uint64{1, 2}, report())
	// Unchanged ranges don't recompute, so the new sample stays active.
	require.Equal(t, uint64(0), owned(TenantRanges{}))
	require.Equal(t, [2]uint64{1, 2}, report())
	// Owning only a's hash deletes b, cost attribution included.
	ingest("b")
	require.Equal(t, [2]uint64{2, 2}, report())
	hash := min(ownedHashOf(s, tenant, "a"), ownedHashOf(s, tenant, "b"))
	require.Equal(t, uint64(1), owned(TenantRanges{InShard: true, Ranges: []uint32{hash, hash}}))
	require.Equal(t, uint64(1), report()[0])
}

func TestReportsActiveSeriesCustomTrackersAndCostAttribution(t *testing.T) {
	tenant := "active-report"
	l := limits.DefaultLimits()
	custom, err := trackers.NewCustomTrackers(map[string]string{"api": `{job="api"}`, "all": `{__name__=~".+"}`})
	require.NoError(t, err)
	l.ActiveSeriesCustomTrackers = custom
	cost, err := trackers.CostAttributionTrackersFromValue(map[string]any{
		"by-team":  map[string]any{"labels": []any{map[string]any{"input": "team"}}},
		"internal": map[string]any{"internal": true, "labels": []any{map[string]any{"input": "job", "output": "service"}}},
	})
	require.NoError(t, err)
	l.CostAttributionTrackers = cost
	l.MaxCostAttributionCardinality = 3
	s := storeWith(t, l)
	now := nowMs()
	ingest := func(job, team string) {
		request := seriesRequest("up", sample{now, 1})
		request.Series[0].Labels = append(request.Series[0].Labels, [2]string{"job", job}, [2]string{"team", team})
		require.NoError(t, s.Ingest(tenant, request))
	}
	for _, series := range [][2]string{{"api", "a"}, {"api", "b"}, {"web", "a"}} {
		ingest(series[0], series[1])
	}
	h := seriesRequest("h")
	h.Series[0].Histograms = []mimirpb.Histogram{histogramAt(now, 3)}
	require.NoError(t, s.Ingest(tenant, h))
	report := func() metrics.ActiveSeriesReport {
		for _, report := range s.ActiveSeriesReport() {
			if report.Tenant == tenant {
				return report
			}
		}
		t.Fatal("no report")
		return metrics.ActiveSeriesReport{}
	}
	first := report()
	require.Equal(t, uint64(4), first.Active)
	require.Equal(t, uint64(1), first.ActiveNativeHistograms)
	require.Equal(t, uint64(3), first.ActiveNativeHistogramBuckets)
	require.Equal(t, []metrics.TrackerCounts{{Name: "all", Counts: [3]uint64{4, 1, 3}}, {Name: "api", Counts: [3]uint64{2, 0, 0}}}, first.CustomTrackers)
	values := func(pairs ...any) []metrics.AttributedValue {
		var out []metrics.AttributedValue
		for index := 0; index < len(pairs); index += 2 {
			out = append(out, metrics.AttributedValue{Values: []string{pairs[index].(string)}, Counts: pairs[index+1].([3]uint64)})
		}
		return out
	}
	byTeam := first.CostAttribution[0]
	require.Equal(t, "by-team", byTeam.Tracker)
	require.False(t, byTeam.Internal || byTeam.Overflow)
	require.Equal(t, values("__missing__", [3]uint64{1, 1, 3}, "a", [3]uint64{2, 0, 0}, "b", [3]uint64{1, 0, 0}), byTeam.Values)
	internal := first.CostAttribution[1]
	require.True(t, internal.Internal)
	require.Equal(t, []string{"service"}, internal.OutputLabels)
	require.Equal(t, values("__missing__", [3]uint64{1, 1, 3}, "api", [3]uint64{2, 0, 0}, "web", [3]uint64{1, 0, 0}), internal.Values)
	// A fourth team exceeds the cardinality: one overflow entry with every series.
	ingest("api", "c")
	second := report()
	byTeam = second.CostAttribution[0]
	require.True(t, byTeam.Overflow)
	require.Equal(t, values(trackers.OverflowValue, [3]uint64{5, 1, 3}), byTeam.Values)
	require.False(t, second.CostAttribution[1].Overflow)
}

func TestPostingsSelectTheSameSeriesAsAFullScan(t *testing.T) {
	byName := newSeriesByName()
	for id := range uint64(400) {
		pairs := [][2]string{
			{"__name__", fmt.Sprintf("metric_%d", id%7)},
			{"job", fmt.Sprintf("job_%d", id%5)},
			{"pod", fmt.Sprintf("pod_%d", id)},
		}
		if id%3 == 0 {
			pairs = append(pairs, [2]string{"zone", "a"})
		}
		slices.SortFunc(pairs, comparePairs)
		stored := labels.FromSorted(pairs)
		byName.insert(stored.Hash(), stored, Series{lastIngestedMs: int64(id)})
		require.Equal(t, int(id)+1, byName.len)
	}
	cases := [][]LabelMatcher{
		{matcher(0, "job", "job_2")},
		{matcher(0, "__name__", "metric_3"), matcher(0, "job", "job_1")},
		{matcher(0, "__name__", "metric_3"), matcher(0, "pod", "pod_101")},
		{matcher(0, "pod", "pod_12"), matcher(0, "zone", "a")},
		{matcher(0, "zone", ""), matcher(0, "job", "job_4")},
		{matcher(2, "__name__", "metric_[12]"), matcher(0, "job", "job_0")},
		{matcher(0, "job", "missing")},
		{matcher(1, "job", "job_0")},
	}
	check := func() {
		for _, c := range cases {
			compiled, err := compileMatchers(c)
			require.NoError(t, err)
			var expected, actual []uint64
			byName.forEach(func(entry *seriesEntry) {
				if matches(entry.labels, compiled) {
					expected = append(expected, entry.hash)
				}
			})
			byName.matching(compiled, func(entry *seriesEntry) bool {
				actual = append(actual, entry.hash)
				return true
			})
			slices.Sort(expected)
			slices.Sort(actual)
			require.Equal(t, expected, actual, "%v", c)
		}
	}
	check()
	// Removing series leaves their ids in the postings, which only add candidates that no entry answers to.
	changed := byName.retain(func(entry *seriesEntry) bool { return entry.series.lastIngestedMs%2 == 0 })
	require.NotEmpty(t, changed)
	require.Equal(t, 200, byName.len)
	require.Equal(t, 200, byName.stale)
	check()
	// The only series with a label going takes the label with it, as the count of series with each label is exact.
	require.Equal(t, uint32(67), byName.labelSeries[labels.Intern("zone")])
	// A removed series that comes back is found once, though its old id is in the postings too.
	for id := range uint64(20) {
		pairs := [][2]string{{"__name__", fmt.Sprintf("metric_%d", id%7)}, {"job", fmt.Sprintf("job_%d", id%5)}, {"pod", fmt.Sprintf("pod_%d", id)}}
		if id%3 == 0 {
			pairs = append(pairs, [2]string{"zone", "a"})
		}
		slices.SortFunc(pairs, comparePairs)
		stored := labels.FromSorted(pairs)
		byName.insert(stored.Hash(), stored, Series{lastIngestedMs: int64(id)})
	}
	check()
	// Rebuilding the postings off the lock, with a series added meanwhile, gives the same answers.
	snapshot := byName.snapshotForPostings()
	built := buildPostings(snapshot)
	stored := labels.FromStrings("__name__", "metric_1", "job", "job_9", "pod", "pod_new")
	byName.insert(stored.Hash(), stored, Series{lastIngestedMs: 1})
	require.True(t, byName.installPostings(built, snapshot))
	require.Zero(t, byName.stale)
	cases = append(cases, []LabelMatcher{matcher(0, "pod", "pod_new")})
	check()
	// Removals meanwhile give the snapshot up.
	snapshot = byName.snapshotForPostings()
	built = buildPostings(snapshot)
	byName.retain(func(entry *seriesEntry) bool { return entry.series.lastIngestedMs != 1 })
	require.False(t, byName.installPostings(built, snapshot))
	check()
}

// A big name group is scanned by the dictionaries of its labels' values for the matchers that aren't equalities, which
// selects the same series as reading each series' labels, for the label a series lacks and for one with many values.
func TestDictionariesSelectTheSameSeriesAsAFullScan(t *testing.T) {
	byName := newSeriesByName()
	insert := func(id uint64) {
		pairs := [][2]string{
			{"__name__", fmt.Sprintf("metric_%d", id%2)},
			{"job", fmt.Sprintf("job_%d", id%7)},
			{"pod", fmt.Sprintf("pod_%d", id)},
		}
		if id%3 == 0 {
			pairs = append(pairs, [2]string{"zone", fmt.Sprintf("zone_%d", id%5)})
		}
		slices.SortFunc(pairs, comparePairs)
		stored := labels.FromSorted(pairs)
		byName.insert(stored.Hash(), stored, Series{})
	}
	for id := range uint64(6000) {
		insert(id)
	}
	cases := [][]LabelMatcher{
		{matcher(0, "__name__", "metric_1"), matcher(2, "job", "job_(1|3)")},
		{matcher(0, "__name__", "metric_1"), matcher(3, "job", "job_[0-4]")},
		{matcher(0, "__name__", "metric_0"), matcher(1, "job", "job_2")},
		{matcher(0, "__name__", "metric_0"), matcher(2, "zone", "zone_1|")},
		{matcher(0, "__name__", "metric_0"), matcher(3, "zone", "zone_.*")},
		{matcher(0, "__name__", "metric_0"), matcher(1, "zone", "")},
		{matcher(0, "__name__", "metric_0"), matcher(2, "zone", "zone_1"), matcher(3, "job", "job_[0-2]")},
		{matcher(0, "__name__", "metric_1"), matcher(2, "pod", "pod_1[0-9]+")},
		{matcher(0, "__name__", "metric_1"), matcher(3, "pod", "pod_1[0-9]+")},
	}
	check := func() {
		for _, c := range cases {
			compiled, err := compileMatchers(c)
			require.NoError(t, err)
			var expected, actual []uint64
			byName.forEach(func(entry *seriesEntry) {
				if matches(entry.labels, compiled) {
					expected = append(expected, entry.hash)
				}
			})
			byName.matching(compiled, func(entry *seriesEntry) bool {
				actual = append(actual, entry.hash)
				return true
			})
			slices.Sort(expected)
			slices.Sort(actual)
			require.Equal(t, expected, actual, "%v", c)
		}
	}
	check()
	tooMany := 0
	for _, dict := range byName.dicts {
		if dict.tooManyAt > 0 {
			tooMany++
		}
	}
	require.NotEmpty(t, byName.dicts)
	require.Equal(t, 1, tooMany, "the pods have too many values for a dictionary, the jobs and zones don't")
	// Series added meanwhile extend the dictionaries, and the ones removed give them up.
	for id := uint64(6000); id < 6500; id++ {
		insert(id)
	}
	check()
	byName.retain(func(entry *seriesEntry) bool { return entry.hash%3 != 0 })
	check()
}

// A selector that comes in again is answered from what it matched before, until the name's series change: what it
// returns is what a scan of the series does, through series added and removed, for each query shard.
func TestCachedSelectorsSelectTheSameSeriesAsAScan(t *testing.T) {
	byName := newSeriesByName()
	next := uint64(0)
	add := func(count int) {
		for range count {
			id := next
			next++
			pairs := [][2]string{{"__name__", "metric"}, {"job", fmt.Sprintf("job_%d", id%7)}, {"pod", fmt.Sprintf("pod_%d", id)}}
			if id%3 == 0 {
				pairs = append(pairs, [2]string{"zone", fmt.Sprintf("zone_%d", id%5)})
			}
			slices.SortFunc(pairs, comparePairs)
			stored := labels.FromSorted(pairs)
			byName.insert(stored.Hash(), stored, Series{shardHash: id})
		}
	}
	add(2000)
	cases := [][]LabelMatcher{
		{matcher(0, "__name__", "metric"), matcher(2, "job", "job_(1|3)")},
		{matcher(0, "__name__", "metric"), matcher(0, "job", "job_2"), matcher(1, "zone", "zone_1")},
		{matcher(0, "__name__", "metric"), matcher(3, "zone", "zone_.*")},
		{matcher(0, "__name__", "metric"), matcher(0, "pod", "pod_10")},
	}
	var cache selectorCache
	check := func() {
		for _, c := range cases {
			compiled, err := compileMatchers(c)
			require.NoError(t, err)
			name, rest, ok := cacheableName(compiled)
			require.True(t, ok)
			selector := cache.get(selectorKey(compiled), 1)
			// Twice: the first time scans and keeps what it found, the second reads it.
			for range 2 {
				var expected, actual []uint64
				byName.matching(compiled, func(entry *seriesEntry) bool {
					expected = append(expected, entry.hash)
					return true
				})
				byName.matchingCached(selector, 0, name, nil, compiled, rest, func(entry *seriesEntry) bool {
					actual = append(actual, entry.hash)
					return true
				})
				slices.Sort(expected)
				slices.Sort(actual)
				require.Equal(t, expected, actual, "%v", c)
			}
		}
	}
	check()
	// Series added, and then removed, change what the selectors match.
	add(500)
	check()
	byName.retain(func(entry *seriesEntry) bool { return entry.series.shardHash%4 != 0 })
	check()
	add(100)
	check()
}

// What the selectors keep is bounded: a selector that matches many series in a shard isn't kept, and the cache drops
// what it holds when it holds too much.
func TestSelectorCacheIsBounded(t *testing.T) {
	var cache selectorCache
	selector := cache.get("a", 2)
	require.Same(t, selector, cache.get("a", 2))
	selector.keep(0, &cachedShard{refs: make([]seriesRef, 10)})
	selector.keep(0, &cachedShard{refs: make([]seriesRef, 4)})
	require.Equal(t, int64(4), cache.refs.Load(), "a shard's refs replace the ones it had")
	inserted := maxCachedTotalRefs/maxCachedRefs + 2
	for index := range inserted {
		cache.get(fmt.Sprint(index), 2).keep(0, &cachedShard{refs: make([]seriesRef, maxCachedRefs)})
	}
	require.Less(t, cache.refs.Load(), int64(maxCachedTotalRefs), "dropped once past the limit")
	require.Less(t, len(cache.selectors), inserted)

	// A selector with more matches in a shard than the limit is scanned each time.
	byName := newSeriesByName()
	for id := range 2 * maxCachedRefs {
		stored := labels.FromStrings("__name__", "metric", "pod", fmt.Sprint(id))
		byName.insert(stored.Hash(), stored, Series{})
	}
	compiled, err := compileMatchers([]LabelMatcher{matcher(0, "__name__", "metric")})
	require.NoError(t, err)
	name, rest, _ := cacheableName(compiled)
	cached := cache.get(selectorKey(compiled), 1)
	byName.matchingCached(cached, 0, name, nil, compiled, rest, func(*seriesEntry) bool { return true })
	require.Nil(t, cached.shards[0].Load())
}
