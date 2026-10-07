// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
	"github.com/grafana/mimir/pkg/storage/seriesstore/limits"
	"github.com/grafana/mimir/pkg/storage/seriesstore/record"
)

func TestSeriesLabelsEncodeLikeProtobuf(t *testing.T) {
	for _, pairs := range [][][2]string{
		{{"__name__", "up"}, {"job", "api"}},
		{{"__name__", "up"}, {"empty", ""}, {"long", strings.Repeat("x", 300)}},
		{},
	} {
		stored := labels.FromSorted(pairs)
		expected := client.QueryStreamSeries{}
		stored.Range(func(name, value string) {
			expected.Labels = append(expected.Labels, mimirpb.LabelAdapter{Name: name, Value: value})
		})
		want, err := expected.Marshal()
		require.NoError(t, err)
		require.Equal(t, want, encodeSeriesLabels(stored, nil), "%v", pairs)
	}
}

func TestMatchersFindSortedLabelsAndTreatMissingLabelsAsEmpty(t *testing.T) {
	stored := labels.FromSorted([][2]string{{"__name__", "metric"}, {"first", "one"}, {"last", "two"}})
	for _, c := range []struct {
		kind        int32
		name, value string
		expected    bool
	}{
		{0, "last", "two", true},
		{0, "last", "one", false},
		{0, "absent", "", true},
		{1, "absent", "", false},
		{1, "absent", "two", true},
		{2, "absent", ".*", true},
		{3, "absent", ".+", true},
	} {
		compiled, err := compileMatchers([]LabelMatcher{matcher(c.kind, c.name, c.value)})
		require.NoError(t, err)
		require.Equal(t, c.expected, matches(stored, compiled), "%+v", c)
	}
}

func TestConflictingSamplesKeepTheFirstValueAndContinue(t *testing.T) {
	s := Default()
	require.NoError(t, s.Ingest("tenant", floatRequest(sample{1, 1})))
	require.NoError(t, s.Ingest("tenant", floatRequest(sample{1, 2})))
	require.NoError(t, s.Ingest("tenant", floatRequest(sample{2, 3})))
	require.Equal(t, []sample{{1, 1}, {2, 3}}, floatSamples(t, s, math.MinInt64, math.MaxInt64))
}

func TestInvalidSeriesDoesNotAbortOtherSeries(t *testing.T) {
	s := Default()
	series := func(pairs ...[2]string) record.DecodedSeries {
		return record.DecodedSeries{Labels: pairs, Samples: []mimirpb.Sample{{TimestampMs: 1, Value: 1}}}
	}
	require.NoError(t, s.Ingest("tenant", record.DecodedRequest{Series: []record.DecodedSeries{
		series([2]string{"name", "a"}, [2]string{"name", "b"}),
		series([2]string{"__name__", "valid"}),
	}}))
	require.Equal(t, uint64(1), s.NumSeries("tenant"))
}

func TestMovesCompletedFloatChunksToDiskAtRangeBoundaries(t *testing.T) {
	directory := t.TempDir()
	s, err := New(20*60*1000, Retention{}, directory)
	require.NoError(t, err)
	defer s.Close()
	samples := samplesRange(0, 2_000, func(index int64) sample { return sample{chunkRangeMs - 3_600_000 + index*15_000, float64(index)} })
	for start := 0; start < len(samples); start += 100 {
		require.NoError(t, s.Ingest("tenant", floatRequest(samples[start:start+100]...)))
	}
	withSeries(t, s, func(series *Series) {
		require.GreaterOrEqual(t, series.chunks.count(), 8)
		require.LessOrEqual(t, series.floatHead.appender.Len(), samplesPerChunk*2)
		for _, chunk := range series.chunks.toSlice() {
			require.Equal(t, rangeEnd(chunk.MinTime), rangeEnd(chunk.MaxTime), "chunk %d..%d crosses a range boundary", chunk.MinTime, chunk.MaxTime)
		}
	})
	entries, err := os.ReadDir(directory)
	require.NoError(t, err)
	require.NotEmpty(t, entries)
	require.Equal(t, samples, floatSamples(t, s, math.MinInt64, math.MaxInt64))
	var recent []sample
	for _, s := range floatSamples(t, s, samples[1_990].t, math.MaxInt64) {
		if s.t >= samples[1_990].t {
			recent = append(recent, s)
		}
	}
	require.Equal(t, samples[1_990:], recent)
	require.LessOrEqual(t, len(query(t, s, samples[1_990].t, math.MaxInt64)), 2)
}

func TestReturnsOutOfOrderSamplesWithInOrderOnes(t *testing.T) {
	s := storeWith(t, outOfOrderLimits())
	require.NoError(t, s.Ingest("tenant", floatRequest(samplesRange(10, 20, func(t int64) sample { return sample{t * 1000, float64(t)} })...)))
	require.NoError(t, s.Ingest("tenant", floatRequest(sample{5_000, 5}, sample{3_000, 3}, sample{5_000, 50})))
	require.NoError(t, s.Ingest("tenant", floatRequest(sample{19_000, 190}, sample{21_000, 21})))
	expected := []sample{{3_000, 3}, {5_000, 5}}
	expected = append(expected, samplesRange(10, 20, func(t int64) sample { return sample{t * 1000, float64(t)} })...)
	expected = append(expected, sample{21_000, 21})
	require.Equal(t, expected, floatSamples(t, s, math.MinInt64, math.MaxInt64))
	require.NoError(t, s.Ingest("tenant", floatRequest(samplesRange(0, outOfOrderCapacity, func(t int64) sample { return sample{100 + t, 0} })...)))
	withSeries(t, s, func(series *Series) {
		require.Equal(t, 1, series.chunks.count())
		require.Len(t, series.ooo(), 2)
	})
	require.Len(t, floatSamples(t, s, math.MinInt64, math.MaxInt64), len(expected)+outOfOrderCapacity)
}

func TestChunkListsKeepEveryChunk(t *testing.T) {
	// Out-of-order chunks go back in time, and a new chunk file restarts references low.
	metas := []ChunkMeta{
		{Ref: 5<<32 | 1_000, MinTime: 1_790_000_000_000, MaxTime: 1_790_001_800_000, Len: 150, Encoding: 4},
		{Ref: 5<<32 | 90_000, MinTime: 1_790_001_800_001, MaxTime: 1_790_003_600_000, Len: 4_000, Encoding: 5},
		{Ref: 6<<32 | 8, MinTime: 1_789_990_000_000, MaxTime: 1_789_999_000_000, Len: 90, Encoding: 4, OutOfOrder: true},
		{Ref: math.MaxUint64, MinTime: math.MinInt64 + 1, MaxTime: math.MaxInt64, Len: math.MaxUint32, Encoding: 0x7f, OutOfOrder: true},
	}
	var list chunkList
	for _, meta := range metas {
		list.push(meta)
	}
	require.Equal(t, metas, list.toSlice())
	require.Equal(t, 4, list.count())
	list.retain(func(meta *ChunkMeta) bool { return !meta.OutOfOrder })
	require.Equal(t, metas[:2], list.toSlice())
	// Two encoded chunks of a stored series cost well under two ChunkMeta structs.
	require.Less(t, len(list), 2*int(unsafe.Sizeof(ChunkMeta{}))/2)
}

func TestMatcherRegexesAreCompiledOnce(t *testing.T) {
	pattern := "api-[0-9]+-compiled-once"
	before := regexCompiles.Load()
	for range 3 {
		compiled, err := compileMatchers([]LabelMatcher{matcher(MatchRegex, "job", pattern)})
		require.NoError(t, err)
		require.True(t, compiled[0].matchesValue("api-12-compiled-once"))
		require.False(t, compiled[0].matchesValue("xapi-12-compiled-once"), "anchored")
	}
	require.Equal(t, uint64(1), regexCompiles.Load()-before)
	_, err := compileMatchers([]LabelMatcher{matcher(MatchRegex, "job", "(")})
	require.Error(t, err)
}

func TestSeriesStaySmall(t *testing.T) {
	// The store keeps every series of the retention inline in its tables, most of them floats no
	// longer written to, so what a series holds inline is paid millions of times.
	require.LessOrEqual(t, unsafe.Sizeof(seriesEntry{}), uintptr(208))
}

func TestKeepsEverySampleTypeOfASeries(t *testing.T) {
	s := Default()
	request := seriesRequest("mixed", sample{1_000, 12.5})
	request.Series[0].CreatedTimestamp = 500
	float := histogramAt(1_200, 1)
	float.Count = &mimirpb.Histogram_CountFloat{CountFloat: 1}
	float.PositiveDeltas = nil
	float.PositiveCounts = []float64{1}
	request.Series[0].Histograms = []mimirpb.Histogram{histogramAt(1_100, 1), float}
	require.NoError(t, s.Ingest("tenant", request))
	var bounds [][2]int64
	for _, chunk := range query(t, s, 0, 2_000) {
		bounds = append(bounds, [2]int64{chunk.StartTimestampMs, chunk.EndTimestampMs})
	}
	require.Equal(t, [][2]int64{{500, 1_000}, {1_100, 1_100}, {1_200, 1_200}}, bounds)
}

func TestPicksTenantsToCompactEarlyLikeMimir(t *testing.T) {
	config := EarlyHeadCompaction{MinInMemorySeries: 100, MinReductionPercentage: 15}
	estimations := []estimation{{"small", 5, 50}, {"big", 30, 10}, {"bigger", 40, 10}}
	// 160 series: getting under 100 needs the two biggest reductions; small drops half.
	require.Equal(t, []string{"bigger", "big", "small"}, tenantsToCompactEarly(160, config, estimations))
	// Under 15% of reductions in total, compacting is not worth it.
	require.Empty(t, tenantsToCompactEarly(1_000, config, []estimation{{"a", 10, 5}}))
}

func TestHeadTicksLookUpOldestSamplesOnlyBeforeTheFirstCompaction(t *testing.T) {
	s := Default()
	start := 10 * hour
	request := seriesRequest("a", samplesRange(0, 241, func(minute int64) sample { return sample{start + minute*60_000, 1} })...)
	request.Series = append(request.Series, seriesOf("b", sample{start + 30*60_000, 1}))
	require.NoError(t, s.Ingest("tenant", request))
	type result struct {
		headMin                              int64
		memory, headChunks, created, removed uint64
	}
	report := func(compact bool) result {
		reports := s.HeadTick(compact, false)
		require.Len(t, reports, 1)
		r := reports[0]
		return result{r.HeadMinTime, r.MemorySeries, r.HeadChunks, r.SeriesCreated, r.SeriesRemoved}
	}
	// Before any compaction, the head's min time is its oldest sample.
	require.Equal(t, start, report(false).headMin)
	require.Equal(t, uint64(2), s.oldestScans.Load())
	compacted := report(true)
	require.Greater(t, compacted.headMin, start, "compaction moved the head's min time")
	scanned := s.oldestScans.Load()
	// From then on, ticks report the same without looking at every series' chunks.
	after := report(false)
	require.Equal(t, scanned, s.oldestScans.Load())
	require.Equal(t, compacted.headMin, after.headMin)
	// The compaction's tick still counted b, which only the next tick finds out of the head.
	require.Equal(t, uint64(1), after.memory)
	require.Equal(t, uint64(1), after.removed)
	require.Equal(t, result{after.headMin, 1, after.headChunks, 0, 0}, report(false))
}

func TestCompactsTheHeadEarlyToTheIdleTimeoutAndRejectsOlderSamples(t *testing.T) {
	tenant := "early"
	s, err := New(1, Retention{}, "")
	require.NoError(t, err)
	s.WithEarlyHeadCompaction(&EarlyHeadCompaction{MinInMemorySeries: 2, MinReductionPercentage: 15})
	now := nowMs()
	for _, series := range []struct {
		name      string
		timestamp int64
	}{{"a", now - 20*60_000}, {"b", now - 10*60_000}} {
		require.NoError(t, s.Ingest(tenant, seriesRequest(series.name, sample{series.timestamp, 1})))
	}
	time.Sleep(5 * time.Millisecond)
	// Every series is inactive; the check runs on the tick after a compaction.
	require.Equal(t, uint64(2), s.HeadTick(true, false)[0].MemorySeries)
	checked := s.HeadTick(false, false)[0]
	require.Equal(t, now-10*60_000+1, checked.HeadMinTime)
	require.Equal(t, uint64(0), s.HeadTick(false, false)[0].MemorySeries)
	// The truncation is Prometheus's min valid time for in-order samples.
	require.NoError(t, s.Ingest(tenant, seriesRequest("c", sample{now - 15*60_000, 1})))
	require.Equal(t, uint64(1), discarded(DiscardOutOfBounds, tenant))
}

func TestEvictsNonOwnedSeriesAfterTheMaxGracePeriodBelowTheThreshold(t *testing.T) {
	tenant := "evict-max"
	l := limits.DefaultLimits()
	l.EarlyHeadCompactionOwnedSeriesThreshold = 400_000_000
	overrides := limits.NewOverrides(l)
	overrides.SetActivePartitions(12)
	s := Default().WithOverrides(overrides).WithNonOwnedEviction(&NonOwnedEviction{MinGraceMs: 30_000, MaxGraceMs: 1})
	for _, name := range []string{"a", "b", "c"} {
		require.NoError(t, s.Ingest(tenant, seriesRequest(name, sample{nowMs(), 1})))
	}
	// Owned ranges that include no series.
	s.SetOwnedRanges(map[string]TenantRanges{tenant: {InShard: true, Ranges: []uint32{0, 0}}})
	first := s.HeadTick(true, true)[0]
	require.Equal(t, [2]uint64{3, 0}, [2]uint64{first.MemorySeries, first.OwnedSeries})
	time.Sleep(1_100 * time.Millisecond)
	evicted := s.HeadTick(true, true)[0]
	require.Equal(t, uint64(3), evicted.NonOwnedEvicted)
	require.Equal(t, uint64(0), evicted.MemorySeries)
}

func ownedHashOf(s *Store, tenantID, name string) uint32 {
	for _, shard := range s.shards {
		shard.RLock()
		if t, ok := shard.tenants[tenantID]; ok {
			var hash uint32
			found := false
			t.series.forEach(func(entry *seriesEntry) {
				if entry.labels.Get("__name__") == name {
					hash, found = entry.series.ownedHash, true
				}
			})
			if found {
				shard.RUnlock()
				return hash
			}
		}
		shard.RUnlock()
	}
	panic("no such series")
}

func TestEvictsNonOwnedSeriesFromTheHeadLikeMimirEarlyCompaction(t *testing.T) {
	tenant := "evict"
	l := limits.DefaultLimits()
	l.EarlyHeadCompactionOwnedSeriesThreshold = 1
	s := storeWith(t, l).WithNonOwnedEviction(&NonOwnedEviction{})
	ingest := func(name string) {
		require.NoError(t, s.Ingest(tenant, seriesRequest(name, sample{nowMs(), 1})))
	}
	ingest("a")
	ingest("b")
	owned := ownedHashOf(s, tenant, "a")
	s.SetOwnedRanges(map[string]TenantRanges{tenant: {InShard: true, Ranges: []uint32{owned, owned}}})
	tick := func(compact bool) HeadReport { return s.HeadTick(compact, true)[0] }
	// The recompute finds b non-owned; nothing is evicted before a compaction.
	first := tick(false)
	require.Equal(t, [2]uint64{2, 1}, [2]uint64{first.MemorySeries, first.OwnedSeries})
	compacted := tick(true)
	require.Equal(t, uint64(1), compacted.NonOwnedEvicted)
	require.Equal(t, uint64(1), compacted.MemorySeries)
	require.Equal(t, uint64(1), compacted.SeriesRemoved)
	require.Equal(t, uint64(1), s.UserStats(tenant, false).NumSeries)
	// Its data stays in a block for label lookups, and a new sample brings it back.
	names, err := s.LabelValues(tenant, "__name__", math.MinInt64, math.MaxInt64, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, names)
	ingest("b")
	back := tick(false)
	require.Equal(t, [2]uint64{2, 1}, [2]uint64{back.MemorySeries, back.SeriesCreated})
}

func TestRestoredSeriesKeepTheirQueryShard(t *testing.T) {
	directory := t.TempDir()
	s, err := New(20*60*1000, Retention{}, directory)
	require.NoError(t, err)
	request := seriesRequest("sharded", sample{1_000, 1})
	request.Series[0].Labels = append(request.Series[0].Labels, [2]string{"n", "1"})
	require.NoError(t, s.Ingest("tenant", request))
	require.NoError(t, s.WriteSnapshot([]SnapshotOffset{{Offset: 1, HasOffset: true, TimestampMs: 1}}))
	require.NoError(t, s.Close())
	restored, err := Restore(20*60*1000, Retention{}, directory, 2)
	require.NoError(t, err)
	require.NotNil(t, restored)
	defer restored.Store.Close()
	shard := func(value string) int {
		views, err := restored.Store.SelectChunks("tenant", math.MinInt64, math.MaxInt64, []LabelMatcher{matcher(MatchEqual, "__query_shard__", value)})
		require.NoError(t, err)
		return len(views)
	}
	// labels.StableHash of the series, from Go, is 2 modulo 5: a missing hash would be 0.
	require.Equal(t, uint64(2), uint64(609_427_224_681_409_917)%5)
	require.Equal(t, 1, shard("3_of_5"))
	require.Equal(t, 0, shard("1_of_5"))
}

func TestShardedHeadQueriesOnlyReadWhatTheyReturn(t *testing.T) {
	s := Default()
	request := record.DecodedRequest{}
	for n := range 64 {
		series := seriesOf("sharded", sample{1_000, 1})
		series.Labels = append(series.Labels, [2]string{"n", strconv.Itoa(n)})
		request.Series = append(request.Series, series)
	}
	require.NoError(t, s.Ingest("tenant", request))
	counters := func() [3]uint64 {
		return [3]uint64{labelValueReads.Load(), chunkBoundsDecodes.Load(), chunkListDecodes.Load()}
	}
	returned := 0
	for index := 1; index <= 16; index++ {
		before := counters()
		views, err := s.SelectChunks("tenant", math.MinInt64, math.MaxInt64, []LabelMatcher{
			matcher(MatchEqual, "__name__", "sharded"),
			matcher(MatchEqual, "__query_shard__", fmt.Sprintf("%d_of_16", index)),
		})
		require.NoError(t, err)
		after := counters()
		require.Equal(t, uint64(0), after[0]-before[0], "the name group already matched the name")
		require.Equal(t, uint64(len(views)), after[1]-before[1], "only returned series decode chunks")
		// Once each: the chunks decoded for their bounds are the ones they return.
		require.Equal(t, uint64(len(views)), after[2]-before[2])
		returned += len(views)
	}
	require.Equal(t, 64, returned)
	// Candidates from a posting list still check the name: a posting only has a value's hash.
	before := counters()
	views, err := s.SelectChunks("tenant", math.MinInt64, math.MaxInt64, []LabelMatcher{matcher(MatchEqual, "__name__", "other"), matcher(MatchEqual, "n", "1")})
	require.NoError(t, err)
	require.Empty(t, views)
	views, err = s.SelectChunks("tenant", math.MinInt64, math.MaxInt64, []LabelMatcher{matcher(MatchRegex, "__name__", "shard.*"), matcher(MatchEqual, "n", "1")})
	require.NoError(t, err)
	require.Len(t, views, 1)
	require.Greater(t, counters()[0], before[0])
}

func TestChunksEncodeLikeProtobuf(t *testing.T) {
	state := uint64(0x243f_6a88_85a3_08d3)
	random := func() uint64 {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		return state
	}
	type testCase struct {
		minTime, maxTime int64
		encoding         int32
		data             []byte
	}
	ones := make([]byte, 300)
	for index := range ones {
		ones[index] = 1
	}
	cases := []testCase{{0, 0, 0, nil}, {-1, math.MinInt64, -1, []byte{0}}, {math.MaxInt64, 1, 4, ones}}
	for range 1000 {
		pick := func(value, choice uint64) int64 {
			switch choice % 4 {
			case 0:
				return 0
			case 1:
				return int64(value) % 1000
			case 2:
				return -(int64(value) % 1_000_000)
			default:
				return int64(value)
			}
		}
		a, b, c := random(), random(), random()
		length := int(random() % 200)
		data := make([]byte, length)
		for index := range data {
			data[index] = byte(a >> (index % 8))
		}
		cases = append(cases, testCase{pick(a, random()), pick(b, random()), int32(pick(c, random())), data})
	}
	for _, c := range cases {
		expected := client.Chunk{StartTimestampMs: c.minTime, EndTimestampMs: c.maxTime, Encoding: c.encoding, Data: c.data}
		want, err := expected.Marshal()
		require.NoError(t, err)
		// gogo writes empty chunk data, which the Rust ingester and proto3 encoders leave out.
		if len(c.data) == 0 {
			want = want[:len(want)-2]
		}
		require.Equal(t, want, encodeChunk(c.minTime, c.maxTime, c.encoding, c.data, nil), "%d %d %d %d", c.minTime, c.maxTime, c.encoding, len(c.data))
	}
}

// A group that lost half its series to head compaction gives back their slots: an entry's slot
// costs more than most series' other memory.
func TestRetainGivesBackTheSlotsOfRemovedSeries(t *testing.T) {
	b := newSeriesByName()
	for n := range 1000 {
		stored := labels.FromStrings("__name__", "m", "pod", fmt.Sprint(n))
		b.insert(stored.Hash(), stored, Series{ref: uint64(n + 1)})
	}
	b.retain(func(entry *seriesEntry) bool { return entry.series.ref%2 == 0 })
	g := b.groups[0]
	require.Len(t, g.entries, 500)
	require.LessOrEqual(t, cap(g.entries), 625)
	// Lookups still find every kept series.
	for n := range 1000 {
		stored := labels.FromStrings("__name__", "m", "pod", fmt.Sprint(n))
		found := g.lookup(stored.Hash(), func(entry *seriesEntry) bool { return entry.labels == stored })
		require.Equal(t, (n+1)%2 == 0, found, n)
	}
}
