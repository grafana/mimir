// SPDX-License-Identifier: AGPL-3.0-only

package loadstats

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

func newTestHead(t testing.TB) *tsdb.Head {
	t.Helper()
	opts := tsdb.DefaultHeadOptions()
	opts.ChunkDirRoot = t.TempDir()
	opts.IsolationDisabled = true
	opts.SecondaryHashFunction = mimirpb.ShardByMetricNameLocalityLabelsFunc("user-1")
	head, err := tsdb.NewHead(nil, nil, nil, nil, opts, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = head.Close() })
	return head
}

func TestRangeSeries_RangesReturnsCopy(t *testing.T) {
	h := NewRangeSeries()

	assert.Empty(t, h.Ranges())

	orig := []assignment.HashRange{
		{Lo: 0, Hi: 999},
		{Lo: 1000, Hi: math.MaxUint32},
	}
	h.SetRanges(orig)

	got := h.Ranges()
	require.Len(t, got, 2)
	assert.Equal(t, uint32(0), got[0].Lo)
	assert.Equal(t, uint32(999), got[0].Hi)
	assert.Equal(t, uint32(1000), got[1].Lo)
	assert.Equal(t, uint32(math.MaxUint32), got[1].Hi)

	got[0] = assignment.HashRange{Lo: 42, Hi: 42}
	again := h.Ranges()
	assert.Equal(t, uint32(0), again[0].Lo, "mutating the returned slice leaked back")
}

func TestRangeSeries_SetRangesSortsAndZerosCounts(t *testing.T) {
	h := NewRangeSeries()
	h.SetRanges([]assignment.HashRange{
		{Lo: 2000, Hi: math.MaxUint32},
		{Lo: 0, Hi: 999},
		{Lo: 1000, Hi: 1999},
	})

	snap := h.Snapshot()
	require.Len(t, snap.Ranges, 3)
	assert.Equal(t, uint32(0), snap.Ranges[0].Lo)
	assert.Equal(t, uint32(1000), snap.Ranges[1].Lo)
	assert.Equal(t, uint32(2000), snap.Ranges[2].Lo)

	require.Len(t, snap.Counts, 3)
	for _, c := range snap.Counts {
		assert.Equal(t, int64(0), c)
	}
}

func TestRangeSeries_SetCountsFor(t *testing.T) {
	h := NewRangeSeries()
	ranges := []assignment.HashRange{
		{Lo: 0, Hi: 999},
		{Lo: 1000, Hi: math.MaxUint32},
	}
	h.SetRanges(ranges)

	applied := h.SetCountsFor(ranges, []int64{42, 1337})
	assert.True(t, applied)

	snap := h.Snapshot()
	require.Len(t, snap.Counts, 2)
	assert.Equal(t, int64(42), snap.Counts[0])
	assert.Equal(t, int64(1337), snap.Counts[1])
}

func TestRangeSeries_SetCountsForDiscardedOnMismatch(t *testing.T) {
	h := NewRangeSeries()
	oldRanges := []assignment.HashRange{
		{Lo: 0, Hi: 999},
		{Lo: 1000, Hi: math.MaxUint32},
	}
	h.SetRanges(oldRanges)

	h.SetRanges([]assignment.HashRange{
		{Lo: 0, Hi: 499},
		{Lo: 500, Hi: 1999},
		{Lo: 2000, Hi: math.MaxUint32},
	})

	applied := h.SetCountsFor(oldRanges, []int64{42, 1337})
	assert.False(t, applied, "counts should be discarded when ranges changed mid-walk")

	snap := h.Snapshot()
	require.Len(t, snap.Counts, 3)
	for _, c := range snap.Counts {
		assert.Equal(t, int64(0), c, "counts must remain zero after discarded update")
	}
}

func TestRangeSeries_SetCountsForLengthMismatch(t *testing.T) {
	h := NewRangeSeries()
	ranges := []assignment.HashRange{
		{Lo: 0, Hi: 999},
		{Lo: 1000, Hi: math.MaxUint32},
	}
	h.SetRanges(ranges)

	applied := h.SetCountsFor(ranges, []int64{42, 1337, 99})
	assert.False(t, applied)

	snap := h.Snapshot()
	require.Len(t, snap.Counts, 2)
}

func TestRangeSeries_SetRangesZerosCountsOnResize(t *testing.T) {
	h := NewRangeSeries()
	ranges := []assignment.HashRange{
		{Lo: 0, Hi: 999},
		{Lo: 1000, Hi: math.MaxUint32},
	}
	h.SetRanges(ranges)
	require.True(t, h.SetCountsFor(ranges, []int64{10, 20}))

	h.SetRanges([]assignment.HashRange{
		{Lo: 0, Hi: 999},
		{Lo: 1000, Hi: 1999},
		{Lo: 2000, Hi: math.MaxUint32},
	})

	snap := h.Snapshot()
	require.Len(t, snap.Counts, 3)
	for _, c := range snap.Counts {
		assert.Equal(t, int64(0), c)
	}
}

func TestRangeSeries_HasRanges(t *testing.T) {
	h := NewRangeSeries()
	assert.False(t, h.HasRanges())

	h.SetRanges([]assignment.HashRange{{Lo: 0, Hi: math.MaxUint32}})
	assert.True(t, h.HasRanges())

	h.SetRanges(nil)
	assert.False(t, h.HasRanges())
}

func TestRangeSeries_ConcurrentAccess(t *testing.T) {
	h := NewRangeSeries()
	ranges := []assignment.HashRange{
		{Lo: 0, Hi: math.MaxUint32 / 2},
		{Lo: math.MaxUint32/2 + 1, Hi: math.MaxUint32},
	}
	h.SetRanges(ranges)

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		for i := int64(0); i < 1000; i++ {
			h.SetCountsFor(ranges, []int64{i, i * 2})
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			snap := h.Snapshot()
			require.Len(t, snap.Ranges, 2)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			h.SetRanges(ranges)
		}
	}()

	wg.Wait()
}

func TestRangeSeries_LogSummary(t *testing.T) {
	h := NewRangeSeries()
	ranges := []assignment.HashRange{
		{Lo: 0, Hi: math.MaxUint32 / 2},
		{Lo: math.MaxUint32/2 + 1, Hi: math.MaxUint32},
	}
	h.SetRanges(ranges)
	require.True(t, h.SetCountsFor(ranges, []int64{100, 200}))

	h.LogSummary(log.NewNopLogger())
}

func TestCountSeriesByHashRange_EmptyRanges(t *testing.T) {
	head := newTestHead(t)

	counts := []int64{}
	n, err := CountSeriesByHashRange(context.Background(), head, nil, counts, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
}

func TestCountSeriesByHashRange_EmptyHead(t *testing.T) {
	head := newTestHead(t)

	ranges := []assignment.HashRange{{Lo: 0, Hi: math.MaxUint32}}
	counts := make([]int64, len(ranges))
	n, err := CountSeriesByHashRange(context.Background(), head, ranges, counts, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
	assert.Equal(t, int64(0), counts[0])
}

func TestCountSeriesByHashRange_AppendsAndAttributesSeries(t *testing.T) {
	const userID = "user-1"
	head := newTestHead(t)

	app := head.Appender(context.Background())
	seriesLabels := make([]labels.Labels, 0, 200)
	for i := 0; i < 50; i++ {
		for _, metric := range []string{"http_requests_total", "cpu_seconds_total", "disk_bytes", "memory_rss"} {
			lset := labels.FromStrings(
				model.MetricNameLabel, metric,
				"instance", fmt.Sprintf("host-%d", i),
				"job", "test",
			)
			_, err := app.Append(0, lset, int64(i), float64(i))
			require.NoError(t, err)
			seriesLabels = append(seriesLabels, lset)
		}
	}
	require.NoError(t, app.Commit())

	totalSeries := len(seriesLabels)

	expected := make(map[uint32]int, totalSeries)
	for _, lset := range seriesLabels {
		h := mimirpb.ShardByMetricNameLocalityLabels(userID, lset.Get(model.MetricNameLabel), lset)
		expected[h]++
	}
	require.NotEmpty(t, expected, "no hashes observed")

	t.Run("full coverage attributes every series", func(t *testing.T) {
		ranges := []assignment.HashRange{
			{Lo: 0, Hi: math.MaxUint32 / 2},
			{Lo: math.MaxUint32/2 + 1, Hi: math.MaxUint32},
		}
		counts := make([]int64, len(ranges))
		examples := make([]string, len(ranges))
		n, err := CountSeriesByHashRange(context.Background(), head, ranges, counts, examples)
		require.NoError(t, err)
		assert.Equal(t, int64(totalSeries), n)
		assert.Equal(t, int64(totalSeries), counts[0]+counts[1])

		// Any range that received series should have a non-empty
		// example whose labels.String() output starts with '{' (the
		// canonical PromQL-style representation we render in the
		// readcache admin page).
		for i := range ranges {
			if counts[i] == 0 {
				continue
			}
			require.NotEmpty(t, examples[i], "range %d had %d series but no example", i, counts[i])
			assert.True(t, strings.HasPrefix(examples[i], "{"), "example %q is not a labels.String() rendering", examples[i])
			assert.Contains(t, examples[i], `__name__=`)
		}

		var want0, want1 int64
		for h, c := range expected {
			if h <= math.MaxUint32/2 {
				want0 += int64(c)
			} else {
				want1 += int64(c)
			}
		}
		assert.Equal(t, want0, counts[0])
		assert.Equal(t, want1, counts[1])
	})

	t.Run("narrow range bounds counts", func(t *testing.T) {
		var seed uint32
		for h := range expected {
			seed = h
			break
		}
		lo := seed
		hi := seed + 1000
		if hi < lo {
			hi = math.MaxUint32
		}

		var want int64
		for h, c := range expected {
			if h >= lo && h <= hi {
				want += int64(c)
			}
		}

		ranges := []assignment.HashRange{{Lo: lo, Hi: hi}}
		counts := make([]int64, len(ranges))
		n, err := CountSeriesByHashRange(context.Background(), head, ranges, counts, nil)
		require.NoError(t, err)
		assert.Equal(t, int64(totalSeries), n)
		assert.Equal(t, want, counts[0])
	})
}

func TestCountSeriesByHashRange_ContextCancellation(t *testing.T) {
	head := newTestHead(t)

	ranges := []assignment.HashRange{{Lo: 0, Hi: math.MaxUint32}}
	counts := make([]int64, len(ranges))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	app := head.Appender(context.Background())
	_, err := app.Append(0, labels.FromStrings(model.MetricNameLabel, "foo"), 0, 0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	_, err = CountSeriesByHashRange(ctx, head, ranges, counts, nil)
	require.ErrorIs(t, err, context.Canceled)
}

func TestLoadSeriesExamples_ToleratesDeletedSeries(t *testing.T) {
	head := newTestHead(t)
	examples := []string{""}

	err := loadSeriesExamples(
		t.Context(),
		head,
		[]chunks.HeadSeriesRef{chunks.HeadSeriesRef(math.MaxUint64)},
		[]bool{true},
		examples,
	)
	require.NoError(t, err)
	assert.Empty(t, examples[0])
}

func BenchmarkCountSeriesByHashRange(b *testing.B) {
	const (
		userID     = "user-1"
		numSeries  = 100_000
		numRanges  = 128
		numMetrics = 1_000
	)

	head := newTestHead(b)
	app := head.Appender(context.Background())
	for i := 0; i < numSeries; i++ {
		ls := labels.FromStrings(
			model.MetricNameLabel, fmt.Sprintf("metric_%d", i%numMetrics),
			"instance", fmt.Sprintf("host-%d", i),
			"job", "benchmark",
		)
		_, err := app.Append(0, ls, int64(i), float64(i))
		require.NoError(b, err)
	}
	require.NoError(b, app.Commit())

	ranges := make([]assignment.HashRange, numRanges)
	rangeWidth := (uint64(math.MaxUint32) + 1) / numRanges
	for i := range ranges {
		lo := uint64(i) * rangeWidth
		hi := lo + rangeWidth - 1
		if i == len(ranges)-1 {
			hi = math.MaxUint32
		}
		ranges[i] = assignment.HashRange{Lo: uint32(lo), Hi: uint32(hi)}
	}

	b.Run("postings-and-rehash", func(b *testing.B) {
		counts := make([]int64, len(ranges))
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			clear(counts)
			_, err := countSeriesByHashRangeWithRehash(context.Background(), userID, head, ranges, counts)
			require.NoError(b, err)
		}
	})

	b.Run("secondary-hash", func(b *testing.B) {
		counts := make([]int64, len(ranges))
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			clear(counts)
			_, err := CountSeriesByHashRange(context.Background(), head, ranges, counts, nil)
			require.NoError(b, err)
		}
	})
}

// countSeriesByHashRangeWithRehash preserves the pre-optimization
// algorithm as a benchmark baseline.
func countSeriesByHashRangeWithRehash(ctx context.Context, userID string, head *tsdb.Head, ranges []assignment.HashRange, counts []int64) (int64, error) {
	idx, err := head.Index()
	if err != nil {
		return 0, err
	}
	defer idx.Close()

	name, value := index.AllPostingsKey()
	postings, err := idx.Postings(ctx, name, value)
	if err != nil {
		return 0, err
	}

	builder := labels.NewScratchBuilder(16)
	var walked int64
	for postings.Next() {
		ref := postings.At()
		builder.Reset()
		if err := idx.Series(ref, &builder, nil); err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			return walked, err
		}
		walked++

		lset := builder.Labels()
		hash := mimirpb.ShardByMetricNameLocalityLabels(userID, lset.Get(model.MetricNameLabel), lset)
		ri := sort.Search(len(ranges), func(i int) bool {
			return ranges[i].Lo > hash
		}) - 1
		if ri >= 0 && ranges[ri].Contains(hash) {
			counts[ri]++
		}
	}
	return walked, postings.Err()
}
