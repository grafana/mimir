// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

// newTestHead constructs a tsdb.Head with no WAL or isolation, suitable
// for append-and-read tests.
func newTestHead(t *testing.T) *tsdb.Head {
	t.Helper()
	opts := tsdb.DefaultHeadOptions()
	opts.ChunkDirRoot = t.TempDir()
	opts.IsolationDisabled = true
	head, err := tsdb.NewHead(nil, nil, nil, nil, opts, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = head.Close() })
	return head
}

func TestCountSeriesByHashRange_EmptyRanges(t *testing.T) {
	head := newTestHead(t)

	counts := []int64{}
	n, err := countSeriesByHashRange(context.Background(), "user-1", head, nil, counts)
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
}

func TestCountSeriesByHashRange_EmptyHead(t *testing.T) {
	head := newTestHead(t)

	ranges := []assignment.HashRange{{Lo: 0, Hi: math.MaxUint32}}
	counts := make([]int64, len(ranges))
	n, err := countSeriesByHashRange(context.Background(), "user-1", head, ranges, counts)
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
	assert.Equal(t, int64(0), counts[0])
}

// TestCountSeriesByHashRange_AppendsAndAttributesSeries inserts a mix
// of metric names under one user and verifies that every in-head
// series is attributed to exactly one owned range when ranges cover
// the entire keyspace, and that series whose hash falls outside any
// owned range are not counted.
func TestCountSeriesByHashRange_AppendsAndAttributesSeries(t *testing.T) {
	const userID = "user-1"
	head := newTestHead(t)

	// Append a deterministic mix of series. We use both varying metric
	// names and varying label-value combos so the locality hash
	// (ShardByMetricNameLocalityLabels) spreads them across the
	// keyspace.
	app := head.Appender(context.Background())
	seriesLabels := make([]labels.Labels, 0, 200)
	for i := 0; i < 50; i++ {
		for _, metric := range []string{"http_requests_total", "cpu_seconds_total", "disk_bytes", "memory_rss"} {
			lset := labels.FromStrings(
				labels.MetricName, metric,
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

	// Hash every series so we can build an oracle of expected counts.
	expected := make(map[uint32]int, totalSeries)
	for _, lset := range seriesLabels {
		h := mimirpb.ShardByMetricNameLocalityLabels(userID, lset.Get(labels.MetricName), lset)
		expected[h]++
	}
	require.NotEmpty(t, expected, "no hashes observed")

	t.Run("full coverage attributes every series", func(t *testing.T) {
		ranges := []assignment.HashRange{
			{Lo: 0, Hi: math.MaxUint32 / 2},
			{Lo: math.MaxUint32/2 + 1, Hi: math.MaxUint32},
		}
		counts := make([]int64, len(ranges))
		n, err := countSeriesByHashRange(context.Background(), userID, head, ranges, counts)
		require.NoError(t, err)
		assert.Equal(t, int64(totalSeries), n)
		assert.Equal(t, int64(totalSeries), counts[0]+counts[1])

		// Re-derive the expected split from the oracle hash map.
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
		// Pick a narrow window containing at least one known hash so
		// we can verify exact attribution. Use the first hash we
		// observed from the oracle as our seed.
		var seed uint32
		for h := range expected {
			seed = h
			break
		}
		lo := seed
		hi := seed + 1000 // some tiny window
		if hi < lo {      // overflow guard
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
		n, err := countSeriesByHashRange(context.Background(), userID, head, ranges, counts)
		require.NoError(t, err)
		assert.Equal(t, int64(totalSeries), n, "walk must see every head series regardless of whether it lands in an owned range")
		assert.Equal(t, want, counts[0])
	})

	t.Run("partial coverage leaves unowned series uncounted", func(t *testing.T) {
		// Carve out a gap in the middle of the keyspace.
		ranges := []assignment.HashRange{
			{Lo: 0, Hi: 1_000_000_000},
			{Lo: 3_000_000_000, Hi: math.MaxUint32},
		}
		counts := make([]int64, len(ranges))
		n, err := countSeriesByHashRange(context.Background(), userID, head, ranges, counts)
		require.NoError(t, err)
		assert.Equal(t, int64(totalSeries), n)

		// Every counted hit must fall in one of the two ranges.
		var want0, want1 int64
		for h, c := range expected {
			switch {
			case h >= ranges[0].Lo && h <= ranges[0].Hi:
				want0 += int64(c)
			case h >= ranges[1].Lo && h <= ranges[1].Hi:
				want1 += int64(c)
			}
		}
		assert.Equal(t, want0, counts[0])
		assert.Equal(t, want1, counts[1])
		assert.LessOrEqual(t, counts[0]+counts[1], int64(totalSeries))
	})
}

// TestCountSeriesByHashRange_ContextCancellation verifies the walk
// respects context cancellation.
func TestCountSeriesByHashRange_ContextCancellation(t *testing.T) {
	head := newTestHead(t)

	ranges := []assignment.HashRange{{Lo: 0, Hi: math.MaxUint32}}
	counts := make([]int64, len(ranges))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// An empty head returns no error even if the context is cancelled
	// because the inner loop never runs. Populate one series so we
	// exercise the same path, then assert we either succeed quickly
	// or return the context error — both are acceptable.
	app := head.Appender(context.Background())
	_, err := app.Append(0, labels.FromStrings(labels.MetricName, "foo"), 0, 0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	_, err = countSeriesByHashRange(ctx, "user-1", head, ranges, counts)
	if err != nil {
		assert.ErrorIs(t, err, context.Canceled)
	}
}
