// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"bytes"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
	"github.com/grafana/mimir/pkg/nautilus/rebalancer"
)

// TestEmitReadcacheSpotlightForPartition_NoOverlap: when no
// hash-range row overlaps any spotlight, no log lines are emitted.
// This is the "this partition holds nothing the rebalancer is
// watching" case and is the steady state for the vast majority of
// partitions.
func TestEmitReadcacheSpotlightForPartition_NoOverlap(t *testing.T) {
	var buf bytes.Buffer
	logger := log.NewLogfmtLogger(&buf)

	spots := []rebalancer.SpotlightedRange{
		{TraceId: "alpha", Lo: 100, Hi: 200, Reason: "phase3-move"},
	}
	rows := []hashRangeCount{
		{Range: assignment.HashRange{Lo: 0, Hi: 50}, Count: 10, SampleRate: 1.5, Example: `{__name__="foo"}`},
		{Range: assignment.HashRange{Lo: 300, Hi: 400}, Count: 20},
	}
	emitReadcacheSpotlightForPartition(logger, "rc-3", 7, spots, rows, "current")
	assert.Empty(t, buf.String(), "no row overlaps any spotlight → no emission")
}

// TestEmitReadcacheSpotlightForPartition_OverlapEmits exercises
// the happy path: one overlapping row emits exactly one log line,
// carrying the spotlight id, partition id, the observed range's
// EWMA, head series count, and example label.
func TestEmitReadcacheSpotlightForPartition_OverlapEmits(t *testing.T) {
	var buf bytes.Buffer
	logger := log.NewLogfmtLogger(&buf)

	spots := []rebalancer.SpotlightedRange{
		{TraceId: "alpha", Lo: 100, Hi: 200, Reason: "phase3-move", FromPartitionId: 5, ToPartitionId: 9},
	}
	// row at [150, 175] is fully inside spotlight [100, 200].
	rows := []hashRangeCount{
		{Range: assignment.HashRange{Lo: 150, Hi: 175}, Count: 1234, SampleRate: 87.5, Example: `{__name__="up", instance="i-1"}`},
	}
	emitReadcacheSpotlightForPartition(logger, "rc-3", 9, spots, rows, "current")

	out := buf.String()
	assert.Contains(t, out, "spotlight_id=alpha")
	assert.Contains(t, out, "observed_partition=9")
	assert.Contains(t, out, "observed_range_lo=150")
	assert.Contains(t, out, "observed_range_hi=175")
	assert.Contains(t, out, "lifecycle=current")
	assert.Contains(t, out, "sample_rate=87.5")
	assert.Contains(t, out, "head_series=1234")
	assert.Contains(t, out, `__name__=\"up\"`)
	assert.Contains(t, out, "instance_id=rc-3")
}

// TestEmitReadcacheSpotlightForPartition_PartialOverlap: a single
// row that partially overlaps still emits — operators want to see
// the bordering range's load because it's evidence of what's
// adjacent to the spotlighted region.
func TestEmitReadcacheSpotlightForPartition_PartialOverlap(t *testing.T) {
	var buf bytes.Buffer
	logger := log.NewLogfmtLogger(&buf)

	spots := []rebalancer.SpotlightedRange{
		{TraceId: "alpha", Lo: 100, Hi: 200},
	}
	rows := []hashRangeCount{
		{Range: assignment.HashRange{Lo: 50, Hi: 150}, Count: 1, SampleRate: 1},
	}
	emitReadcacheSpotlightForPartition(logger, "", 0, spots, rows, "historical")
	assert.Contains(t, buf.String(), "spotlight_id=alpha")
}

// TestEmitReadcacheSpotlightForPartition_MultipleSpotlightsOneRow:
// one range row that overlaps two different spotlights produces
// one log line per (spotlight, row) pair. That's intentional — each
// spotlight is independent and downstream consumers grep by
// spotlight_id.
func TestEmitReadcacheSpotlightForPartition_MultipleSpotlightsOneRow(t *testing.T) {
	var buf bytes.Buffer
	logger := log.NewLogfmtLogger(&buf)

	spots := []rebalancer.SpotlightedRange{
		{TraceId: "alpha", Lo: 100, Hi: 200},
		{TraceId: "beta", Lo: 150, Hi: 250},
	}
	rows := []hashRangeCount{
		{Range: assignment.HashRange{Lo: 170, Hi: 180}, Count: 5, SampleRate: 0.5},
	}
	emitReadcacheSpotlightForPartition(logger, "", 0, spots, rows, "current")

	out := buf.String()
	assert.Equal(t, 1, strings.Count(out, "spotlight_id=alpha"))
	assert.Equal(t, 1, strings.Count(out, "spotlight_id=beta"))
}

// TestReadcacheSpotlightTracker_SetSnapshot guards the concurrent
// contract: snapshot returns whatever the most recent set call
// installed and no defensive copy is required because callers
// iterate read-only.
func TestReadcacheSpotlightTracker_SetSnapshot(t *testing.T) {
	tr := newReadcacheSpotlightTracker()
	require.Empty(t, tr.snapshot())

	want := []rebalancer.SpotlightedRange{
		{TraceId: "x", Lo: 1, Hi: 2},
	}
	tr.setSpotlights(want)
	got := tr.snapshot()
	require.Equal(t, want, got)

	tr.setSpotlights(nil)
	require.Empty(t, tr.snapshot())
}

// TestHashRangesOverlapBounds_Closed: both ranges are closed
// intervals; ranges touching at a single hash value overlap. Mirrors
// the rebalancer's hashRangesOverlap test for safety.
func TestHashRangesOverlapBounds_Closed(t *testing.T) {
	t.Run("disjoint", func(t *testing.T) {
		assert.False(t, hashRangesOverlapBounds(assignment.HashRange{Lo: 0, Hi: 10}, 11, 20))
	})
	t.Run("touch", func(t *testing.T) {
		assert.True(t, hashRangesOverlapBounds(assignment.HashRange{Lo: 0, Hi: 10}, 10, 20))
	})
	t.Run("contained", func(t *testing.T) {
		assert.True(t, hashRangesOverlapBounds(assignment.HashRange{Lo: 5, Hi: 6}, 0, 10))
	})
}
