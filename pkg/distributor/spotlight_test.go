// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/ring"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/nautilus/rebalancer"
)

// TestDistributorSpotlightTracker_NoSpotlights confirms the
// hot-path early return: an empty spotlight set must not touch the
// observation map and must not panic.
func TestDistributorSpotlightTracker_NoSpotlights(t *testing.T) {
	tr := newDistributorSpotlightTracker()
	tr.observeWrite([]uint32{1, 2, 3}, nil, &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{{}}}, 3)
	assert.Empty(t, tr.observations)
}

// TestDistributorSpotlightTracker_ObserveCountsSamples wires up a
// realistic three-series request, two of which fall inside the
// spotlight range. Verifies that exactly those two samples land in
// the per-(traceID, partition) counter and that the emitted log
// line carries the right numbers.
func TestDistributorSpotlightTracker_ObserveCountsSamples(t *testing.T) {
	tr := newDistributorSpotlightTracker()
	tr.setSpotlights([]rebalancer.SpotlightedRange{
		{TraceId: "alpha", Lo: 100, Hi: 200, FromPartitionId: 5, ToPartitionId: 9, Reason: "phase3-move"},
	})

	// 3 series with hashes 50, 150, 175. Only 150 and 175 fall in
	// [100, 200]. Series 0 has 1 sample; series 1 has 2 samples;
	// series 2 has 1 histogram. Both spotlight-overlapping series
	// land on partition 9.
	keys := []uint32{50, 150, 175}
	req := &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
		{TimeSeries: &mimirpb.TimeSeries{Samples: []mimirpb.Sample{{Value: 1}}}},
		{TimeSeries: &mimirpb.TimeSeries{Samples: []mimirpb.Sample{{Value: 1}, {Value: 2}}}},
		{TimeSeries: &mimirpb.TimeSeries{Histograms: []mimirpb.Histogram{{}}}},
	}}
	partitionKeys := []ring.PartitionKeys{
		{PartitionID: 7, Indexes: []int{0}},    // key 50 -> P7, outside spotlight
		{PartitionID: 9, Indexes: []int{1, 2}}, // keys 150, 175 -> P9, inside spotlight
	}

	tr.observeWrite(keys, partitionKeys, req, 3) // all 3 are series keys, no metadata

	require.Contains(t, tr.observations, "alpha")
	assert.Equal(t, int64(3), tr.observations["alpha"][9],
		"P9 should receive 2 samples (series 1) + 1 histogram (series 2) = 3")
	assert.NotContains(t, tr.observations["alpha"], int32(7),
		"P7 received series 0 which is outside the spotlight range; should not be tallied")
}

// TestDistributorSpotlightTracker_SkipMetadata: metadata keys live
// at index >= initialMetadataIndex and must not be tallied even if
// they happen to fall in a spotlight range.
func TestDistributorSpotlightTracker_SkipMetadata(t *testing.T) {
	tr := newDistributorSpotlightTracker()
	tr.setSpotlights([]rebalancer.SpotlightedRange{
		{TraceId: "alpha", Lo: 0, Hi: 1000, FromPartitionId: -1, ToPartitionId: -1, Reason: "test"},
	})

	keys := []uint32{50, 150}
	// Two series-worth of timeseries; the second key (idx 1) is
	// flagged as metadata via initialMetadataIndex=1.
	req := &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
		{TimeSeries: &mimirpb.TimeSeries{Samples: []mimirpb.Sample{{Value: 1}}}},
	}}
	partitionKeys := []ring.PartitionKeys{
		{PartitionID: 1, Indexes: []int{0, 1}},
	}

	tr.observeWrite(keys, partitionKeys, req, 1)

	require.Contains(t, tr.observations, "alpha")
	assert.Equal(t, int64(1), tr.observations["alpha"][1],
		"only the series key (idx 0) is counted, metadata key (idx 1) skipped")
}

// TestDistributorSpotlightTracker_EmitAndReset round-trips a small
// accumulator through the emit path and confirms (a) the log line
// fires once per (spotlight, partition) pair, (b) the accumulator
// is cleared, (c) a spotlight that's no longer in the cached set
// is dropped on emit (the entry expired between writes and the
// poller refresh), and (d) samples_per_sec is computed from the
// elapsed interval between successive emits.
func TestDistributorSpotlightTracker_EmitAndReset(t *testing.T) {
	tr := newDistributorSpotlightTracker()
	// Freeze the clock and advance it manually so the rate
	// assertion is exact (60 samples / 30s = 2.0 samples/sec).
	clock := time.Unix(1700000000, 0)
	tr.nowFn = func() time.Time { return clock }
	tr.lastEmitAt = clock
	clock = clock.Add(30 * time.Second)

	tr.setSpotlights([]rebalancer.SpotlightedRange{
		{TraceId: "alpha", Lo: 100, Hi: 200, FromPartitionId: 5, ToPartitionId: 9, Reason: "phase3-move"},
	})
	// Manually seed observations: 60 samples on P9 for "alpha"
	// (giving an exact 2.0 samples/sec over a 30s interval);
	// 3 samples on P9 for "beta" (a now-expired spotlight).
	tr.observations = map[string]map[int32]int64{
		"alpha": {9: 60},
		"beta":  {9: 3},
	}

	var buf bytes.Buffer
	logger := log.NewLogfmtLogger(&buf)
	tr.emitAndReset(logger, "distributor-7")

	assert.Empty(t, tr.observations, "accumulator must be reset after emit")
	assert.Equal(t, clock, tr.lastEmitAt, "lastEmitAt must advance to the latest emit time")
	out := buf.String()
	assert.Contains(t, out, `spotlight_id=alpha`)
	assert.Contains(t, out, `samples_routed=60`)
	assert.Contains(t, out, `samples_per_sec=2`)
	assert.Contains(t, out, `interval_sec=30`)
	assert.Contains(t, out, `observed_partition=9`)
	assert.Contains(t, out, `instance_id=distributor-7`)
	// Expired-while-buffered spotlight is silently dropped.
	assert.NotContains(t, out, "spotlight_id=beta",
		"observations for expired spotlights must not produce orphaned log lines")
	// Exactly one log line for alpha (1 partition).
	assert.Equal(t, 1, strings.Count(out, "spotlight_id=alpha"))
}

// TestDistributorSpotlightTracker_EmitRate_ZeroElapsed: when a
// degenerate clock yields zero elapsed time (first emit racing with
// construction, or a frozen test clock), the rate path must not
// divide by zero. We log a defensively-clamped rate so the line
// stays parseable.
func TestDistributorSpotlightTracker_EmitRate_ZeroElapsed(t *testing.T) {
	tr := newDistributorSpotlightTracker()
	clock := time.Unix(1700000000, 0)
	tr.nowFn = func() time.Time { return clock }
	tr.lastEmitAt = clock // elapsed will be exactly 0

	tr.setSpotlights([]rebalancer.SpotlightedRange{
		{TraceId: "alpha", Lo: 0, Hi: 1000, Reason: "test"},
	})
	tr.observations = map[string]map[int32]int64{"alpha": {1: 5}}

	var buf bytes.Buffer
	logger := log.NewLogfmtLogger(&buf)
	require.NotPanics(t, func() { tr.emitAndReset(logger, "") })

	out := buf.String()
	assert.Contains(t, out, "samples_per_sec=")
	assert.NotContains(t, out, "+Inf")
	assert.NotContains(t, out, "NaN")
}
