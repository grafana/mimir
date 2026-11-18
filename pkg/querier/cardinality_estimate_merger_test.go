// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/axiomhq/hyperloglog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// Every group carries one count per bucket, or one count without a step.
func TestEstimateMerger_FailsOnMalformedGroups(t *testing.T) {
	m := newEstimateMerger(context.Background(), false)
	err := m.add("store-gateway", "up", nil, nil, nil)
	require.Error(t, err, "a group with no counts and no step")
	assert.Contains(t, err.Error(), `a store-gateway returned 0 buckets for "up" instead of 1`)
	assert.Equal(t, http.StatusInternalServerError, estimateHTTPStatus(err))
	assert.Empty(t, m.counts, "nothing is stored for the group")

	err = m.add("ingester", "up", []int64{1, 2}, nil, nil)
	require.Error(t, err, "two counts without a step")
	assert.Contains(t, err.Error(), `an ingester returned 2 buckets for "up" instead of 1`)
}

func TestEstimateMerger_RefusesSketchOfAnotherPrecision(t *testing.T) {
	marshal := func(precision uint8, hashes ...uint64) []byte {
		sk, err := hyperloglog.NewSketch(precision, false)
		require.NoError(t, err)
		for _, h := range hashes {
			sk.InsertHash(h)
		}
		b, err := sk.MarshalBinary()
		require.NoError(t, err)
		return b
	}
	other := marshal(cardinality.SketchPrecision+1, 1, 2)

	// The first sketch of a group is checked, not only a later one.
	m := newEstimateMerger(context.Background(), true)
	err := m.add("ingester", "up", nil, nil, other)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `an ingester sent a sketch for "up" with another precision`)
	assert.Equal(t, http.StatusUnprocessableEntity, estimateHTTPStatus(err))
	assert.NotErrorIs(t, err, errCountsNotSupported)
	assert.Empty(t, estimateRefusalLimit(err), "it is not a limit refusal")
	assert.Empty(t, m.sketches, "nothing is stored for the refused group")

	require.NoError(t, m.add("store-gateway", "up", nil, nil, marshal(cardinality.SketchPrecision, 1, 2)))
	err = m.add("store-gateway", "up", nil, nil, other)
	require.Error(t, err)
	assert.Equal(t, http.StatusUnprocessableEntity, estimateHTTPStatus(err))
}

func TestEstimateMerger(t *testing.T) {
	// An HLL can't tell small integers apart, which label-set hashes never are.
	spread := func(n uint64) uint64 {
		n += 0x9e3779b97f4a7c15
		n = (n ^ (n >> 30)) * 0xbf58476d1ce4e5b9
		n = (n ^ (n >> 27)) * 0x94d049bb133111eb
		return n ^ (n >> 31)
	}
	spreadAll := func(ns ...uint64) []uint64 {
		out := make([]uint64, len(ns))
		for i, n := range ns {
			out[i] = spread(n)
		}
		return out
	}
	sketchOf := func(hashes ...uint64) []byte {
		sk, err := hyperloglog.NewSketch(cardinality.SketchPrecision, false)
		require.NoError(t, err)
		for _, h := range hashes {
			sk.InsertHash(spread(h))
		}
		b, err := sk.MarshalBinary()
		require.NoError(t, err)
		return b
	}
	type add struct {
		source, value string
		counts        []int64
		hashes        []uint64
		sketch        []byte
	}
	for name, tc := range map[string]struct {
		dedup     bool
		buckets   int
		adds      []add
		wantCount map[string][]int64
		estimated []string
		wantErr   string
	}{
		"counts of sources that share no series are added up": {
			buckets: 2,
			adds: []add{
				{source: "store-gateway", value: "up", counts: []int64{1, 2}},
				{source: "store-gateway", value: "up", counts: []int64{3, 4}},
				{source: "store-gateway", value: "x", counts: []int64{5, 6}},
			},
			wantCount: map[string][]int64{"up": {4, 6}, "x": {5, 6}},
		},
		"sources that disagree on the number of buckets fail": {
			buckets: 2,
			adds: []add{
				{source: "store-gateway", value: "up", counts: []int64{1, 2}},
				{source: "store-gateway", value: "up", counts: []int64{3}},
			},
			wantErr: "a store-gateway returned 1 buckets for \"up\" instead of 2",
		},
		"one count per group fails against the asked buckets": {
			buckets: 3,
			adds:    []add{{source: "store-gateway", value: "up", counts: []int64{6}}},
			wantErr: "a store-gateway returned 1 buckets for \"up\" instead of 3",
		},
		"hashes are unioned, so a series in two sources counts once": {
			dedup: true,
			adds: []add{
				{source: "store-gateway", value: "up", hashes: []uint64{1, 2, 3}},
				{source: "ingester", value: "up", hashes: []uint64{3, 4}},
			},
			wantCount: map[string][]int64{"up": {4}},
		},
		"a sketch from one source takes in the hashes of another and marks the group estimated": {
			dedup: true,
			adds: []add{
				{source: "store-gateway", value: "up", sketch: sketchOf(1, 2, 3)},
				{source: "ingester", value: "up", hashes: spreadAll(3, 4)},
				{source: "ingester", value: "exact", hashes: spreadAll(9)},
			},
			wantCount: map[string][]int64{"up": {4}, "exact": {1}},
			estimated: []string{"up"},
		},
		"sketches of two sources are merged": {
			dedup: true,
			adds: []add{
				{source: "store-gateway", value: "up", sketch: sketchOf(1, 2, 3)},
				{source: "store-gateway", value: "up", sketch: sketchOf(3, 4)},
			},
			wantCount: map[string][]int64{"up": {4}},
			estimated: []string{"up"},
		},
		"a bad sketch is refused with the source named": {
			dedup:   true,
			adds:    []add{{source: "store-gateway", value: "up", sketch: []byte("not a sketch")}},
			wantErr: "a store-gateway returned a bad sketch for \"up\"",
		},
	} {
		t.Run(name, func(t *testing.T) {
			m := newEstimateMerger(context.Background(), tc.dedup)
			m.buckets = tc.buckets
			var err error
			for _, a := range tc.adds {
				if err = m.add(a.source, a.value, a.counts, a.hashes, a.sketch); err != nil {
					break
				}
			}
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			var res CardinalityEstimateResult
			require.NoError(t, m.finish(&res))
			assert.Equal(t, tc.wantCount, res.Counts)
			var got []string
			for v := range res.Estimated {
				got = append(got, v)
			}
			assert.ElementsMatch(t, tc.estimated, got)
		})
	}
}

// A deduplicating group is its own hash set, so many small groups must reach the memory limit at what the sets hold.
func TestEstimateMerger_ChargesHashGroupsAsSets(t *testing.T) {
	// 1,000 one-hash groups take 1,000 * (GroupBytes + 5 + HashBytes) = 221,000 bytes; 64 per group would be 93,000.
	tracker := limiter.NewMemoryConsumptionTracker(context.Background(), 150_000, prometheus.NewCounter(prometheus.CounterOpts{Name: "rejected"}), "test")
	m := newEstimateMerger(limiter.AddMemoryTrackerToContext(context.Background(), tracker), true)
	var err error
	for i := 0; i < 1000 && err == nil; i++ {
		err = m.add("store-gateway", fmt.Sprintf("m%04d", i), nil, []uint64{uint64(i)}, nil)
	}
	require.Error(t, err)
	assert.LessOrEqual(t, tracker.CurrentEstimatedMemoryConsumptionBytes(), uint64(150_000))
}
