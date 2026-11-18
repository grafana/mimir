// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rankedSeries has six metric names, mN with 7-N series, so mN is the Nth largest and the table totals 21 series.
func rankedSeries() []labels.Labels {
	var out []labels.Labels
	for n := 1; n <= 6; n++ {
		for i := range 7 - n {
			out = append(out, labels.FromStrings("__name__", fmt.Sprintf("m%d", n), "job", "api", "pod", fmt.Sprintf("p%d", i)))
		}
	}
	return out
}

func TestNamesByRank(t *testing.T) {
	values := []string{"a", "b", "c", "d"}
	assert.Equal(t, []string{"a", "c"}, namesByRank(values, []int{1, 3}))
	assert.Equal(t, []string{"b", "d"}, namesByRank(values, []int{2, 4, 5, 5000}), "ranks past the end are skipped")
	assert.Equal(t, []string{"d"}, namesByRank(values, []int{50, 5000}), "the smallest name when no rank fits")
	assert.Equal(t, []string{"a"}, namesByRank(values, []int{1, 1}))
	assert.Empty(t, namesByRank(nil, []int{1}))
}

// TestPickTargetsByRank: names are drawn at the ranks, from the newest block range of a bounds answer, with no exact tenant-wide table.
func TestPickTargetsByRank(t *testing.T) {
	f, srv := newFakeFrontend(t, rankedSeries()...)
	r, _ := newTestRunner(srv)
	cfg := testTargets()
	cfg.ranks = []int{1, 3, 6, 5000}
	got, err := pickTargets(context.Background(), r.client, cfg, testTargetWindows, log.NewNopLogger(), func() {})
	require.NoError(t, err)
	assert.Equal(t, []string{"m1", "m3", "m6"}, got.names, "rank 5,000 is past the end of the table")
	assert.Equal(t, []string{"api"}, got.matcherValues)
	assert.Equal(t, int64(21), got.tenantSeries, "the range's upper total")
	assert.Zero(t, f.estimates, "no exact estimate is sent")
	assert.Equal(t, 2, f.boundsCalls, "one for the names, one for the matcher label")
}

// TestPickTargetsBoundsReadsTheNewestRange: with several block ranges, the names come from the last one that has groups.
func TestPickTargetsBoundsReadsTheNewestRange(t *testing.T) {
	f, srv := newFakeFrontend(t, rankedSeries()...)
	f.rangeLen = time.Hour
	r, _ := newTestRunner(srv)
	// Three one-hour ranges; the newest has m2 in front.
	tw := targetWindows{blocks: window{start: testWindow.start - 2*time.Hour.Milliseconds(), end: testWindow.end}, recent: testWindow}
	f.boundsHook = func(_ int, res *boundsResponse) {
		if res.GroupBy != "__name__" {
			return
		}
		require.Len(t, res.Ranges, 3)
		for i := range res.Ranges[2].Groups {
			if res.Ranges[2].Groups[i].Value == "m2" {
				res.Ranges[2].Groups[i].Upper += 100
			}
		}
		g := res.Ranges[2].Groups
		g[0], g[1] = g[1], g[0]
	}
	cfg := testTargets()
	cfg.ranks = []int{1, 2}
	got, err := pickTargets(context.Background(), r.client, cfg, tw, log.NewNopLogger(), func() {})
	require.NoError(t, err)
	assert.Equal(t, []string{"m2", "m1"}, got.names)
}

// TestPickTargetsHead: the head source reads the heads' row set, and the bounds source falls back to it when no block range answers.
func TestPickTargetsHead(t *testing.T) {
	cfg := testTargets()
	cfg.ranks = []int{1, 2}

	f, srv := newFakeFrontend(t, rankedSeries()...)
	f.headFrom = testWindow.start
	r, _ := newTestRunner(srv)

	cfg.source = targetSourceHead
	got, err := pickTargets(context.Background(), r.client, cfg, testTargetWindows, log.NewNopLogger(), func() {})
	require.NoError(t, err)
	assert.Equal(t, []string{"m1", "m2"}, got.names)
	assert.Equal(t, int64(21), got.tenantSeries, "the heads' in-memory total")
	assert.Equal(t, 2, f.boundsCalls)

	// No block range ends before the heads begin, so bounds asks twice for each table.
	f.boundsCalls = 0
	cfg.source = targetSourceBounds
	got, err = pickTargets(context.Background(), r.client, cfg, testTargetWindows, log.NewNopLogger(), func() {})
	require.NoError(t, err)
	assert.Equal(t, []string{"m1", "m2"}, got.names)
	assert.Equal(t, 4, f.boundsCalls)

	// A window that doesn't reach the ingesters has no head row set.
	f.headFrom = 0
	cfg.source = targetSourceHead
	_, err = pickTargets(context.Background(), r.client, cfg, testTargetWindows, log.NewNopLogger(), func() {})
	require.ErrorContains(t, err, "doesn't reach the ingesters")
}

// TestPickTargetsLabelValues: the label values endpoint is capped, so it is asked for no more than the limit, and ranks past it are skipped.
func TestPickTargetsLabelValues(t *testing.T) {
	f, srv := newFakeFrontend(t, rankedSeries()...)
	f.labelValuesCardinalityLimit = 3
	r, _ := newTestRunner(srv)
	cfg := testTargets()
	cfg.source, cfg.ranks, cfg.labelValuesLimit = targetSourceLabelValues, []int{2, 3, 5000}, 3
	got, err := pickTargets(context.Background(), r.client, cfg, testTargetWindows, log.NewNopLogger(), func() {})
	require.NoError(t, err)
	assert.Equal(t, []string{"m2", "m3"}, got.names)
	assert.Equal(t, int64(21), got.tenantSeries)
	assert.Zero(t, f.boundsCalls)

	// A limit above the tenant's cap is refused.
	cfg.labelValuesLimit = 4
	cfg.ranks = []int{4}
	_, err = pickTargets(context.Background(), r.client, cfg, testTargetWindows, log.NewNopLogger(), func() {})
	var he *httpError
	require.ErrorAs(t, err, &he)
	assert.Equal(t, http.StatusBadRequest, he.status)
}

// TestPickTargetsRefusedBounds: a refused bounds request fails target picking with the route's refusal.
func TestPickTargetsRefusedBounds(t *testing.T) {
	f, srv := newFakeFrontend(t, rankedSeries()...)
	f.estimateError = func() (int, string) {
		return http.StatusUnprocessableEntity, "the request would hold more than 5 bytes in the store-gateway while counting, the tenant's cardinality estimate memory limit"
	}
	r, _ := newTestRunner(srv)
	_, err := pickTargets(context.Background(), r.client, testTargets(), testTargetWindows, log.NewNopLogger(), func() {})
	require.ErrorContains(t, err, "largest metric names")
	var he *httpError
	require.ErrorAs(t, err, &he)
	assert.Equal(t, classMemoryLimit, classifyRefusal(sideEstimate, he))
}
