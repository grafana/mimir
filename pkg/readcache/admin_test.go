// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/nautilus/assignment"
	"github.com/grafana/mimir/pkg/util/validation"
)

// TestReadcache_AdminPage_RendersOwnedPartitionsAndRanges exercises
// the readcache admin page end-to-end: load some current and
// historical ranges into a running readcache via the same RPC path
// the rebalancer uses, then verify the rendered HTML reflects them.
// The test asserts on the data the page is supposed to convey
// (partition ids, current vs historical range counts, hex range
// formatting) rather than full HTML structure so cosmetic template
// edits don't bounce CI.
func TestReadcache_AdminPage_RendersOwnedPartitionsAndRanges(t *testing.T) {
	cfg := newTestConfig(t, true, 4)
	cfg.OwnedPartitions = "0,1"

	limits := validation.NewOverrides(validation.Limits{}, nil)
	r, err := New(cfg, limits, nil, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, services.StartAndAwaitRunning(ctx, r))
	defer func() { _ = services.StopAndAwaitTerminated(ctx, r) }()

	// Initial assignment: P0 owns [0, 99], P1 owns [100, 199].
	_, err = r.setHashRanges(ctx, &ingester_client.SetHashRangesRequest{
		Ranges: []ingester_client.HashRangeEntry{
			{Lo: 0, Hi: 99, PartitionId: 0},
			{Lo: 100, Hi: 199, PartitionId: 1},
		},
	})
	require.NoError(t, err)

	// Slicer round moves [0, 99] off P0 onto P1 — this leaves
	// historical residue on P0.
	_, err = r.setHashRanges(ctx, &ingester_client.SetHashRangesRequest{
		Ranges: []ingester_client.HashRangeEntry{
			{Lo: 100, Hi: 199, PartitionId: 1},
			{Lo: 0, Hi: 99, PartitionId: 1},
		},
	})
	require.NoError(t, err)

	// Inject walker results so the page has non-zero per-range counts.
	r.partitionMu.RLock()
	p0 := r.partitions[0]
	p1 := r.partitions[1]
	r.partitionMu.RUnlock()
	require.NotNil(t, p0)
	require.NotNil(t, p1)

	p0Snap := p0.ranges.rangesSnapshot()
	require.Equal(t, []assignment.HashRange{hr(0, 99)}, p0Snap,
		"P0 should have its formerly-current range [0,99] as historical now")
	require.True(t, p0.ranges.applyWalkResult(p0Snap, []int64{1234}))

	p1Snap := p1.ranges.rangesSnapshot()
	require.Equal(t, []assignment.HashRange{hr(0, 99), hr(100, 199)}, p1Snap)
	require.True(t, p1.ranges.applyWalkResult(p1Snap, []int64{56, 78}))

	// Now hit ServeHTTP.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, AdminPathPrefix, nil)
	r.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Header().Get("Content-Type"), "text/html")

	body := rec.Body.String()

	assert.Contains(t, body, cfg.InstanceID, "instance ID should be in the page")
	assert.Contains(t, body, "P0", "owned partition 0 should be listed")
	assert.Contains(t, body, "P1", "owned partition 1 should be listed")

	// Range formatting (8-hex lo-hi). The current and historical
	// ranges should both render. P1's current ranges are [0, 99]
	// and [100, 199]; P0's historical range is [0, 99].
	assert.Contains(t, body, "00000000-00000063", "[0, 99] should render as 8-hex")
	assert.Contains(t, body, "00000064-000000c7", "[100, 199] should render as 8-hex")

	// The per-range counts should appear. fmtSeries renders 1234 as
	// "1.23K", 56 and 78 as plain integers.
	assert.Contains(t, body, "1.23K", "residue count should render")
	assert.True(t, strings.Contains(body, ">56<") || strings.Contains(body, "56\n"), "p1 growth count should render")

	// The "no partitions" empty-state must NOT appear because we own
	// two partitions.
	assert.NotContains(t, body, "owns no partitions",
		"the empty-state message must not render when we own partitions")
}

// TestReadcache_AdminPage_EmptyState exercises the path where no
// partitions are owned: the page should still render successfully and
// surface the empty-state message rather than blowing up on an
// empty partitions map.
func TestReadcache_AdminPage_EmptyState(t *testing.T) {
	cfg := newTestConfig(t, true, 4)
	cfg.OwnedPartitions = ""

	limits := validation.NewOverrides(validation.Limits{}, nil)
	r, err := New(cfg, limits, nil, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, services.StartAndAwaitRunning(ctx, r))
	defer func() { _ = services.StopAndAwaitTerminated(ctx, r) }()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, AdminPathPrefix, nil)
	r.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, cfg.InstanceID)
	assert.Contains(t, body, "owns no partitions",
		"empty state copy should appear when no partitions are owned")
}
