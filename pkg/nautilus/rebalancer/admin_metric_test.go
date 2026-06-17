// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/nautilus/assignment"
	"github.com/grafana/mimir/pkg/nautilus/readcacheassignment"
)

// metricLookupTestRebalancer seeds a Rebalancer with one keyspace
// timeline: until `moveAt` the whole keyspace lives on P1 (owned by
// rc-old), after `moveAt` the half of keyspace containing the
// metric's hash range moves to P2 (owned by rc-new). This produces a
// lookup where window-vs-instant partition counts differ — the churn
// amplification the tool exists to expose.
func metricLookupTestRebalancer(t *testing.T, now, moveAt time.Time, lo uint32) *Rebalancer {
	t.Helper()

	require.Greater(t, lo, uint32(0), "test fixture assumes the metric's hash range doesn't start at 0")

	store := newLogStore()
	store.seedFromEntries([]assignment.LogEntry{
		// Before the move: one tile spanning everything on P1.
		{Range: assignment.HashRange{Lo: 0, Hi: math.MaxUint32}, PartitionID: 1, From: moveAt.Add(-time.Hour), To: moveAt},
		// After the move: the slice holding the metric's range goes
		// to P2, the rest stays on P1.
		{Range: assignment.HashRange{Lo: lo, Hi: math.MaxUint32}, PartitionID: 2, From: moveAt, To: now.Add(time.Hour)},
		{Range: assignment.HashRange{Lo: 0, Hi: lo - 1}, PartitionID: 1, From: moveAt, To: now.Add(time.Hour)},
	})

	rcStore := newReadcacheLogStore()
	rcStore.seedFromEntries([]readcacheassignment.LogEntry{
		{PartitionID: 1, InstanceID: "rc-old", From: moveAt.Add(-time.Hour), To: now.Add(time.Hour)},
		{PartitionID: 2, InstanceID: "rc-new", From: moveAt, To: now.Add(time.Hour)},
	})

	return &Rebalancer{
		store:          store,
		readcacheStore: rcStore,
		clock:          newFakeClock(now),
	}
}

func TestServeMetricLookup(t *testing.T) {
	const (
		user   = "user-1"
		metric = "some_metric"
	)
	now := time.Unix(100000, 0)
	moveAt := now.Add(-5 * time.Minute)
	lo, hi := mimirpb.MetricNameHashRange(user, metric)
	r := metricLookupTestRebalancer(t, now, moveAt, lo)

	t.Run("json resolves range, window partitions, and owners", func(t *testing.T) {
		req := httptest.NewRequest("GET", fmt.Sprintf("%s/metric?user=%s&metric=%s&window=15m&format=json", adminPathPrefix, user, metric), nil)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		require.Equal(t, 200, rec.Code, rec.Body.String())

		var got metricLookupData
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))

		assert.Equal(t, lo, got.Lo)
		assert.Equal(t, hi, got.Hi)

		// The 15m window spans the move at now-5m: both the pre-move
		// tile (P1) and the post-move tile (P2) overlap the metric's
		// hash range, while only P2's tile is active at `now`.
		assert.Equal(t, 2, got.NumPartitionsWindow, "window must union across the range->partition move")
		assert.Equal(t, 1, got.NumPartitionsNow, "only the post-move partition is active at now")
		assert.Equal(t, 2, got.DistinctReadcaches)

		require.Len(t, got.Partitions, 2)
		assert.Equal(t, int32(1), got.Partitions[0].PartitionID)
		require.Len(t, got.Partitions[0].Owners, 1)
		assert.Equal(t, "rc-old", got.Partitions[0].Owners[0].InstanceID)
		assert.Equal(t, int32(2), got.Partitions[1].PartitionID)
		require.Len(t, got.Partitions[1].Owners, 1)
		assert.Equal(t, "rc-new", got.Partitions[1].Owners[0].InstanceID)
	})

	t.Run("short window excludes the pre-move partition", func(t *testing.T) {
		req := httptest.NewRequest("GET", fmt.Sprintf("%s/metric?user=%s&metric=%s&window=1m&format=json", adminPathPrefix, user, metric), nil)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		require.Equal(t, 200, rec.Code)

		var got metricLookupData
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
		assert.Equal(t, 1, got.NumPartitionsWindow, "a 1m window post-move must not reach back to P1")
		require.Len(t, got.Partitions, 1)
		assert.Equal(t, int32(2), got.Partitions[0].PartitionID)
	})

	t.Run("html renders the hash range and owners", func(t *testing.T) {
		req := httptest.NewRequest("GET", fmt.Sprintf("%s/metric?user=%s&metric=%s", adminPathPrefix, user, metric), nil)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		require.Equal(t, 200, rec.Code)

		body := rec.Body.String()
		assert.Contains(t, body, formatHexRange(lo, hi))
		assert.Contains(t, body, "rc-old")
		assert.Contains(t, body, "rc-new")
		assert.True(t, strings.Contains(body, "P1") && strings.Contains(body, "P2"))
	})

	t.Run("missing params is a 400", func(t *testing.T) {
		req := httptest.NewRequest("GET", adminPathPrefix+"/metric?user=user-1", nil)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		assert.Equal(t, 400, rec.Code)
	})

	t.Run("bad window is a 400", func(t *testing.T) {
		req := httptest.NewRequest("GET", adminPathPrefix+"/metric?user=user-1&metric=up&window=banana", nil)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		assert.Equal(t, 400, rec.Code)
	})
}
