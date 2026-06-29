// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"bytes"
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/usagetracker/tenantshard"
)

func TestTrackerStore_ShardStats(t *testing.T) {
	const idleTimeout = 20 * time.Minute
	limits := limiterMock{"user-a": 100, "user-b": 100}
	now := time.Date(2020, 1, 1, 1, 2, 3, 0, time.UTC)

	store := newTrackerStore(idleTimeout, 85, log.NewNopLogger(), limits, noopEvents{}, false, 0)

	_, err := store.trackSeries(context.Background(), "user-b", []uint64{1, 2, 3, 4, 5}, now)
	require.NoError(t, err)
	_, err = store.trackSeries(context.Background(), "user-a", []uint64{10, 20, 30}, now)
	require.NoError(t, err)

	rows := store.shardStats()
	require.Len(t, rows, 2*shards)

	// Rows are sorted by tenant, then by shard.
	require.True(t, slices.IsSortedFunc(rows, func(a, b ShardStats) int {
		if a.Tenant != b.Tenant {
			return strings.Compare(a.Tenant, b.Tenant)
		}
		return a.Shard - b.Shard
	}))

	// The sum of resident elements across a tenant's shards equals its series count.
	residentByTenant := map[string]uint32{}
	for _, r := range rows {
		residentByTenant[r.Tenant] += r.Resident
		require.GreaterOrEqual(t, r.Limit, r.Resident)
	}
	for tenant, count := range store.seriesCountsForTests() {
		require.Equal(t, uint32(count), residentByTenant[tenant], "tenant %s", tenant)
	}
}

func TestUsageTrackerPartitionTemplates(t *testing.T) {
	// partitions.gohtml renders the list of owned partitions.
	var buf bytes.Buffer
	require.NoError(t, partitionsPageTemplate.Execute(&buf, partitionsPageContents{
		Now:        time.Now(),
		StaticRoot: "../static/",
		Partitions: []int32{0, 3, 7},
	}))
	require.Contains(t, buf.String(), `href="partitions/3"`)
	require.Contains(t, buf.String(), "../static/bootstrap-5.1.3.min.css")

	// partition.gohtml renders the per-(tenant, shard) table.
	buf.Reset()
	require.NoError(t, partitionPageTemplate.Execute(&buf, partitionPageContents{
		Now:        time.Now(),
		StaticRoot: "../../static/",
		Partition:  7,
		Shards: []ShardStats{
			{Tenant: "user-a", Shard: 0, Stats: tenantshard.Stats{Resident: 12, Dead: 1, Limit: 32, Length: 8, Rehashes: 2}},
		},
	}))
	out := buf.String()
	require.Contains(t, out, "Partition 7")
	require.Contains(t, out, "user-a")
	require.Contains(t, out, "Rehashes")
	require.Contains(t, out, "../../static/bootstrap-5.1.3.min.css")
}
