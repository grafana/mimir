// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/nautilus/assignment"
	"github.com/grafana/mimir/pkg/nautilus/readcacheassignment"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/validation"
)

// readcacheTestDistributor builds a minimal Distributor wired with a
// nautilus range->partition assignment log (evenly splitting the hash
// space across the given partitions) and a partition->readcache
// instance log owned according to ownerFor. Both logs are live at
// `now`. Only the fields getReadcacheReplicationSetsForQuery touches
// are populated.
func readcacheTestDistributor(t *testing.T, now time.Time, partitions []int32, ownerFor func(int32) (string, bool)) *Distributor {
	t.Helper()

	d := &Distributor{now: func() time.Time { return now }}

	al := assignment.NewLog()
	require.True(t, al.Apply(now, assignment.EvenSplit(partitions), 5*time.Minute, time.Minute))
	d.nautilusLog.Store(al)

	var entries []readcacheassignment.LogEntry
	for _, p := range partitions {
		owner, ok := ownerFor(p)
		if !ok {
			continue
		}
		entries = append(entries, readcacheassignment.LogEntry{
			PartitionID: p,
			InstanceID:  owner,
			From:        now.Add(-time.Minute),
			To:          now.Add(5 * time.Minute),
		})
	}
	d.readcacheLog.Store(readcacheassignment.NewLogFromEntries(entries))

	return d
}

func TestDistributor_GetReadcacheReplicationSetsForQuery(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	const userID = "user-1"
	partitions := []int32{0, 1, 2, 3}

	// N:1 ownership: rc-a owns even partitions, rc-b owns odd ones.
	ownerFor := func(p int32) (string, bool) {
		if p%2 == 0 {
			return "rc-a", true
		}
		return "rc-b", true
	}

	t.Run("full-fanout resolves every partition to a single-instance readcache set", func(t *testing.T) {
		d := readcacheTestDistributor(t, now, partitions, ownerFor)

		// No exact __name__ matcher -> fan out to all partitions.
		sets, partitionByInstance, err := d.getReadcacheReplicationSetsForQuery(userID, []*labels.Matcher{mustEqualMatcher("bar", "baz")})
		require.NoError(t, err)
		require.Len(t, sets, len(partitions))
		require.Len(t, partitionByInstance, len(partitions))

		var gotPartitions []int32
		for _, rs := range sets {
			require.Len(t, rs.Instances, 1, "each readcache replication set targets exactly one instance")
			inst := rs.Instances[0]

			partID, ok := partitionByInstance[inst.Id]
			require.True(t, ok, "instance %q must be in the hint map", inst.Id)
			gotPartitions = append(gotPartitions, partID)

			// The synthetic instance ID encodes the current owner
			// and partition; the owner must match the readcache log.
			owner, _ := ownerFor(partID)
			assert.Equal(t, fmt.Sprintf("%s/p%d", owner, partID), inst.Id)
		}
		sort.Slice(gotPartitions, func(i, j int) bool { return gotPartitions[i] < gotPartitions[j] })
		assert.Equal(t, partitions, gotPartitions, "fan-out must cover every partition exactly once")
	})

	t.Run("named path resolves only the partitions overlapping the metric hash range", func(t *testing.T) {
		d := readcacheTestDistributor(t, now, partitions, ownerFor)

		nameMatcher := mustEqualMatcher(model.MetricNameLabel, "some_metric")
		sets, partitionByInstance, err := d.getReadcacheReplicationSetsForQuery(userID, []*labels.Matcher{nameMatcher})
		require.NoError(t, err)
		require.NotEmpty(t, sets)

		// The resolved partitions must match what the active table
		// reports as overlapping the metric name's hash range.
		lo, hi := mimirpb.MetricNameHashRange(userID, "some_metric")
		want := d.nautilusLog.Load().ActiveTable(now).PartitionsOverlapping(lo, hi)
		require.NotEmpty(t, want)

		var got []int32
		for _, rs := range sets {
			require.Len(t, rs.Instances, 1)
			got = append(got, partitionByInstance[rs.Instances[0].Id])
		}
		sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
		sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
		assert.Equal(t, want, got)
		// A metric name's hash range is far narrower than a quarter
		// of the space, so it resolves to a strict subset.
		assert.Less(t, len(got), len(partitions), "named query must not fan out to all partitions")
	})

	t.Run("missing assignment log is a hard failure with no ingester fallback", func(t *testing.T) {
		d := &Distributor{now: func() time.Time { return now }}
		_, _, err := d.getReadcacheReplicationSetsForQuery(userID, []*labels.Matcher{mustEqualMatcher("bar", "baz")})
		require.Error(t, err)
		var rcErr errReadcacheRoutingUnavailable
		assert.True(t, errors.As(err, &rcErr), "want errReadcacheRoutingUnavailable, got %T", err)
	})

	t.Run("missing readcache log is a hard failure", func(t *testing.T) {
		d := &Distributor{now: func() time.Time { return now }}
		al := assignment.NewLog()
		require.True(t, al.Apply(now, assignment.EvenSplit(partitions), 5*time.Minute, time.Minute))
		d.nautilusLog.Store(al)

		_, _, err := d.getReadcacheReplicationSetsForQuery(userID, []*labels.Matcher{mustEqualMatcher("bar", "baz")})
		require.Error(t, err)
		var rcErr errReadcacheRoutingUnavailable
		assert.True(t, errors.As(err, &rcErr))
	})

	t.Run("partition without a current readcache owner is a hard failure", func(t *testing.T) {
		// Partition 3 has no owner in the log.
		d := readcacheTestDistributor(t, now, partitions, func(p int32) (string, bool) {
			if p == 3 {
				return "", false
			}
			return "rc-a", true
		})

		_, _, err := d.getReadcacheReplicationSetsForQuery(userID, []*labels.Matcher{mustEqualMatcher("bar", "baz")})
		require.Error(t, err)
		var rcErr errReadcacheRoutingUnavailable
		assert.True(t, errors.As(err, &rcErr))
	})
}

// TestDistributor_QueryClientForInstance_NautilusNoIngesterFallback
// asserts that for a nautilus-only tenant, queryClientForInstance
// never falls back to the ingester pool: when readcache can't be
// resolved it returns an error rather than dialing an ingester. The
// distributor here has a nil ingesterPool, so any accidental fallback
// would panic instead of silently passing.
func TestDistributor_QueryClientForInstance_NautilusNoIngesterFallback(t *testing.T) {
	const userID = "user-1"
	ctx := user.InjectOrgID(context.Background(), userID)

	limits := validation.NewOverrides(validation.Limits{
		NautilusIngestRouting: validation.NautilusIngestRoutingNautilus,
		ReadcacheReadRouting:  validation.ReadcacheReadRoutingNautilus,
	}, nil)

	d := &Distributor{
		now:    time.Now,
		limits: limits,
		// readcachePool and ingesterPool left nil on purpose.
	}
	require.True(t, d.shouldRouteReadToReadcache(ctx))

	hits := newReadcacheHitTracker()
	cli, viaReadcache, err := d.queryClientForInstance(ctx, ring.InstanceDesc{Id: "rc-a/p0"}, map[string]int32{"rc-a/p0": 0}, hits, nil)
	require.Error(t, err, "nautilus-only routing must not fall back to ingesters")
	assert.Nil(t, cli)
	assert.False(t, viaReadcache)
	var rcErr errReadcacheRoutingUnavailable
	assert.True(t, errors.As(err, &rcErr), "want errReadcacheRoutingUnavailable, got %T", err)
}

// TestDistributor_QueryStream_NautilusRoutingDoesNotDialIngesters is
// the read-path analogue of the write-side nautilus-required
// behaviour: when a tenant is on readcache_read_routing=nautilus-only
// but the assignment log is not live, QueryStream fails outright and
// must not dial any ingester (those ingesters hold no data for the
// tenant, whose data lives on the nautilus_ingest topic).
func TestDistributor_QueryStream_NautilusRoutingDoesNotDialIngesters(t *testing.T) {
	const tenantID = "user"

	ctx := user.InjectOrgID(context.Background(), tenantID)
	ctx = limiter.ContextWithNewUnlimitedMemoryConsumptionTracker(ctx)
	dedup := limiter.NewSeriesDeduplicatorMetrics(prometheus.NewPedanticRegistry())
	ctx = limiter.ContextWithNewSeriesLabelsDeduplicator(ctx, dedup)

	limits := prepareDefaultLimits()
	limits.NautilusIngestRouting = validation.NautilusIngestRoutingNautilus
	limits.ReadcacheReadRouting = validation.ReadcacheReadRoutingNautilus

	cfg := prepConfig{
		numDistributors:         1,
		ingestStorageEnabled:    true,
		ingestStoragePartitions: 4,
		ingesterStateByZone: map[string]ingesterZoneState{
			"zone-a": {states: []ingesterState{ingesterStateHappy, ingesterStateHappy, ingesterStateHappy, ingesterStateHappy}},
		},
		ingesterDataByZone:   map[string][]*mimirpb.WriteRequest{},
		ingesterDataTenantID: tenantID,
		replicationFactor:    1,
		limits:               limits,
	}

	distributors, ingesters, _, _ := prepare(t, cfg)
	require.Len(t, distributors, 1)
	d := distributors[0]
	test.Poll(t, 5*time.Second, 4, func() interface{} {
		return d.partitionsRing.PartitionRing().PartitionsCount()
	})

	// No rebalancer address was configured, so neither the
	// assignment log nor the readcache log is live.
	require.Nil(t, d.GetReadcacheLog())

	queryMetrics := stats.NewQueryMetrics(prometheus.NewPedanticRegistry())
	_, err := d.QueryStream(ctx, queryMetrics, 0, 10, false, nil, mustEqualMatcher(model.MetricNameLabel, "foo"))
	require.Error(t, err, "nautilus-only read must fail when the assignment log is not live")

	assert.Zero(t, countMockIngestersCalls(ingesters, "QueryStream"),
		"nautilus-only routing must not dial ingesters even when readcache is unavailable")
}
