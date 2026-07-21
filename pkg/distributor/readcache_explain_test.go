// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/validation"
)

func TestDistributor_ExplainReadcacheQuery(t *testing.T) {
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	const userID = "user-1"
	partitions := []int32{0, 1, 2, 3}

	from := model.TimeFromUnixNano(now.UnixNano())
	to := model.TimeFromUnixNano(now.Add(time.Minute).UnixNano())

	// rc-a owns even partitions, rc-b owns odd ones.
	ownerFor := func(p int32) (string, bool) {
		if p%2 == 0 {
			return "rc-a", true
		}
		return "rc-b", true
	}

	countCalls := func(plan ReadcacheQueryPlan) int {
		n := 0
		for _, p := range plan.Partitions {
			n += len(p.Calls)
		}
		return n
	}

	t.Run("full-fanout enumerates one QueryStream call per partition owner", func(t *testing.T) {
		d := readcacheTestDistributor(t, now, partitions, ownerFor)

		plan := d.ExplainReadcacheQuery(context.Background(), userID, from, to, []*labels.Matcher{mustEqualMatcher("bar", "baz")})
		require.Empty(t, plan.Unavailable)
		assert.False(t, plan.Named)
		require.Len(t, plan.Partitions, len(partitions))
		assert.Equal(t, len(partitions), plan.TotalCalls)
		assert.Equal(t, plan.TotalCalls, countCalls(plan))

		for _, p := range plan.Partitions {
			require.Len(t, p.Calls, 1, "each partition has one owner in this fixture")
			owner, _ := ownerFor(p.PartitionID)
			assert.Equal(t, owner, p.Calls[0].Owner)
			assert.Equal(t, fmt.Sprintf("%s/p%d", owner, p.PartitionID), p.Calls[0].InstanceID)
		}
	})

	t.Run("named query narrows to the overlapping partitions", func(t *testing.T) {
		d := readcacheTestDistributor(t, now, partitions, ownerFor)

		plan := d.ExplainReadcacheQuery(context.Background(), userID, from, to, []*labels.Matcher{mustEqualMatcher(model.MetricNameLabel, "some_metric")})
		require.Empty(t, plan.Unavailable)
		assert.True(t, plan.Named)
		assert.Equal(t, "some_metric", plan.MetricName)
		assert.NotEmpty(t, plan.Partitions)
		assert.Less(t, len(plan.Partitions), len(partitions), "a single metric-name hash range must not touch every partition")
		assert.Equal(t, plan.TotalCalls, countCalls(plan))
	})

	t.Run("routing-enabled flag reflects the tenant limit", func(t *testing.T) {
		d := readcacheTestDistributor(t, now, partitions, ownerFor)
		d.limits = validation.NewOverrides(validation.Limits{
			ReadcacheReadRouting:  validation.ReadcacheReadRoutingNautilus,
			NautilusIngestRouting: validation.NautilusIngestRoutingNautilus,
		}, nil)

		plan := d.ExplainReadcacheQuery(context.Background(), userID, from, to, []*labels.Matcher{mustEqualMatcher("bar", "baz")})
		assert.True(t, plan.RoutingEnabled)
		require.Empty(t, plan.Unavailable)
	})

	t.Run("missing nautilus assignment log reports unavailable", func(t *testing.T) {
		d := &Distributor{
			now:    func() time.Time { return now },
			limits: validation.NewOverrides(validation.Limits{}, nil),
		}
		plan := d.ExplainReadcacheQuery(context.Background(), userID, from, to, nil)
		assert.NotEmpty(t, plan.Unavailable)
		assert.Empty(t, plan.Partitions)
		assert.Zero(t, plan.TotalCalls)
	})
}
