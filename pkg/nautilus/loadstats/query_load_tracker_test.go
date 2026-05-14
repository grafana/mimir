// SPDX-License-Identifier: AGPL-3.0-only

package loadstats

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
)

func TestTracker_AttributeRoutesByHint(t *testing.T) {
	tr := NewTracker("cortex_readcache")

	tr.Attribute(nil, 100)
	tr.Attribute(&client.QueryAttributionHint{PartitionId: 0}, 200)
	tr.Attribute(&client.QueryAttributionHint{PartitionId: 7}, 300)
	tr.Attribute(&client.QueryAttributionHint{PartitionId: 7}, -10)

	tr.Tick()

	snap := tr.Snapshot()
	require.Len(t, snap.PerPartition, 2, "should track partitions 0 and 7")

	byPID := map[int32]float64{}
	for _, p := range snap.PerPartition {
		byPID[p.PartitionID] = p.SamplesEWMA
	}
	assert.Greater(t, byPID[7], byPID[0], "partition 7 received more samples than 0")
	assert.Greater(t, snap.Unnamed, 0.0, "unnamed bucket should be populated")
	assert.Greater(t, byPID[0], 0.0, "partition 0 should be a real bucket, not the unnamed one")
}

func TestTracker_PartitionZeroIsNotUnnamed(t *testing.T) {
	tr := NewTracker("cortex_readcache")

	tr.Attribute(&client.QueryAttributionHint{PartitionId: 0}, 1000)
	tr.Tick()

	snap := tr.Snapshot()
	assert.Equal(t, 0.0, snap.Unnamed, "explicit partition_id=0 should not bill the unnamed bucket")
	require.Len(t, snap.PerPartition, 1)
	assert.Equal(t, int32(0), snap.PerPartition[0].PartitionID)
	assert.Greater(t, snap.PerPartition[0].SamplesEWMA, 0.0)
}

func TestTracker_EWMASmoothing(t *testing.T) {
	tr := NewTracker("cortex_readcache")

	tr.Attribute(&client.QueryAttributionHint{PartitionId: 5}, 1500)
	tr.Tick()
	snap1 := tr.Snapshot()
	require.Len(t, snap1.PerPartition, 1)
	first := snap1.PerPartition[0].SamplesEWMA
	require.Greater(t, first, 0.0)

	tr.Tick()
	snap2 := tr.Snapshot()
	second := snap2.PerPartition[0].SamplesEWMA
	assert.Less(t, second, first, "EWMA should decay when no new events arrive")

	for i := 0; i < 20; i++ {
		tr.Tick()
	}
	snap3 := tr.Snapshot()
	third := snap3.PerPartition[0].SamplesEWMA
	assert.Less(t, third, second)
	assert.Greater(t, third, 0.0, "EWMA decays exponentially, never to exact zero in finite steps")
}

func TestTracker_PrometheusCollect(t *testing.T) {
	tr := NewTracker("cortex_readcache")

	tr.Attribute(nil, 1000)
	tr.Attribute(&client.QueryAttributionHint{PartitionId: 3}, 2000)
	tr.Attribute(&client.QueryAttributionHint{PartitionId: 0}, 500)
	tr.Tick()

	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(tr)

	families, err := reg.Gather()
	require.NoError(t, err)

	var (
		hasNamed, hasUnnamed bool
		partitionLabels      []string
	)
	for _, fam := range families {
		switch fam.GetName() {
		case "cortex_readcache_query_samples_named_ewma":
			hasNamed = true
			for _, m := range fam.GetMetric() {
				for _, lp := range m.GetLabel() {
					if lp.GetName() == "partition" {
						partitionLabels = append(partitionLabels, lp.GetValue())
					}
				}
			}
		case "cortex_readcache_query_samples_unnamed_ewma":
			hasUnnamed = true
			require.Len(t, fam.GetMetric(), 1)
			require.NotNil(t, fam.GetMetric()[0].GetGauge())
			assert.Greater(t, fam.GetMetric()[0].GetGauge().GetValue(), 0.0)
		}
	}
	assert.True(t, hasNamed)
	assert.True(t, hasUnnamed)
	all := strings.Join(partitionLabels, ",")
	assert.Contains(t, all, "0")
	assert.Contains(t, all, "3")
}
