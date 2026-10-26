// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func resourceTestProfile() SeriesProfile {
	p := SeriesProfile{
		Tenants: 1, SeriesPerTenant: Percentiles{"0": 1, "1": 1}, SeriesPerMetric: 100, LabelsPerMetric: Percentiles{"0": 3, "1": 3},
		ValuesPerLabel: Percentiles{"0": 10, "1": 10}, ValueLength: Percentiles{"0": 8, "1": 8}, LabelNames: 10,
		ScrapeIntervals: map[string]float64{"15": 1}, SeriesPerTarget: 10,
	}
	p.Resources = &ResourceProfile{
		Share: 1, SeriesPerResource: 50, DescriptiveAttributes: Percentiles{"0": 4, "1": 4}, ValueLength: Percentiles{"0": 12, "1": 12},
		IntShare: 0.25, AttributeNames: 8, Services: 3, IdentityChurnPerHour: 0.5, DescriptiveChurnPerHour: 1, TargetInfoIntervalSeconds: 60,
	}
	return p
}

// A tenant's series map over its resources at the profile's fan-out, and a resource's series churn with its identity.
func TestResourceModel(t *testing.T) {
	w := newWorld(resourceTestProfile(), 1000, 1)
	tenant := w.tenants[0]
	require.NotNil(t, tenant.resources)
	require.Equal(t, 20, tenant.resources.count, "1000 series at 50 per resource")

	seriesPerResource := map[int]int{}
	for _, m := range tenant.metrics {
		require.True(t, m.resources)
		for slot := range m.slots {
			seriesPerResource[m.resourceOf(slot)] += m.seriesPerSlot
		}
	}
	require.Len(t, seriesPerResource, 20)

	m := tenant.metrics[0]
	resource := m.resourceOf(0)
	for _, at := range []int64{0, 1_800_000, 3_600_000, 7_200_000} {
		require.Equal(t, w.resourceGeneration(tenant, resource, at), w.generation(m, 0, at), "the series' generation is its resource's")
	}
	first, later := w.resourceEntry(tenant, resource, 0), w.resourceEntry(tenant, resource, 3*3_600_000)
	require.Equal(t, "service.instance.id", first.Attributes[2].Key)
	require.NotEqual(t, first.Attributes[2].StringValue, later.Attributes[2].StringValue, "an identity lives 2 hours")
	require.Len(t, first.Attributes, 3+4)
}

// The feed's requests carry each series' resource, and target_info series, which carry none, and a resource's series
// come at its time.
func TestFeedRequestsCarryResources(t *testing.T) {
	w := newWorld(resourceTestProfile(), 1000, 1)
	f := &feed{world: w, requestSeries: 1 << 20, leValues: []string{"+Inf"}}
	var series []*mimirpb.TimeSeries
	tenant := w.tenants[0]
	for s := int64(0); s < 60_000; s += 1000 {
		for _, m := range append(tenant.metrics, tenant.resources.targetInfo) {
			m.slotsAt(s, func(slot int, at int64) {
				f.scrape(m, slot, at, func(ts *mimirpb.TimeSeries) { series = append(series, ts) })
			})
		}
	}
	targetInfos := 0
	// Each resource's offset into the 15-second interval of its series.
	offsets := map[uint32]int64{}
	for _, ts := range series {
		if metricName(ts.Labels) == "target_info" {
			targetInfos++
			require.Zero(t, ts.ResourceRef)
			continue
		}
		require.NotZero(t, ts.ResourceRef)
		offset := ts.Samples[0].TimestampMs % 15_000
		if known, ok := offsets[ts.ResourceRef]; ok {
			require.Equal(t, known, offset, "a resource's series come at its time")
		}
		offsets[ts.ResourceRef] = offset
	}
	require.Len(t, offsets, 20)
	require.Equal(t, 20, targetInfos, "a sample a minute of each resource's")
}

// The feed's records carry their series' resources in tables of their own, as the distributor writes them, with a
// resource's series together.
func TestFeedRecordsCarryResourceTables(t *testing.T) {
	p := resourceTestProfile()
	p.SeriesPerRequest, p.PartitionsPerTenant = 100, 3
	w := newWorld(p, 2_000, 1)
	f, err := newFeed(w, "", 0)
	require.NoError(t, err)
	t.Cleanup(f.close)
	start := time.Now().UnixMilli() / 60_000 * 60_000
	for s := start; s < start+60_000; s += 1000 {
		require.NoError(t, f.produceSecond(context.Background(), s, s+1000 == start+60_000))
	}

	var counter compositionCounter
	consumeRecords(t, f, func(record *kgo.Record, req *mimirpb.WriteRequest) {
		counter.add(string(record.Key), req)
		require.Equal(t, mimirpb.OTLP, req.Source)
		used := make([]bool, len(req.ResourceTable))
		for _, ts := range req.Timeseries {
			if metricName(ts.Labels) == "target_info" {
				require.Zero(t, ts.ResourceRef)
				continue
			}
			require.NotZero(t, ts.ResourceRef)
			require.LessOrEqual(t, int(ts.ResourceRef), len(req.ResourceTable))
			used[ts.ResourceRef-1] = true
		}
		require.NotContains(t, used, false, "a record's table has only its series' resources")
	})
	c := counter.composition()
	require.Equal(t, 1.0, c.OTLPSeriesShare)
	require.Less(t, c.ResourcesPerRecord, 3.0, "a resource's series come together")
	require.Greater(t, c.SeriesPerRecordResource, 10.0)
	require.Positive(t, c.TableBytesPerRecord)
	// A minute has 4 samples of each of a resource's 50 series, and one of its target_info.
	require.InDelta(t, 200, c.SeriesPerTargetInfo, 20)
}
