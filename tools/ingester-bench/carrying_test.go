// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
)

// Carrying per-series metadata makes records bigger, and a profile whose series send none carries nothing.
func TestCarryingCost(t *testing.T) {
	profile, err := loadProfile("native-metadata/threshold.json")
	require.NoError(t, err)
	c, err := carryingCost(profile, 100_000, 10)
	require.NoError(t, err)
	require.Greater(t, c.WithoutBytesPerSample, 0.0)
	require.Greater(t, c.Increase, 0.0)

	profile.Series.MetadataShare = 0
	c, err = carryingCost(profile, 100_000, 10)
	require.NoError(t, err)
	require.Equal(t, c.WithBytesPerSample, c.WithoutBytesPerSample)
}

// Carrying resources makes records bigger by their tables and refs, which a ref in a lower field makes a little
// smaller. Whether symbolizing the tables' strings would depends on how often they repeat in a record.
func TestCarryingCostOfResources(t *testing.T) {
	profile, err := loadProfile("native-metadata/threshold.json")
	require.NoError(t, err)
	profile.Series.Resources = &ResourceProfile{
		Share: 0.5, SeriesPerResource: 100, DescriptiveAttributes: Percentiles{"0": 10, "0.5": 20, "1": 40}, ValueLength: Percentiles{"0": 24, "1": 24},
		IntShare: 0.1, AttributeNames: 40, Services: 50, TargetInfoIntervalSeconds: 60,
	}
	c, err := carryingCost(profile, 100_000, 10)
	require.NoError(t, err)
	r := c.Resources
	require.NotNil(t, r)
	require.Greater(t, r.Increase, 0.0)
	require.Greater(t, r.TableShare, 0.0)
	require.Positive(t, r.SymbolizedBytesPerSample)
	require.Less(t, r.LowFieldIncrease, r.Increase)
	require.Greater(t, r.LowFieldIncrease, 0.0)
}

// Symbolizing a record's resource table saves the bytes of the keys and values that repeat, in the table or among the
// labels, and costs a ref's for each, and a new symbol's for those that don't.
func TestEncodingEstimate(t *testing.T) {
	estimate := func(table []mimirpb.ResourceEntry) *encodingEstimate {
		req := &mimirpb.WriteRequest{ResourceTable: table, Source: mimirpb.OTLP}
		for i := range table {
			req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
				Labels:      []mimirpb.LabelAdapter{{Name: "__name__", Value: "requests_total"}, {Name: "pod", Value: "checkout-7d4f9"}},
				Samples:     []mimirpb.Sample{{TimestampMs: int64(i), Value: 1}},
				ResourceRef: uint32(i + 1),
			}})
		}
		records, _, err := ingest.RecordSerializerFromVersion(2).ToRecords("ingest", 0, "tenant", req, 1<<20)
		require.NoError(t, err)
		require.Len(t, records, 1)
		e := &encodingEstimate{}
		require.NoError(t, e.add(records[0].Value, ingest.ParseRecordVersion(records[0])))
		return e
	}
	entry := func(name, pod string) mimirpb.ResourceEntry {
		return mimirpb.ResourceEntry{Attributes: []mimirpb.ResourceAttribute{
			{Key: "service.name", StringValue: name}, {Key: "k8s.pod.name", StringValue: pod}, {Key: "host.cores", Type: mimirpb.RESOURCE_ATTRIBUTE_INT, IntValue: 8},
		}}
	}
	// Two entries with the keys and the pod's name, which a label has, in common.
	repeating := estimate([]mimirpb.ResourceEntry{entry("checkout", "checkout-7d4f9"), entry("checkout-canary", "checkout-7d4f9")})
	require.EqualValues(t, 2, repeating.seriesWithRefs)
	require.Positive(t, repeating.tableBytes)
	require.Positive(t, repeating.symbolizedSaving)
	// One entry whose strings are all new costs their refs.
	unique := estimate([]mimirpb.ResourceEntry{entry("payments", "payments-5b8c2")})
	require.Negative(t, unique.symbolizedSaving)
}
