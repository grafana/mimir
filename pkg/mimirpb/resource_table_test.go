// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"fmt"
	"testing"

	"github.com/prometheus/common/model"
	rw2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	"github.com/stretchr/testify/require"
)

func testResource(service string) ResourceEntry {
	return ResourceEntry{Attributes: []ResourceAttribute{
		{Key: "service.name", StringValue: service},
		{Key: "host.cores", Type: RESOURCE_ATTRIBUTE_INT, IntValue: 8},
	}}
}

func testResourceSeries(name string, resourceRef uint32, withData bool) PreallocTimeseries {
	ts := &TimeSeries{Labels: []LabelAdapter{{Name: model.MetricNameLabel, Value: name}}, ResourceRef: resourceRef}
	if withData {
		ts.Samples = []Sample{{TimestampMs: 1, Value: 1}}
	}
	return PreallocTimeseries{TimeSeries: ts}
}

// decodeRecord marshals an RW2 request and decodes it as an ingester decodes a record.
func decodeRecord(t *testing.T, req *WriteRequest) *PreallocWriteRequest {
	t.Helper()
	data, err := req.Marshal()
	require.NoError(t, err)
	decoded := &PreallocWriteRequest{UnmarshalFromRW2: true}
	require.NoError(t, decoded.Unmarshal(data))
	return decoded
}

// requireResources checks that each decoded series refers to the resource the test names it after, or to none.
func requireResources(t *testing.T, req *PreallocWriteRequest, want map[string]string) {
	t.Helper()
	for _, ts := range req.Timeseries {
		name := ts.Labels[0].Value
		if ts.ResourceRef == 0 {
			require.Empty(t, want[name], name)
			continue
		}
		require.Equal(t, testResource(want[name]), req.ResourceTable[ts.ResourceRef-1], name)
	}
}

func TestResourceTableInRecords(t *testing.T) {
	req := &WriteRequest{
		Timeseries: []PreallocTimeseries{
			testResourceSeries("a", 1, true),
			testResourceSeries("b", 3, true),
			testResourceSeries("c", 0, true),
			testResourceSeries("d", 3, true),
			testResourceSeries("e", 2, false),
		},
		ResourceTable: []ResourceEntry{testResource("one"), testResource("two"), testResource("three")},
	}

	rw2Req, err := FromWriteRequestToRW2Request(req, nil, 0)
	require.NoError(t, err)
	// Only the resources of series with data, in the order they're first referred to.
	require.Equal(t, []ResourceEntry{testResource("one"), testResource("three")}, rw2Req.ResourceTable)
	refs := make([]uint32, 0, len(rw2Req.TimeseriesRW2))
	for _, ts := range rw2Req.TimeseriesRW2 {
		refs = append(refs, ts.ResourceRef)
	}
	require.Equal(t, []uint32{1, 2, 0, 2, 0}, refs)
	// The request's series keep their refs, into its own table.
	require.Equal(t, uint32(3), req.Timeseries[1].ResourceRef)

	requireResources(t, decodeRecord(t, rw2Req), map[string]string{"a": "one", "b": "three", "d": "three"})

	t.Run("a ref outside the table", func(t *testing.T) {
		req := &WriteRequest{Timeseries: []PreallocTimeseries{testResourceSeries("a", 2, true)}, ResourceTable: []ResourceEntry{testResource("one")}}
		_, err := FromWriteRequestToRW2Request(req, nil, 0)
		require.Error(t, err)
	})

	t.Run("a decoded ref outside the table", func(t *testing.T) {
		rw2Req.TimeseriesRW2[0].ResourceRef = 3
		data, err := rw2Req.Marshal()
		require.NoError(t, err)
		decoded := &PreallocWriteRequest{UnmarshalFromRW2: true}
		require.ErrorIs(t, decoded.Unmarshal(data), errorInvalidResourceRef)
	})

	t.Run("a decoder that doesn't know resources", func(t *testing.T) {
		rw2Req, err := FromWriteRequestToRW2Request(req, nil, 0)
		require.NoError(t, err)
		data, err := rw2Req.Marshal()
		require.NoError(t, err)
		var upstream rw2.Request
		require.NoError(t, upstream.Unmarshal(data))
		require.Len(t, upstream.Timeseries, len(req.Timeseries))
		for i, ts := range upstream.Timeseries {
			require.Equal(t, req.Timeseries[i].Labels[0].Value, upstream.Symbols[ts.LabelsRefs[1]])
		}
	})
}

// Requests split from one share its series and its table, whose indexes the series' refs stay, so that converting one
// of them doesn't disturb another, as when the same series goes to partitions and to ingesters.
func TestResourceTableForIndexes(t *testing.T) {
	req := &WriteRequest{
		Timeseries: []PreallocTimeseries{
			testResourceSeries("a", 1, true),
			testResourceSeries("b", 2, true),
			testResourceSeries("c", 3, true),
		},
		ResourceTable: []ResourceEntry{testResource("one"), testResource("two"), testResource("three")},
	}
	first := req.ForIndexes([]int{0, 2}, len(req.Timeseries))
	second := req.ForIndexes([]int{1, 2}, len(req.Timeseries))
	require.Same(t, req.Timeseries[2].TimeSeries, first.Timeseries[1].TimeSeries)

	for _, c := range []struct {
		req  *WriteRequest
		want map[string]string
	}{
		{first, map[string]string{"a": "one", "c": "three"}},
		{second, map[string]string{"b": "two", "c": "three"}},
		{first, map[string]string{"a": "one", "c": "three"}},
	} {
		rw2Req, err := FromWriteRequestToRW2Request(c.req, nil, 0)
		require.NoError(t, err)
		require.Len(t, rw2Req.ResourceTable, 2)
		requireResources(t, decodeRecord(t, rw2Req), c.want)
	}
}

func TestResourceTableSplitting(t *testing.T) {
	const series = 200
	req := &WriteRequest{}
	want := map[string]string{}
	for i := range 10 {
		req.ResourceTable = append(req.ResourceTable, testResource(fmt.Sprintf("service-%d", i)))
	}
	for i := range series {
		name := fmt.Sprintf("series_%d", i)
		// Every seventh series has no resource.
		var ref uint32
		if i%7 != 0 {
			ref = uint32(i%len(req.ResourceTable)) + 1
			want[name] = fmt.Sprintf("service-%d", ref-1)
		}
		req.Timeseries = append(req.Timeseries, testResourceSeries(name, ref, true))
	}
	rw2Req, err := FromWriteRequestToRW2Request(req, nil, 0)
	require.NoError(t, err)

	const maxSize = 600
	parts := SplitWriteRequestByMaxMarshalSizeRW2(rw2Req, rw2Req.Size(), maxSize, 0, nil)
	require.Greater(t, len(parts), 5)
	seen := 0
	for _, part := range parts {
		require.LessOrEqual(t, part.Size(), maxSize)
		require.LessOrEqual(t, len(part.ResourceTable), len(req.ResourceTable))
		decoded := decodeRecord(t, part)
		requireResources(t, decoded, want)
		seen += len(decoded.Timeseries)
	}
	require.Equal(t, series, seen)
}
