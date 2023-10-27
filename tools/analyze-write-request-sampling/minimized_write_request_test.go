package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestMinimizeWriteRequest(t *testing.T) {
	req := &mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{
			{
				TimeSeries: &mimirpb.TimeSeries{
					Labels: []mimirpb.LabelAdapter{
						{Name: "__name__", Value: "metric_1"},
						{Name: "cluster", Value: "dev"},
					},
					Samples: []mimirpb.Sample{
						{TimestampMs: 10, Value: 20},
						{TimestampMs: 20, Value: 30},
					},
				},
			}, {
				TimeSeries: &mimirpb.TimeSeries{
					Labels: []mimirpb.LabelAdapter{
						{Name: "__name__", Value: "metric_2"},
						{Name: "cluster", Value: "dev"},
					},
					Samples: []mimirpb.Sample{
						{TimestampMs: 30, Value: 40},
						{TimestampMs: 40, Value: 50},
					},
				},
			},
		},
	}

	minReq := minimizeWriteRequest(req)
	assert.Equal(t, "__name__metric_1clusterdevmetric_2", minReq.Symbols)
	require.Len(t, minReq.Timeseries, 2)

	require.Len(t, minReq.Timeseries[0].LabelSymbols, 4)
	assert.Equal(t, packRef(0, 8), minReq.Timeseries[0].LabelSymbols[0])
	assert.Equal(t, packRef(8, 8), minReq.Timeseries[0].LabelSymbols[1])
	assert.Equal(t, packRef(16, 7), minReq.Timeseries[0].LabelSymbols[2])
	assert.Equal(t, packRef(23, 3), minReq.Timeseries[0].LabelSymbols[3])

	require.Len(t, minReq.Timeseries[1].LabelSymbols, 4)
	assert.Equal(t, packRef(0, 8), minReq.Timeseries[1].LabelSymbols[0])
	assert.Equal(t, packRef(26, 8), minReq.Timeseries[1].LabelSymbols[1])
	assert.Equal(t, packRef(16, 7), minReq.Timeseries[1].LabelSymbols[2])
	assert.Equal(t, packRef(23, 3), minReq.Timeseries[1].LabelSymbols[3])
}

func TestHyperMinimizeWriteRequest(t *testing.T) {
	req := &mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{
			{
				TimeSeries: &mimirpb.TimeSeries{
					Labels: []mimirpb.LabelAdapter{
						{Name: "__name__", Value: "metric_1"},
						{Name: "cluster", Value: "dev"},
					},
					Samples: []mimirpb.Sample{
						{TimestampMs: 10, Value: 20},
						{TimestampMs: 20, Value: 30},
					},
				},
			}, {
				TimeSeries: &mimirpb.TimeSeries{
					Labels: []mimirpb.LabelAdapter{
						{Name: "__name__", Value: "metric_2"},
						{Name: "cluster", Value: "dev"},
					},
					Samples: []mimirpb.Sample{
						{TimestampMs: 30, Value: 40},
						{TimestampMs: 40, Value: 50},
					},
				},
			},
		},
	}

	minReq := hyperMinimizeWriteRequest(req)
	assert.Equal(t, "__name__metric_1clusterdevmetric_2", minReq.Symbols)
	assert.Equal(t, []uint32{0, 8, 16, 23, 26}, minReq.SymbolOffsets)
	assert.Equal(t, []uint32{8, 8, 7, 3, 8}, minReq.SymbolLength)

	require.Len(t, minReq.Timeseries, 2)
	assert.Equal(t, []uint32{0, 1, 2, 3}, minReq.Timeseries[0].LabelSymbolIds)
	assert.Equal(t, []uint32{0, 4, 2, 3}, minReq.Timeseries[1].LabelSymbolIds)
}

func TestPackAndUnpackRef(t *testing.T) {
	tests := []struct {
		offset int
		length int
	}{
		{0, 0},
		{0, 10},
		{10, 0},
		{10, 10},
		{512, 512},
		{(1024 * 1024) - 1, 4096},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("offset = %d length = %d", test.offset, test.length), func(t *testing.T) {
			ref := packRef(test.offset, test.length)
			actualOffset, actualLength := unpackRef(ref)

			assert.Equal(t, test.offset, actualOffset)
			assert.Equal(t, test.length, actualLength)
		})
	}
}
