// SPDX-License-Identifier: AGPL-3.0-only

package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHashRangeStatsResponse_MarshalUnmarshal(t *testing.T) {
	original := &HashRangeStatsResponse{
		Rates: []HashRangeRate{
			{Lo: 0, Hi: 1000, ActiveSeries: 12345},
			{Lo: 1001, Hi: 2000, ActiveSeries: 0},
			{Lo: 2001, Hi: 4294967295, ActiveSeries: 9876543210},
		},
		TotalActiveSeries: 9876555555,
	}

	data, err := original.Marshal()
	require.NoError(t, err)

	restored := &HashRangeStatsResponse{}
	err = restored.Unmarshal(data)
	require.NoError(t, err)

	assert.Equal(t, int64(9876555555), restored.TotalActiveSeries)
	require.Len(t, restored.Rates, 3)
	assert.Equal(t, uint32(0), restored.Rates[0].Lo)
	assert.Equal(t, uint32(1000), restored.Rates[0].Hi)
	assert.Equal(t, int64(12345), restored.Rates[0].ActiveSeries)

	assert.Equal(t, uint32(1001), restored.Rates[1].Lo)
	assert.Equal(t, uint32(2000), restored.Rates[1].Hi)
	assert.Equal(t, int64(0), restored.Rates[1].ActiveSeries)

	assert.Equal(t, uint32(2001), restored.Rates[2].Lo)
	assert.Equal(t, uint32(4294967295), restored.Rates[2].Hi)
	assert.Equal(t, int64(9876543210), restored.Rates[2].ActiveSeries)
}

func TestHashRangeStatsResponse_EmptyRoundTrip(t *testing.T) {
	original := &HashRangeStatsResponse{}

	data, err := original.Marshal()
	require.NoError(t, err)
	assert.Empty(t, data)

	restored := &HashRangeStatsResponse{}
	err = restored.Unmarshal(data)
	require.NoError(t, err)
	assert.Empty(t, restored.Rates)
}

func TestHashRangeStatsResponse_Size(t *testing.T) {
	resp := &HashRangeStatsResponse{
		Rates: []HashRangeRate{
			{Lo: 0, Hi: 1000, ActiveSeries: 1},
			{Lo: 1001, Hi: 2000, ActiveSeries: 2},
		},
	}

	size := resp.Size()
	data, err := resp.Marshal()
	require.NoError(t, err)
	assert.Equal(t, size, len(data))
}

func TestSetHashRangesRequest_MarshalUnmarshal(t *testing.T) {
	original := &SetHashRangesRequest{
		Ranges: []HashRangeEntry{
			{Lo: 0, Hi: 1000},
			{Lo: 1001, Hi: 4294967295},
		},
	}

	data, err := original.Marshal()
	require.NoError(t, err)
	require.NotEmpty(t, data)

	restored := &SetHashRangesRequest{}
	err = restored.Unmarshal(data)
	require.NoError(t, err)

	require.Len(t, restored.Ranges, 2)
	assert.Equal(t, uint32(0), restored.Ranges[0].Lo)
	assert.Equal(t, uint32(1000), restored.Ranges[0].Hi)
	assert.Equal(t, uint32(1001), restored.Ranges[1].Lo)
	assert.Equal(t, uint32(4294967295), restored.Ranges[1].Hi)
}

func TestSetHashRangesRequest_Size(t *testing.T) {
	req := &SetHashRangesRequest{
		Ranges: []HashRangeEntry{
			{Lo: 0, Hi: 1000},
			{Lo: 1001, Hi: 4294967295},
		},
	}

	size := req.Size()
	data, err := req.Marshal()
	require.NoError(t, err)
	assert.Equal(t, size, len(data))
}

func TestSetHashRangesResponse_MarshalUnmarshal(t *testing.T) {
	resp := &SetHashRangesResponse{}

	data, err := resp.Marshal()
	require.NoError(t, err)
	assert.Empty(t, data)

	err = resp.Unmarshal(data)
	require.NoError(t, err)
}
