// SPDX-License-Identifier: AGPL-3.0-only

package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHashRangeStatsResponse_MarshalUnmarshal(t *testing.T) {
	original := &HashRangeStatsResponse{
		NumBuckets:       256,
		SamplesPerSecond: make([]float64, 256),
	}
	original.SamplesPerSecond[0] = 100.5
	original.SamplesPerSecond[127] = 200.25
	original.SamplesPerSecond[255] = 300.75

	data, err := original.Marshal()
	require.NoError(t, err)

	restored := &HashRangeStatsResponse{}
	err = restored.Unmarshal(data)
	require.NoError(t, err)

	assert.Equal(t, original.NumBuckets, restored.NumBuckets)
	require.Len(t, restored.SamplesPerSecond, 256)
	assert.InDelta(t, 100.5, restored.SamplesPerSecond[0], 0.001)
	assert.InDelta(t, 200.25, restored.SamplesPerSecond[127], 0.001)
	assert.InDelta(t, 300.75, restored.SamplesPerSecond[255], 0.001)

	// Verify zero buckets are zero.
	assert.Equal(t, 0.0, restored.SamplesPerSecond[1])
	assert.Equal(t, 0.0, restored.SamplesPerSecond[254])
}

func TestHashRangeStatsResponse_EmptyRoundTrip(t *testing.T) {
	original := &HashRangeStatsResponse{}

	data, err := original.Marshal()
	require.NoError(t, err)
	assert.Empty(t, data)

	restored := &HashRangeStatsResponse{}
	err = restored.Unmarshal(data)
	require.NoError(t, err)

	assert.Equal(t, uint32(0), restored.NumBuckets)
	assert.Empty(t, restored.SamplesPerSecond)
}

func TestHashRangeStatsRequest_MarshalUnmarshal(t *testing.T) {
	req := &HashRangeStatsRequest{}

	data, err := req.Marshal()
	require.NoError(t, err)
	assert.Empty(t, data)

	err = req.Unmarshal(data)
	require.NoError(t, err)
}

func TestHashRangeStatsResponse_Size(t *testing.T) {
	resp := &HashRangeStatsResponse{
		NumBuckets:       256,
		SamplesPerSecond: make([]float64, 256),
	}
	resp.SamplesPerSecond[0] = 1.0

	size := resp.Size()
	data, err := resp.Marshal()
	require.NoError(t, err)
	assert.Equal(t, size, len(data))
}
