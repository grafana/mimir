// SPDX-License-Identifier: AGPL-3.0-only

package hlltracker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/distributor/hlltracker/hyperloglog"
)

func TestPartitionHLLCodec_EncodeDecode(t *testing.T) {
	codec := NewPartitionHLLCodec()

	// Create test state
	hll := hyperloglog.New(11)
	for i := uint32(0); i < 100; i++ {
		hll.Add(i)
	}

	originalState := &PartitionHLLState{
		PartitionID: 42,
		UnixMinute:  time.Now().Unix() / 60,
		HLL:         hll,
		UpdatedAtMs: time.Now().UnixMilli(),
	}

	// Encode
	data, err := codec.Encode(originalState)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	// Decode
	decoded, err := codec.Decode(data)
	require.NoError(t, err)
	require.NotNil(t, decoded)

	decodedState, ok := decoded.(*PartitionHLLState)
	require.True(t, ok, "decoded object should be *PartitionHLLState")

	// Verify fields
	assert.Equal(t, originalState.PartitionID, decodedState.PartitionID)
	assert.Equal(t, originalState.UnixMinute, decodedState.UnixMinute)
	assert.Equal(t, originalState.UpdatedAtMs, decodedState.UpdatedAtMs)
	assert.Equal(t, originalState.HLL.B, decodedState.HLL.B)
	assert.Equal(t, originalState.HLL.M, decodedState.HLL.M)
	assert.Equal(t, originalState.HLL.Registers, decodedState.HLL.Registers)

	// Verify cardinality is preserved
	assert.Equal(t, originalState.HLL.Count(), decodedState.HLL.Count())
}

func TestPartitionHLLCodec_EmptyHLL(t *testing.T) {
	codec := NewPartitionHLLCodec()

	// Create empty HLL
	hll := hyperloglog.New(11)

	state := &PartitionHLLState{
		PartitionID: 1,
		UnixMinute:  12345,
		HLL:         hll,
		UpdatedAtMs: 67890,
	}

	// Encode
	data, err := codec.Encode(state)
	require.NoError(t, err)

	// Decode
	decoded, err := codec.Decode(data)
	require.NoError(t, err)

	decodedState := decoded.(*PartitionHLLState)
	assert.Equal(t, uint64(0), decodedState.HLL.Count())
}

func TestPartitionHLLCodec_DifferentPrecisions(t *testing.T) {
	codec := NewPartitionHLLCodec()

	precisions := []uint8{4, 8, 11, 14, 16}

	for _, precision := range precisions {
		t.Run(string(rune(precision+'0')), func(t *testing.T) {
			hll := hyperloglog.New(precision)
			for i := uint32(0); i < 50; i++ {
				hll.Add(i)
			}

			state := &PartitionHLLState{
				PartitionID: 5,
				UnixMinute:  100,
				HLL:         hll,
				UpdatedAtMs: 200,
			}

			// Encode
			data, err := codec.Encode(state)
			require.NoError(t, err)

			// Decode
			decoded, err := codec.Decode(data)
			require.NoError(t, err)

			decodedState := decoded.(*PartitionHLLState)
			assert.Equal(t, precision, decodedState.HLL.B)
			assert.Equal(t, uint32(1<<precision), decodedState.HLL.M)
		})
	}
}

func TestPartitionHLLCodec_InvalidInput(t *testing.T) {
	codec := NewPartitionHLLCodec()

	tests := []struct {
		name  string
		input []byte
		error string
	}{
		{
			name:  "empty data",
			input: []byte{},
			error: "data too short",
		},
		{
			name:  "too short",
			input: make([]byte, 10),
			error: "data too short",
		},
		{
			name:  "nil",
			input: nil,
			error: "data too short",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := codec.Decode(tt.input)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.error)
		})
	}
}

func TestPartitionHLLCodec_EncodeInvalidInput(t *testing.T) {
	codec := NewPartitionHLLCodec()

	tests := []struct {
		name  string
		input interface{}
		error string
	}{
		{
			name:  "wrong type",
			input: "not a state",
			error: "expected *PartitionHLLState",
		},
		{
			name:  "nil HLL",
			input: &PartitionHLLState{PartitionID: 1, HLL: nil},
			error: "HLL is nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := codec.Encode(tt.input)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.error)
		})
	}
}

func TestPartitionHLLCodec_CodecID(t *testing.T) {
	codec := NewPartitionHLLCodec()
	assert.Equal(t, "partition-hll-state", codec.CodecID())
}

func TestPartitionHLLCodec_RoundTripLargeCardinality(t *testing.T) {
	codec := NewPartitionHLLCodec()

	// Create HLL with many series
	hll := hyperloglog.New(14)
	for i := uint32(0); i < 100000; i++ {
		hll.Add(i)
	}

	state := &PartitionHLLState{
		PartitionID: 999,
		UnixMinute:  54321,
		HLL:         hll,
		UpdatedAtMs: 98765,
	}

	// Encode
	data, err := codec.Encode(state)
	require.NoError(t, err)

	// Decode
	decoded, err := codec.Decode(data)
	require.NoError(t, err)

	decodedState := decoded.(*PartitionHLLState)

	// Verify cardinality is approximately preserved (within HLL error)
	original := state.HLL.Count()
	recovered := decodedState.HLL.Count()
	errorPct := float64(abs(int64(original)-int64(recovered))) / float64(original) * 100

	// For precision 14 with 100K items, error should be < 5%
	assert.Less(t, errorPct, 5.0, "cardinality error should be < 5%%")
}

func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}
