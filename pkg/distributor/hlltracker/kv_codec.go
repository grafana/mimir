// SPDX-License-Identifier: AGPL-3.0-only

package hlltracker

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/grafana/dskit/kv/codec"

	"github.com/grafana/mimir/pkg/distributor/hlltracker/hyperloglog"
)

// PartitionHLLState represents the HyperLogLog state for a single partition and minute.
// This is stored in the memberlist KV store and synchronized across distributors.
type PartitionHLLState struct {
	PartitionID  int32
	UnixMinute   int64
	HLL          *hyperloglog.HyperLogLog
	UpdatedAtMs  int64
}

// partitionHLLCodec implements codec.Codec for PartitionHLLState.
type partitionHLLCodec struct{}

// NewPartitionHLLCodec creates a new codec for PartitionHLLState.
func NewPartitionHLLCodec() codec.Codec {
	return &partitionHLLCodec{}
}

// Decode deserializes a PartitionHLLState from bytes.
// Format:
// - partition_id (int32, 4 bytes)
// - unix_minute (int64, 8 bytes)
// - precision (uint8, 1 byte)
// - updated_at_ms (int64, 8 bytes)
// - hll_registers_len (int32, 4 bytes)
// - hll_registers (bytes, variable length)
func (c *partitionHLLCodec) Decode(data []byte) (interface{}, error) {
	if len(data) < 25 { // Minimum: 4 + 8 + 1 + 8 + 4 = 25 bytes
		return nil, fmt.Errorf("data too short: expected at least 25 bytes, got %d", len(data))
	}

	buf := bytes.NewReader(data)

	var partitionID int32
	if err := binary.Read(buf, binary.LittleEndian, &partitionID); err != nil {
		return nil, fmt.Errorf("failed to read partition_id: %w", err)
	}

	var unixMinute int64
	if err := binary.Read(buf, binary.LittleEndian, &unixMinute); err != nil {
		return nil, fmt.Errorf("failed to read unix_minute: %w", err)
	}

	var precision uint8
	if err := binary.Read(buf, binary.LittleEndian, &precision); err != nil {
		return nil, fmt.Errorf("failed to read precision: %w", err)
	}

	var updatedAtMs int64
	if err := binary.Read(buf, binary.LittleEndian, &updatedAtMs); err != nil {
		return nil, fmt.Errorf("failed to read updated_at_ms: %w", err)
	}

	var registersLen int32
	if err := binary.Read(buf, binary.LittleEndian, &registersLen); err != nil {
		return nil, fmt.Errorf("failed to read registers_len: %w", err)
	}

	if registersLen < 0 || registersLen > 1<<20 { // Sanity check: max 1MB
		return nil, fmt.Errorf("invalid registers_len: %d", registersLen)
	}

	registers := make([]byte, registersLen)
	if _, err := buf.Read(registers); err != nil {
		return nil, fmt.Errorf("failed to read registers: %w", err)
	}

	// Reconstruct HyperLogLog
	hll := hyperloglog.New(precision)
	if int32(len(hll.Registers)) != registersLen {
		return nil, fmt.Errorf("register count mismatch: expected %d, got %d", len(hll.Registers), registersLen)
	}
	copy(hll.Registers, registers)

	return &PartitionHLLState{
		PartitionID: partitionID,
		UnixMinute:  unixMinute,
		HLL:         hll,
		UpdatedAtMs: updatedAtMs,
	}, nil
}

// Encode serializes a PartitionHLLState to bytes.
func (c *partitionHLLCodec) Encode(obj interface{}) ([]byte, error) {
	state, ok := obj.(*PartitionHLLState)
	if !ok {
		return nil, fmt.Errorf("expected *PartitionHLLState, got %T", obj)
	}

	if state.HLL == nil {
		return nil, fmt.Errorf("HLL is nil")
	}

	// Calculate size
	size := 4 + 8 + 1 + 8 + 4 + len(state.HLL.Registers)
	buf := bytes.NewBuffer(make([]byte, 0, size))

	if err := binary.Write(buf, binary.LittleEndian, state.PartitionID); err != nil {
		return nil, fmt.Errorf("failed to write partition_id: %w", err)
	}

	if err := binary.Write(buf, binary.LittleEndian, state.UnixMinute); err != nil {
		return nil, fmt.Errorf("failed to write unix_minute: %w", err)
	}

	if err := binary.Write(buf, binary.LittleEndian, state.HLL.B); err != nil {
		return nil, fmt.Errorf("failed to write precision: %w", err)
	}

	if err := binary.Write(buf, binary.LittleEndian, state.UpdatedAtMs); err != nil {
		return nil, fmt.Errorf("failed to write updated_at_ms: %w", err)
	}

	registersLen := int32(len(state.HLL.Registers))
	if err := binary.Write(buf, binary.LittleEndian, registersLen); err != nil {
		return nil, fmt.Errorf("failed to write registers_len: %w", err)
	}

	if _, err := buf.Write(state.HLL.Registers); err != nil {
		return nil, fmt.Errorf("failed to write registers: %w", err)
	}

	return buf.Bytes(), nil
}

// CodecID returns the codec ID for PartitionHLLState.
func (c *partitionHLLCodec) CodecID() string {
	return "partition-hll-state"
}
