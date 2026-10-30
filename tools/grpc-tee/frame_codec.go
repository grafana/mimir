// SPDX-License-Identifier: AGPL-3.0-only

package grpctee

import (
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
)

// Frame holds the raw bytes of one gRPC message.
// The frame codec fills it without any decoding.
type Frame []byte

// FrameCodec handles messages as raw bytes.
type FrameCodec struct{}

func (FrameCodec) Name() string { return "proto" }
func (FrameCodec) Marshal(v any) ([]byte, error) {
	if f, ok := v.(*Frame); ok {
		return *f, nil
	}
	data, err := encoding.GetCodecV2("proto").Marshal(v)
	if err != nil {
		return nil, err
	}
	defer data.Free()
	return data.Materialize(), nil
}

func (FrameCodec) Unmarshal(data []byte, v any) error {
	if f, ok := v.(*Frame); ok {
		*f = append((*f)[:0], data...)
		return nil
	}
	return encoding.GetCodecV2("proto").Unmarshal(mem.BufferSlice{mem.SliceBuffer(data)}, v)
}
