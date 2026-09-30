// SPDX-License-Identifier: AGPL-3.0-only

package service

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/mem"

	// Registers Mimir's protobuf codec, which marshals the gogo messages without reflection.
	_ "github.com/grafana/mimir/pkg/mimirpb"
)

// RawMessage is a response the service encoded itself, sent as it is: QueryStream responses are
// written straight from the store's encoded labels and chunks rather than built as messages and
// marshalled again.
type RawMessage []byte

type codec struct {
	base encoding.CodecV2
}

func (c codec) Marshal(v any) (mem.BufferSlice, error) {
	if raw, ok := v.(RawMessage); ok {
		return mem.BufferSlice{mem.SliceBuffer(raw)}, nil
	}
	return c.base.Marshal(v)
}

func (c codec) Unmarshal(data mem.BufferSlice, v any) error {
	return c.base.Unmarshal(data, v)
}

func (c codec) Name() string { return c.base.Name() }

// ServerCodec is the server option that sends RawMessage responses as they are and every other
// message with Mimir's protobuf codec.
func ServerCodec() grpc.ServerOption {
	return grpc.ForceServerCodecV2(codec{base: encoding.GetCodecV2(proto.Name)})
}
