// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/siderolabs/grpc-proxy/blob/v0.5.2/proxy/codec.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Siderolabs.

package main

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/encoding"
	gproto "google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/mem"
)

// frame is one gRPC message that the proxy forwards without decoding it.
type frame struct {
	payload []byte
}

// newFrameCodec returns a codec that marshals and unmarshals frames as raw bytes.
// For all other message types, it uses the default protobuf codec.
func newFrameCodec() encoding.CodecV2 {
	parent := encoding.GetCodecV2(gproto.Name)
	if parent == nil {
		panic(errors.New(`no codec named "proto" found`))
	}
	return &frameCodec{parent: parent}
}

type frameCodec struct {
	parent encoding.CodecV2
}

func (c *frameCodec) Marshal(v any) (mem.BufferSlice, error) {
	f, ok := v.(*frame)
	if !ok {
		return c.parent.Marshal(v)
	}
	// gRPC can write the buffer after SendMsg returns. This is safe because the proxy never changes a payload after
	// it received it, so the codec does not copy the payload.
	return mem.BufferSlice{mem.SliceBuffer(f.payload)}, nil
}

func (c *frameCodec) Unmarshal(data mem.BufferSlice, v any) error {
	f, ok := v.(*frame)
	if !ok {
		return c.parent.Unmarshal(data, v)
	}
	// Materialize copies the data, so the payload does not refer to a pooled gRPC buffer.
	f.payload = data.Materialize()
	return nil
}

func (c *frameCodec) Name() string {
	return fmt.Sprintf("proxy>%s", c.parent.Name())
}
