// Copyright 2024 Siderolabs. All Rights Reserved.
// See LICENSE for licensing terms.

package proxy

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/encoding"
	gproto "google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/mem"
)

// Codec returns a proxying [encoding.CodecV2] with the default protobuf codec as parent.
//
// See [CodecWithParent].
func Codec() encoding.CodecV2 {
	if c := encoding.GetCodecV2(gproto.Name); c != nil {
		return CodecWithParent(c)
	}

	panic(errors.New(`no codec named "proto" found`))
}

// CodecWithParent returns a proxying [encoding.CodecV2] with a user provided codec as parent.
//
// This codec is *crucial* to the functioning of the proxy. It allows the proxy server to be oblivious
// to the schema of the forwarded messages. It basically treats a gRPC message frame as raw bytes.
// However, if the server handler, or the client caller are not proxy-internal functions it will fall back
// to trying to decode the message using a fallback codec.
func CodecWithParent(fallback encoding.CodecV2) encoding.CodecV2 {
	return &rawCodec{parentCodec: fallback}
}

type rawCodec struct {
	parentCodec encoding.CodecV2
}

type frame struct {
	payload []byte
}

// NewFrame constructs a frame for raw codec.
func NewFrame(payload []byte) any {
	return &frame{payload: payload}
}

func (c *rawCodec) Marshal(v any) (data mem.BufferSlice, err error) {
	f, ok := v.(*frame)
	if !ok {
		return c.parentCodec.Marshal(v)
	}

	if mem.IsBelowBufferPoolingThreshold(len(f.payload)) {
		data = append(data, mem.SliceBuffer(f.payload))
	} else {
		pool := mem.DefaultBufferPool()
		buf := pool.Get(len(f.payload))
		copy(*buf, f.payload)

		data = append(data, mem.NewBuffer(buf, pool))
	}

	return data, nil
}

func (c *rawCodec) Unmarshal(data mem.BufferSlice, v any) error {
	dst, ok := v.(*frame)
	if !ok {
		return c.parentCodec.Unmarshal(data, v)
	}

	dst.payload = data.Materialize()

	return nil
}

func (c *rawCodec) Name() string {
	return fmt.Sprintf("proxy>%s", c.parentCodec.Name())
}
