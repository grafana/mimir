// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"fmt"

	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/mem"
	protobufproto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
)

func init() {
	c := encoding.GetCodecV2(proto.Name)
	encoding.RegisterCodecV2(&disablePoolingOnUnmarshalWorkaroundCodec{inner: c})
}

type disablePoolingOnUnmarshalWorkaroundCodec struct {
	inner encoding.CodecV2
}

func (c *disablePoolingOnUnmarshalWorkaroundCodec) Marshal(v any) (out mem.BufferSlice, err error) {
	return c.inner.Marshal(v)
}

func messageV2Of(v any) protobufproto.Message {
	switch v := v.(type) {
	case protoadapt.MessageV1:
		return protoadapt.MessageV2Of(v)
	case protoadapt.MessageV2:
		return v
	default:
		return nil
	}
}

// Unmarshal is the same as google.golang.org/grpc/encoding/proto.codecV2's Unmarshal, but without buffer reuse,
// as explained below.
func (c *disablePoolingOnUnmarshalWorkaroundCodec) Unmarshal(data mem.BufferSlice, v any) error {
	// Not freeing buf below ensures the buffer is never reused, which allows us to continue
	// using our custom unmarshalling logic that retains a reference the underlying byte slice to avoid copying.
	//
	// This is a temporary workaround until we've modified all instances of this unmarshalling logic to use something that
	// only returns the buffer to the pool when the response is no longer required.
	//
	// Once we've fixed all instances of this unmarshalling logic, disablePoolingOnUnmarshalWorkaroundCodec is no longer
	// required.

	vv := messageV2Of(v)
	if vv == nil {
		return fmt.Errorf("failed to unmarshal, message is %T, want proto.Message", v)
	}

	buf := data.MaterializeToBuffer(mem.DefaultBufferPool())
	// HACK: never call buf.Free() - see above.

	return protobufproto.Unmarshal(buf.ReadOnlyData(), vv)
}

func (c *disablePoolingOnUnmarshalWorkaroundCodec) Name() string {
	return c.inner.Name()
}
