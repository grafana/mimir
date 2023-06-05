// SPDX-License-Identifier: AGPL-3.0-only

package client

import (
	"context"
	"fmt"

	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// SerializedWriteRequest is the wire format of a mimirpb.WriteRequest.
// The main purpose of defining this type is to be able to store the pre-serialized write request,
// so that the client can reuse serialization buffers.
type SerializedWriteRequest struct {
	Payload []byte
}

// Marshal returns the wire format of this message.
func (m *SerializedWriteRequest) Marshal() (data []byte, err error) {
	return m.Payload, nil
}
func (m *SerializedWriteRequest) Reset() {}
func (m *SerializedWriteRequest) String() string {
	return fmt.Sprintf("&SerializedWriteRequest{Payload: %v}", m.Payload)
}
func (m *SerializedWriteRequest) ProtoMessage() {}

type IngesterClientExt interface {
	IngesterClient
	PushSerialized(ctx context.Context, in *SerializedWriteRequest, opts ...grpc.CallOption) (*mimirpb.WriteResponse, error)
}

func (c *ingesterClient) PushSerialized(ctx context.Context, in *SerializedWriteRequest, opts ...grpc.CallOption) (*mimirpb.WriteResponse, error) {
	out := new(mimirpb.WriteResponse)
	err := c.cc.Invoke(ctx, "/cortex.Ingester/Push", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}
