// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"errors"
	"fmt"

	"github.com/gogo/protobuf/proto"
)

const (
	// backendTypeOpaque forwards requests without decoding them.
	backendTypeOpaque = "opaque"
)

var errUnknownMethod = errors.New("unknown method")

// MessageCodec decodes the gRPC messages of one backend type.
type MessageCodec interface {
	// DecodeRequest decodes a request message of fullMethod.
	// It returns errUnknownMethod if the codec does not know fullMethod.
	DecodeRequest(fullMethod string, data []byte) (proto.Message, error)

	// DecodeResponse decodes a response message of fullMethod.
	// It returns errUnknownMethod if the codec does not know fullMethod.
	DecodeResponse(fullMethod string, data []byte) (proto.Message, error)
}

// newMessageCodec returns the codec for backendType.
// It returns a nil codec for backendTypeOpaque.
func newMessageCodec(backendType string) (MessageCodec, error) {
	switch backendType {
	case backendTypeOpaque:
		return nil, nil
	case backendTypeStoreGateway:
		return newStoreGatewayCodec(), nil
	default:
		return nil, fmt.Errorf("unknown backend type %q", backendType)
	}
}

// methodTypes holds the constructors of the request and response messages of one gRPC method.
type methodTypes struct {
	newRequest  func() proto.Message
	newResponse func() proto.Message
}

// methodTableCodec is a MessageCodec that decodes protobuf messages with a table of message types for each method.
type methodTableCodec struct {
	methods map[string]methodTypes
}

func (c methodTableCodec) DecodeRequest(fullMethod string, data []byte) (proto.Message, error) {
	types, ok := c.methods[fullMethod]
	if !ok {
		return nil, errUnknownMethod
	}
	return decodeMessage(types.newRequest(), data)
}

func (c methodTableCodec) DecodeResponse(fullMethod string, data []byte) (proto.Message, error) {
	types, ok := c.methods[fullMethod]
	if !ok {
		return nil, errUnknownMethod
	}
	return decodeMessage(types.newResponse(), data)
}

func decodeMessage(msg proto.Message, data []byte) (proto.Message, error) {
	if err := proto.Unmarshal(data, msg); err != nil {
		return nil, fmt.Errorf("failed to decode %T: %w", msg, err)
	}
	return msg, nil
}
