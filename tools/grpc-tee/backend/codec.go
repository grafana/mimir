// SPDX-License-Identifier: AGPL-3.0-only

package backend

import (
	"fmt"

	"github.com/gogo/protobuf/proto"
)

// MethodCodec decodes the gRPC messages of one backend type.
type MethodCodec interface {
	// DecodeRequest decodes a request message of fullMethod.
	// It returns errUnknownMethod if the codec does not know fullMethod.
	DecodeRequest(fullMethod string, data []byte) (proto.Message, error)

	// DecodeResponse decodes a response message of fullMethod.
	// It returns errUnknownMethod if the codec does not know fullMethod.
	DecodeResponse(fullMethod string, data []byte) (proto.Message, error)
}

type UnknownMethodError struct {
	method string
}

func (e UnknownMethodError) Error() string {
	return fmt.Sprintf("unknown gRPC method: %s", e.method)
}

type DecodeError struct {
	msg proto.Message
	err error
}

func (e DecodeError) Error() string {
	return fmt.Sprintf("failed to decode %T: %v", e.msg, e.err)
}

func (e DecodeError) Unwrap() error { return e.err }

// MethodTypes holds factory funcs for request and response messages of one gRPC method.
type MethodTypes struct {
	NewReq func() proto.Message
	NewRes func() proto.Message
}

type MethodCodecTable struct {
	t map[string]MethodTypes
}

func NewMethodCodecTable() *MethodCodecTable {
	return &MethodCodecTable{t: make(map[string]MethodTypes)}
}

func (mct *MethodCodecTable) RegisterMethodCodec(fullMethod string, types MethodTypes) {
	mct.t[fullMethod] = types
}

func (mct *MethodCodecTable) DecodeRequest(fullMethod string, data []byte) (proto.Message, error) {
	types, ok := mct.t[fullMethod]
	if !ok {
		return nil, &UnknownMethodError{fullMethod}
	}
	return decodeMessage(types.NewReq(), data)
}

func (mct *MethodCodecTable) DecodeResponse(fullMethod string, data []byte) (proto.Message, error) {
	types, ok := mct.t[fullMethod]
	if !ok {
		return nil, &UnknownMethodError{fullMethod}
	}
	return decodeMessage(types.NewRes(), data)
}

func decodeMessage(msg proto.Message, data []byte) (proto.Message, error) {
	if err := proto.Unmarshal(data, msg); err != nil {
		return nil, DecodeError{msg: msg, err: err}
	}
	return msg, nil
}
