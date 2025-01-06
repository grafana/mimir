package csproto

import (
	"errors"

	"google.golang.org/protobuf/proto"
)

var (
	// ErrMarshaler is returned the Marshal() function when a message is passed in that does not
	// match any of the supported behaviors
	ErrMarshaler = errors.New("message does not implement csproto.Marshaler")
	// ErrUnmarshaler is returned the Unmarshal() function when a message is passed in that does not
	// match any of the supported behaviors
	ErrUnmarshaler = errors.New("message does not implement csproto.Unmarshaler")
)

// ProtoV1Sizer defines the interface for a type that provides custom Protobuf V1 sizing logic.
type ProtoV1Sizer interface {
	XXX_Size() int
}

// ProtoV1Marshaler defines the interface for a type that provides custom Protobuf V1 marshaling logic.
type ProtoV1Marshaler interface {
	ProtoV1Sizer
	XXX_Marshal(b []byte, deterministic bool) ([]byte, error)
}

// ProtoV1Unmarshaler defines the interface a type that provides custom Protobuf V1 unmarshaling.
type ProtoV1Unmarshaler interface {
	XXX_Unmarshal([]byte) error
}

// Sizer defines a message that can pre-calculate the required size for storing the binary Protobuf
// encoding of its contents.
type Sizer interface {
	// Size returns the size, in bytes, required to serialize a message into binary Protobuf format.
	Size() int
}

// Marshaler defines a message that can marshal itself into binary Protobuf format.
type Marshaler interface {
	// Marshal allocates a buffer large enough to hold this message, writes the contents of the
	// message into it, then returns the buffer.
	Marshal() ([]byte, error)
}

// MarshalerTo defines a message that can marshal itself into a provided buffer in binary Protobuf format.
type MarshalerTo interface {
	// MarshalTo writes the contents of this message into dest using the binary Protobuf encoding.
	//
	// It is up to the caller to ensure that the buffer is large enough to hold the serialized message
	// data.
	MarshalTo(dest []byte) error
}

// Unmarshaler defines a message that can unmarshal binary Protobuf data into itself.
type Unmarshaler interface {
	// Unmarshal decodes the binary Protobuf data into this message.
	Unmarshal([]byte) error
}

// Marshal marshals msg to binary Protobuf format, delegating to the appropriate underlying
// Protobuf API based on the concrete type of msg.
func Marshal(msg interface{}) ([]byte, error) {
	if pm, ok := msg.(Marshaler); ok {
		return pm.Marshal()
	}

	if pm, ok := msg.(ProtoV1Marshaler); ok {
		siz := pm.XXX_Size()
		b := make([]byte, 0, siz)
		return pm.XXX_Marshal(b, false)
	}

	if pm, ok := msg.(proto.Message); ok {
		return proto.Marshal(pm)
	}

	return nil, ErrMarshaler
}

// Unmarshal decodes the specified Protobuf data into msg, delegating to the appropriate underlying
// Protobuf API based on the concrete type of msg.
func Unmarshal(data []byte, msg interface{}) error {
	if pu, ok := msg.(Unmarshaler); ok {
		return pu.Unmarshal(data)
	}

	if pu, ok := msg.(ProtoV1Unmarshaler); ok {
		return pu.XXX_Unmarshal(data)
	}

	if pm, ok := msg.(proto.Message); ok {
		return proto.Unmarshal(data, pm)
	}

	return ErrUnmarshaler
}
