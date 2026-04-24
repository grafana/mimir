// This file provides gogo-protobuf compatibility methods for standard protobuf types.
// Mimir's own .pb.go files are generated with protoc-gen-gogo (gogoslick), which emits
// code that calls Marshal/Unmarshal/Size methods directly on embedded message fields.
// Since alertmanager v0.32.0 switched clusterpb to standard protoc-gen-go, those methods
// no longer exist. This shim bridges the gap so the generated code compiles unmodified.
//
// This file lives in vendor/ and must be re-added after running `go mod vendor`.

package clusterpb

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

var marshalOpts = proto.MarshalOptions{Deterministic: true}

// marshalToSizedBuffer serializes m into the tail of dAtA, matching the gogo-proto
// MarshalToSizedBuffer contract: bytes are written right-aligned and the number of
// bytes written is returned.
func marshalToSizedBuffer(m proto.Message, dAtA []byte) (int, error) {
	b, err := marshalOpts.Marshal(m)
	if err != nil {
		return 0, err
	}
	if len(b) > len(dAtA) {
		return 0, fmt.Errorf("proto: %T marshaled to %d bytes, buffer is %d", m, len(b), len(dAtA))
	}
	copy(dAtA[len(dAtA)-len(b):], b)
	return len(b), nil
}

// --- FullState compat ---

func (m *FullState) Marshal() ([]byte, error) {
	return marshalOpts.Marshal(m)
}

func (m *FullState) MarshalTo(dAtA []byte) (int, error) {
	return m.MarshalToSizedBuffer(dAtA[:proto.Size(m)])
}

func (m *FullState) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	return marshalToSizedBuffer(m, dAtA)
}

func (m *FullState) Size() int {
	return proto.Size(m)
}

func (m *FullState) Unmarshal(dAtA []byte) error {
	return proto.Unmarshal(dAtA, m)
}

// --- Part compat ---

func (m *Part) Marshal() ([]byte, error) {
	return marshalOpts.Marshal(m)
}

func (m *Part) MarshalTo(dAtA []byte) (int, error) {
	return m.MarshalToSizedBuffer(dAtA[:proto.Size(m)])
}

func (m *Part) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	return marshalToSizedBuffer(m, dAtA)
}

func (m *Part) Size() int {
	return proto.Size(m)
}

func (m *Part) Unmarshal(dAtA []byte) error {
	return proto.Unmarshal(dAtA, m)
}
