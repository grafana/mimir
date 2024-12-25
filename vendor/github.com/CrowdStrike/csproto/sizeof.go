package csproto

import (
	"math/bits"

	"google.golang.org/protobuf/proto"
)

// SizeOfTagKey returns the number of bytes required to hold the Protobuf varint encoding of k.
func SizeOfTagKey(k int) int {
	return SizeOfVarint(uint64(uint(k) << 3))
}

// SizeOfVarint returns the number of bytes required to hold the Protobuf varint encoding of v.
func SizeOfVarint(v uint64) int {
	return (bits.Len64(v|1) + 6) / 7
}

// SizeOfZigZag returns the number of bytes required to hold the zig zag encoding of v.
func SizeOfZigZag(v uint64) int {
	return SizeOfVarint((v << 1) ^ uint64((int64(v) >> 63)))
}

// Size returns the encoded size of msg.
func Size(msg interface{}) int {
	if pm, ok := msg.(Sizer); ok {
		return pm.Size()
	}

	if pm, ok := msg.(ProtoV1Sizer); ok {
		return pm.XXX_Size()
	}

	if pm, ok := msg.(proto.Message); ok {
		return proto.Size(pm)
	}

	return 0
}
