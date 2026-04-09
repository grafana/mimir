// SPDX-License-Identifier: AGPL-3.0-only

package client

import (
	"context"
	"encoding/json"
	"math"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// HashRangeStatsRequest is the request message for the HashRangeStats RPC.
type HashRangeStatsRequest struct{}

// HashRangeStatsResponse is the response message for the HashRangeStats RPC.
// It contains a fixed-resolution histogram of ingestion rates across the
// 32-bit hash space.
type HashRangeStatsResponse struct {
	NumBuckets       uint32    `json:"num_buckets"`
	SamplesPerSecond []float64 `json:"samples_per_second"`
}

func (m *HashRangeStatsRequest) Reset()         {}
func (m *HashRangeStatsRequest) String() string { return "{}" }
func (m *HashRangeStatsRequest) ProtoMessage()  {}
func (m *HashRangeStatsRequest) Marshal() ([]byte, error) {
	return nil, nil
}
func (m *HashRangeStatsRequest) MarshalTo(dAtA []byte) (int, error) {
	return 0, nil
}
func (m *HashRangeStatsRequest) Unmarshal(dAtA []byte) error {
	return nil
}
func (m *HashRangeStatsRequest) Size() int {
	return 0
}

func (m *HashRangeStatsResponse) Reset()         { *m = HashRangeStatsResponse{} }
func (m *HashRangeStatsResponse) String() string { b, _ := json.Marshal(m); return string(b) }
func (m *HashRangeStatsResponse) ProtoMessage()  {}

func (m *HashRangeStatsResponse) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *HashRangeStatsResponse) MarshalTo(dAtA []byte) (int, error) {
	var i int

	// Field 1: num_buckets (varint, field number 1)
	if m.NumBuckets != 0 {
		dAtA[i] = 0x08 // field 1, wire type 0
		i++
		i = encodeVarintHashRangeStats(dAtA, i, uint64(m.NumBuckets))
	}

	// Field 2: samples_per_second (packed repeated double, field number 2)
	if len(m.SamplesPerSecond) > 0 {
		dAtA[i] = 0x12 // field 2, wire type 2 (length-delimited)
		i++
		byteLen := len(m.SamplesPerSecond) * 8
		i = encodeVarintHashRangeStats(dAtA, i, uint64(byteLen))
		for _, v := range m.SamplesPerSecond {
			bits := math.Float64bits(v)
			dAtA[i] = byte(bits)
			dAtA[i+1] = byte(bits >> 8)
			dAtA[i+2] = byte(bits >> 16)
			dAtA[i+3] = byte(bits >> 24)
			dAtA[i+4] = byte(bits >> 32)
			dAtA[i+5] = byte(bits >> 40)
			dAtA[i+6] = byte(bits >> 48)
			dAtA[i+7] = byte(bits >> 56)
			i += 8
		}
	}

	return i, nil
}

func (m *HashRangeStatsResponse) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		// Read tag
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if iNdEx >= l {
				return io_ErrUnexpectedEOF()
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)

		switch fieldNum {
		case 1: // num_buckets
			if wireType != 0 {
				return errInvalidWireType
			}
			var v uint32
			for shift := uint(0); ; shift += 7 {
				if iNdEx >= l {
					return io_ErrUnexpectedEOF()
				}
				b := dAtA[iNdEx]
				iNdEx++
				v |= uint32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.NumBuckets = v
		case 2: // samples_per_second (packed doubles)
			if wireType == 2 {
				// Length-delimited (packed)
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					if iNdEx >= l {
						return io_ErrUnexpectedEOF()
					}
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				if packedLen < 0 || iNdEx+packedLen > l {
					return io_ErrUnexpectedEOF()
				}
				count := packedLen / 8
				if m.SamplesPerSecond == nil {
					m.SamplesPerSecond = make([]float64, 0, count)
				}
				for j := 0; j < count; j++ {
					bits := uint64(dAtA[iNdEx]) |
						uint64(dAtA[iNdEx+1])<<8 |
						uint64(dAtA[iNdEx+2])<<16 |
						uint64(dAtA[iNdEx+3])<<24 |
						uint64(dAtA[iNdEx+4])<<32 |
						uint64(dAtA[iNdEx+5])<<40 |
						uint64(dAtA[iNdEx+6])<<48 |
						uint64(dAtA[iNdEx+7])<<56
					iNdEx += 8
					m.SamplesPerSecond = append(m.SamplesPerSecond, math.Float64frombits(bits))
				}
			} else if wireType == 1 {
				// Single double
				if iNdEx+8 > l {
					return io_ErrUnexpectedEOF()
				}
				bits := uint64(dAtA[iNdEx]) |
					uint64(dAtA[iNdEx+1])<<8 |
					uint64(dAtA[iNdEx+2])<<16 |
					uint64(dAtA[iNdEx+3])<<24 |
					uint64(dAtA[iNdEx+4])<<32 |
					uint64(dAtA[iNdEx+5])<<40 |
					uint64(dAtA[iNdEx+6])<<48 |
					uint64(dAtA[iNdEx+7])<<56
				iNdEx += 8
				m.SamplesPerSecond = append(m.SamplesPerSecond, math.Float64frombits(bits))
			} else {
				return errInvalidWireType
			}
		default:
			// Skip unknown fields
			if wireType == 0 {
				for shift := uint(0); ; shift += 7 {
					if iNdEx >= l {
						return io_ErrUnexpectedEOF()
					}
					if dAtA[iNdEx]&0x80 == 0 {
						iNdEx++
						break
					}
					iNdEx++
				}
			} else if wireType == 1 {
				iNdEx += 8
			} else if wireType == 2 {
				var length int
				for shift := uint(0); ; shift += 7 {
					if iNdEx >= l {
						return io_ErrUnexpectedEOF()
					}
					b := dAtA[iNdEx]
					iNdEx++
					length |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				iNdEx += length
			} else if wireType == 5 {
				iNdEx += 4
			} else {
				return errInvalidWireType
			}
		}
	}
	return nil
}

func (m *HashRangeStatsResponse) Size() int {
	var n int
	if m.NumBuckets != 0 {
		n += 1 + sovHashRangeStats(uint64(m.NumBuckets))
	}
	if len(m.SamplesPerSecond) > 0 {
		byteLen := len(m.SamplesPerSecond) * 8
		n += 1 + sovHashRangeStats(uint64(byteLen)) + byteLen
	}
	return n
}

func sovHashRangeStats(x uint64) (n int) {
	return sovIngester(x)
}

func encodeVarintHashRangeStats(dAtA []byte, offset int, v uint64) int {
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return offset + 1
}

var errInvalidWireType = status.Error(codes.Internal, "invalid wire type")

func io_ErrUnexpectedEOF() error {
	return status.Error(codes.Internal, "unexpected EOF")
}

// HashRangeStatsClient is the client-side interface for the HashRangeStats RPC.
type HashRangeStatsClient interface {
	HashRangeStats(ctx context.Context, in *HashRangeStatsRequest, opts ...grpc.CallOption) (*HashRangeStatsResponse, error)
}

func (c *ingesterClient) HashRangeStats(ctx context.Context, in *HashRangeStatsRequest, opts ...grpc.CallOption) (*HashRangeStatsResponse, error) {
	out := new(HashRangeStatsResponse)
	err := c.cc.Invoke(ctx, "/cortex.Ingester/HashRangeStats", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}
