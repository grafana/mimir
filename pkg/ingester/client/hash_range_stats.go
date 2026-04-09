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

// --- HashRangeStats RPC types ---

// HashRangeStatsRequest is the request message for the HashRangeStats RPC.
type HashRangeStatsRequest struct{}

func (m *HashRangeStatsRequest) Reset()                {}
func (m *HashRangeStatsRequest) String() string         { return "{}" }
func (m *HashRangeStatsRequest) ProtoMessage()          {}
func (m *HashRangeStatsRequest) Marshal() ([]byte, error) { return nil, nil }
func (m *HashRangeStatsRequest) MarshalTo(dAtA []byte) (int, error) { return 0, nil }
func (m *HashRangeStatsRequest) Unmarshal(dAtA []byte) error { return nil }
func (m *HashRangeStatsRequest) Size() int { return 0 }

// HashRangeRate is a single hash range with its ingestion rate.
type HashRangeRate struct {
	Lo               uint32  `json:"lo"`
	Hi               uint32  `json:"hi"`
	SamplesPerSecond float64 `json:"samples_per_second"`
}

// HashRangeStatsResponse returns per-range ingestion rates.
type HashRangeStatsResponse struct {
	Rates []HashRangeRate `json:"rates"`
}

func (m *HashRangeStatsResponse) Reset()         { *m = HashRangeStatsResponse{} }
func (m *HashRangeStatsResponse) String() string { b, _ := json.Marshal(m); return string(b) }
func (m *HashRangeStatsResponse) ProtoMessage()  {}

// Wire format:
//   message HashRangeStatsResponse {
//     repeated HashRangeRate rates = 1;
//   }
//   message HashRangeRate {
//     uint32 lo = 1;
//     uint32 hi = 2;
//     double samples_per_second = 3;
//   }
// Field 1 is length-delimited (each rate is a sub-message).

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
	i := 0
	for _, r := range m.Rates {
		subSize := hashRangeRateSize(&r)
		dAtA[i] = 0x0a // field 1, wire type 2
		i++
		i = encodeVarintHashRangeStats(dAtA, i, uint64(subSize))

		if r.Lo != 0 {
			dAtA[i] = 0x08
			i++
			i = encodeVarintHashRangeStats(dAtA, i, uint64(r.Lo))
		}
		if r.Hi != 0 {
			dAtA[i] = 0x10
			i++
			i = encodeVarintHashRangeStats(dAtA, i, uint64(r.Hi))
		}
		if r.SamplesPerSecond != 0 {
			dAtA[i] = 0x19 // field 3, wire type 1 (64-bit)
			i++
			bits := math.Float64bits(r.SamplesPerSecond)
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

func hashRangeRateSize(r *HashRangeRate) int {
	n := 0
	if r.Lo != 0 {
		n += 1 + sovHashRangeStats(uint64(r.Lo))
	}
	if r.Hi != 0 {
		n += 1 + sovHashRangeStats(uint64(r.Hi))
	}
	if r.SamplesPerSecond != 0 {
		n += 1 + 8
	}
	return n
}

func (m *HashRangeStatsResponse) Size() int {
	n := 0
	for _, r := range m.Rates {
		sub := hashRangeRateSize(&r)
		n += 1 + sovHashRangeStats(uint64(sub)) + sub
	}
	return n
}

func (m *HashRangeStatsResponse) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
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

		if fieldNum == 1 && wireType == 2 {
			var msgLen int
			for shift := uint(0); ; shift += 7 {
				if iNdEx >= l {
					return io_ErrUnexpectedEOF()
				}
				b := dAtA[iNdEx]
				iNdEx++
				msgLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msgLen < 0 || iNdEx+msgLen > l {
				return io_ErrUnexpectedEOF()
			}
			var r HashRangeRate
			if err := unmarshalHashRangeRate(dAtA[iNdEx:iNdEx+msgLen], &r); err != nil {
				return err
			}
			m.Rates = append(m.Rates, r)
			iNdEx += msgLen
		} else {
			if err := skipField(dAtA, &iNdEx, l, wireType); err != nil {
				return err
			}
		}
	}
	return nil
}

func unmarshalHashRangeRate(dAtA []byte, r *HashRangeRate) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
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
		case 1: // lo
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
			r.Lo = v
		case 2: // hi
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
			r.Hi = v
		case 3: // samples_per_second (double, wire type 1)
			if wireType != 1 {
				return errInvalidWireType
			}
			if iNdEx+8 > l {
				return io_ErrUnexpectedEOF()
			}
			bits := uint64(dAtA[iNdEx]) | uint64(dAtA[iNdEx+1])<<8 | uint64(dAtA[iNdEx+2])<<16 | uint64(dAtA[iNdEx+3])<<24 |
				uint64(dAtA[iNdEx+4])<<32 | uint64(dAtA[iNdEx+5])<<40 | uint64(dAtA[iNdEx+6])<<48 | uint64(dAtA[iNdEx+7])<<56
			iNdEx += 8
			r.SamplesPerSecond = math.Float64frombits(bits)
		default:
			if err := skipField(dAtA, &iNdEx, l, wireType); err != nil {
				return err
			}
		}
	}
	return nil
}

// --- SetHashRanges RPC types ---

// SetHashRangesRequest tells the ingester which hash ranges it owns.
type SetHashRangesRequest struct {
	Ranges []HashRangeEntry `json:"ranges"`
}

// HashRangeEntry is a hash range [Lo, Hi].
type HashRangeEntry struct {
	Lo uint32 `json:"lo"`
	Hi uint32 `json:"hi"`
}

type SetHashRangesResponse struct{}

func (m *SetHashRangesRequest) Reset()         { *m = SetHashRangesRequest{} }
func (m *SetHashRangesRequest) String() string { b, _ := json.Marshal(m); return string(b) }
func (m *SetHashRangesRequest) ProtoMessage()  {}

// Wire format:
//   message SetHashRangesRequest {
//     repeated HashRangeEntry ranges = 1;
//   }
//   message HashRangeEntry { uint32 lo = 1; uint32 hi = 2; }

func (m *SetHashRangesRequest) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *SetHashRangesRequest) MarshalTo(dAtA []byte) (int, error) {
	i := 0
	for _, r := range m.Ranges {
		subSize := hashRangeEntrySize(&r)
		dAtA[i] = 0x0a // field 1, wire type 2
		i++
		i = encodeVarintHashRangeStats(dAtA, i, uint64(subSize))
		if r.Lo != 0 {
			dAtA[i] = 0x08
			i++
			i = encodeVarintHashRangeStats(dAtA, i, uint64(r.Lo))
		}
		if r.Hi != 0 {
			dAtA[i] = 0x10
			i++
			i = encodeVarintHashRangeStats(dAtA, i, uint64(r.Hi))
		}
	}
	return i, nil
}

func hashRangeEntrySize(r *HashRangeEntry) int {
	n := 0
	if r.Lo != 0 {
		n += 1 + sovHashRangeStats(uint64(r.Lo))
	}
	if r.Hi != 0 {
		n += 1 + sovHashRangeStats(uint64(r.Hi))
	}
	return n
}

func (m *SetHashRangesRequest) Size() int {
	n := 0
	for _, r := range m.Ranges {
		sub := hashRangeEntrySize(&r)
		n += 1 + sovHashRangeStats(uint64(sub)) + sub
	}
	return n
}

func (m *SetHashRangesRequest) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
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
		if fieldNum == 1 && wireType == 2 {
			var msgLen int
			for shift := uint(0); ; shift += 7 {
				if iNdEx >= l {
					return io_ErrUnexpectedEOF()
				}
				b := dAtA[iNdEx]
				iNdEx++
				msgLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msgLen < 0 || iNdEx+msgLen > l {
				return io_ErrUnexpectedEOF()
			}
			var e HashRangeEntry
			if err := unmarshalHashRangeEntry(dAtA[iNdEx:iNdEx+msgLen], &e); err != nil {
				return err
			}
			m.Ranges = append(m.Ranges, e)
			iNdEx += msgLen
		} else {
			if err := skipField(dAtA, &iNdEx, l, wireType); err != nil {
				return err
			}
		}
	}
	return nil
}

func unmarshalHashRangeEntry(dAtA []byte, e *HashRangeEntry) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
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
		case 1:
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
			e.Lo = v
		case 2:
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
			e.Hi = v
		default:
			if err := skipField(dAtA, &iNdEx, l, wireType); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *SetHashRangesResponse) Reset()                        {}
func (m *SetHashRangesResponse) String() string                { return "{}" }
func (m *SetHashRangesResponse) ProtoMessage()                 {}
func (m *SetHashRangesResponse) Marshal() ([]byte, error)      { return nil, nil }
func (m *SetHashRangesResponse) MarshalTo(dAtA []byte) (int, error) { return 0, nil }
func (m *SetHashRangesResponse) Unmarshal(dAtA []byte) error   { return nil }
func (m *SetHashRangesResponse) Size() int                     { return 0 }

// --- Helpers ---

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

func skipField(dAtA []byte, iNdEx *int, l int, wireType int) error {
	switch wireType {
	case 0: // varint
		for {
			if *iNdEx >= l {
				return io_ErrUnexpectedEOF()
			}
			if dAtA[*iNdEx]&0x80 == 0 {
				*iNdEx++
				return nil
			}
			*iNdEx++
		}
	case 1: // 64-bit
		*iNdEx += 8
	case 2: // length-delimited
		var length int
		for shift := uint(0); ; shift += 7 {
			if *iNdEx >= l {
				return io_ErrUnexpectedEOF()
			}
			b := dAtA[*iNdEx]
			*iNdEx++
			length |= int(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		*iNdEx += length
	case 5: // 32-bit
		*iNdEx += 4
	default:
		return errInvalidWireType
	}
	if *iNdEx > l {
		return io_ErrUnexpectedEOF()
	}
	return nil
}

// --- gRPC client methods ---

func (c *ingesterClient) HashRangeStats(ctx context.Context, in *HashRangeStatsRequest, opts ...grpc.CallOption) (*HashRangeStatsResponse, error) {
	out := new(HashRangeStatsResponse)
	err := c.cc.Invoke(ctx, "/cortex.Ingester/HashRangeStats", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *ingesterClient) SetHashRanges(ctx context.Context, in *SetHashRangesRequest, opts ...grpc.CallOption) (*SetHashRangesResponse, error) {
	out := new(SetHashRangesResponse)
	err := c.cc.Invoke(ctx, "/cortex.Ingester/SetHashRanges", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}
