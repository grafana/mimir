// SPDX-License-Identifier: AGPL-3.0-only

package protobuf

import (
	"fmt"
	"io"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func UnmarshalLabelAdapter(la *mimirpb.LabelAdapter, data []byte) error {
	l := len(data)
	index := 0
	for index < l {
		preIndex := index
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return mimirpb.ErrIntOverflowMimir
			}
			if index >= l {
				return io.ErrUnexpectedEOF
			}
			b := data[index]
			index++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: LabelPair: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: LabelPair: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Name", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return mimirpb.ErrIntOverflowMimir
				}
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return mimirpb.ErrInvalidLengthMimir
			}
			postIndex := index + byteLen
			if postIndex < 0 {
				return mimirpb.ErrInvalidLengthMimir
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			la.Name = string(data[index:postIndex])
			index = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Value", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return mimirpb.ErrIntOverflowMimir
				}
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return mimirpb.ErrInvalidLengthMimir
			}
			postIndex := index + byteLen
			if postIndex < 0 {
				return mimirpb.ErrInvalidLengthMimir
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			la.Value = string(data[index:postIndex])
			index = postIndex
		default:
			index = preIndex
			skippy, err := skipMimir(data[index:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return mimirpb.ErrInvalidLengthMimir
			}
			if (index + skippy) < 0 {
				return mimirpb.ErrInvalidLengthMimir
			}
			if (index + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			index += skippy
		}
	}
	if index > l {
		return io.ErrUnexpectedEOF
	}

	return nil
}

func skipMimir(data []byte) (n int, err error) {
	l := len(data)
	index := 0
	for index < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, mimirpb.ErrIntOverflowMimir
			}
			if index >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := data[index]
			index++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, mimirpb.ErrIntOverflowMimir
				}
				if index >= l {
					return 0, io.ErrUnexpectedEOF
				}
				index++
				if data[index-1] < 0x80 {
					break
				}
			}
			return index, nil
		case 1:
			index += 8
			return index, nil
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, mimirpb.ErrIntOverflowMimir
				}
				if index >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if length < 0 {
				return 0, mimirpb.ErrInvalidLengthMimir
			}
			index += length
			if index < 0 {
				return 0, mimirpb.ErrInvalidLengthMimir
			}
			return index, nil
		case 3:
			for {
				var innerWire uint64
				start := index
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return 0, mimirpb.ErrIntOverflowMimir
					}
					if index >= l {
						return 0, io.ErrUnexpectedEOF
					}
					b := data[index]
					index++
					innerWire |= (uint64(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				innerWireType := int(innerWire & 0x7)
				if innerWireType == 4 {
					break
				}
				next, err := skipMimir(data[start:])
				if err != nil {
					return 0, err
				}
				index = start + next
				if index < 0 {
					return 0, mimirpb.ErrInvalidLengthMimir
				}
			}
			return index, nil
		case 4:
			return index, nil
		case 5:
			index += 4
			return index, nil
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
	}
	panic("unreachable")
}
