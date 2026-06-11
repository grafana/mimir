// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"fmt"
	"io"
	math_bits "math/bits"
)

// This file preserves parts of the gogoproto-generated API that the wiresmith
// compiler does not emit, so that hand-written code (in this package and in
// consumers) keeps compiling unchanged after the gogoproto -> wiresmith
// migration:
//
//   - varint helpers (sovMimir/encodeVarintMimir/skipMimir) used by the
//     hand-written marshalling code (LabelAdapter, request splitting, ...).
//
// (The unprefixed enum constant aliases that used to live here are gone:
// mimir.proto now sets (wiresmith.options.enum_no_prefix_all) = true and the
// generated constants match the old gogoproto names directly.)

// Varint helpers copied from the gogoproto-generated mimir.pb.go.

var (
	ErrInvalidLengthMimir        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowMimir          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupMimir = fmt.Errorf("proto: unexpected end of group")
)

func sovMimir(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}

func encodeVarintMimir(dAtA []byte, offset int, v uint64) int {
	offset -= sovMimir(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}

func skipMimir(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowMimir
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
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
					return 0, ErrIntOverflowMimir
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
		case 1:
			iNdEx += 8
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowMimir
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if length < 0 {
				return 0, ErrInvalidLengthMimir
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupMimir
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthMimir
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}
