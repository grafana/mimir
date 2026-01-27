// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

// UnmarshalWriteRequestRW2Native unmarshals a WriteRequestRW2 in native format.
// Unlike the standard Unmarshal, this keeps RW2 data in RW2 format (symbol table + uint32 refs)
// instead of converting to RW1 format (embedded strings). This avoids 10-15x memory expansion.
func UnmarshalWriteRequestRW2Native(dAtA []byte) (*WriteRequestRW2, error) {
	req := &WriteRequestRW2{}
	l := len(dAtA)
	iNdEx := 0

	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return nil, ErrIntOverflowMimir
			}
			if iNdEx >= l {
				return nil, io.ErrUnexpectedEOF
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
		if wireType == 4 {
			return nil, fmt.Errorf("proto: WriteRequestRW2: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return nil, fmt.Errorf("proto: WriteRequestRW2: illegal tag %d (wire type %d)", fieldNum, wire)
		}

		switch fieldNum {
		case 4: // symbols
			if wireType != 2 {
				return nil, fmt.Errorf("proto: wrong wireType = %d for field Symbols", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return nil, ErrIntOverflowMimir
				}
				if iNdEx >= l {
					return nil, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return nil, ErrInvalidLengthMimir
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return nil, ErrInvalidLengthMimir
			}
			if postIndex > l {
				return nil, io.ErrUnexpectedEOF
			}
			req.Symbols = append(req.Symbols, string(dAtA[iNdEx:postIndex]))
			iNdEx = postIndex
		case 5: // timeseries
			if wireType != 2 {
				return nil, fmt.Errorf("proto: wrong wireType = %d for field Timeseries", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return nil, ErrIntOverflowMimir
				}
				if iNdEx >= l {
					return nil, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return nil, ErrInvalidLengthMimir
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return nil, ErrInvalidLengthMimir
			}
			if postIndex > l {
				return nil, io.ErrUnexpectedEOF
			}
			// Use our custom native unmarshal instead of the blocked one
			ts, err := UnmarshalTimeSeriesRW2Native(dAtA[iNdEx:postIndex])
			if err != nil {
				return nil, err
			}
			req.Timeseries = append(req.Timeseries, ts)
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipMimir(dAtA[iNdEx:])
			if err != nil {
				return nil, err
			}
			if skippy < 0 || (iNdEx+skippy) < 0 {
				return nil, ErrInvalidLengthMimir
			}
			if (iNdEx + skippy) > l {
				return nil, io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	return req, nil
}

// UnmarshalTimeSeriesRW2Native unmarshals a TimeSeriesRW2 message in native format,
// keeping the uint32 label references instead of converting them to strings.
// This avoids the memory expansion of symbol table lookups.
func UnmarshalTimeSeriesRW2Native(dAtA []byte) (TimeSeriesRW2, error) {
	var ts TimeSeriesRW2
	l := len(dAtA)
	iNdEx := 0

	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ts, ErrIntOverflowMimir
			}
			if iNdEx >= l {
				return ts, io.ErrUnexpectedEOF
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
		if wireType == 4 {
			return ts, fmt.Errorf("proto: TimeSeriesRW2: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return ts, fmt.Errorf("proto: TimeSeriesRW2: illegal tag %d (wire type %d)", fieldNum, wire)
		}

		switch fieldNum {
		case 1: // labels_refs
			if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ts, ErrIntOverflowMimir
					}
					if iNdEx >= l {
						return ts, io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				if packedLen < 0 {
					return ts, ErrInvalidLengthMimir
				}
				postIndex := iNdEx + packedLen
				if postIndex < 0 {
					return ts, ErrInvalidLengthMimir
				}
				if postIndex > l {
					return ts, io.ErrUnexpectedEOF
				}
				for iNdEx < postIndex {
					var v uint32
					for shift := uint(0); ; shift += 7 {
						if shift >= 64 {
							return ts, ErrIntOverflowMimir
						}
						if iNdEx >= l {
							return ts, io.ErrUnexpectedEOF
						}
						b := dAtA[iNdEx]
						iNdEx++
						v |= uint32(b&0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					ts.LabelsRefs = append(ts.LabelsRefs, v)
				}
			}
		case 2: // samples
			if wireType != 2 {
				return ts, fmt.Errorf("proto: wrong wireType = %d for field Samples", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ts, ErrIntOverflowMimir
				}
				if iNdEx >= l {
					return ts, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ts, ErrInvalidLengthMimir
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ts, ErrInvalidLengthMimir
			}
			if postIndex > l {
				return ts, io.ErrUnexpectedEOF
			}
			ts.Samples = append(ts.Samples, Sample{})
			if err := ts.Samples[len(ts.Samples)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return ts, err
			}
			iNdEx = postIndex
		case 3: // histograms
			if wireType != 2 {
				return ts, fmt.Errorf("proto: wrong wireType = %d for field Histograms", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ts, ErrIntOverflowMimir
				}
				if iNdEx >= l {
					return ts, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ts, ErrInvalidLengthMimir
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ts, ErrInvalidLengthMimir
			}
			if postIndex > l {
				return ts, io.ErrUnexpectedEOF
			}
			ts.Histograms = append(ts.Histograms, Histogram{})
			if err := ts.Histograms[len(ts.Histograms)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return ts, err
			}
			iNdEx = postIndex
		case 4: // exemplars - parse as ExemplarRW2
			if wireType != 2 {
				return ts, fmt.Errorf("proto: wrong wireType = %d for field Exemplars", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ts, ErrIntOverflowMimir
				}
				if iNdEx >= l {
					return ts, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ts, ErrInvalidLengthMimir
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ts, ErrInvalidLengthMimir
			}
			if postIndex > l {
				return ts, io.ErrUnexpectedEOF
			}
			// Parse ExemplarRW2 manually
			exemplar, err := unmarshalExemplarRW2(dAtA[iNdEx:postIndex])
			if err != nil {
				return ts, err
			}
			ts.Exemplars = append(ts.Exemplars, exemplar)
			iNdEx = postIndex
		case 5: // metadata
			if wireType != 2 {
				return ts, fmt.Errorf("proto: wrong wireType = %d for field Metadata", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ts, ErrIntOverflowMimir
				}
				if iNdEx >= l {
					return ts, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ts, ErrInvalidLengthMimir
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ts, ErrInvalidLengthMimir
			}
			if postIndex > l {
				return ts, io.ErrUnexpectedEOF
			}
			// Parse MetadataRW2 manually
			metadata, err := unmarshalMetadataRW2(dAtA[iNdEx:postIndex])
			if err != nil {
				return ts, err
			}
			ts.Metadata = metadata
			iNdEx = postIndex
		case 6: // created_timestamp
			if wireType != 0 {
				return ts, fmt.Errorf("proto: wrong wireType = %d for field CreatedTimestamp", wireType)
			}
			ts.CreatedTimestamp = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ts, ErrIntOverflowMimir
				}
				if iNdEx >= l {
					return ts, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				ts.CreatedTimestamp |= int64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipMimir(dAtA[iNdEx:])
			if err != nil {
				return ts, err
			}
			if skippy < 0 || (iNdEx+skippy) < 0 {
				return ts, ErrInvalidLengthMimir
			}
			if (iNdEx + skippy) > l {
				return ts, io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	return ts, nil
}

// unmarshalExemplarRW2 parses an ExemplarRW2 message
func unmarshalExemplarRW2(dAtA []byte) (ExemplarRW2, error) {
	var ex ExemplarRW2
	l := len(dAtA)
	iNdEx := 0

	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ex, ErrIntOverflowMimir
			}
			if iNdEx >= l {
				return ex, io.ErrUnexpectedEOF
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
		case 1: // labels_refs
			if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ex, ErrIntOverflowMimir
					}
					if iNdEx >= l {
						return ex, io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				if packedLen < 0 {
					return ex, ErrInvalidLengthMimir
				}
				postIndex := iNdEx + packedLen
				if postIndex > l {
					return ex, io.ErrUnexpectedEOF
				}
				for iNdEx < postIndex {
					var v uint32
					for shift := uint(0); ; shift += 7 {
						if shift >= 64 {
							return ex, ErrIntOverflowMimir
						}
						if iNdEx >= l {
							return ex, io.ErrUnexpectedEOF
						}
						b := dAtA[iNdEx]
						iNdEx++
						v |= uint32(b&0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					ex.LabelsRefs = append(ex.LabelsRefs, v)
				}
			}
		case 2: // value
			if wireType != 1 {
				return ex, fmt.Errorf("proto: wrong wireType = %d for field Value", wireType)
			}
			var v uint64
			if (iNdEx + 8) > l {
				return ex, io.ErrUnexpectedEOF
			}
			v = uint64(binary.LittleEndian.Uint64(dAtA[iNdEx:]))
			iNdEx += 8
			ex.Value = math.Float64frombits(v)
		case 3: // timestamp
			if wireType != 0 {
				return ex, fmt.Errorf("proto: wrong wireType = %d for field Timestamp", wireType)
			}
			ex.Timestamp = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ex, ErrIntOverflowMimir
				}
				if iNdEx >= l {
					return ex, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				ex.Timestamp |= int64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipMimir(dAtA[iNdEx:])
			if err != nil {
				return ex, err
			}
			if skippy < 0 || (iNdEx+skippy) < 0 {
				return ex, ErrInvalidLengthMimir
			}
			if (iNdEx + skippy) > l {
				return ex, io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	return ex, nil
}

// unmarshalMetadataRW2 parses a MetadataRW2 message
func unmarshalMetadataRW2(dAtA []byte) (MetadataRW2, error) {
	var md MetadataRW2
	l := len(dAtA)
	iNdEx := 0

	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return md, ErrIntOverflowMimir
			}
			if iNdEx >= l {
				return md, io.ErrUnexpectedEOF
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
		case 1: // type
			if wireType != 0 {
				return md, fmt.Errorf("proto: wrong wireType = %d for field Type", wireType)
			}
			md.Type = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return md, ErrIntOverflowMimir
				}
				if iNdEx >= l {
					return md, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				md.Type |= MetadataRW2_MetricType(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2: // help_ref
			if wireType != 0 {
				return md, fmt.Errorf("proto: wrong wireType = %d for field HelpRef", wireType)
			}
			md.HelpRef = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return md, ErrIntOverflowMimir
				}
				if iNdEx >= l {
					return md, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				md.HelpRef |= uint32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 3: // unit_ref
			if wireType != 0 {
				return md, fmt.Errorf("proto: wrong wireType = %d for field UnitRef", wireType)
			}
			md.UnitRef = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return md, ErrIntOverflowMimir
				}
				if iNdEx >= l {
					return md, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				md.UnitRef |= uint32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipMimir(dAtA[iNdEx:])
			if err != nil {
				return md, err
			}
			if skippy < 0 || (iNdEx+skippy) < 0 {
				return md, ErrInvalidLengthMimir
			}
			if (iNdEx + skippy) > l {
				return md, io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	return md, nil
}
