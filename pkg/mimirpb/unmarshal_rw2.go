// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	encoding_binary "encoding/binary"
	"fmt"
	"io"
	"math"
	"strings"
)

// This file contains the Remote Write 2.0 unmarshalling code that decodes RW2
// wire format (TimeSeriesRW2/ExemplarRW2/MetadataRW2 messages) directly into
// the RW1 in-memory representation (TimeSeries/Exemplar/MetricMetadata),
// resolving symbol references on the fly.
//
// Under gogoproto these functions lived inside the generated mimir.pb.go via
// the expected-diff mechanism (tools/apply-expected-diffs.sh), because they
// were derived from generated unmarshallers. With wiresmith they are plain
// hand-written code; the expected diff only redirects the generated
// WriteRequest unmarshal to call into them and stubs out the generated
// TimeSeriesRW2/ExemplarRW2/MetadataRW2 unmarshallers (see
// mimir.pb.go.expdiff).
//
// The function bodies are copied verbatim from the previously-committed
// (patched) gogoproto mimir.pb.go.

func (m *TimeSeries) UnmarshalRW2(dAtA []byte, symbols *rw2PagedSymbols, metadata metadataSet, skipNormalizeMetricName bool) error {
	var metricName string
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowMimir
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
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
			return fmt.Errorf("proto: TimeSeriesRW2: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: TimeSeriesRW2: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType == 0 {
				return errorOddNumberOfLabelRefs
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowMimir
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				if packedLen < 0 {
					return ErrInvalidLengthMimir
				}
				postIndex := iNdEx + packedLen
				if postIndex < 0 {
					return ErrInvalidLengthMimir
				}
				if postIndex > l {
					return io.ErrUnexpectedEOF
				}
				var elementCount int
				var count int
				for _, integer := range dAtA[iNdEx:postIndex] {
					if integer < 128 {
						count++
					}
				}
				elementCount = count
				if elementCount%2 != 0 {
					return errorOddNumberOfLabelRefs
				}
				if elementCount != 0 && len(m.Labels) == 0 {
					m.Labels = make([]LabelAdapter, 0, elementCount/2)
				}
				idx := 0
				metricNameLabel := false
				for iNdEx < postIndex {
					var v uint32
					for shift := uint(0); ; shift += 7 {
						if shift >= 64 {
							return ErrIntOverflowMimir
						}
						if iNdEx >= l {
							return io.ErrUnexpectedEOF
						}
						b := dAtA[iNdEx]
						iNdEx++
						v |= uint32(b&0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					if idx%2 == 0 {
						labelName, err := symbols.get(v)
						if err != nil {
							return errorInvalidLabelRef
						}
						m.Labels = append(m.Labels, LabelAdapter{Name: labelName})
						if labelName == "__name__" {
							metricNameLabel = true
						}
					} else {
						labelValue, err := symbols.get(v)
						if err != nil {
							return errorInvalidLabelRef
						}
						m.Labels[len(m.Labels)-1].Value = labelValue
						if metricNameLabel {
							metricName = labelValue
							metricNameLabel = false
						}
					}
					idx++
				}
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field LabelsRefs", wireType)
			}
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Samples", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMimir
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthMimir
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthMimir
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Samples = append(m.Samples, Sample{})
			if err := m.Samples[len(m.Samples)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Histograms", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMimir
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthMimir
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthMimir
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Histograms = append(m.Histograms, Histogram{})
			if err := m.Histograms[len(m.Histograms)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 4:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Exemplars", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMimir
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthMimir
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthMimir
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if !m.SkipUnmarshalingExemplars {
				m.Exemplars = append(m.Exemplars, Exemplar{})
				if err := m.Exemplars[len(m.Exemplars)-1].UnmarshalRW2(dAtA[iNdEx:postIndex], symbols); err != nil {
					return err
				}
			}
			iNdEx = postIndex
		case 5:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Metadata", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMimir
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthMimir
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthMimir
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if err := MetricMetadataUnmarshalRW2(dAtA[iNdEx:postIndex], symbols, metadata, metricName, skipNormalizeMetricName); err != nil {
				return err
			}
			iNdEx = postIndex
		case 6:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field CreatedTimestamp", wireType)
			}
			m.CreatedTimestamp = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMimir
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.CreatedTimestamp |= int64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipMimir(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthMimir
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (m *Exemplar) UnmarshalRW2(dAtA []byte, symbols *rw2PagedSymbols) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowMimir
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
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
			return fmt.Errorf("proto: ExemplarRW2: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: ExemplarRW2: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType == 0 {
				return errorOddNumberOfExemplarLabelRefs
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowMimir
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				if packedLen < 0 {
					return ErrInvalidLengthMimir
				}
				postIndex := iNdEx + packedLen
				if postIndex < 0 {
					return ErrInvalidLengthMimir
				}
				if postIndex > l {
					return io.ErrUnexpectedEOF
				}
				var elementCount int
				var count int
				for _, integer := range dAtA[iNdEx:postIndex] {
					if integer < 128 {
						count++
					}
				}
				elementCount = count
				if elementCount%2 != 0 {
					return errorOddNumberOfExemplarLabelRefs
				}
				if elementCount != 0 && len(m.Labels) == 0 {
					m.Labels = make([]LabelAdapter, 0, elementCount/2)
				}
				idx := 0
				for iNdEx < postIndex {
					var v uint32
					for shift := uint(0); ; shift += 7 {
						if shift >= 64 {
							return ErrIntOverflowMimir
						}
						if iNdEx >= l {
							return io.ErrUnexpectedEOF
						}
						b := dAtA[iNdEx]
						iNdEx++
						v |= uint32(b&0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					if idx%2 == 0 {
						labelName, err := symbols.get(v)
						if err != nil {
							return errorInvalidExemplarLabelRef
						}
						m.Labels = append(m.Labels, LabelAdapter{Name: labelName})
					} else {
						labelValue, err := symbols.get(v)
						if err != nil {
							return errorInvalidExemplarLabelRef
						}
						m.Labels[len(m.Labels)-1].Value = labelValue
					}
					idx++
				}
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field LabelsRefs", wireType)
			}
		case 2:
			if wireType != 1 {
				return fmt.Errorf("proto: wrong wireType = %d for field Value", wireType)
			}
			var v uint64
			if (iNdEx + 8) > l {
				return io.ErrUnexpectedEOF
			}
			v = uint64(encoding_binary.LittleEndian.Uint64(dAtA[iNdEx:]))
			iNdEx += 8
			m.Value = float64(math.Float64frombits(v))
		case 3:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Timestamp", wireType)
			}
			m.TimestampMs = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMimir
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.TimestampMs |= int64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipMimir(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthMimir
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func MetricMetadataUnmarshalRW2(dAtA []byte, symbols *rw2PagedSymbols, metadata metadataSet, metricName string, skipNormalizeMetricName bool) error {
	var (
		err                  error
		help                 string
		metricType           MetadataRW2_MetricType
		normalizedMetricName string
		unit                 string
	)
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowMimir
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
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
			return fmt.Errorf("proto: MetadataRW2: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: MetadataRW2: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Type", wireType)
			}
			metricType = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMimir
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				metricType |= MetadataRW2_MetricType(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 3:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field HelpRef", wireType)
			}
			helpRef := uint32(0)
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMimir
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				helpRef |= uint32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			help, err = symbols.get(helpRef)
			if err != nil {
				return errorInvalidHelpRef
			}
		case 4:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field UnitRef", wireType)
			}
			unitRef := uint32(0)
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMimir
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				unitRef |= uint32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			unit, err = symbols.get(unitRef)
			if err != nil {
				return errorInvalidUnitRef
			}
		default:
			iNdEx = preIndex
			skippy, err := skipMimir(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthMimir
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	if skipNormalizeMetricName {
		normalizedMetricName = metricName
	} else {
		normalizedMetricName, _ = normalizeMetricName(metricName, metricType)
	}
	if len(normalizedMetricName) == 0 {
		return nil
	}
	if len(unit) > 0 || len(help) > 0 || metricType != 0 {
		metadata.add(normalizedMetricName, MetricMetadata{
			MetricFamilyName: strings.Clone(normalizedMetricName),
			Help:             strings.Clone(help),
			Unit:             strings.Clone(unit),
			Type:             MetricMetadata_MetricType(metricType),
		})
	}

	return nil
}
