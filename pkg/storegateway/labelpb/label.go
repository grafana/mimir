// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/labelpb/label.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

// Package containing proto and JSON serializable Labels and ZLabels (no copy) structs used to
// identify series. This package expose no-copy converters to Prometheus labels.Labels.

package labelpb

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
)

var (
	sep = []byte{'\xff'}
)

func noAllocString(buf []byte) string {
	return *(*string)(unsafe.Pointer(&buf))
}

// ZLabelsFromPromLabels converts Prometheus labels to slice of labelpb.ZLabel in type unsafe manner.
// It reuses the same memory. Caller should abort using passed labels.Labels.
func ZLabelsFromPromLabels(lset labels.Labels) []ZLabel {
	return *(*[]ZLabel)(unsafe.Pointer(&lset))
}

// ZLabelsToPromLabels convert slice of labelpb.ZLabel to Prometheus labels in type unsafe manner.
// It reuses the same memory. Caller should abort using passed []ZLabel.
// NOTE: Use with care. ZLabels holds memory from the whole protobuf unmarshal, so the returned
// Prometheus Labels will hold this memory as well.
func ZLabelsToPromLabels(lset []ZLabel) labels.Labels {
	return *(*labels.Labels)(unsafe.Pointer(&lset))
}

// ZLabel is a Label (also easily transformable to Prometheus labels.Labels) that can be unmarshalled from protobuf
// reusing the same memory address for string bytes.
// NOTE: While unmarshalling it uses exactly same bytes that were allocated for protobuf. This mean that *whole* protobuf
// bytes will be not GC-ed as long as ZLabels are referenced somewhere. Use it carefully, only for short living
// protobuf message processing.
type ZLabel Label

func (m *ZLabel) MarshalTo(data []byte) (int, error) {
	f := Label(*m)
	return f.MarshalTo(data)
}

func (m *ZLabel) MarshalToSizedBuffer(data []byte) (int, error) {
	f := Label(*m)
	return f.MarshalToSizedBuffer(data)
}

// Unmarshal unmarshalls gRPC protobuf into ZLabel struct. ZLabel string is directly using bytes passed in `data`.
// To use it add (gogoproto.customtype) = "github.com/grafana/mimir/pkg/storegateway/labelpb.ZLabel" to proto field tag.
// NOTE: This exists in internal Google protobuf implementation, but not in open source one: https://news.ycombinator.com/item?id=23588882
func (m *ZLabel) Unmarshal(data []byte) error {
	l := len(data)

	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowTypes
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := data[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: ZLabel: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: ZLabel: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Name", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthTypes
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthTypes
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Name = noAllocString(data[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Value", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthTypes
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthTypes
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Value = noAllocString(data[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipTypes(data[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthTypes
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthTypes
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

func (m *ZLabel) UnmarshalJSON(entry []byte) error {
	f := Label(*m)
	if err := json.Unmarshal(entry, &f); err != nil {
		return errors.Wrapf(err, "labels: label field unmarshal: %v", string(entry))
	}
	*m = ZLabel(f)
	return nil
}

func (m *ZLabel) Marshal() ([]byte, error) {
	f := Label(*m)
	return f.Marshal()
}

func (m *ZLabel) MarshalJSON() ([]byte, error) {
	return json.Marshal(Label(*m))
}

// Size implements proto.Sizer.
func (m *ZLabel) Size() (n int) {
	f := Label(*m)
	return f.Size()
}

// Equal implements proto.Equaler.
func (m *ZLabel) Equal(other ZLabel) bool {
	return m.Name == other.Name && m.Value == other.Value
}

// Compare implements proto.Comparer.
func (m *ZLabel) Compare(other ZLabel) int {
	if c := strings.Compare(m.Name, other.Name); c != 0 {
		return c
	}
	return strings.Compare(m.Value, other.Value)
}
