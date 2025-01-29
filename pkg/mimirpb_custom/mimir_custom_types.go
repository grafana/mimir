package mimirpb_custom

import (
	"fmt"
	math_bits "math/bits"
	"unsafe"
)

// Reference imports to suppress errors if they are not otherwise used.
//var _ = proto.Marshal
//var _ = fmt.Errorf
//var _ = math.Inf

// LabelAdapter is a labels.Label that can be marshalled to/from protos.
// type LabelAdapter labels.Label
//
// func (m *LabelAdapter) Reset()      { *m = LabelAdapter{} }
// func (*LabelAdapter) ProtoMessage() {}
//
//	func (*LabelAdapter) Descriptor() ([]byte, []int) {
//		return fileDescriptor_c4ae32fdfdd30bf8, []int{0}
//	}
//
//	func (m *LabelAdapter) XXX_Unmarshal(b []byte) error {
//		return m.Unmarshal(b)
//	}
//
//	func (m *LabelAdapter) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
//		if deterministic {
//			return xxx_messageInfo_LabelAdapter.Marshal(b, m, deterministic)
//		} else {
//			b = b[:cap(b)]
//			n, err := m.MarshalToSizedBuffer(b)
//			if err != nil {
//				return nil, err
//			}
//			return b[:n], nil
//		}
//	}
//
//	func (m *LabelAdapter) XXX_Merge(src proto.Message) {
//		xxx_messageInfo_LabelAdapter.Merge(m, src)
//	}
//
//	func (m *LabelAdapter) XXX_Size() int {
//		return m.Size()
//	}
//
//	func (m *LabelAdapter) XXX_DiscardUnknown() {
//		xxx_messageInfo_LabelAdapter.DiscardUnknown(m)
//	}
//
// var xxx_messageInfo_LabelAdapter proto.InternalMessageInfo
//
//	func (m *LabelAdapter) GetName() string {
//		if m != nil {
//			return m.Name
//		}
//		return ""
//	}
//
//	func (m *LabelAdapter) GetValue() string {
//		if m != nil {
//			return m.Value
//		}
//		return ""
//	}
//
//	func init() {
//		proto.RegisterType((*LabelAdapter)(nil), "cortexpb_custom.LabelAdapter")
//	}
//
// func init() { proto.RegisterFile("mimir_custom_types.proto", fileDescriptor_c4ae32fdfdd30bf8) }
//
//	var fileDescriptor_c4ae32fdfdd30bf8 = []byte{
//		// 178 bytes of a gzipped FileDescriptorProto
//		0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0x92, 0xc8, 0xcd, 0xcc, 0xcd,
//		0x2c, 0x8a, 0x4f, 0x2e, 0x2d, 0x2e, 0xc9, 0xcf, 0x8d, 0x2f, 0xa9, 0x2c, 0x48, 0x2d, 0xd6, 0x2b,
//		0x28, 0xca, 0x2f, 0xc9, 0x17, 0xe2, 0x4f, 0xce, 0x2f, 0x2a, 0x49, 0xad, 0x28, 0x48, 0x82, 0x4a,
//		0x2a, 0x59, 0x70, 0xf1, 0xf8, 0x24, 0x26, 0xa5, 0xe6, 0x38, 0xa6, 0x24, 0x16, 0x94, 0xa4, 0x16,
//		0x09, 0x09, 0x71, 0xb1, 0xe4, 0x25, 0xe6, 0xa6, 0x4a, 0x30, 0x2a, 0x30, 0x6a, 0x70, 0x06, 0x81,
//		0xd9, 0x42, 0x22, 0x5c, 0xac, 0x65, 0x89, 0x39, 0xa5, 0xa9, 0x12, 0x4c, 0x60, 0x41, 0x08, 0xc7,
//		0xc9, 0xe5, 0xc2, 0x43, 0x39, 0x86, 0x1b, 0x0f, 0xe5, 0x18, 0x3e, 0x3c, 0x94, 0x63, 0x6c, 0x78,
//		0x24, 0xc7, 0xb8, 0xe2, 0x91, 0x1c, 0xe3, 0x89, 0x47, 0x72, 0x8c, 0x17, 0x1e, 0xc9, 0x31, 0x3e,
//		0x78, 0x24, 0xc7, 0xf8, 0xe2, 0x91, 0x1c, 0xc3, 0x87, 0x47, 0x72, 0x8c, 0x13, 0x1e, 0xcb, 0x31,
//		0x5c, 0x78, 0x2c, 0xc7, 0x70, 0xe3, 0xb1, 0x1c, 0x43, 0x14, 0x1f, 0xd8, 0x69, 0x70, 0xfb, 0x93,
//		0xd8, 0xc0, 0xee, 0x32, 0x06, 0x04, 0x00, 0x00, 0xff, 0xff, 0xb8, 0x5c, 0x1d, 0x91, 0xb3, 0x00,
//		0x00, 0x00,
//	}
func (this *LabelAdapter) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	that1, ok := that.(*LabelAdapter)
	if !ok {
		that2, ok := that.(LabelAdapter)
		if ok {
			that1 = &that2
		} else {
			return false
		}
	}
	if that1 == nil {
		return this == nil
	} else if this == nil {
		return false
	}
	if this.Name != that1.Name {
		return false
	}
	if this.Value != that1.Value {
		return false
	}
	return true
}

//	func (this *LabelAdapter) GoString() string {
//		if this == nil {
//			return "nil"
//		}
//		s := make([]string, 0, 6)
//		s = append(s, "&mimirpb_custom.LabelAdapter{")
//		s = append(s, "Name: "+fmt.Sprintf("%#v", this.Name)+",\n")
//		s = append(s, "Value: "+fmt.Sprintf("%#v", this.Value)+",\n")
//		s = append(s, "}")
//		return strings.Join(s, "")
//	}
//
//	func valueToGoStringMimirCustomTypes(v interface{}, typ string) string {
//		rv := reflect.ValueOf(v)
//		if rv.IsNil() {
//			return "nil"
//		}
//		pv := reflect.Indirect(rv).Interface()
//		return fmt.Sprintf("func(v %v) *%v { return &v } ( %#v )", typ, typ, pv)
//	}
//
//	func (m *LabelAdapter) Marshal() (dAtA []byte, err error) {
//		size := m.Size()
//		dAtA = make([]byte, size)
//		n, err := m.MarshalToSizedBuffer(dAtA[:size])
//		if err != nil {
//			return nil, err
//		}
//		return dAtA[:n], nil
//	}
//
//	func (m *LabelAdapter) MarshalTo(dAtA []byte) (int, error) {
//		size := m.Size()
//		return m.MarshalToSizedBuffer(dAtA[:size])
//	}

func (m *LabelAdapter) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Value) > 0 {
		i -= len(m.Value)
		copy(dAtA[i:], m.Value)
		i = encodeVarintMimirCustomTypes(dAtA, i, uint64(len(m.Value)))
		i--
		dAtA[i] = 0x12
	}
	if len(m.Name) > 0 {
		i -= len(m.Name)
		copy(dAtA[i:], m.Name)
		i = encodeVarintMimirCustomTypes(dAtA, i, uint64(len(m.Name)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func encodeVarintMimirCustomTypes(dAtA []byte, offset int, v uint64) int {
	offset -= sovMimirCustomTypes(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}

//func (m *LabelAdapter) Size() (n int) {
//	if m == nil {
//		return 0
//	}
//	var l int
//	_ = l
//	l = len(m.Name)
//	if l > 0 {
//		n += 1 + l + sovMimirCustomTypes(uint64(l))
//	}
//	l = len(m.Value)
//	if l > 0 {
//		n += 1 + l + sovMimirCustomTypes(uint64(l))
//	}
//	return n
//}

func sovMimirCustomTypes(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozMimirCustomTypes(x uint64) (n int) {
	return sovMimirCustomTypes(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}

//func (this *LabelAdapter) String() string {
//	if this == nil {
//		return "nil"
//	}
//	s := strings.Join([]string{`&LabelAdapter{`,
//		`Name:` + fmt.Sprintf("%v", this.Name) + `,`,
//		`Value:` + fmt.Sprintf("%v", this.Value) + `,`,
//		`}`,
//	}, "")
//	return s
//}

//func valueToStringMimirCustomTypes(v interface{}) string {
//	rv := reflect.ValueOf(v)
//	if rv.IsNil() {
//		return "nil"
//	}
//	pv := reflect.Indirect(rv).Interface()
//	return fmt.Sprintf("*%v", pv)
//}
//func (m *LabelAdapter) Unmarshal(dAtA []byte) error {
//	l := len(dAtA)
//	iNdEx := 0
//	for iNdEx < l {
//		preIndex := iNdEx
//		var wire uint64
//		for shift := uint(0); ; shift += 7 {
//			if shift >= 64 {
//				return ErrIntOverflowMimirCustomTypes
//			}
//			if iNdEx >= l {
//				return io.ErrUnexpectedEOF
//			}
//			b := dAtA[iNdEx]
//			iNdEx++
//			wire |= uint64(b&0x7F) << shift
//			if b < 0x80 {
//				break
//			}
//		}
//		fieldNum := int32(wire >> 3)
//		wireType := int(wire & 0x7)
//		if wireType == 4 {
//			return fmt.Errorf("proto: LabelAdapter: wiretype end group for non-group")
//		}
//		if fieldNum <= 0 {
//			return fmt.Errorf("proto: LabelAdapter: illegal tag %d (wire type %d)", fieldNum, wire)
//		}
//		switch fieldNum {
//		case 1:
//			if wireType != 2 {
//				return fmt.Errorf("proto: wrong wireType = %d for field Name", wireType)
//			}
//			var stringLen uint64
//			for shift := uint(0); ; shift += 7 {
//				if shift >= 64 {
//					return ErrIntOverflowMimirCustomTypes
//				}
//				if iNdEx >= l {
//					return io.ErrUnexpectedEOF
//				}
//				b := dAtA[iNdEx]
//				iNdEx++
//				stringLen |= uint64(b&0x7F) << shift
//				if b < 0x80 {
//					break
//				}
//			}
//			intStringLen := int(stringLen)
//			if intStringLen < 0 {
//				return ErrInvalidLengthMimirCustomTypes
//			}
//			postIndex := iNdEx + intStringLen
//			if postIndex < 0 {
//				return ErrInvalidLengthMimirCustomTypes
//			}
//			if postIndex > l {
//				return io.ErrUnexpectedEOF
//			}
//			m.Name = yoloString(dAtA[iNdEx:postIndex])
//			iNdEx = postIndex
//		case 2:
//			if wireType != 2 {
//				return fmt.Errorf("proto: wrong wireType = %d for field Value", wireType)
//			}
//			var stringLen uint64
//			for shift := uint(0); ; shift += 7 {
//				if shift >= 64 {
//					return ErrIntOverflowMimirCustomTypes
//				}
//				if iNdEx >= l {
//					return io.ErrUnexpectedEOF
//				}
//				b := dAtA[iNdEx]
//				iNdEx++
//				stringLen |= uint64(b&0x7F) << shift
//				if b < 0x80 {
//					break
//				}
//			}
//			intStringLen := int(stringLen)
//			if intStringLen < 0 {
//				return ErrInvalidLengthMimirCustomTypes
//			}
//			postIndex := iNdEx + intStringLen
//			if postIndex < 0 {
//				return ErrInvalidLengthMimirCustomTypes
//			}
//			if postIndex > l {
//				return io.ErrUnexpectedEOF
//			}
//			m.Value = yoloString(dAtA[iNdEx:postIndex])
//			iNdEx = postIndex
//		default:
//			iNdEx = preIndex
//			skippy, err := skipMimirCustomTypes(dAtA[iNdEx:])
//			if err != nil {
//				return err
//			}
//			if skippy < 0 {
//				return ErrInvalidLengthMimirCustomTypes
//			}
//			if (iNdEx + skippy) < 0 {
//				return ErrInvalidLengthMimirCustomTypes
//			}
//			if (iNdEx + skippy) > l {
//				return io.ErrUnexpectedEOF
//			}
//			iNdEx += skippy
//		}
//	}
//
//	if iNdEx > l {
//		return io.ErrUnexpectedEOF
//	}
//	return nil
//}
//func skipMimirCustomTypes(dAtA []byte) (n int, err error) {
//	l := len(dAtA)
//	iNdEx := 0
//	for iNdEx < l {
//		var wire uint64
//		for shift := uint(0); ; shift += 7 {
//			if shift >= 64 {
//				return 0, ErrIntOverflowMimirCustomTypes
//			}
//			if iNdEx >= l {
//				return 0, io.ErrUnexpectedEOF
//			}
//			b := dAtA[iNdEx]
//			iNdEx++
//			wire |= (uint64(b) & 0x7F) << shift
//			if b < 0x80 {
//				break
//			}
//		}
//		wireType := int(wire & 0x7)
//		switch wireType {
//		case 0:
//			for shift := uint(0); ; shift += 7 {
//				if shift >= 64 {
//					return 0, ErrIntOverflowMimirCustomTypes
//				}
//				if iNdEx >= l {
//					return 0, io.ErrUnexpectedEOF
//				}
//				iNdEx++
//				if dAtA[iNdEx-1] < 0x80 {
//					break
//				}
//			}
//			return iNdEx, nil
//		case 1:
//			iNdEx += 8
//			return iNdEx, nil
//		case 2:
//			var length int
//			for shift := uint(0); ; shift += 7 {
//				if shift >= 64 {
//					return 0, ErrIntOverflowMimirCustomTypes
//				}
//				if iNdEx >= l {
//					return 0, io.ErrUnexpectedEOF
//				}
//				b := dAtA[iNdEx]
//				iNdEx++
//				length |= (int(b) & 0x7F) << shift
//				if b < 0x80 {
//					break
//				}
//			}
//			if length < 0 {
//				return 0, ErrInvalidLengthMimirCustomTypes
//			}
//			iNdEx += length
//			if iNdEx < 0 {
//				return 0, ErrInvalidLengthMimirCustomTypes
//			}
//			return iNdEx, nil
//		case 3:
//			for {
//				var innerWire uint64
//				var start int = iNdEx
//				for shift := uint(0); ; shift += 7 {
//					if shift >= 64 {
//						return 0, ErrIntOverflowMimirCustomTypes
//					}
//					if iNdEx >= l {
//						return 0, io.ErrUnexpectedEOF
//					}
//					b := dAtA[iNdEx]
//					iNdEx++
//					innerWire |= (uint64(b) & 0x7F) << shift
//					if b < 0x80 {
//						break
//					}
//				}
//				innerWireType := int(innerWire & 0x7)
//				if innerWireType == 4 {
//					break
//				}
//				next, err := skipMimirCustomTypes(dAtA[start:])
//				if err != nil {
//					return 0, err
//				}
//				iNdEx = start + next
//				if iNdEx < 0 {
//					return 0, ErrInvalidLengthMimirCustomTypes
//				}
//			}
//			return iNdEx, nil
//		case 4:
//			return iNdEx, nil
//		case 5:
//			iNdEx += 4
//			return iNdEx, nil
//		default:
//			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
//		}
//	}
//	panic("unreachable")
//}

var (
	ErrInvalidLengthMimirCustomTypes = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowMimirCustomTypes   = fmt.Errorf("proto: integer overflow")
)

func yoloString(buf []byte) string {
	return *((*string)(unsafe.Pointer(&buf)))
}

var (
	ErrInvalidLengthMimir = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowMimir   = fmt.Errorf("proto: integer overflow")
)

//func DecodeString(d *csproto.Decoder) (string, error) {
//	if d.offset >= len(d.p) {
//		return "", io.ErrUnexpectedEOF
//	}
//	b, err := d.DecodeBytes()
//	if err != nil {
//		return "", fmt.Errorf("invalid data at byte %d: %w", d.offset, err)
//	}
//	switch d.mode {
//	case DecoderModeFast:
//		return *(*string)(unsafe.Pointer(&b)), nil //nolint: gosec // using unsafe on purpose
//
//	default:
//		// safe mode by default
//		return string(b), nil
//	}
//}
