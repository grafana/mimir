// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: alerts.proto

package alertspb

import (
	fmt "fmt"
	_ "github.com/gogo/protobuf/gogoproto"
	proto "github.com/gogo/protobuf/proto"
	clusterpb "github.com/prometheus/alertmanager/cluster/clusterpb"
	io "io"
	math "math"
	math_bits "math/bits"
	reflect "reflect"
	strings "strings"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion3 // please upgrade the proto package

type AlertConfigDesc struct {
	User      string          `protobuf:"bytes,1,opt,name=user,proto3" json:"user,omitempty"`
	RawConfig string          `protobuf:"bytes,2,opt,name=raw_config,json=rawConfig,proto3" json:"raw_config,omitempty"`
	Templates []*TemplateDesc `protobuf:"bytes,3,rep,name=templates,proto3" json:"templates,omitempty"`
}

func (m *AlertConfigDesc) Reset()      { *m = AlertConfigDesc{} }
func (*AlertConfigDesc) ProtoMessage() {}
func (*AlertConfigDesc) Descriptor() ([]byte, []int) {
	return fileDescriptor_20493709c38b81dc, []int{0}
}
func (m *AlertConfigDesc) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *AlertConfigDesc) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_AlertConfigDesc.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *AlertConfigDesc) XXX_Merge(src proto.Message) {
	xxx_messageInfo_AlertConfigDesc.Merge(m, src)
}
func (m *AlertConfigDesc) XXX_Size() int {
	return m.Size()
}
func (m *AlertConfigDesc) XXX_DiscardUnknown() {
	xxx_messageInfo_AlertConfigDesc.DiscardUnknown(m)
}

var xxx_messageInfo_AlertConfigDesc proto.InternalMessageInfo

func (m *AlertConfigDesc) GetUser() string {
	if m != nil {
		return m.User
	}
	return ""
}

func (m *AlertConfigDesc) GetRawConfig() string {
	if m != nil {
		return m.RawConfig
	}
	return ""
}

func (m *AlertConfigDesc) GetTemplates() []*TemplateDesc {
	if m != nil {
		return m.Templates
	}
	return nil
}

type TemplateDesc struct {
	Filename string `protobuf:"bytes,1,opt,name=filename,proto3" json:"filename,omitempty"`
	Body     string `protobuf:"bytes,2,opt,name=body,proto3" json:"body,omitempty"`
}

func (m *TemplateDesc) Reset()      { *m = TemplateDesc{} }
func (*TemplateDesc) ProtoMessage() {}
func (*TemplateDesc) Descriptor() ([]byte, []int) {
	return fileDescriptor_20493709c38b81dc, []int{1}
}
func (m *TemplateDesc) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *TemplateDesc) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_TemplateDesc.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *TemplateDesc) XXX_Merge(src proto.Message) {
	xxx_messageInfo_TemplateDesc.Merge(m, src)
}
func (m *TemplateDesc) XXX_Size() int {
	return m.Size()
}
func (m *TemplateDesc) XXX_DiscardUnknown() {
	xxx_messageInfo_TemplateDesc.DiscardUnknown(m)
}

var xxx_messageInfo_TemplateDesc proto.InternalMessageInfo

func (m *TemplateDesc) GetFilename() string {
	if m != nil {
		return m.Filename
	}
	return ""
}

func (m *TemplateDesc) GetBody() string {
	if m != nil {
		return m.Body
	}
	return ""
}

type FullStateDesc struct {
	State *clusterpb.FullState `protobuf:"bytes,1,opt,name=state,proto3" json:"state,omitempty"`
}

func (m *FullStateDesc) Reset()      { *m = FullStateDesc{} }
func (*FullStateDesc) ProtoMessage() {}
func (*FullStateDesc) Descriptor() ([]byte, []int) {
	return fileDescriptor_20493709c38b81dc, []int{2}
}
func (m *FullStateDesc) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *FullStateDesc) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_FullStateDesc.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *FullStateDesc) XXX_Merge(src proto.Message) {
	xxx_messageInfo_FullStateDesc.Merge(m, src)
}
func (m *FullStateDesc) XXX_Size() int {
	return m.Size()
}
func (m *FullStateDesc) XXX_DiscardUnknown() {
	xxx_messageInfo_FullStateDesc.DiscardUnknown(m)
}

var xxx_messageInfo_FullStateDesc proto.InternalMessageInfo

func (m *FullStateDesc) GetState() *clusterpb.FullState {
	if m != nil {
		return m.State
	}
	return nil
}

type GrafanaAlertConfigDesc struct {
	User               string `protobuf:"bytes,1,opt,name=user,proto3" json:"user,omitempty"`
	RawConfig          string `protobuf:"bytes,2,opt,name=raw_config,json=rawConfig,proto3" json:"raw_config,omitempty"`
	Hash               string `protobuf:"bytes,4,opt,name=hash,proto3" json:"hash,omitempty"`
	CreatedAtTimestamp int64  `protobuf:"varint,5,opt,name=created_at_timestamp,json=createdAtTimestamp,proto3" json:"created_at_timestamp,omitempty"`
	Default            bool   `protobuf:"varint,7,opt,name=default,proto3" json:"default,omitempty"`
}

func (m *GrafanaAlertConfigDesc) Reset()      { *m = GrafanaAlertConfigDesc{} }
func (*GrafanaAlertConfigDesc) ProtoMessage() {}
func (*GrafanaAlertConfigDesc) Descriptor() ([]byte, []int) {
	return fileDescriptor_20493709c38b81dc, []int{3}
}
func (m *GrafanaAlertConfigDesc) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *GrafanaAlertConfigDesc) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_GrafanaAlertConfigDesc.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *GrafanaAlertConfigDesc) XXX_Merge(src proto.Message) {
	xxx_messageInfo_GrafanaAlertConfigDesc.Merge(m, src)
}
func (m *GrafanaAlertConfigDesc) XXX_Size() int {
	return m.Size()
}
func (m *GrafanaAlertConfigDesc) XXX_DiscardUnknown() {
	xxx_messageInfo_GrafanaAlertConfigDesc.DiscardUnknown(m)
}

var xxx_messageInfo_GrafanaAlertConfigDesc proto.InternalMessageInfo

func (m *GrafanaAlertConfigDesc) GetUser() string {
	if m != nil {
		return m.User
	}
	return ""
}

func (m *GrafanaAlertConfigDesc) GetRawConfig() string {
	if m != nil {
		return m.RawConfig
	}
	return ""
}

func (m *GrafanaAlertConfigDesc) GetHash() string {
	if m != nil {
		return m.Hash
	}
	return ""
}

func (m *GrafanaAlertConfigDesc) GetCreatedAtTimestamp() int64 {
	if m != nil {
		return m.CreatedAtTimestamp
	}
	return 0
}

func (m *GrafanaAlertConfigDesc) GetDefault() bool {
	if m != nil {
		return m.Default
	}
	return false
}

func init() {
	proto.RegisterType((*AlertConfigDesc)(nil), "alerts.AlertConfigDesc")
	proto.RegisterType((*TemplateDesc)(nil), "alerts.TemplateDesc")
	proto.RegisterType((*FullStateDesc)(nil), "alerts.FullStateDesc")
	proto.RegisterType((*GrafanaAlertConfigDesc)(nil), "alerts.GrafanaAlertConfigDesc")
}

func init() { proto.RegisterFile("alerts.proto", fileDescriptor_20493709c38b81dc) }

var fileDescriptor_20493709c38b81dc = []byte{
	// 403 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xac, 0x51, 0xbd, 0xae, 0xd3, 0x30,
	0x18, 0x8d, 0x49, 0xee, 0xbd, 0xa9, 0xb9, 0x08, 0x64, 0x55, 0x28, 0xaa, 0x84, 0x89, 0x32, 0x45,
	0x0c, 0x09, 0xba, 0x6c, 0x0c, 0x57, 0x6a, 0x41, 0x20, 0x31, 0x86, 0x4e, 0x2c, 0x95, 0x93, 0x3a,
	0x3f, 0x52, 0x12, 0x47, 0xb6, 0xa3, 0xc2, 0xc6, 0x23, 0xf0, 0x08, 0x8c, 0xbc, 0x01, 0xaf, 0xc0,
	0xd8, 0xb1, 0x23, 0x4d, 0x97, 0x8e, 0x7d, 0x04, 0x14, 0x3b, 0x69, 0x79, 0x80, 0x3b, 0xe5, 0x9c,
	0x9c, 0x73, 0xbe, 0x1f, 0x7f, 0xf0, 0x96, 0x94, 0x94, 0x4b, 0x11, 0x34, 0x9c, 0x49, 0x86, 0xae,
	0x35, 0x9b, 0x4d, 0x33, 0x96, 0x31, 0xf5, 0x2b, 0xec, 0x91, 0x56, 0x67, 0x8b, 0xac, 0x90, 0x79,
	0x1b, 0x07, 0x09, 0xab, 0xc2, 0x86, 0xb3, 0x8a, 0xca, 0x9c, 0xb6, 0x22, 0x54, 0x99, 0x8a, 0xd4,
	0x24, 0xa3, 0x3c, 0x4c, 0xca, 0x56, 0xc8, 0xcb, 0xb7, 0x89, 0x47, 0xa4, 0x6b, 0x78, 0x5f, 0xe1,
	0xd3, 0x79, 0xef, 0x7f, 0xc7, 0xea, 0xb4, 0xc8, 0xde, 0x53, 0x91, 0x20, 0x04, 0xad, 0x56, 0x50,
	0xee, 0x00, 0x17, 0xf8, 0x93, 0x48, 0x61, 0xf4, 0x02, 0x42, 0x4e, 0x36, 0xab, 0x44, 0xb9, 0x9c,
	0x47, 0x4a, 0x99, 0x70, 0xb2, 0xd1, 0x31, 0x74, 0x07, 0x27, 0x92, 0x56, 0x4d, 0x49, 0x24, 0x15,
	0x8e, 0xe9, 0x9a, 0xfe, 0xe3, 0xbb, 0x69, 0x30, 0x6c, 0xb2, 0x1c, 0x84, 0xbe, 0x76, 0x74, 0xb1,
	0x79, 0xf7, 0xf0, 0xf6, 0x7f, 0x09, 0xcd, 0xa0, 0x9d, 0x16, 0x25, 0xad, 0x49, 0x45, 0x87, 0xd6,
	0x67, 0xde, 0x8f, 0x14, 0xb3, 0xf5, 0xb7, 0xa1, 0xb1, 0xc2, 0xde, 0x1c, 0x3e, 0xf9, 0xd0, 0x96,
	0xe5, 0x67, 0x39, 0x16, 0x78, 0x05, 0xaf, 0x44, 0x4f, 0x54, 0xba, 0x1f, 0xe0, 0xbc, 0x73, 0x70,
	0x36, 0x46, 0xda, 0xf2, 0xd6, 0x3a, 0xfe, 0x7c, 0x69, 0x78, 0xbf, 0x01, 0x7c, 0xfe, 0x91, 0x93,
	0x94, 0xd4, 0xe4, 0x01, 0x1e, 0x01, 0x41, 0x2b, 0x27, 0x22, 0x77, 0x2c, 0x1d, 0xe9, 0x31, 0x7a,
	0x0d, 0xa7, 0x09, 0xa7, 0x44, 0xd2, 0xf5, 0x8a, 0xc8, 0x95, 0x2c, 0x2a, 0x2a, 0x24, 0xa9, 0x1a,
	0xe7, 0xca, 0x05, 0xbe, 0x19, 0xa1, 0x41, 0x9b, 0xcb, 0xe5, 0xa8, 0x20, 0x07, 0xde, 0xac, 0x69,
	0x4a, 0xda, 0x52, 0x3a, 0x37, 0x2e, 0xf0, 0xed, 0x68, 0xa4, 0x7a, 0xe6, 0x4f, 0x96, 0x6d, 0x3e,
	0xb3, 0x16, 0xf7, 0xdb, 0x3d, 0x36, 0x76, 0x7b, 0x6c, 0x9c, 0xf6, 0x18, 0x7c, 0xef, 0x30, 0xf8,
	0xd5, 0x61, 0xf0, 0xa7, 0xc3, 0x60, 0xdb, 0x61, 0xf0, 0xb7, 0xc3, 0xe0, 0xd8, 0x61, 0xe3, 0xd4,
	0x61, 0xf0, 0xe3, 0x80, 0x8d, 0xed, 0x01, 0x1b, 0xbb, 0x03, 0x36, 0xbe, 0xd8, 0xfa, 0x24, 0x4d,
	0x1c, 0x5f, 0xab, 0xeb, 0xbf, 0xf9, 0x17, 0x00, 0x00, 0xff, 0xff, 0x71, 0xad, 0x28, 0x26, 0x6f,
	0x02, 0x00, 0x00,
}

func (this *AlertConfigDesc) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	that1, ok := that.(*AlertConfigDesc)
	if !ok {
		that2, ok := that.(AlertConfigDesc)
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
	if this.User != that1.User {
		return false
	}
	if this.RawConfig != that1.RawConfig {
		return false
	}
	if len(this.Templates) != len(that1.Templates) {
		return false
	}
	for i := range this.Templates {
		if !this.Templates[i].Equal(that1.Templates[i]) {
			return false
		}
	}
	return true
}
func (this *TemplateDesc) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	that1, ok := that.(*TemplateDesc)
	if !ok {
		that2, ok := that.(TemplateDesc)
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
	if this.Filename != that1.Filename {
		return false
	}
	if this.Body != that1.Body {
		return false
	}
	return true
}
func (this *AlertConfigDesc) GoString() string {
	if this == nil {
		return "nil"
	}
	s := make([]string, 0, 7)
	s = append(s, "&alertspb.AlertConfigDesc{")
	s = append(s, "User: "+fmt.Sprintf("%#v", this.User)+",\n")
	s = append(s, "RawConfig: "+fmt.Sprintf("%#v", this.RawConfig)+",\n")
	if this.Templates != nil {
		s = append(s, "Templates: "+fmt.Sprintf("%#v", this.Templates)+",\n")
	}
	s = append(s, "}")
	return strings.Join(s, "")
}
func (this *TemplateDesc) GoString() string {
	if this == nil {
		return "nil"
	}
	s := make([]string, 0, 6)
	s = append(s, "&alertspb.TemplateDesc{")
	s = append(s, "Filename: "+fmt.Sprintf("%#v", this.Filename)+",\n")
	s = append(s, "Body: "+fmt.Sprintf("%#v", this.Body)+",\n")
	s = append(s, "}")
	return strings.Join(s, "")
}
func (this *FullStateDesc) GoString() string {
	if this == nil {
		return "nil"
	}
	s := make([]string, 0, 5)
	s = append(s, "&alertspb.FullStateDesc{")
	if this.State != nil {
		s = append(s, "State: "+fmt.Sprintf("%#v", this.State)+",\n")
	}
	s = append(s, "}")
	return strings.Join(s, "")
}
func (this *GrafanaAlertConfigDesc) GoString() string {
	if this == nil {
		return "nil"
	}
	s := make([]string, 0, 9)
	s = append(s, "&alertspb.GrafanaAlertConfigDesc{")
	s = append(s, "User: "+fmt.Sprintf("%#v", this.User)+",\n")
	s = append(s, "RawConfig: "+fmt.Sprintf("%#v", this.RawConfig)+",\n")
	s = append(s, "Hash: "+fmt.Sprintf("%#v", this.Hash)+",\n")
	s = append(s, "CreatedAtTimestamp: "+fmt.Sprintf("%#v", this.CreatedAtTimestamp)+",\n")
	s = append(s, "Default: "+fmt.Sprintf("%#v", this.Default)+",\n")
	s = append(s, "}")
	return strings.Join(s, "")
}
func valueToGoStringAlerts(v interface{}, typ string) string {
	rv := reflect.ValueOf(v)
	if rv.IsNil() {
		return "nil"
	}
	pv := reflect.Indirect(rv).Interface()
	return fmt.Sprintf("func(v %v) *%v { return &v } ( %#v )", typ, typ, pv)
}
func (m *AlertConfigDesc) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *AlertConfigDesc) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *AlertConfigDesc) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Templates) > 0 {
		for iNdEx := len(m.Templates) - 1; iNdEx >= 0; iNdEx-- {
			{
				size, err := m.Templates[iNdEx].MarshalToSizedBuffer(dAtA[:i])
				if err != nil {
					return 0, err
				}
				i -= size
				i = encodeVarintAlerts(dAtA, i, uint64(size))
			}
			i--
			dAtA[i] = 0x1a
		}
	}
	if len(m.RawConfig) > 0 {
		i -= len(m.RawConfig)
		copy(dAtA[i:], m.RawConfig)
		i = encodeVarintAlerts(dAtA, i, uint64(len(m.RawConfig)))
		i--
		dAtA[i] = 0x12
	}
	if len(m.User) > 0 {
		i -= len(m.User)
		copy(dAtA[i:], m.User)
		i = encodeVarintAlerts(dAtA, i, uint64(len(m.User)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *TemplateDesc) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *TemplateDesc) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *TemplateDesc) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Body) > 0 {
		i -= len(m.Body)
		copy(dAtA[i:], m.Body)
		i = encodeVarintAlerts(dAtA, i, uint64(len(m.Body)))
		i--
		dAtA[i] = 0x12
	}
	if len(m.Filename) > 0 {
		i -= len(m.Filename)
		copy(dAtA[i:], m.Filename)
		i = encodeVarintAlerts(dAtA, i, uint64(len(m.Filename)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *FullStateDesc) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *FullStateDesc) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *FullStateDesc) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.State != nil {
		{
			size, err := m.State.MarshalToSizedBuffer(dAtA[:i])
			if err != nil {
				return 0, err
			}
			i -= size
			i = encodeVarintAlerts(dAtA, i, uint64(size))
		}
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *GrafanaAlertConfigDesc) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *GrafanaAlertConfigDesc) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *GrafanaAlertConfigDesc) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.Default {
		i--
		if m.Default {
			dAtA[i] = 1
		} else {
			dAtA[i] = 0
		}
		i--
		dAtA[i] = 0x38
	}
	if m.CreatedAtTimestamp != 0 {
		i = encodeVarintAlerts(dAtA, i, uint64(m.CreatedAtTimestamp))
		i--
		dAtA[i] = 0x28
	}
	if len(m.Hash) > 0 {
		i -= len(m.Hash)
		copy(dAtA[i:], m.Hash)
		i = encodeVarintAlerts(dAtA, i, uint64(len(m.Hash)))
		i--
		dAtA[i] = 0x22
	}
	if len(m.RawConfig) > 0 {
		i -= len(m.RawConfig)
		copy(dAtA[i:], m.RawConfig)
		i = encodeVarintAlerts(dAtA, i, uint64(len(m.RawConfig)))
		i--
		dAtA[i] = 0x12
	}
	if len(m.User) > 0 {
		i -= len(m.User)
		copy(dAtA[i:], m.User)
		i = encodeVarintAlerts(dAtA, i, uint64(len(m.User)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func encodeVarintAlerts(dAtA []byte, offset int, v uint64) int {
	offset -= sovAlerts(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *AlertConfigDesc) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.User)
	if l > 0 {
		n += 1 + l + sovAlerts(uint64(l))
	}
	l = len(m.RawConfig)
	if l > 0 {
		n += 1 + l + sovAlerts(uint64(l))
	}
	if len(m.Templates) > 0 {
		for _, e := range m.Templates {
			l = e.Size()
			n += 1 + l + sovAlerts(uint64(l))
		}
	}
	return n
}

func (m *TemplateDesc) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Filename)
	if l > 0 {
		n += 1 + l + sovAlerts(uint64(l))
	}
	l = len(m.Body)
	if l > 0 {
		n += 1 + l + sovAlerts(uint64(l))
	}
	return n
}

func (m *FullStateDesc) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.State != nil {
		l = m.State.Size()
		n += 1 + l + sovAlerts(uint64(l))
	}
	return n
}

func (m *GrafanaAlertConfigDesc) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.User)
	if l > 0 {
		n += 1 + l + sovAlerts(uint64(l))
	}
	l = len(m.RawConfig)
	if l > 0 {
		n += 1 + l + sovAlerts(uint64(l))
	}
	l = len(m.Hash)
	if l > 0 {
		n += 1 + l + sovAlerts(uint64(l))
	}
	if m.CreatedAtTimestamp != 0 {
		n += 1 + sovAlerts(uint64(m.CreatedAtTimestamp))
	}
	if m.Default {
		n += 2
	}
	return n
}

func sovAlerts(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozAlerts(x uint64) (n int) {
	return sovAlerts(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (this *AlertConfigDesc) String() string {
	if this == nil {
		return "nil"
	}
	repeatedStringForTemplates := "[]*TemplateDesc{"
	for _, f := range this.Templates {
		repeatedStringForTemplates += strings.Replace(f.String(), "TemplateDesc", "TemplateDesc", 1) + ","
	}
	repeatedStringForTemplates += "}"
	s := strings.Join([]string{`&AlertConfigDesc{`,
		`User:` + fmt.Sprintf("%v", this.User) + `,`,
		`RawConfig:` + fmt.Sprintf("%v", this.RawConfig) + `,`,
		`Templates:` + repeatedStringForTemplates + `,`,
		`}`,
	}, "")
	return s
}
func (this *TemplateDesc) String() string {
	if this == nil {
		return "nil"
	}
	s := strings.Join([]string{`&TemplateDesc{`,
		`Filename:` + fmt.Sprintf("%v", this.Filename) + `,`,
		`Body:` + fmt.Sprintf("%v", this.Body) + `,`,
		`}`,
	}, "")
	return s
}
func (this *FullStateDesc) String() string {
	if this == nil {
		return "nil"
	}
	s := strings.Join([]string{`&FullStateDesc{`,
		`State:` + strings.Replace(fmt.Sprintf("%v", this.State), "FullState", "clusterpb.FullState", 1) + `,`,
		`}`,
	}, "")
	return s
}
func (this *GrafanaAlertConfigDesc) String() string {
	if this == nil {
		return "nil"
	}
	s := strings.Join([]string{`&GrafanaAlertConfigDesc{`,
		`User:` + fmt.Sprintf("%v", this.User) + `,`,
		`RawConfig:` + fmt.Sprintf("%v", this.RawConfig) + `,`,
		`Hash:` + fmt.Sprintf("%v", this.Hash) + `,`,
		`CreatedAtTimestamp:` + fmt.Sprintf("%v", this.CreatedAtTimestamp) + `,`,
		`Default:` + fmt.Sprintf("%v", this.Default) + `,`,
		`}`,
	}, "")
	return s
}
func valueToStringAlerts(v interface{}) string {
	rv := reflect.ValueOf(v)
	if rv.IsNil() {
		return "nil"
	}
	pv := reflect.Indirect(rv).Interface()
	return fmt.Sprintf("*%v", pv)
}
func (m *AlertConfigDesc) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowAlerts
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
			return fmt.Errorf("proto: AlertConfigDesc: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: AlertConfigDesc: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field User", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowAlerts
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
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
				return ErrInvalidLengthAlerts
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthAlerts
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.User = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field RawConfig", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowAlerts
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
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
				return ErrInvalidLengthAlerts
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthAlerts
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.RawConfig = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Templates", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowAlerts
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
				return ErrInvalidLengthAlerts
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthAlerts
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Templates = append(m.Templates, &TemplateDesc{})
			if err := m.Templates[len(m.Templates)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipAlerts(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthAlerts
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthAlerts
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
func (m *TemplateDesc) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowAlerts
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
			return fmt.Errorf("proto: TemplateDesc: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: TemplateDesc: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Filename", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowAlerts
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
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
				return ErrInvalidLengthAlerts
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthAlerts
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Filename = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Body", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowAlerts
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
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
				return ErrInvalidLengthAlerts
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthAlerts
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Body = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipAlerts(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthAlerts
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthAlerts
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
func (m *FullStateDesc) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowAlerts
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
			return fmt.Errorf("proto: FullStateDesc: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: FullStateDesc: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field State", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowAlerts
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
				return ErrInvalidLengthAlerts
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthAlerts
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.State == nil {
				m.State = &clusterpb.FullState{}
			}
			if err := m.State.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipAlerts(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthAlerts
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthAlerts
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
func (m *GrafanaAlertConfigDesc) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowAlerts
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
			return fmt.Errorf("proto: GrafanaAlertConfigDesc: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: GrafanaAlertConfigDesc: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field User", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowAlerts
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
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
				return ErrInvalidLengthAlerts
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthAlerts
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.User = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field RawConfig", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowAlerts
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
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
				return ErrInvalidLengthAlerts
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthAlerts
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.RawConfig = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 4:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Hash", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowAlerts
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
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
				return ErrInvalidLengthAlerts
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthAlerts
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Hash = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 5:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field CreatedAtTimestamp", wireType)
			}
			m.CreatedAtTimestamp = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowAlerts
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.CreatedAtTimestamp |= int64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 7:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Default", wireType)
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowAlerts
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				v |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.Default = bool(v != 0)
		default:
			iNdEx = preIndex
			skippy, err := skipAlerts(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthAlerts
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthAlerts
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
func skipAlerts(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowAlerts
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
					return 0, ErrIntOverflowAlerts
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
			return iNdEx, nil
		case 1:
			iNdEx += 8
			return iNdEx, nil
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowAlerts
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
				return 0, ErrInvalidLengthAlerts
			}
			iNdEx += length
			if iNdEx < 0 {
				return 0, ErrInvalidLengthAlerts
			}
			return iNdEx, nil
		case 3:
			for {
				var innerWire uint64
				var start int = iNdEx
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return 0, ErrIntOverflowAlerts
					}
					if iNdEx >= l {
						return 0, io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					innerWire |= (uint64(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				innerWireType := int(innerWire & 0x7)
				if innerWireType == 4 {
					break
				}
				next, err := skipAlerts(dAtA[start:])
				if err != nil {
					return 0, err
				}
				iNdEx = start + next
				if iNdEx < 0 {
					return 0, ErrInvalidLengthAlerts
				}
			}
			return iNdEx, nil
		case 4:
			return iNdEx, nil
		case 5:
			iNdEx += 4
			return iNdEx, nil
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
	}
	panic("unreachable")
}

var (
	ErrInvalidLengthAlerts = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowAlerts   = fmt.Errorf("proto: integer overflow")
)
