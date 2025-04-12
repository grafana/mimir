// SPDX-License-Identifier: AGPL-3.0-only

package protobuf

import (
	"fmt"
	"io"
	"slices"
	"sync"
	"unsafe"

	"github.com/grafana/mimir/pkg/mimirpb"
)

var (
	labelBufferPool = sync.Pool{
		New: func() any {
			return []byte{}
		},
	}
)

type LabelBuffer struct {
	buf []byte
}

func NewLabelBuffer() LabelBuffer {
	return LabelBuffer{
		buf: labelBufferPool.Get().([]byte)[:0],
	}
}

func (b *LabelBuffer) Capacity() int {
	return cap(b.buf) - len(b.buf)
}

func (b *LabelBuffer) Reserve(size int) {
	if b.buf == nil {
		b.buf = labelBufferPool.Get().([]byte)[:0]
	}
	if len(b.buf) > 0 {
		panic(fmt.Errorf("LabelBuffer.Reserve: len(b.buf) > 0: %d", len(b.buf)))
	}
	if cap(b.buf) < size {
		b.buf = make([]byte, 0, size)
	}
}

func (b *LabelBuffer) Add(data []byte) string {
	oldLen := len(b.buf)
	newLen := oldLen + len(data)
	if cap(b.buf)-len(b.buf) < len(data) {
		// TODO: Always make sure enough capacity has been reserved up front.
		b.buf = slices.Grow(b.buf, len(data))
	}
	b.buf = b.buf[0:newLen]
	copy(b.buf[oldLen:newLen], data)

	return yoloString(b.buf[oldLen:newLen])
}

func (b *LabelBuffer) Release() {
	//nolint:staticcheck
	labelBufferPool.Put(b.buf[:0])
	b.buf = nil
}

func UnmarshalLabelAdapter(la *mimirpb.LabelAdapter, data []byte, buf *LabelBuffer, calcSize bool) (int, error) {
	bufSize := 0
	l := len(data)
	index := 0
	for index < l {
		preIndex := index
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return bufSize, mimirpb.ErrIntOverflowMimir
			}
			if index >= l {
				return bufSize, io.ErrUnexpectedEOF
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
			return bufSize, fmt.Errorf("proto: LabelPair: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return bufSize, fmt.Errorf("proto: LabelPair: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return bufSize, fmt.Errorf("proto: wrong wireType = %d for field Name", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return bufSize, mimirpb.ErrIntOverflowMimir
				}
				if index >= l {
					return bufSize, io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return bufSize, mimirpb.ErrInvalidLengthMimir
			}
			postIndex := index + byteLen
			if postIndex < 0 {
				return bufSize, mimirpb.ErrInvalidLengthMimir
			}
			if postIndex > l {
				return bufSize, io.ErrUnexpectedEOF
			}
			if calcSize {
				bufSize += postIndex - index
			} else {
				la.Name = buf.Add(data[index:postIndex])
			}
			index = postIndex
		case 2:
			if wireType != 2 {
				return bufSize, fmt.Errorf("proto: wrong wireType = %d for field Value", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return bufSize, mimirpb.ErrIntOverflowMimir
				}
				if index >= l {
					return bufSize, io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return bufSize, mimirpb.ErrInvalidLengthMimir
			}
			postIndex := index + byteLen
			if postIndex < 0 {
				return bufSize, mimirpb.ErrInvalidLengthMimir
			}
			if postIndex > l {
				return bufSize, io.ErrUnexpectedEOF
			}
			if calcSize {
				bufSize += postIndex - index
			} else {
				la.Value = buf.Add(data[index:postIndex])
			}
			index = postIndex
		default:
			index = preIndex
			skippy, err := skipMimir(data[index:])
			if err != nil {
				return bufSize, err
			}
			if skippy < 0 {
				return bufSize, mimirpb.ErrInvalidLengthMimir
			}
			if (index + skippy) < 0 {
				return bufSize, mimirpb.ErrInvalidLengthMimir
			}
			if (index + skippy) > l {
				return bufSize, io.ErrUnexpectedEOF
			}
			index += skippy
		}
	}
	if index > l {
		return bufSize, io.ErrUnexpectedEOF
	}

	return bufSize, nil
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

func yoloString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b)) // nolint:gosec
}
