// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import "bytes"

// This file implements the wiresmith customtype contract
// (SizeWiresmith/MarshalWiresmith/UnmarshalWiresmith/EqualWiresmith/CompareWiresmith)
// for the custom types referenced from mimir.proto via
// (wiresmith.options.customtype). The methods delegate to the pre-existing
// gogo-style implementations so the zero-copy unmarshal and marshal-cache
// optimizations keep working unchanged.

// SizeWiresmith implements the wiresmith customtype contract. It returns the
// cached marshalled size when available (see PreallocTimeseries.Marshal).
func (p *PreallocTimeseries) SizeWiresmith() int {
	return p.Size()
}

// MarshalWiresmith implements the wiresmith customtype contract: buf is
// exactly SizeWiresmith() bytes, so the gogo-style reverse writer fills it
// completely.
func (p *PreallocTimeseries) MarshalWiresmith(buf []byte) (int, error) {
	return p.MarshalToSizedBuffer(buf)
}

// UnmarshalWiresmith implements the wiresmith customtype contract for the
// Remote Write 1.0 path. The RW2 path never goes through this method: the
// expected-diff patch on the generated WriteRequest unmarshal calls
// PreallocTimeseries.Unmarshal with RW2 symbols directly.
func (p *PreallocTimeseries) UnmarshalWiresmith(buf []byte) error {
	return p.Unmarshal(buf, nil, nil, false)
}

// EqualWiresmith implements the wiresmith customtype contract.
func (p *PreallocTimeseries) EqualWiresmith(other any) bool {
	o, ok := other.(PreallocTimeseries)
	if !ok {
		return false
	}
	return p.Equal(o.TimeSeries)
}

// CompareWiresmith implements the wiresmith customtype contract.
func (p *PreallocTimeseries) CompareWiresmith(other any) int {
	o, ok := other.(PreallocTimeseries)
	if !ok {
		return -1
	}
	return p.Compare(o.TimeSeries)
}

// SizeWiresmith implements the wiresmith customtype contract.
func (bs *LabelAdapter) SizeWiresmith() int {
	return bs.Size()
}

// MarshalWiresmith implements the wiresmith customtype contract: buf is
// exactly SizeWiresmith() bytes, so the gogo-style reverse writer fills it
// completely.
func (bs *LabelAdapter) MarshalWiresmith(buf []byte) (int, error) {
	return bs.MarshalToSizedBuffer(buf)
}

// UnmarshalWiresmith implements the wiresmith customtype contract. It keeps
// yolo-string references into buf instead of copying; see the type docs.
func (bs *LabelAdapter) UnmarshalWiresmith(buf []byte) error {
	return bs.Unmarshal(buf)
}

// EqualWiresmith implements the wiresmith customtype contract.
func (bs *LabelAdapter) EqualWiresmith(other any) bool {
	o, ok := other.(LabelAdapter)
	if !ok {
		return false
	}
	return bs.Name == o.Name && bs.Value == o.Value
}

// CompareWiresmith implements the wiresmith customtype contract.
func (bs *LabelAdapter) CompareWiresmith(other any) int {
	o, ok := other.(LabelAdapter)
	if !ok {
		return -1
	}
	return bs.Compare(o)
}

// SizeWiresmith implements the wiresmith customtype contract.
func (t *UnsafeByteSlice) SizeWiresmith() int {
	return t.Size()
}

// MarshalWiresmith implements the wiresmith customtype contract: buf is exactly
// SizeWiresmith() bytes; copy the raw bytes forward into it.
func (t *UnsafeByteSlice) MarshalWiresmith(buf []byte) (int, error) {
	return t.MarshalTo(buf)
}

// UnmarshalWiresmith implements the wiresmith customtype contract. It keeps an
// unsafe reference into buf rather than copying; see UnsafeByteSlice docs.
func (t *UnsafeByteSlice) UnmarshalWiresmith(buf []byte) error {
	return t.Unmarshal(buf)
}

// EqualWiresmith implements the wiresmith customtype contract.
func (t *UnsafeByteSlice) EqualWiresmith(other any) bool {
	o, ok := other.(UnsafeByteSlice)
	if !ok {
		return false
	}
	return t.Equal(o)
}

// CompareWiresmith implements the wiresmith customtype contract.
func (t *UnsafeByteSlice) CompareWiresmith(other any) int {
	o, ok := other.(UnsafeByteSlice)
	if !ok {
		return -1
	}
	return bytes.Compare([]byte(*t), []byte(o))
}
