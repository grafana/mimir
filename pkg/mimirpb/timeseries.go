// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/cortexpb/timeseries.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package mimirpb

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"unsafe"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/util/zeropool"
	"golang.org/x/exp/slices"
)

const (
	minPreallocatedTimeseries         = 100
	minPreallocatedLabels             = 20
	minPreallocatedSamplesPerSeries   = 10
	minPreallocatedExemplarsPerSeries = 1
	maxPreallocatedExemplarsPerSeries = 10
)

var (
	preallocTimeseriesSlicePool = zeropool.New(func() []PreallocTimeseries {
		return make([]PreallocTimeseries, 0, minPreallocatedTimeseries)
	})

	timeSeriesPool = sync.Pool{
		New: func() interface{} {
			return &TimeSeries{
				Labels:     make([]LabelAdapter, 0, minPreallocatedLabels),
				Samples:    make([]Sample, 0, minPreallocatedSamplesPerSeries),
				Exemplars:  make([]Exemplar, 0, minPreallocatedExemplarsPerSeries),
				Histograms: nil,
			}
		},
	}

	// yoloSlicePool is a pool of byte slices which are used to back the yoloStrings of this package.
	yoloSlicePool = sync.Pool{
		New: func() interface{} {
			// The initial cap of 200 is an arbitrary number which has been chosen because the default
			// of 0 is guaranteed to be insufficient, so any number greater than 0 would be better.
			// 200 should be enough to back all the strings of one TimeSeries in many cases.
			val := make([]byte, 0, 200)
			return &val
		},
	}
)

// PreallocWriteRequest is a WriteRequest which preallocs slices on Unmarshal.
type PreallocWriteRequest struct {
	WriteRequest

	// SkipUnmarshalingExemplars is an optimization to not unmarshal exemplars when they are disabled by the config anyway.
	SkipUnmarshalingExemplars bool
}

// Unmarshal implements proto.Message.
// Copied from the protobuf generated code, the only change is that in case 1 the value of .SkipUnmarshalingExemplars
// gets copied into the PreallocTimeseries{} object which gets appended to Timeseries.
func (p *PreallocWriteRequest) Unmarshal(dAtA []byte) error {
	p.Timeseries = PreallocTimeseriesSliceFromPool()
	p.WriteRequest.skipUnmarshalingExemplars = p.SkipUnmarshalingExemplars
	return p.WriteRequest.Unmarshal(dAtA)
}

func (p *WriteRequest) ClearTimeseriesUnmarshalData() {
	for idx := range p.Timeseries {
		p.Timeseries[idx].clearUnmarshalData()
	}
}

// PreallocTimeseries is a TimeSeries which preallocs slices on Unmarshal.
type PreallocTimeseries struct {
	*TimeSeries

	// yoloSlice may contain a reference to the byte slice which was used to back the yolo strings
	// of the properties and sub-properties of this TimeSeries.
	// If it is set to a non-nil value then it must be returned to the yoloSlicePool on cleanup,
	// if it is set to nil then it can be ignored because the backing byte slice came from somewhere else.
	yoloSlice *[]byte

	// Original data used for unmarshalling this PreallocTimeseries. When set, Marshal methods will return it
	// instead of doing full marshalling again. This assumes that this instance hasn't changed.
	marshalledData []byte

	skipUnmarshalingExemplars bool
}

// RemoveLabel removes the label labelName from this timeseries, if it exists.
func (p *PreallocTimeseries) RemoveLabel(labelName string) {
	for i := 0; i < len(p.Labels); i++ {
		pair := p.Labels[i]
		if pair.Name == labelName {
			p.Labels = append(p.Labels[:i], p.Labels[i+1:]...)
			p.clearUnmarshalData()
			return
		}
	}
}

func (p *PreallocTimeseries) SetLabels(lbls []LabelAdapter) {
	p.Labels = lbls

	// We can't reuse raw unmarshalled data for the timeseries after setting new labels.
	// (Maybe we could, if labels are exactly the same, but it's expensive to check.)
	p.clearUnmarshalData()
}

// RemoveEmptyLabelValues remove labels with value=="" from this timeseries, updating the slice in-place.
func (p *PreallocTimeseries) RemoveEmptyLabelValues() {
	modified := false
	for i := len(p.Labels) - 1; i >= 0; i-- {
		if p.Labels[i].Value == "" {
			p.Labels = append(p.Labels[:i], p.Labels[i+1:]...)
			modified = true
		}
	}
	if modified {
		p.clearUnmarshalData()
	}
}

// SortLabelsIfNeeded sorts labels if they were not sorted before.
func (p *PreallocTimeseries) SortLabelsIfNeeded() {
	// no need to run sort.Slice, if labels are already sorted, which is most of the time.
	// we can avoid extra memory allocations (mostly interface-related) this way.
	sorted := true
	last := ""
	for _, l := range p.Labels {
		if last > l.Name {
			sorted = false
			break
		}
		last = l.Name
	}

	if sorted {
		return
	}

	slices.SortFunc(p.Labels, func(a, b LabelAdapter) int {
		switch {
		case a.Name < b.Name:
			return -1
		case a.Name > b.Name:
			return 1
		default:
			return 0
		}
	})
	p.clearUnmarshalData()
}

func (p *PreallocTimeseries) ClearExemplars() {
	ClearExemplars(p.TimeSeries)
	p.clearUnmarshalData()
}

func (p *PreallocTimeseries) ResizeExemplars(newSize int) {
	if len(p.Exemplars) <= newSize {
		return
	}
	// Name and Value may point into a large gRPC buffer, so clear the reference in each exemplar to allow GC
	for i := newSize; i < len(p.Exemplars); i++ {
		for j := range p.Exemplars[i].Labels {
			p.Exemplars[i].Labels[j].Name = ""
			p.Exemplars[i].Labels[j].Value = ""
		}
	}
	p.Exemplars = p.Exemplars[:newSize]
	p.clearUnmarshalData()
}

func (p *PreallocTimeseries) HistogramsUpdated() {
	p.clearUnmarshalData()
}

// DeleteExemplarByMovingLast deletes the exemplar by moving the last one on top and shortening the slice.
func (p *PreallocTimeseries) DeleteExemplarByMovingLast(ix int) {
	last := len(p.Exemplars) - 1
	if ix < last {
		p.Exemplars[ix] = p.Exemplars[last]
	}
	p.Exemplars = p.Exemplars[:last]
	p.clearUnmarshalData()
}

func (p *PreallocTimeseries) SortExemplars() {
	sort.Slice(p.Exemplars, func(i, j int) bool {
		return p.Exemplars[i].TimestampMs < p.Exemplars[j].TimestampMs
	})
	p.clearUnmarshalData()
}

// clearUnmarshalData removes cached unmarshalled version of the message.
func (p *PreallocTimeseries) clearUnmarshalData() {
	p.marshalledData = nil
}

var TimeseriesUnmarshalCachingEnabled = true

// Unmarshal implements proto.Message. Input data slice is retained.
// Copied from the protobuf generated code, the only change is that in case 3 the exemplars don't get unmarshaled
// if p.skipUnmarshalingExemplars is false.
func (p *PreallocTimeseries) Unmarshal(dAtA []byte) error {
	if TimeseriesUnmarshalCachingEnabled {
		p.marshalledData = dAtA
	}
	p.TimeSeries = TimeseriesFromPool()
	p.TimeSeries.SkipUnmarshalingExemplars = p.skipUnmarshalingExemplars
	return p.TimeSeries.Unmarshal(dAtA)
}

func (p *PreallocTimeseries) Size() int {
	if p.marshalledData != nil {
		return len(p.marshalledData)
	}
	return p.TimeSeries.Size()
}

func (p *PreallocTimeseries) Marshal() ([]byte, error) {
	if p.marshalledData != nil {
		return p.marshalledData, nil
	}
	return p.TimeSeries.Marshal()
}

func (p *PreallocTimeseries) MarshalTo(buf []byte) (int, error) {
	if p.marshalledData != nil && len(buf) >= len(p.marshalledData) {
		copy(buf, p.marshalledData)
		return len(p.marshalledData), nil
	}
	return p.TimeSeries.MarshalTo(buf)
}

func (p *PreallocTimeseries) MarshalToSizedBuffer(buf []byte) (int, error) {
	if p.marshalledData != nil && len(buf) >= len(p.marshalledData) {
		copy(buf, p.marshalledData)
		return len(p.marshalledData), nil
	}
	return p.TimeSeries.MarshalToSizedBuffer(buf)
}

// LabelAdapter is a labels.Label that can be marshalled to/from protos.
type LabelAdapter labels.Label

// Marshal implements proto.Marshaller.
func (bs *LabelAdapter) Marshal() ([]byte, error) {
	size := bs.Size()
	buf := make([]byte, size)
	n, err := bs.MarshalToSizedBuffer(buf[:size])
	if err != nil {
		return nil, err
	}
	return buf[:n], err
}

func (bs *LabelAdapter) MarshalTo(dAtA []byte) (int, error) {
	size := bs.Size()
	return bs.MarshalToSizedBuffer(dAtA[:size])
}

// MarshalTo implements proto.Marshaller.
func (bs *LabelAdapter) MarshalToSizedBuffer(buf []byte) (n int, err error) {
	ls := (*labels.Label)(bs)
	i := len(buf)
	if len(ls.Value) > 0 {
		i -= len(ls.Value)
		copy(buf[i:], ls.Value)
		i = encodeVarintMimir(buf, i, uint64(len(ls.Value)))
		i--
		buf[i] = 0x12
	}
	if len(ls.Name) > 0 {
		i -= len(ls.Name)
		copy(buf[i:], ls.Name)
		i = encodeVarintMimir(buf, i, uint64(len(ls.Name)))
		i--
		buf[i] = 0xa
	}
	return len(buf) - i, nil
}

// Unmarshal a LabelAdapter, implements proto.Unmarshaller.
// NB this is a copy of the autogenerated code to unmarshal a LabelPair,
// with the byte copying replaced with a yoloString.
func (bs *LabelAdapter) Unmarshal(dAtA []byte) error {
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
					return ErrIntOverflowMimir
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthMimir
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthMimir
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			bs.Name = yoloString(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Value", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMimir
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthMimir
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthMimir
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			bs.Value = yoloString(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipMimir(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthMimir
			}
			if (iNdEx + skippy) < 0 {
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

func yoloString(buf []byte) string {
	return *((*string)(unsafe.Pointer(&buf)))
}

// Size implements proto.Sizer.
func (bs *LabelAdapter) Size() (n int) {
	ls := (*labels.Label)(bs)
	if bs == nil {
		return 0
	}
	var l int
	_ = l
	l = len(ls.Name)
	if l > 0 {
		n += 1 + l + sovMimir(uint64(l))
	}
	l = len(ls.Value)
	if l > 0 {
		n += 1 + l + sovMimir(uint64(l))
	}
	return n
}

// Equal implements proto.Equaler.
func (bs *LabelAdapter) Equal(other LabelAdapter) bool {
	return bs.Name == other.Name && bs.Value == other.Value
}

// Compare implements proto.Comparer.
func (bs *LabelAdapter) Compare(other LabelAdapter) int {
	if c := strings.Compare(bs.Name, other.Name); c != 0 {
		return c
	}
	return strings.Compare(bs.Value, other.Value)
}

// PreallocTimeseriesSliceFromPool retrieves a slice of PreallocTimeseries from a sync.Pool.
// ReuseSlice should be called once done.
func PreallocTimeseriesSliceFromPool() []PreallocTimeseries {
	return preallocTimeseriesSlicePool.Get()
}

// ReuseSlice puts the slice back into a sync.Pool for reuse.
func ReuseSlice(ts []PreallocTimeseries) {
	if cap(ts) == 0 {
		return
	}

	for i := range ts {
		ReusePreallocTimeseries(&ts[i])
	}

	preallocTimeseriesSlicePool.Put(ts[:0])
}

// TimeseriesFromPool retrieves a pointer to a TimeSeries from a sync.Pool.
// ReuseTimeseries should be called once done, unless ReuseSlice was called on the slice that contains this TimeSeries.
func TimeseriesFromPool() *TimeSeries {
	return timeSeriesPool.Get().(*TimeSeries)
}

// ReuseTimeseries puts the timeseries back into a sync.Pool for reuse.
func ReuseTimeseries(ts *TimeSeries) {
	// Name and Value may point into a large gRPC buffer, so clear the reference to allow GC
	for i := 0; i < len(ts.Labels); i++ {
		ts.Labels[i].Name = ""
		ts.Labels[i].Value = ""
	}
	ts.Labels = ts.Labels[:0]
	ts.Samples = ts.Samples[:0]
	ts.Histograms = ts.Histograms[:0]

	ClearExemplars(ts)
	timeSeriesPool.Put(ts)
}

// ClearExemplars safely removes exemplars from TimeSeries.
func ClearExemplars(ts *TimeSeries) {
	// When cleaning exemplars, retain the slice only if its capacity is not bigger than
	// the desired max preallocated size. This allow us to ensure we don't put very large
	// slices back to the pool (e.g. a few requests with an huge number of exemplars may cause
	// in-use heap memory to significantly increase, because the slices allocated by such poison
	// requests would be reused by other requests with a normal number of exemplars).
	if cap(ts.Exemplars) > maxPreallocatedExemplarsPerSeries {
		ts.Exemplars = nil
		return
	}

	// Name and Value may point into a large gRPC buffer, so clear the reference in each exemplar to allow GC
	for i := range ts.Exemplars {
		for j := range ts.Exemplars[i].Labels {
			ts.Exemplars[i].Labels[j].Name = ""
			ts.Exemplars[i].Labels[j].Value = ""
		}
	}
	ts.Exemplars = ts.Exemplars[:0]
}

// ReusePreallocTimeseries puts the timeseries and the yoloSlice back into their respective pools for re-use.
func ReusePreallocTimeseries(ts *PreallocTimeseries) {
	if ts.TimeSeries != nil {
		ReuseTimeseries(ts.TimeSeries)
	}

	if ts.yoloSlice != nil {
		reuseYoloSlice(ts.yoloSlice)
		ts.yoloSlice = nil
	}

	ts.marshalledData = nil
}

func yoloSliceFromPool() *[]byte {
	return yoloSlicePool.Get().(*[]byte)
}

func reuseYoloSlice(val *[]byte) {
	*val = (*val)[:0]
	yoloSlicePool.Put(val)
}

// DeepCopyTimeseries copies the timeseries of one PreallocTimeseries into another one.
// It copies all the properties, sub-properties and strings by value to ensure that the two timeseries are not sharing
// anything after the deep copying.
// The returned PreallocTimeseries has a yoloSlice property which should be returned to the yoloSlicePool on cleanup.
func DeepCopyTimeseries(dst, src PreallocTimeseries, keepHistograms, keepExemplars bool) PreallocTimeseries {
	if dst.TimeSeries == nil {
		dst.TimeSeries = TimeseriesFromPool()
	}

	srcTs := src.TimeSeries
	dstTs := dst.TimeSeries

	// Prepare a buffer which is large enough to hold all the label names and values of src.
	requiredYoloSliceCap := countTotalLabelLen(srcTs, keepExemplars)
	dst.yoloSlice = yoloSliceFromPool()
	buf := ensureCap(dst.yoloSlice, requiredYoloSliceCap)

	// Copy the time series labels by using the prepared buffer.
	dstTs.Labels, buf = copyToYoloLabels(buf, dstTs.Labels, srcTs.Labels)

	// Copy the samples.
	if cap(dstTs.Samples) < len(srcTs.Samples) {
		dstTs.Samples = make([]Sample, len(srcTs.Samples))
	} else {
		dstTs.Samples = dstTs.Samples[:len(srcTs.Samples)]
	}
	copy(dstTs.Samples, srcTs.Samples)

	// Copy the histograms.
	if keepHistograms {
		if cap(dstTs.Histograms) < len(srcTs.Histograms) {
			dstTs.Histograms = make([]Histogram, len(srcTs.Histograms))
		} else {
			dstTs.Histograms = dstTs.Histograms[:len(srcTs.Histograms)]
		}
		for i := range srcTs.Histograms {
			dstTs.Histograms[i] = copyHistogram(srcTs.Histograms[i])
		}
	} else {
		dstTs.Histograms = nil
	}

	// Prepare the slice of exemplars.
	if keepExemplars {
		if cap(dstTs.Exemplars) < len(srcTs.Exemplars) {
			dstTs.Exemplars = make([]Exemplar, len(srcTs.Exemplars))
		} else {
			dstTs.Exemplars = dstTs.Exemplars[:len(srcTs.Exemplars)]
		}

		for exemplarIdx := range srcTs.Exemplars {
			// Copy the exemplar labels by using the prepared buffer.
			dstTs.Exemplars[exemplarIdx].Labels, buf = copyToYoloLabels(buf, dstTs.Exemplars[exemplarIdx].Labels, srcTs.Exemplars[exemplarIdx].Labels)

			// Copy the other exemplar properties.
			dstTs.Exemplars[exemplarIdx].Value = srcTs.Exemplars[exemplarIdx].Value
			dstTs.Exemplars[exemplarIdx].TimestampMs = srcTs.Exemplars[exemplarIdx].TimestampMs
		}
	} else {
		dstTs.Exemplars = dstTs.Exemplars[:0]
	}

	return dst
}

// ensureCap takes a pointer to a byte slice and ensures that the capacity of the referred slice is at least equal to
// the given capacity, if not then the byte slice referred to by the pointer gets replaced with a new, larger one.
// The return value is the byte slice which is now referred by the given pointer which has at least the given capacity.
func ensureCap(bufRef *[]byte, requiredCap int) []byte {
	buf := *bufRef
	if cap(buf) >= requiredCap {
		return buf[:0]
	}

	buf = make([]byte, 0, requiredCap)
	*bufRef = buf
	return buf
}

// countTotalLabelLen takes a time series and calculates the sum of the lengths of all label names and values.
func countTotalLabelLen(ts *TimeSeries, includeExemplars bool) int {
	var labelLen int
	for _, label := range ts.Labels {
		labelLen += len(label.Name) + len(label.Value)
	}

	if includeExemplars {
		for _, exemplar := range ts.Exemplars {
			for _, label := range exemplar.Labels {
				labelLen += len(label.Name) + len(label.Value)
			}
		}
	}

	return labelLen
}

// copyToYoloLabels copies the values of src to dst, it uses the given buffer to store all the string values in it.
// The returned buffer is the remainder of the given buffer, which remains unused after the copying is complete.
func copyToYoloLabels(buf []byte, dst, src []LabelAdapter) ([]LabelAdapter, []byte) {
	if cap(dst) < len(src) {
		dst = make([]LabelAdapter, len(src))
	} else {
		dst = dst[:len(src)]
	}

	for labelIdx := range src {
		dst[labelIdx].Name, buf = copyToYoloString(buf, src[labelIdx].Name)
		dst[labelIdx].Value, buf = copyToYoloString(buf, src[labelIdx].Value)
	}

	return dst, buf
}

// copyToYoloString takes a string and creates a new string which uses the given buffer as underlying byte array.
// It requires that the buffer has a capacity which is greater than or equal to the length of the source string.
func copyToYoloString(buf []byte, src string) (string, []byte) {
	buf = buf[:len(src)]
	copy(buf, *((*[]byte)(unsafe.Pointer(&src))))
	return yoloString(buf), buf[len(buf):]
}

// copyHistogram copies the given histogram by value.
// The returned histogram does not share any memory with the given one.
func copyHistogram(src Histogram) Histogram {
	var (
		dstCount     isHistogram_Count
		dstZeroCount isHistogram_ZeroCount
	)
	// Copy count.
	switch src.Count.(type) {
	case *Histogram_CountInt:
		dstCount = &Histogram_CountInt{CountInt: src.GetCountInt()}
	default:
		dstCount = &Histogram_CountFloat{CountFloat: src.GetCountFloat()}
	}

	// Copy zero count.
	switch src.ZeroCount.(type) {
	case *Histogram_ZeroCountInt:
		dstZeroCount = &Histogram_ZeroCountInt{ZeroCountInt: src.GetZeroCountInt()}
	default:
		dstZeroCount = &Histogram_ZeroCountFloat{ZeroCountFloat: src.GetZeroCountFloat()}
	}

	return Histogram{
		Count:          dstCount,
		Sum:            src.Sum,
		Schema:         src.Schema,
		ZeroThreshold:  src.ZeroThreshold,
		ZeroCount:      dstZeroCount,
		NegativeSpans:  slices.Clone(src.NegativeSpans),
		NegativeDeltas: slices.Clone(src.NegativeDeltas),
		NegativeCounts: slices.Clone(src.NegativeCounts),
		PositiveSpans:  slices.Clone(src.PositiveSpans),
		PositiveDeltas: slices.Clone(src.PositiveDeltas),
		PositiveCounts: slices.Clone(src.PositiveCounts),
		ResetHint:      src.ResetHint,
		Timestamp:      src.Timestamp,
	}
}

// ForIndexes builds a new WriteRequest from the given WriteRequest, containing only the timeseries and metadata for the given indexes.
// It assumes the indexes before the initialMetadataIndex are timeseries, and the rest are metadata.
func (p *WriteRequest) ForIndexes(indexes []int, initialMetadataIndex int) *WriteRequest {
	var timeseriesCount, metadataCount int
	for _, i := range indexes {
		if i >= initialMetadataIndex {
			metadataCount++
		} else {
			timeseriesCount++
		}
	}

	timeseries := preallocSliceIfNeeded[PreallocTimeseries](timeseriesCount)
	metadata := preallocSliceIfNeeded[*MetricMetadata](metadataCount)

	for _, i := range indexes {
		if i >= initialMetadataIndex {
			metadata = append(metadata, p.Metadata[i-initialMetadataIndex])
		} else {
			timeseries = append(timeseries, p.Timeseries[i])
		}
	}

	return &WriteRequest{
		Timeseries:          timeseries,
		Metadata:            metadata,
		Source:              p.Source,
		SkipLabelValidation: p.SkipLabelValidation,
	}
}

func preallocSliceIfNeeded[T any](size int) []T {
	if size > 0 {
		return make([]T, 0, size)
	}
	return nil
}
