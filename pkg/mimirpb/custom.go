// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"bytes"
	"fmt"
	"math"

	gogoproto "github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/model/histogram"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/mem"
	protobufproto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
)

func init() {
	c := encoding.GetCodecV2(proto.Name)
	encoding.RegisterCodecV2(&codecV2{codec: c})
}

// codecV2 customizes gRPC marshalling and unmarshalling.
// We customize marshalling in order to use optimized paths when possible.
// We customize unmarshalling in order to use an optimized path when possible,
// and to retain the unmarshalling buffer when necessary.
type codecV2 struct {
	codec encoding.CodecV2
}

var _ encoding.CodecV2 = &codecV2{}

func messageV2Of(v any) protobufproto.Message {
	switch v := v.(type) {
	case protoadapt.MessageV1:
		return protoadapt.MessageV2Of(v)
	case protoadapt.MessageV2:
		return v
	default:
		panic(fmt.Errorf("unrecognized message type %T", v))
	}
}

func (c *codecV2) Marshal(v any) (mem.BufferSlice, error) {
	vv := messageV2Of(v)
	if vv == nil {
		return nil, fmt.Errorf("proto: failed to marshal, message is %T, want proto.Message", v)
	}

	var size int
	if sizer, ok := v.(gogoproto.Sizer); ok {
		size = sizer.Size()
	} else {
		size = protobufproto.Size(vv)
	}

	var data mem.BufferSlice
	// MarshalWithSize should be the most optimized method.
	if marshaler, ok := v.(MarshalerWithSize); ok {
		buf, err := marshaler.MarshalWithSize(size)
		if err != nil {
			return nil, err
		}
		data = append(data, mem.SliceBuffer(buf))
	} else if mem.IsBelowBufferPoolingThreshold(size) {
		marshaler, ok := v.(gogoproto.Marshaler)
		var buf []byte
		var err error
		if ok {
			buf, err = marshaler.Marshal()
		} else {
			buf, err = protobufproto.Marshal(vv)
		}
		if err != nil {
			return nil, err
		}
		data = append(data, mem.SliceBuffer(buf))
	} else {
		pool := mem.DefaultBufferPool()
		buf := pool.Get(size)
		var err error
		marshaler, ok := v.(SizedMarshaler)
		if ok {
			_, err = marshaler.MarshalToSizedBuffer((*buf)[:size])
		} else {
			_, err = (protobufproto.MarshalOptions{}).MarshalAppend((*buf)[:0], vv)
		}
		if err != nil {
			pool.Put(buf)
			return nil, err
		}
		data = append(data, mem.NewBuffer(buf, pool))
	}

	return data, nil
}

// mem.DefaultBufferPool() only has five sizes: 256 B, 4 KB, 16 KB, 32 KB and 1 MB.
// This means that for messages between 32 KB and 1 MB, we may over-allocate by up to 992 KB,
// or ~97%. If we have a lot of messages in this range, we can waste a lot of memory.
// So instead, we create our own buffer pool with more sizes to reduce this wasted space, and
// also include pools for larger sizes up to 256 MB.
var unmarshalSlicePool = mem.NewTieredBufferPool(unmarshalSlicePoolSizes()...)

func unmarshalSlicePoolSizes() []int {
	var sizes []int

	for s := 256; s <= 256<<20; s <<= 1 {
		sizes = append(sizes, s)
	}

	return sizes
}

// Unmarshal customizes gRPC unmarshalling.
// If v implements MessageWithBufferRef, its SetBuffer method is called with the unmarshalling buffer and the buffer's reference count gets incremented.
// The latter means that v's FreeBuffer method should be called when v is no longer used.
func (c *codecV2) Unmarshal(data mem.BufferSlice, v any) error {
	buf := data.MaterializeToBuffer(unmarshalSlicePool)
	// Decrement buf's reference count. Note though that if v implements MessageWithBufferRef,
	// we increase buf's reference count first so it doesn't go to zero.
	defer buf.Free()

	if unmarshaler, ok := v.(gogoproto.Unmarshaler); ok {
		if err := unmarshaler.Unmarshal(buf.ReadOnlyData()); err != nil {
			return err
		}
	} else {
		vv := messageV2Of(v)
		if err := protobufproto.Unmarshal(buf.ReadOnlyData(), vv); err != nil {
			return err
		}
	}

	if holder, ok := v.(MessageWithBufferRef); ok {
		buf.Ref()
		holder.SetBuffer(buf)
	}

	return nil
}

func (c *codecV2) Name() string {
	return c.codec.Name()
}

// MessageWithBufferRef is an unmarshalling buffer retaining protobuf message.
type MessageWithBufferRef interface {
	// SetBuffer sets the unmarshalling buffer.
	SetBuffer(mem.Buffer)
	// FreeBuffer drops the unmarshalling buffer reference.
	FreeBuffer()
}

// BufferHolder is a base type for protobuf messages that keep unsafe references to the unmarshalling buffer.
type BufferHolder struct {
	buffer mem.Buffer
}

func (m *BufferHolder) SetBuffer(buf mem.Buffer) {
	m.buffer = buf
}

func (m *BufferHolder) FreeBuffer() {
	if m.buffer != nil {
		m.buffer.Free()
		m.buffer = nil
	}
}

var _ MessageWithBufferRef = &BufferHolder{}

// MinTimestamp returns the minimum timestamp (milliseconds) among all series
// in the WriteRequest. Returns math.MaxInt64 if the request is empty.
func (m *WriteRequest) MinTimestamp() int64 {
	min := int64(math.MaxInt64)

	for _, series := range m.Timeseries {
		for _, entry := range series.Samples {
			if entry.TimestampMs < min {
				min = entry.TimestampMs
			}
		}

		for _, entry := range series.Histograms {
			if entry.Timestamp < min {
				min = entry.Timestamp
			}
		}

		for _, entry := range series.Exemplars {
			if entry.TimestampMs < min {
				min = entry.TimestampMs
			}
		}
	}

	return min
}

// IsEmpty returns whether the WriteRequest has no data to ingest.
func (m *WriteRequest) IsEmpty() bool {
	return len(m.Timeseries) == 0 && len(m.Metadata) == 0
}

// MetadataSize is like Size() but returns only the marshalled size of Metadata field.
func (m *WriteRequest) MetadataSize() int {
	var l, n int

	for _, e := range m.Metadata {
		l = e.Size()
		n += 1 + l + sovMimir(uint64(l))
	}

	return n
}

// TimeseriesSize is like Size() but returns only the marshalled size of Timeseries field.
func (m *WriteRequest) TimeseriesSize() int {
	var l, n int

	for _, e := range m.Timeseries {
		l = e.Size()
		n += 1 + l + sovMimir(uint64(l))
	}

	return n
}

func (h Histogram) IsFloatHistogram() bool {
	_, ok := h.GetCount().(*Histogram_CountFloat)
	return ok
}

func (h Histogram) IsGauge() bool {
	return h.ResetHint == Histogram_GAUGE
}

// ReduceResolution will reduce the resolution of the histogram by one level.
// Returns the resulting bucket count and an error if the histogram is not
// possible to reduce further.
func (h *Histogram) ReduceResolution() (int, error) {
	if h.IsFloatHistogram() {
		return h.reduceFloatResolution()
	}

	return h.reduceIntResolution()
}

const (
	// MinimumHistogramSchema is the minimum schema for a histogram.
	// Currently, it is -4, hardcoded and not exported from either
	// Prometheus or client_golang, so we define it here.
	MinimumHistogramSchema = -4

	// MaximumHistogramSchema is the minimum schema for a histogram.
	// Currently, it is 8, hardcoded and not exported from either
	// Prometheus or client_golang, so we define it here.
	MaximumHistogramSchema = 8

	NativeHistogramsWithCustomBucketsSchema = -53
)

func (h *Histogram) reduceFloatResolution() (int, error) {
	if h.Schema == MinimumHistogramSchema {
		return 0, fmt.Errorf("cannot reduce resolution of histogram with schema %d", h.Schema)
	}
	h.PositiveSpans, h.PositiveCounts = reduceResolution(h.PositiveSpans, h.PositiveCounts, h.Schema, h.Schema-1, false)
	h.NegativeSpans, h.NegativeCounts = reduceResolution(h.NegativeSpans, h.NegativeCounts, h.Schema, h.Schema-1, false)
	h.Schema--
	return len(h.PositiveDeltas) + len(h.NegativeDeltas), nil
}

func (h *Histogram) reduceIntResolution() (int, error) {
	if h.Schema == MinimumHistogramSchema {
		return 0, fmt.Errorf("cannot reduce resolution of histogram with schema %d", h.Schema)
	}
	h.PositiveSpans, h.PositiveDeltas = reduceResolution(h.PositiveSpans, h.PositiveDeltas, h.Schema, h.Schema-1, true)
	h.NegativeSpans, h.NegativeDeltas = reduceResolution(h.NegativeSpans, h.NegativeDeltas, h.Schema, h.Schema-1, true)
	h.Schema--
	return len(h.PositiveDeltas) + len(h.NegativeDeltas), nil
}

// reduceResolution reduces the input spans, buckets in origin schema to the spans, buckets in target schema.
// The target schema must be smaller than the original schema.
// Set deltaBuckets to true if the provided buckets are
// deltas. Set it to false if the buckets contain absolute counts.
// This function is ported from Prometheus: https://github.com/prometheus/prometheus/blob/main/model/histogram/generic.go#L608
// https://github.com/prometheus/prometheus/blob/acc114fe553b660cefc71a0311792ef8be4a186a/model/histogram/generic.go#L608
func reduceResolution[IBC histogram.InternalBucketCount](originSpans []BucketSpan, originBuckets []IBC, originSchema, targetSchema int32, deltaBuckets bool) ([]BucketSpan, []IBC) {
	var (
		targetSpans           []BucketSpan // The spans in the target schema.
		targetBuckets         []IBC        // The bucket counts in the target schema.
		bucketIdx             int32        // The index of bucket in the origin schema.
		bucketCountIdx        int          // The position of a bucket in origin bucket count slice `originBuckets`.
		targetBucketIdx       int32        // The index of bucket in the target schema.
		lastBucketCount       IBC          // The last visited bucket's count in the origin schema.
		lastTargetBucketIdx   int32        // The index of the last added target bucket.
		lastTargetBucketCount IBC
	)

	for _, span := range originSpans {
		// Determine the index of the first bucket in this span.
		bucketIdx += span.Offset
		for j := 0; j < int(span.Length); j++ {
			// Determine the index of the bucket in the target schema from the index in the original schema.
			targetBucketIdx = targetIdx(bucketIdx, originSchema, targetSchema)

			switch {
			case len(targetSpans) == 0:
				// This is the first span in the targetSpans.
				span := BucketSpan{
					Offset: targetBucketIdx,
					Length: 1,
				}
				targetSpans = append(targetSpans, span)
				targetBuckets = append(targetBuckets, originBuckets[bucketCountIdx])
				lastTargetBucketIdx = targetBucketIdx
				lastBucketCount = originBuckets[bucketCountIdx]
				lastTargetBucketCount = originBuckets[bucketCountIdx]

			case lastTargetBucketIdx == targetBucketIdx:
				// The current bucket has to be merged into the same target bucket as the previous bucket.
				if deltaBuckets {
					lastBucketCount += originBuckets[bucketCountIdx]
					targetBuckets[len(targetBuckets)-1] += lastBucketCount
					lastTargetBucketCount += lastBucketCount
				} else {
					targetBuckets[len(targetBuckets)-1] += originBuckets[bucketCountIdx]
				}

			case (lastTargetBucketIdx + 1) == targetBucketIdx:
				// The current bucket has to go into a new target bucket,
				// and that bucket is next to the previous target bucket,
				// so we add it to the current target span.
				targetSpans[len(targetSpans)-1].Length++
				lastTargetBucketIdx++
				if deltaBuckets {
					lastBucketCount += originBuckets[bucketCountIdx]
					targetBuckets = append(targetBuckets, lastBucketCount-lastTargetBucketCount)
					lastTargetBucketCount = lastBucketCount
				} else {
					targetBuckets = append(targetBuckets, originBuckets[bucketCountIdx])
				}

			case (lastTargetBucketIdx + 1) < targetBucketIdx:
				// The current bucket has to go into a new target bucket,
				// and that bucket is separated by a gap from the previous target bucket,
				// so we need to add a new target span.
				span := BucketSpan{
					Offset: targetBucketIdx - lastTargetBucketIdx - 1,
					Length: 1,
				}
				targetSpans = append(targetSpans, span)
				lastTargetBucketIdx = targetBucketIdx
				if deltaBuckets {
					lastBucketCount += originBuckets[bucketCountIdx]
					targetBuckets = append(targetBuckets, lastBucketCount-lastTargetBucketCount)
					lastTargetBucketCount = lastBucketCount
				} else {
					targetBuckets = append(targetBuckets, originBuckets[bucketCountIdx])
				}
			}

			bucketIdx++
			bucketCountIdx++
		}
	}

	return targetSpans, targetBuckets
}

// targetIdx returns the bucket index in the target schema for the given bucket
// index idx in the original schema.
func targetIdx(idx, originSchema, targetSchema int32) int32 {
	return ((idx - 1) >> (originSchema - targetSchema)) + 1
}

// UnsafeByteSlice is an alternative to the default handling of []byte values in protobuf messages.
// Unlike the default protobuf implementation, when unmarshalling, UnsafeByteSlice holds a reference to the
// subslice of the original protobuf-encoded bytes, rather than copying them from the encoded buffer to a second slice.
// This reduces memory pressure when unmarshalling byte slices, at the cost of retaining the full buffer in memory. This
// tradeoff is usually only worthwhile when the protobuf message is dominated by []byte values (eg. when sending chunks
// to queriers).
//
// The implementations of all other methods are identical to those generated by the protobuf compiler.
//
// Note that UnsafeByteSlice will not behave correctly if the protobuf-encoded bytes are reused (eg. if the
// buffer is used for one request and then pooled and reused for a later request). At the time of writing, the gRPC
// client does not do this, but there is a TODO to investigate it (see
// https://github.com/grafana/mimir/blob/117f0d68785b8a3ca07a8b1dda5a350b17cdc09b/vendor/google.golang.org/grpc/rpc_util.go#L575-L576).
type UnsafeByteSlice []byte

func (t *UnsafeByteSlice) MarshalTo(data []byte) (n int, err error) {
	copy(data, *t)

	return t.Size(), nil
}

func (t *UnsafeByteSlice) Unmarshal(data []byte) error {
	*t = data

	return nil
}

func (t *UnsafeByteSlice) Size() int {
	return len(*t)
}

func (t UnsafeByteSlice) Equal(other UnsafeByteSlice) bool {
	return bytes.Equal(t, other)
}

// SizedMarshaler supports marshaling a protobuf message to a sized buffer.
type SizedMarshaler interface {
	// MarshalToSizedBuffer writes the wire-format encoding of m to b.
	MarshalToSizedBuffer(b []byte) (int, error)
}

// MarshalerWithSize supports marshaling a protobuf message with a predetermined buffer size.
type MarshalerWithSize interface {
	// MarshalWithSize returns a wire format byte slice of the given size.
	MarshalWithSize(size int) ([]byte, error)
}
