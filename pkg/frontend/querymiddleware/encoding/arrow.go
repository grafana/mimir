// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/a6e891966f5d0cbfbedf8b659784e5ee43497930/web/api/v1/api.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package encoding

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/apache/arrow/go/v11/arrow/ipc"
	"github.com/apache/arrow/go/v11/arrow/memory"
	"github.com/prometheus/common/model"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/pool"
)

// This is based on https://github.com/prometheus/prometheus/pull/11591 and https://github.com/prometheus/prometheus/compare/csmarchbanks/optmize-arrow-responses.

type ArrowCodec struct {
	allocator  memory.Allocator
	readerPool sync.Pool
	bufferPool sync.Pool
}

func NewArrowCodec() Codec {
	return &ArrowCodec{
		allocator:  newArrowAllocator(),
		readerPool: sync.Pool{New: func() any { return &bytes.Reader{} }},
		bufferPool: sync.Pool{New: func() any { return &bytes.Buffer{} }},
	}
}

func (c *ArrowCodec) Decode(b []byte) (querymiddleware.PrometheusResponse, error) {
	buf := c.readerPool.Get().(*bytes.Reader)
	buf.Reset(b)
	defer c.readerPool.Put(buf)

	resultType, err := c.decodeResultType(buf)
	if err != nil {
		return querymiddleware.PrometheusResponse{}, err
	}

	var result []querymiddleware.SampleStream

	switch resultType {
	case model.ValScalar.String(), model.ValMatrix.String():
		result, err = c.decodeMatrix(buf)
		if err != nil {
			return querymiddleware.PrometheusResponse{}, err
		}

	case model.ValVector.String():
		result, err = c.decodeVector(buf)
		if err != nil {
			return querymiddleware.PrometheusResponse{}, err
		}

	default:
		panic(fmt.Sprintf("unknown result type %v", resultType))
	}

	resp := querymiddleware.PrometheusResponse{
		Status: "success",
		Data: &querymiddleware.PrometheusData{
			ResultType: resultType,
			Result:     result,
		},
	}

	return resp, nil
}

// HACK: see note on Encode about dealing with the result type in a better way
func (c *ArrowCodec) decodeResultType(r io.ByteReader) (string, error) {
	b, err := r.ReadByte()
	if err != nil {
		return "", err
	}

	switch b {
	case byte(model.ValScalar):
		return model.ValScalar.String(), nil
	case byte(model.ValVector):
		return model.ValVector.String(), nil
	case byte(model.ValMatrix):
		return model.ValMatrix.String(), nil
	default:
		return "", fmt.Errorf("unknown result type 0x%x", b)
	}
}

type LengthReader interface {
	io.Reader
	Len() int
}

func (c *ArrowCodec) decodeMatrix(r LengthReader) ([]querymiddleware.SampleStream, error) {
	// TODO: is there some way we could pre-allocate this?
	var result []querymiddleware.SampleStream

	for r.Len() > 0 {
		series, err := c.decodeMatrixSeries(r)

		if err != nil {
			return nil, err
		}

		result = append(result, series)
	}

	return result, nil
}

func (c *ArrowCodec) decodeMatrixSeries(r io.Reader) (querymiddleware.SampleStream, error) {
	reader, err := ipc.NewReader(r, ipc.WithAllocator(c.allocator))
	if err != nil {
		return querymiddleware.SampleStream{}, err
	}

	defer reader.Release()

	if !reader.Next() {
		if reader.Err() != nil {
			return querymiddleware.SampleStream{}, fmt.Errorf("expected to read a record, but got error: %w", reader.Err())
		}

		return querymiddleware.SampleStream{}, errors.New("expected to read a record, but reached end of stream before reading any records")
	}

	rec := reader.Record()

	// TODO: should we verify that the schema matches what we expect?
	timestampColumn := rec.Column(0).(*array.Timestamp)
	valueColumn := rec.Column(1).(*array.Float64)
	samples := make([]mimirpb.Sample, rec.NumRows())

	for i := 0; i < int(rec.NumRows()); i++ {
		samples[i] = mimirpb.Sample{
			TimestampMs: int64(timestampColumn.Value(i)),
			Value:       valueColumn.Value(i),
		}
	}

	series := querymiddleware.SampleStream{
		Samples: samples,
		Labels:  labelsFrom(rec.Schema().Metadata()),
	}

	// Important: this Next() call reads the trailing parts of the message, advancing the position of r
	// to the beginning of the next message.
	if reader.Next() {
		return querymiddleware.SampleStream{}, errors.New("expected to read only a single record, but received more than one")
	}

	return series, nil
}

func (c *ArrowCodec) decodeVector(r io.Reader) ([]querymiddleware.SampleStream, error) {
	reader, err := ipc.NewReader(r, ipc.WithAllocator(c.allocator))
	if err != nil {
		return nil, err
	}

	defer reader.Release()

	// TODO: is there some way we could pre-allocate this?
	var result []querymiddleware.SampleStream

	for reader.Next() {
		rec := reader.Record()

		// TODO: should we verify that the schema matches what we expect?
		labelNamesColumn := rec.Column(0).(*array.List)
		labelNamesColumnValues := labelNamesColumn.ListValues().(*array.String)
		labelValuesColumn := rec.Column(1).(*array.List)
		labelValuesColumnValues := labelValuesColumn.ListValues().(*array.String)
		timestampColumn := rec.Column(2).(*array.Timestamp)
		valueColumn := rec.Column(3).(*array.Float64)

		labelNameOffsets := labelNamesColumn.Offsets()
		labelValueOffsets := labelValuesColumn.Offsets()

		for rowIdx := 0; rowIdx < int(rec.NumRows()); rowIdx++ {
			firstNameOffset := int(labelNameOffsets[rowIdx])
			lastNameOffset := int(labelNameOffsets[rowIdx+1])
			firstValueOffset := int(labelValueOffsets[rowIdx])
			lastValueOffset := int(labelValueOffsets[rowIdx+1])

			if firstNameOffset != firstValueOffset || lastNameOffset != lastValueOffset {
				return nil, fmt.Errorf("have different number of label names (first offset %v, last offset %v) and values (first offset %v, last offset %v)", firstNameOffset, lastNameOffset, firstValueOffset, lastValueOffset)
			}

			labelCount := lastNameOffset - firstNameOffset
			labels := make([]mimirpb.LabelAdapter, labelCount)

			for i := 0; i < labelCount; i++ {
				// We must clone the strings as we can't rely on the backing byte buffer not changing.
				name := strings.Clone(labelNamesColumnValues.Value(i + firstNameOffset))
				value := strings.Clone(labelValuesColumnValues.Value(i + firstNameOffset))

				labels[i] = mimirpb.LabelAdapter{
					Name:  name,
					Value: value,
				}
			}

			result = append(result, querymiddleware.SampleStream{
				Samples: []mimirpb.Sample{
					{
						TimestampMs: int64(timestampColumn.Value(rowIdx)),
						Value:       valueColumn.Value(rowIdx),
					},
				},
				Labels: labels,
			})
		}
	}

	if reader.Err() != nil {
		return nil, err
	}

	return result, nil
}

// TODO: how to encode result type (scalar / vector / matrix)? For now, we send a single byte at the start of the stream to signal what data type it contains
// - content-type header in response?
// - attached as metadata to entire stream (is this possible?) or to record?
// - infer from schema and shape of data?
// TODO: how to encode errors? Send as JSON instead?
func (c *ArrowCodec) Encode(prometheusResponse querymiddleware.PrometheusResponse) ([]byte, error) {
	buf := c.bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer c.bufferPool.Put(buf)

	switch prometheusResponse.Data.ResultType {
	case model.ValScalar.String():
		buf.WriteByte(byte(model.ValScalar)) // HACK: see comment above about encoding result type

		if err := c.encodeMatrix(buf, prometheusResponse.Data); err != nil {
			return nil, err
		}
	case model.ValVector.String():
		buf.WriteByte(byte(model.ValVector)) // HACK: see comment above about encoding result type

		if err := c.encodeVector(buf, prometheusResponse.Data); err != nil {
			return nil, err
		}
	case model.ValMatrix.String():
		buf.WriteByte(byte(model.ValMatrix)) // HACK: see comment above about encoding result type

		if err := c.encodeMatrix(buf, prometheusResponse.Data); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown result type %v", prometheusResponse.Data.ResultType)
	}

	// buf.Bytes() is only valid until the next modification of buf,
	// so we can't return it directly given we pool buffers.
	res := make([]byte, buf.Len())
	copy(res, buf.Bytes())

	return res, nil
}

func (c *ArrowCodec) encodeMatrix(w io.Writer, data *querymiddleware.PrometheusData) error {
	// TODO: Chris' implementation reserves a builder with the length of the longest series, but that doesn't seem to work
	// - it panics when there is more than one series due to the builders having nil internal buffers
	timestampsBuilder := array.NewTimestampBuilder(c.allocator, &arrow.TimestampType{Unit: arrow.Millisecond})
	defer timestampsBuilder.Release()

	valuesBuilder := array.NewFloat64Builder(c.allocator)
	defer valuesBuilder.Release()

	fields := []arrow.Field{
		{Name: "t", Type: timestampsBuilder.Type()},
		{Name: "v", Type: valuesBuilder.Type()},
	}

	for _, series := range data.Result {
		if err := c.encodeMatrixSeries(w, &series, fields, timestampsBuilder, valuesBuilder); err != nil {
			return err
		}
	}

	return nil
}

func (c *ArrowCodec) encodeMatrixSeries(w io.Writer, series *querymiddleware.SampleStream, fields []arrow.Field, timestampsBuilder *array.TimestampBuilder, valuesBuilder *array.Float64Builder) error {
	sampleCount := len(series.Samples)
	timestampsBuilder.Reserve(sampleCount)
	valuesBuilder.Reserve(sampleCount)

	for _, sample := range series.Samples {
		timestampsBuilder.UnsafeAppend(arrow.Timestamp(sample.TimestampMs))
		valuesBuilder.UnsafeAppend(sample.Value)
	}

	timestamps := timestampsBuilder.NewArray()
	defer timestamps.Release()

	values := valuesBuilder.NewArray()
	defer values.Release()

	metadata := arrowMetadataFrom(series.Labels)
	seriesSchema := arrow.NewSchema(fields, &metadata)
	rec := array.NewRecord(seriesSchema, []arrow.Array{timestamps, values}, int64(sampleCount))
	defer rec.Release()

	writer := ipc.NewWriter(w, ipc.WithAllocator(c.allocator), ipc.WithSchema(seriesSchema))
	defer writer.Close()

	if err := writer.Write(rec); err != nil {
		return err
	}

	return nil
}

func (c *ArrowCodec) encodeVector(w io.Writer, data *querymiddleware.PrometheusData) error {
	seriesCount := len(data.Result)
	labelNamesBuilder := array.NewListBuilder(c.allocator, &arrow.StringType{})
	labelNamesBuilder.Reserve(seriesCount)
	defer labelNamesBuilder.Release()

	labelValuesBuilder := array.NewListBuilder(c.allocator, &arrow.StringType{})
	labelValuesBuilder.Reserve(seriesCount)
	defer labelValuesBuilder.Release()

	timestampsBuilder := array.NewTimestampBuilder(c.allocator, &arrow.TimestampType{Unit: arrow.Millisecond})
	timestampsBuilder.Reserve(seriesCount)
	defer timestampsBuilder.Release()

	valuesBuilder := array.NewFloat64Builder(c.allocator)
	valuesBuilder.Reserve(seriesCount)
	defer valuesBuilder.Release()

	fields := []arrow.Field{
		{Name: "labelNames", Type: labelNamesBuilder.Type()},
		{Name: "labelValues", Type: labelNamesBuilder.Type()},
		{Name: "t", Type: timestampsBuilder.Type()},
		{Name: "v", Type: valuesBuilder.Type()},
	}

	schema := arrow.NewSchema(fields, nil)
	writer := ipc.NewWriter(w, ipc.WithAllocator(c.allocator), ipc.WithSchema(schema))
	defer writer.Close()

	for _, series := range data.Result {
		if err := c.encodeVectorSeries(&series, labelNamesBuilder, labelValuesBuilder, timestampsBuilder, valuesBuilder); err != nil {
			return err
		}
	}

	labelNames := labelNamesBuilder.NewArray()
	defer labelNames.Release()

	labelValues := labelValuesBuilder.NewArray()
	defer labelValues.Release()

	timestamps := timestampsBuilder.NewArray()
	defer timestamps.Release()

	values := valuesBuilder.NewArray()
	defer values.Release()

	rec := array.NewRecord(schema, []arrow.Array{labelNames, labelValues, timestamps, values}, int64(len(data.Result)))
	defer rec.Release()

	return writer.Write(rec)
}

func (c *ArrowCodec) encodeVectorSeries(series *querymiddleware.SampleStream, labelNamesBuilder *array.ListBuilder, labelValuesBuilder *array.ListBuilder, timestampsBuilder *array.TimestampBuilder, valuesBuilder *array.Float64Builder) error {
	labelNamesBuilder.Append(true)
	labelNameListBuilder := labelNamesBuilder.ValueBuilder().(*array.StringBuilder)
	labelNameListBuilder.Reserve(len(series.Labels))

	labelValuesBuilder.Append(true)
	labelValueListBuilder := labelValuesBuilder.ValueBuilder().(*array.StringBuilder)
	labelValueListBuilder.Reserve(len(series.Labels))

	for _, label := range series.Labels {
		labelNameListBuilder.Append(label.Name)
		labelValueListBuilder.Append(label.Value)
	}

	if len(series.Samples) != 1 {
		return fmt.Errorf("expected vector series to have one sample, but has %v", len(series.Samples))
	}

	sample := series.Samples[0]
	timestampsBuilder.UnsafeAppend(arrow.Timestamp(sample.TimestampMs))
	valuesBuilder.UnsafeAppend(sample.Value)

	return nil
}

func arrowMetadataFrom(labels []mimirpb.LabelAdapter) arrow.Metadata {
	keys := make([]string, 0, len(labels))
	values := make([]string, 0, len(labels))

	for _, l := range labels {
		keys = append(keys, l.Name)
		values = append(values, l.Value)
	}

	return arrow.NewMetadata(keys, values)
}

func labelsFrom(metadata arrow.Metadata) []mimirpb.LabelAdapter {
	labels := make([]mimirpb.LabelAdapter, metadata.Len())

	for i := 0; i < metadata.Len(); i++ {
		labels[i] = mimirpb.LabelAdapter{
			Name:  metadata.Keys()[i],
			Value: metadata.Values()[i],
		}
	}

	return labels
}

type arrowAllocator struct {
	base memory.Allocator
	pool *pool.UnlimitedBucketedBytes
}

func newArrowAllocator() *arrowAllocator {
	base := memory.DefaultAllocator

	return &arrowAllocator{
		base: base,
		pool: pool.NewUnlimitedBucketedBytes(1e3, 100e6, 3),
	}
}

func (a *arrowAllocator) Allocate(size int) []byte {
	b := *a.pool.Get(size)
	return b[:size]
}

func (a *arrowAllocator) Reallocate(size int, b []byte) []byte {
	if cap(b) >= size {
		return b[:size]
	}
	newB := *a.pool.Get(size)
	newB = newB[:size]
	copy(newB, b)

	// TODO: this wasn't in Chris' original implementation
	a.Free(b)
	return newB
}

func (a *arrowAllocator) Free(b []byte) {
	// TODO: this wasn't in Chris' original implementation
	if b == nil {
		return
	}

	a.pool.Put(&b)
}
