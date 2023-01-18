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

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/apache/arrow/go/v11/arrow/ipc"
	"github.com/apache/arrow/go/v11/arrow/memory"
	"github.com/prometheus/common/model"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/mimirpb"
)

// This is based on https://github.com/prometheus/prometheus/pull/11591 and https://github.com/prometheus/prometheus/compare/csmarchbanks/optmize-arrow-responses.

type ArrowCodec struct{}

func (c ArrowCodec) Decode(b []byte) (querymiddleware.PrometheusResponse, error) {
	buf := bytes.NewReader(b)
	resultType, err := c.decodeResultType(buf)
	if err != nil {
		return querymiddleware.PrometheusResponse{}, err
	}

	pool := memory.NewGoAllocator()

	// TODO: is there some way we could pre-allocate this?
	var result []querymiddleware.SampleStream

	for {
		series, err := c.decodeSeries(buf, pool)

		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return querymiddleware.PrometheusResponse{}, err
		}

		result = append(result, series)
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
func (c ArrowCodec) decodeResultType(r io.ByteReader) (string, error) {
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

func (c ArrowCodec) decodeSeries(r io.Reader, pool memory.Allocator) (querymiddleware.SampleStream, error) {
	reader, err := ipc.NewReader(r, ipc.WithAllocator(pool))
	if err != nil {
		return querymiddleware.SampleStream{}, err
	}

	defer reader.Release()

	if !reader.Next() {
		if reader.Err() != nil {
			return querymiddleware.SampleStream{}, fmt.Errorf("expected to read a record, but got error: %w", err)
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

// TODO: how to encode result type (scalar / vector / matrix)? For now, we send a single byte at the start of the stream to signal what data type it contains
// - content-type header in response?
// - attached as metadata to entire stream (is this possible?) or to record?
// - infer from schema and shape of data?
// TODO: how to encode errors? Send as JSON instead?
func (c ArrowCodec) Encode(prometheusResponse querymiddleware.PrometheusResponse) ([]byte, error) {
	buf := &bytes.Buffer{}

	switch prometheusResponse.Data.ResultType {
	case model.ValScalar.String():
		buf.WriteByte(byte(model.ValScalar)) // HACK: see comment above about encoding result type

		if err := c.encodeMatrix(buf, prometheusResponse.Data); err != nil {
			return nil, err
		}
	case model.ValVector.String():
		buf.WriteByte(byte(model.ValVector)) // HACK: see comment above about encoding result type

		// TODO: implement more efficient schema for vectors?
		if err := c.encodeMatrix(buf, prometheusResponse.Data); err != nil {
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

	return buf.Bytes(), nil
}

func (c ArrowCodec) encodeMatrix(w io.Writer, data *querymiddleware.PrometheusData) error {
	// TODO: replace with Chris' more efficient implementation
	// TODO: share amongst all encodeMatrix() calls
	pool := memory.NewGoAllocator()

	// TODO: Chris' implementation reserves a builder with the length of the longest series, but that doesn't seem to work
	// - it panics when there is more than one series due to the builders having nil internal buffers
	timestampsBuilder := array.NewTimestampBuilder(pool, &arrow.TimestampType{Unit: arrow.Millisecond})
	defer timestampsBuilder.Release()

	valuesBuilder := array.NewFloat64Builder(pool)
	defer valuesBuilder.Release()

	fields := []arrow.Field{
		{Name: "t", Type: timestampsBuilder.Type()},
		{Name: "v", Type: valuesBuilder.Type()},
	}

	for _, series := range data.Result {
		if err := c.encodeSeries(w, &series, fields, timestampsBuilder, valuesBuilder, pool); err != nil {
			return err
		}
	}

	return nil
}

func (c ArrowCodec) encodeSeries(w io.Writer, series *querymiddleware.SampleStream, fields []arrow.Field, timestampsBuilder *array.TimestampBuilder, valuesBuilder *array.Float64Builder, allocator memory.Allocator) error {
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

	// TODO: move earlier?
	writer := ipc.NewWriter(w, ipc.WithAllocator(allocator), ipc.WithSchema(seriesSchema))
	defer writer.Close()

	if err := writer.Write(rec); err != nil {
		return err
	}

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
