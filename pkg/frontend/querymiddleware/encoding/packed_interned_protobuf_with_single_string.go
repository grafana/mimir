// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"fmt"

	"github.com/prometheus/common/model"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware/encoding/packedinternedsinglestringquerypb"
	"github.com/grafana/mimir/pkg/mimirpb"
)

type PackedInternedProtobufWithSingleStringCodec struct{}

func (c PackedInternedProtobufWithSingleStringCodec) Decode(b []byte) (querymiddleware.PrometheusResponse, error) {
	var resp packedinternedsinglestringquerypb.QueryResponse

	if err := resp.Unmarshal(b); err != nil {
		return querymiddleware.PrometheusResponse{}, err
	}

	var prometheusData querymiddleware.PrometheusData

	switch d := resp.Data.(type) {
	case *packedinternedsinglestringquerypb.QueryResponse_Scalar:
		prometheusData = c.decodeScalar(d.Scalar)
	case *packedinternedsinglestringquerypb.QueryResponse_Vector:
		prometheusData = c.decodeVector(d.Vector)
	case *packedinternedsinglestringquerypb.QueryResponse_Matrix:
		prometheusData = c.decodeMatrix(d.Matrix)
	default:
		return querymiddleware.PrometheusResponse{}, fmt.Errorf("unknown data type %T", resp.Data)
	}

	return querymiddleware.PrometheusResponse{
		Status:    resp.Status,
		Data:      &prometheusData,
		ErrorType: resp.ErrorType,
		Error:     resp.Error,
	}, nil
}

func (c PackedInternedProtobufWithSingleStringCodec) decodeScalar(d *packedinternedsinglestringquerypb.ScalarData) querymiddleware.PrometheusData {
	return querymiddleware.PrometheusData{
		ResultType: model.ValScalar.String(),
		Result: []querymiddleware.SampleStream{
			{
				Samples: []mimirpb.Sample{
					{
						Value:       d.Value,
						TimestampMs: d.Timestamp,
					},
				},
			},
		},
	}
}

func (c PackedInternedProtobufWithSingleStringCodec) decodeVector(d *packedinternedsinglestringquerypb.VectorData) querymiddleware.PrometheusData {
	// TODO: check that the number of metrics, timestamps and values is the same
	sampleCount := len(d.Metrics)
	result := make([]querymiddleware.SampleStream, sampleCount)
	symbols := c.decodeSymbols(d.SymbolTable)

	for sampleIdx := 0; sampleIdx < sampleCount; sampleIdx++ {
		metricSymbols := d.Metrics[sampleIdx].MetricSymbols
		labelCount := len(metricSymbols) / 2
		labels := make([]mimirpb.LabelAdapter, labelCount)

		for labelIdx := 0; labelIdx < labelCount; labelIdx++ {
			labels[labelIdx] = mimirpb.LabelAdapter{
				Name:  symbols[metricSymbols[2*labelIdx]],
				Value: symbols[metricSymbols[2*labelIdx+1]],
			}
		}

		result[sampleIdx] = querymiddleware.SampleStream{
			Labels: labels,
			Samples: []mimirpb.Sample{
				{
					Value:       d.Values[sampleIdx],
					TimestampMs: d.Timestamps[sampleIdx],
				},
			},
		}
	}

	return querymiddleware.PrometheusData{
		ResultType: model.ValVector.String(),
		Result:     result,
	}
}

func (c PackedInternedProtobufWithSingleStringCodec) decodeMatrix(d *packedinternedsinglestringquerypb.MatrixData) querymiddleware.PrometheusData {
	result := make([]querymiddleware.SampleStream, len(d.Series))
	symbols := c.decodeSymbols(d.SymbolTable)

	for seriesIdx, series := range d.Series {
		labelCount := len(series.MetricSymbols) / 2
		labels := make([]mimirpb.LabelAdapter, labelCount)

		for labelIdx := 0; labelIdx < labelCount; labelIdx++ {
			labels[labelIdx] = mimirpb.LabelAdapter{
				Name:  symbols[series.MetricSymbols[2*labelIdx]],
				Value: symbols[series.MetricSymbols[2*labelIdx+1]],
			}
		}

		// TODO: check that number of timestamps == number of values
		sampleCount := len(series.Timestamps)
		samples := make([]mimirpb.Sample, sampleCount)

		for sampleIdx := 0; sampleIdx < sampleCount; sampleIdx++ {
			samples[sampleIdx] = mimirpb.Sample{
				Value:       series.Values[sampleIdx],
				TimestampMs: series.Timestamps[sampleIdx],
			}
		}

		result[seriesIdx] = querymiddleware.SampleStream{
			Labels:  labels,
			Samples: samples,
		}
	}

	return querymiddleware.PrometheusData{
		ResultType: model.ValMatrix.String(),
		Result:     result,
	}
}

func (c PackedInternedProtobufWithSingleStringCodec) Encode(prometheusResponse querymiddleware.PrometheusResponse) ([]byte, error) {
	resp := packedinternedsinglestringquerypb.QueryResponse{
		Status:    prometheusResponse.Status,
		ErrorType: prometheusResponse.ErrorType,
		Error:     prometheusResponse.Error,
	}

	switch prometheusResponse.Data.ResultType {
	case model.ValScalar.String():
		scalar := c.encodeScalar(prometheusResponse.Data)
		resp.Data = &packedinternedsinglestringquerypb.QueryResponse_Scalar{Scalar: &scalar}
	case model.ValVector.String():
		vector := c.encodeVector(prometheusResponse.Data)
		resp.Data = &packedinternedsinglestringquerypb.QueryResponse_Vector{Vector: &vector}
	case model.ValMatrix.String():
		matrix := c.encodeMatrix(prometheusResponse.Data)
		resp.Data = &packedinternedsinglestringquerypb.QueryResponse_Matrix{Matrix: &matrix}
	default:
		return nil, fmt.Errorf("unknown result type %v", prometheusResponse.Data.ResultType)
	}

	return resp.Marshal()
}

func (c PackedInternedProtobufWithSingleStringCodec) encodeScalar(data *querymiddleware.PrometheusData) packedinternedsinglestringquerypb.ScalarData {
	if len(data.Result) != 1 {
		panic(fmt.Sprintf("scalar data should have 1 stream, but has %v", len(data.Result)))
	}

	stream := data.Result[0]

	if len(stream.Samples) != 1 {
		panic(fmt.Sprintf("scalar data stream should have 1 sample, but has %v", len(stream.Samples)))
	}

	sample := stream.Samples[0]

	return packedinternedsinglestringquerypb.ScalarData{
		Value:     sample.Value,
		Timestamp: sample.TimestampMs,
	}
}

func (c PackedInternedProtobufWithSingleStringCodec) encodeVector(data *querymiddleware.PrometheusData) packedinternedsinglestringquerypb.VectorData {
	metrics := make([]packedinternedsinglestringquerypb.Metric, len(data.Result))
	values := make([]float64, len(data.Result))
	timestamps := make([]int64, len(data.Result))
	symbolTableBuilder := newSymbolTableBuilder()

	for sampleIdx, stream := range data.Result {
		if len(stream.Samples) != 1 {
			panic(fmt.Sprintf("vector data stream should have 1 sample, but has %v", len(stream.Samples)))
		}

		metricSymbols := make([]uint64, len(stream.Labels)*2)

		for labelIdx, label := range stream.Labels {
			metricSymbols[2*labelIdx] = symbolTableBuilder.GetOrPutSymbol(label.Name)
			metricSymbols[2*labelIdx+1] = symbolTableBuilder.GetOrPutSymbol(label.Value)
		}

		metrics[sampleIdx].MetricSymbols = metricSymbols
		values[sampleIdx] = stream.Samples[0].Value
		timestamps[sampleIdx] = stream.Samples[0].TimestampMs
	}

	symbols, symbolLengths := symbolTableBuilder.Build()

	return packedinternedsinglestringquerypb.VectorData{
		SymbolTable: packedinternedsinglestringquerypb.SymbolTable{
			Symbols:       symbols,
			SymbolLengths: symbolLengths,
		},
		Metrics:    metrics,
		Values:     values,
		Timestamps: timestamps,
	}
}

func (c PackedInternedProtobufWithSingleStringCodec) encodeMatrix(data *querymiddleware.PrometheusData) packedinternedsinglestringquerypb.MatrixData {
	series := make([]packedinternedsinglestringquerypb.MatrixSeries, len(data.Result))
	symbolTableBuilder := newSymbolTableBuilder()

	for seriesIdx, stream := range data.Result {
		metricSymbols := make([]uint64, len(stream.Labels)*2)

		for labelIdx, label := range stream.Labels {
			metricSymbols[2*labelIdx] = symbolTableBuilder.GetOrPutSymbol(label.Name)
			metricSymbols[2*labelIdx+1] = symbolTableBuilder.GetOrPutSymbol(label.Value)
		}

		values := make([]float64, len(stream.Samples))
		timestamps := make([]int64, len(stream.Samples))

		for sampleIdx, sample := range stream.Samples {
			values[sampleIdx] = sample.Value
			timestamps[sampleIdx] = sample.TimestampMs
		}

		series[seriesIdx] = packedinternedsinglestringquerypb.MatrixSeries{
			MetricSymbols: metricSymbols,
			Values:        values,
			Timestamps:    timestamps,
		}
	}

	symbols, symbolLengths := symbolTableBuilder.Build()

	return packedinternedsinglestringquerypb.MatrixData{
		SymbolTable: packedinternedsinglestringquerypb.SymbolTable{
			Symbols:       symbols,
			SymbolLengths: symbolLengths,
		},
		Series: series,
	}
}

func (c PackedInternedProtobufWithSingleStringCodec) decodeSymbols(table packedinternedsinglestringquerypb.SymbolTable) []string {
	if len(table.SymbolLengths) == 0 {
		return nil
	}

	symbols := make([]string, len(table.SymbolLengths))
	startOfSymbol := 0

	for i := 0; i < len(table.SymbolLengths); i++ {
		symbolLength := int(table.SymbolLengths[i])
		symbols[i] = table.Symbols[startOfSymbol : startOfSymbol+symbolLength]
		startOfSymbol += symbolLength
	}

	return symbols
}
