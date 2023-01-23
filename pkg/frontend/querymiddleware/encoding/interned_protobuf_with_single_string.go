// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"fmt"

	"github.com/prometheus/common/model"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware/encoding/internedsinglestringquerypb"
	"github.com/grafana/mimir/pkg/mimirpb"
)

type InternedProtobufWithSingleStringCodec struct{}

func (c InternedProtobufWithSingleStringCodec) Decode(b []byte) (querymiddleware.PrometheusResponse, error) {
	var resp internedsinglestringquerypb.QueryResponse

	if err := resp.Unmarshal(b); err != nil {
		return querymiddleware.PrometheusResponse{}, err
	}

	var prometheusData querymiddleware.PrometheusData

	switch d := resp.Data.(type) {
	case *internedsinglestringquerypb.QueryResponse_Scalar:
		prometheusData = c.decodeScalar(d.Scalar)
	case *internedsinglestringquerypb.QueryResponse_Vector:
		prometheusData = c.decodeVector(d.Vector)
	case *internedsinglestringquerypb.QueryResponse_Matrix:
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

func (c InternedProtobufWithSingleStringCodec) decodeScalar(d *internedsinglestringquerypb.ScalarData) querymiddleware.PrometheusData {
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

func (c InternedProtobufWithSingleStringCodec) decodeVector(d *internedsinglestringquerypb.VectorData) querymiddleware.PrometheusData {
	result := make([]querymiddleware.SampleStream, len(d.Samples))
	symbols := c.decodeSymbols(d.SymbolTable)

	for sampleIdx, sample := range d.Samples {
		labelCount := len(sample.MetricSymbols) / 2
		labels := make([]mimirpb.LabelAdapter, labelCount)

		for labelIdx := 0; labelIdx < labelCount; labelIdx++ {
			labels[labelIdx] = mimirpb.LabelAdapter{
				Name:  symbols[sample.MetricSymbols[2*labelIdx]],
				Value: symbols[sample.MetricSymbols[2*labelIdx+1]],
			}
		}

		result[sampleIdx] = querymiddleware.SampleStream{
			Labels: labels,
			Samples: []mimirpb.Sample{
				{
					Value:       sample.Value,
					TimestampMs: sample.Timestamp,
				},
			},
		}
	}

	return querymiddleware.PrometheusData{
		ResultType: model.ValVector.String(),
		Result:     result,
	}
}

func (c InternedProtobufWithSingleStringCodec) decodeMatrix(d *internedsinglestringquerypb.MatrixData) querymiddleware.PrometheusData {
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

		samples := make([]mimirpb.Sample, len(series.Samples))

		for sampleIdx, sample := range series.Samples {
			samples[sampleIdx] = mimirpb.Sample{
				Value:       sample.Value,
				TimestampMs: sample.Timestamp,
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

func (c InternedProtobufWithSingleStringCodec) Encode(prometheusResponse querymiddleware.PrometheusResponse) ([]byte, error) {
	resp := internedsinglestringquerypb.QueryResponse{
		Status:    prometheusResponse.Status,
		ErrorType: prometheusResponse.ErrorType,
		Error:     prometheusResponse.Error,
	}

	switch prometheusResponse.Data.ResultType {
	case model.ValScalar.String():
		scalar := c.encodeScalar(prometheusResponse.Data)
		resp.Data = &internedsinglestringquerypb.QueryResponse_Scalar{Scalar: &scalar}
	case model.ValVector.String():
		vector := c.encodeVector(prometheusResponse.Data)
		resp.Data = &internedsinglestringquerypb.QueryResponse_Vector{Vector: &vector}
	case model.ValMatrix.String():
		matrix := c.encodeMatrix(prometheusResponse.Data)
		resp.Data = &internedsinglestringquerypb.QueryResponse_Matrix{Matrix: &matrix}
	default:
		return nil, fmt.Errorf("unknown result type %v", prometheusResponse.Data.ResultType)
	}

	return resp.Marshal()
}

func (c InternedProtobufWithSingleStringCodec) encodeScalar(data *querymiddleware.PrometheusData) internedsinglestringquerypb.ScalarData {
	if len(data.Result) != 1 {
		panic(fmt.Sprintf("scalar data should have 1 stream, but has %v", len(data.Result)))
	}

	stream := data.Result[0]

	if len(stream.Samples) != 1 {
		panic(fmt.Sprintf("scalar data stream should have 1 sample, but has %v", len(stream.Samples)))
	}

	sample := stream.Samples[0]

	return internedsinglestringquerypb.ScalarData{
		Value:     sample.Value,
		Timestamp: sample.TimestampMs,
	}
}

func (c InternedProtobufWithSingleStringCodec) encodeVector(data *querymiddleware.PrometheusData) internedsinglestringquerypb.VectorData {
	samples := make([]internedsinglestringquerypb.VectorSample, len(data.Result))
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

		samples[sampleIdx] = internedsinglestringquerypb.VectorSample{
			MetricSymbols: metricSymbols,
			Value:         stream.Samples[0].Value,
			Timestamp:     stream.Samples[0].TimestampMs,
		}
	}

	symbols, symbolLengths := symbolTableBuilder.Build()

	return internedsinglestringquerypb.VectorData{
		SymbolTable: internedsinglestringquerypb.SymbolTable{
			Symbols:       symbols,
			SymbolLengths: symbolLengths,
		},
		Samples: samples,
	}
}

func (c InternedProtobufWithSingleStringCodec) encodeMatrix(data *querymiddleware.PrometheusData) internedsinglestringquerypb.MatrixData {
	series := make([]internedsinglestringquerypb.MatrixSeries, len(data.Result))
	symbolTableBuilder := newSymbolTableBuilder()

	for seriesIdx, stream := range data.Result {
		metricSymbols := make([]uint64, len(stream.Labels)*2)

		for labelIdx, label := range stream.Labels {
			metricSymbols[2*labelIdx] = symbolTableBuilder.GetOrPutSymbol(label.Name)
			metricSymbols[2*labelIdx+1] = symbolTableBuilder.GetOrPutSymbol(label.Value)
		}

		samples := make([]internedsinglestringquerypb.MatrixSample, len(stream.Samples))

		for sampleIdx, sample := range stream.Samples {
			samples[sampleIdx] = internedsinglestringquerypb.MatrixSample{
				Value:     sample.Value,
				Timestamp: sample.TimestampMs,
			}
		}

		series[seriesIdx] = internedsinglestringquerypb.MatrixSeries{
			MetricSymbols: metricSymbols,
			Samples:       samples,
		}
	}

	symbols, symbolLengths := symbolTableBuilder.Build()

	return internedsinglestringquerypb.MatrixData{
		SymbolTable: internedsinglestringquerypb.SymbolTable{
			Symbols:       symbols,
			SymbolLengths: symbolLengths,
		},
		Series: series,
	}
}

func (c InternedProtobufWithSingleStringCodec) decodeSymbols(table internedsinglestringquerypb.SymbolTable) []string {
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
