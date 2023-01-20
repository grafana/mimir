// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"fmt"

	"github.com/prometheus/common/model"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware/encoding/packedinternedquerypb"
	"github.com/grafana/mimir/pkg/mimirpb"
)

type PackedInternedProtobufCodec struct{}

func (c PackedInternedProtobufCodec) Decode(b []byte) (querymiddleware.PrometheusResponse, error) {
	var resp packedinternedquerypb.QueryResponse

	if err := resp.Unmarshal(b); err != nil {
		return querymiddleware.PrometheusResponse{}, err
	}

	var prometheusData querymiddleware.PrometheusData

	switch d := resp.Data.(type) {
	case *packedinternedquerypb.QueryResponse_Scalar:
		prometheusData = c.decodeScalar(d.Scalar)
	case *packedinternedquerypb.QueryResponse_Vector:
		prometheusData = c.decodeVector(d.Vector)
	case *packedinternedquerypb.QueryResponse_Matrix:
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

func (c PackedInternedProtobufCodec) decodeScalar(d *packedinternedquerypb.ScalarData) querymiddleware.PrometheusData {
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

func (c PackedInternedProtobufCodec) decodeVector(d *packedinternedquerypb.VectorData) querymiddleware.PrometheusData {
	// TODO: check that the number of metrics, timestamps and values is the same
	sampleCount := len(d.Metrics)
	result := make([]querymiddleware.SampleStream, sampleCount)

	for sampleIdx := 0; sampleIdx < sampleCount; sampleIdx++ {
		metricSymbols := d.Metrics[sampleIdx].MetricSymbols
		labelCount := len(metricSymbols) / 2
		labels := make([]mimirpb.LabelAdapter, labelCount)

		for labelIdx := 0; labelIdx < labelCount; labelIdx++ {
			labels[labelIdx] = mimirpb.LabelAdapter{
				Name:  d.Symbols[metricSymbols[2*labelIdx]],
				Value: d.Symbols[metricSymbols[2*labelIdx+1]],
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

func (c PackedInternedProtobufCodec) decodeMatrix(d *packedinternedquerypb.MatrixData) querymiddleware.PrometheusData {
	result := make([]querymiddleware.SampleStream, len(d.Series))

	for seriesIdx, series := range d.Series {
		labelCount := len(series.MetricSymbols) / 2
		labels := make([]mimirpb.LabelAdapter, labelCount)

		for labelIdx := 0; labelIdx < labelCount; labelIdx++ {
			labels[labelIdx] = mimirpb.LabelAdapter{
				Name:  d.Symbols[series.MetricSymbols[2*labelIdx]],
				Value: d.Symbols[series.MetricSymbols[2*labelIdx+1]],
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

func (c PackedInternedProtobufCodec) Encode(prometheusResponse querymiddleware.PrometheusResponse) ([]byte, error) {
	resp := packedinternedquerypb.QueryResponse{
		Status:    prometheusResponse.Status,
		ErrorType: prometheusResponse.ErrorType,
		Error:     prometheusResponse.Error,
	}

	switch prometheusResponse.Data.ResultType {
	case model.ValScalar.String():
		scalar := c.encodeScalar(prometheusResponse.Data)
		resp.Data = &packedinternedquerypb.QueryResponse_Scalar{Scalar: &scalar}
	case model.ValVector.String():
		vector := c.encodeVector(prometheusResponse.Data)
		resp.Data = &packedinternedquerypb.QueryResponse_Vector{Vector: &vector}
	case model.ValMatrix.String():
		matrix := c.encodeMatrix(prometheusResponse.Data)
		resp.Data = &packedinternedquerypb.QueryResponse_Matrix{Matrix: &matrix}
	default:
		return nil, fmt.Errorf("unknown result type %v", prometheusResponse.Data.ResultType)
	}

	return resp.Marshal()
}

func (c PackedInternedProtobufCodec) encodeScalar(data *querymiddleware.PrometheusData) packedinternedquerypb.ScalarData {
	if len(data.Result) != 1 {
		panic(fmt.Sprintf("scalar data should have 1 stream, but has %v", len(data.Result)))
	}

	stream := data.Result[0]

	if len(stream.Samples) != 1 {
		panic(fmt.Sprintf("scalar data stream should have 1 sample, but has %v", len(stream.Samples)))
	}

	sample := stream.Samples[0]

	return packedinternedquerypb.ScalarData{
		Value:     sample.Value,
		Timestamp: sample.TimestampMs,
	}
}

func (c PackedInternedProtobufCodec) encodeVector(data *querymiddleware.PrometheusData) packedinternedquerypb.VectorData {
	metrics := make([]packedinternedquerypb.Metric, len(data.Result))
	values := make([]float64, len(data.Result))
	timestamps := make([]int64, len(data.Result))
	invertedSymbols := map[string]uint64{} // TODO: might be able to save resizing this by scanning through response once and allocating a map big enough to hold all symbols (ie. not just unique symbols)

	for sampleIdx, stream := range data.Result {
		if len(stream.Samples) != 1 {
			panic(fmt.Sprintf("vector data stream should have 1 sample, but has %v", len(stream.Samples)))
		}

		metricSymbols := make([]uint64, len(stream.Labels)*2)

		for labelIdx, label := range stream.Labels {
			if _, ok := invertedSymbols[label.Name]; !ok {
				invertedSymbols[label.Name] = uint64(len(invertedSymbols))
			}

			if _, ok := invertedSymbols[label.Value]; !ok {
				invertedSymbols[label.Value] = uint64(len(invertedSymbols))
			}

			metricSymbols[2*labelIdx] = invertedSymbols[label.Name]
			metricSymbols[2*labelIdx+1] = invertedSymbols[label.Value]
		}

		metrics[sampleIdx].MetricSymbols = metricSymbols
		values[sampleIdx] = stream.Samples[0].Value
		timestamps[sampleIdx] = stream.Samples[0].TimestampMs
	}

	symbols := make([]string, len(invertedSymbols))

	for s, i := range invertedSymbols {
		symbols[i] = s
	}

	return packedinternedquerypb.VectorData{
		Symbols:    symbols,
		Metrics:    metrics,
		Values:     values,
		Timestamps: timestamps,
	}
}

func (c PackedInternedProtobufCodec) encodeMatrix(data *querymiddleware.PrometheusData) packedinternedquerypb.MatrixData {
	series := make([]packedinternedquerypb.MatrixSeries, len(data.Result))
	invertedSymbols := map[string]uint64{} // TODO: might be able to save resizing this by scanning through response once and allocating a map big enough to hold all symbols (ie. not just unique symbols)

	for seriesIdx, stream := range data.Result {
		metricSymbols := make([]uint64, len(stream.Labels)*2)

		for labelIdx, label := range stream.Labels {
			if _, ok := invertedSymbols[label.Name]; !ok {
				invertedSymbols[label.Name] = uint64(len(invertedSymbols))
			}

			if _, ok := invertedSymbols[label.Value]; !ok {
				invertedSymbols[label.Value] = uint64(len(invertedSymbols))
			}

			metricSymbols[2*labelIdx] = invertedSymbols[label.Name]
			metricSymbols[2*labelIdx+1] = invertedSymbols[label.Value]
		}

		values := make([]float64, len(stream.Samples))
		timestamps := make([]int64, len(stream.Samples))

		for sampleIdx, sample := range stream.Samples {
			values[sampleIdx] = sample.Value
			timestamps[sampleIdx] = sample.TimestampMs
		}

		series[seriesIdx] = packedinternedquerypb.MatrixSeries{
			MetricSymbols: metricSymbols,
			Values:        values,
			Timestamps:    timestamps,
		}
	}

	symbols := make([]string, len(invertedSymbols))

	for s, i := range invertedSymbols {
		symbols[i] = s
	}

	return packedinternedquerypb.MatrixData{
		Symbols: symbols,
		Series:  series,
	}
}
