// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"fmt"

	"github.com/prometheus/common/model"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware/encoding/uninternedquerypb"
	"github.com/grafana/mimir/pkg/mimirpb"
)

type UninternedProtobufCodec struct{}

func (c UninternedProtobufCodec) Decode(b []byte) (querymiddleware.PrometheusResponse, error) {
	var resp uninternedquerypb.QueryResponse

	if err := resp.Unmarshal(b); err != nil {
		return querymiddleware.PrometheusResponse{}, err
	}

	var prometheusData querymiddleware.PrometheusData

	switch d := resp.Data.(type) {
	case *uninternedquerypb.QueryResponse_Scalar:
		prometheusData = decodeUninternedScalar(d.Scalar)
	case *uninternedquerypb.QueryResponse_Vector:
		prometheusData = decodeUninternedVector(d.Vector)
	case *uninternedquerypb.QueryResponse_Matrix:
		prometheusData = decodeUninternedMatrix(d.Matrix)
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

func decodeUninternedScalar(d *uninternedquerypb.ScalarData) querymiddleware.PrometheusData {
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

func decodeUninternedVector(d *uninternedquerypb.VectorData) querymiddleware.PrometheusData {
	result := make([]querymiddleware.SampleStream, len(d.Samples))

	for sampleIdx, sample := range d.Samples {
		labelCount := len(sample.Metric) / 2
		labels := make([]mimirpb.LabelAdapter, labelCount)

		for labelIdx := 0; labelIdx < labelCount; labelIdx++ {
			labels[labelIdx] = mimirpb.LabelAdapter{
				Name:  sample.Metric[2*labelIdx],
				Value: sample.Metric[2*labelIdx+1],
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

func decodeUninternedMatrix(d *uninternedquerypb.MatrixData) querymiddleware.PrometheusData {
	result := make([]querymiddleware.SampleStream, len(d.Series))

	for seriesIdx, series := range d.Series {
		labelCount := len(series.Metric) / 2
		labels := make([]mimirpb.LabelAdapter, labelCount)

		for labelIdx := 0; labelIdx < labelCount; labelIdx++ {
			labels[labelIdx] = mimirpb.LabelAdapter{
				Name:  series.Metric[2*labelIdx],
				Value: series.Metric[2*labelIdx+1],
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

func (c UninternedProtobufCodec) Encode(prometheusResponse querymiddleware.PrometheusResponse) ([]byte, error) {
	resp := uninternedquerypb.QueryResponse{
		Status:    prometheusResponse.Status,
		ErrorType: prometheusResponse.ErrorType,
		Error:     prometheusResponse.Error,
	}

	switch prometheusResponse.Data.ResultType {
	case model.ValScalar.String():
		scalar := encodeUninternedPrometheusScalar(prometheusResponse.Data)
		resp.Data = &uninternedquerypb.QueryResponse_Scalar{Scalar: &scalar}
	case model.ValVector.String():
		vector := encodeUninternedPrometheusVector(prometheusResponse.Data)
		resp.Data = &uninternedquerypb.QueryResponse_Vector{Vector: &vector}
	case model.ValMatrix.String():
		matrix := encodeUninternedPrometheusMatrix(prometheusResponse.Data)
		resp.Data = &uninternedquerypb.QueryResponse_Matrix{Matrix: &matrix}
	default:
		return nil, fmt.Errorf("unknown result type %v", prometheusResponse.Data.ResultType)
	}

	return resp.Marshal()
}

func encodeUninternedPrometheusScalar(data *querymiddleware.PrometheusData) uninternedquerypb.ScalarData {
	if len(data.Result) != 1 {
		panic(fmt.Sprintf("scalar data should have 1 stream, but has %v", len(data.Result)))
	}

	stream := data.Result[0]

	if len(stream.Samples) != 1 {
		panic(fmt.Sprintf("scalar data stream should have 1 sample, but has %v", len(stream.Samples)))
	}

	sample := stream.Samples[0]

	return uninternedquerypb.ScalarData{
		Value:     sample.Value,
		Timestamp: sample.TimestampMs,
	}
}

func encodeUninternedPrometheusVector(data *querymiddleware.PrometheusData) uninternedquerypb.VectorData {
	samples := make([]uninternedquerypb.VectorSample, len(data.Result))

	for sampleIdx, stream := range data.Result {
		if len(stream.Samples) != 1 {
			panic(fmt.Sprintf("vector data stream should have 1 sample, but has %v", len(stream.Samples)))
		}

		metric := make([]string, len(stream.Labels)*2)

		for labelIdx, label := range stream.Labels {
			metric[2*labelIdx] = label.Name
			metric[2*labelIdx+1] = label.Value
		}

		samples[sampleIdx] = uninternedquerypb.VectorSample{
			Metric:    metric,
			Value:     stream.Samples[0].Value,
			Timestamp: stream.Samples[0].TimestampMs,
		}
	}

	return uninternedquerypb.VectorData{
		Samples: samples,
	}
}

func encodeUninternedPrometheusMatrix(data *querymiddleware.PrometheusData) uninternedquerypb.MatrixData {
	series := make([]uninternedquerypb.MatrixSeries, len(data.Result))

	for seriesIdx, stream := range data.Result {
		metric := make([]string, len(stream.Labels)*2)

		for labelIdx, label := range stream.Labels {
			metric[2*labelIdx] = label.Name
			metric[2*labelIdx+1] = label.Value
		}

		samples := make([]uninternedquerypb.MatrixSample, len(stream.Samples))

		for sampleIdx, sample := range stream.Samples {
			samples[sampleIdx] = uninternedquerypb.MatrixSample{
				Value:     sample.Value,
				Timestamp: sample.TimestampMs,
			}
		}

		series[seriesIdx] = uninternedquerypb.MatrixSeries{
			Metric:  metric,
			Samples: samples,
		}
	}

	return uninternedquerypb.MatrixData{
		Series: series,
	}
}
