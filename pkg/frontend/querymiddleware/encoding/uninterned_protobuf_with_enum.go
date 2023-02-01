// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"fmt"

	"github.com/prometheus/common/model"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware/encoding/uninternedquerywithenumpb"
	"github.com/grafana/mimir/pkg/mimirpb"
)

type UninternedProtobufWithEnumCodec struct{}

func (c UninternedProtobufWithEnumCodec) Decode(b []byte) (querymiddleware.PrometheusResponse, error) {
	var resp uninternedquerywithenumpb.QueryResponse

	if err := resp.Unmarshal(b); err != nil {
		return querymiddleware.PrometheusResponse{}, err
	}

	var prometheusData querymiddleware.PrometheusData

	switch d := resp.Data.(type) {
	case *uninternedquerywithenumpb.QueryResponse_Scalar:
		prometheusData = c.decodeScalar(d.Scalar)
	case *uninternedquerywithenumpb.QueryResponse_Vector:
		prometheusData = c.decodeVector(d.Vector)
	case *uninternedquerywithenumpb.QueryResponse_Matrix:
		prometheusData = c.decodeMatrix(d.Matrix)
	default:
		return querymiddleware.PrometheusResponse{}, fmt.Errorf("unknown data type %T", resp.Data)
	}

	status, err := resp.Status.ToPrometheusString()
	if err != nil {
		return querymiddleware.PrometheusResponse{}, err
	}

	errorType, err := resp.ErrorType.ToPrometheusString()
	if err != nil {
		return querymiddleware.PrometheusResponse{}, err
	}

	return querymiddleware.PrometheusResponse{
		Status:    status,
		Data:      &prometheusData,
		ErrorType: errorType,
		Error:     resp.Error,
	}, nil
}

func (c UninternedProtobufWithEnumCodec) decodeScalar(d *uninternedquerywithenumpb.ScalarData) querymiddleware.PrometheusData {
	return querymiddleware.PrometheusData{
		ResultType: model.ValScalar.String(),
		Result: []querymiddleware.SampleStream{
			{
				Samples: []mimirpb.Sample{
					{
						Value:       d.Value,
						TimestampMs: d.TimestampMilliseconds,
					},
				},
			},
		},
	}
}

func (c UninternedProtobufWithEnumCodec) decodeVector(d *uninternedquerywithenumpb.VectorData) querymiddleware.PrometheusData {
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
					TimestampMs: sample.TimestampMilliseconds,
				},
			},
		}
	}

	return querymiddleware.PrometheusData{
		ResultType: model.ValVector.String(),
		Result:     result,
	}
}

func (c UninternedProtobufWithEnumCodec) decodeMatrix(d *uninternedquerywithenumpb.MatrixData) querymiddleware.PrometheusData {
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
				TimestampMs: sample.TimestampMilliseconds,
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

func (c UninternedProtobufWithEnumCodec) Encode(prometheusResponse querymiddleware.PrometheusResponse) ([]byte, error) {
	status, err := uninternedquerywithenumpb.StatusFromPrometheusString(prometheusResponse.Status)
	if err != nil {
		return nil, err
	}

	errorType, err := uninternedquerywithenumpb.ErrorTypeFromPrometheusString(prometheusResponse.ErrorType)
	if err != nil {
		return nil, err
	}

	resp := uninternedquerywithenumpb.QueryResponse{
		Status:    status,
		ErrorType: errorType,
		Error:     prometheusResponse.Error,
	}

	switch prometheusResponse.Data.ResultType {
	case model.ValScalar.String():
		scalar := c.encodePrometheusScalar(prometheusResponse.Data)
		resp.Data = &uninternedquerywithenumpb.QueryResponse_Scalar{Scalar: &scalar}
	case model.ValVector.String():
		vector := c.encodePrometheusVector(prometheusResponse.Data)
		resp.Data = &uninternedquerywithenumpb.QueryResponse_Vector{Vector: &vector}
	case model.ValMatrix.String():
		matrix := c.encodePrometheusMatrix(prometheusResponse.Data)
		resp.Data = &uninternedquerywithenumpb.QueryResponse_Matrix{Matrix: &matrix}
	default:
		return nil, fmt.Errorf("unknown result type %v", prometheusResponse.Data.ResultType)
	}

	return resp.Marshal()
}

func (c UninternedProtobufWithEnumCodec) encodePrometheusScalar(data *querymiddleware.PrometheusData) uninternedquerywithenumpb.ScalarData {
	if len(data.Result) != 1 {
		panic(fmt.Sprintf("scalar data should have 1 stream, but has %v", len(data.Result)))
	}

	stream := data.Result[0]

	if len(stream.Samples) != 1 {
		panic(fmt.Sprintf("scalar data stream should have 1 sample, but has %v", len(stream.Samples)))
	}

	sample := stream.Samples[0]

	return uninternedquerywithenumpb.ScalarData{
		Value:                 sample.Value,
		TimestampMilliseconds: sample.TimestampMs,
	}
}

func (c UninternedProtobufWithEnumCodec) encodePrometheusVector(data *querymiddleware.PrometheusData) uninternedquerywithenumpb.VectorData {
	samples := make([]uninternedquerywithenumpb.VectorSample, len(data.Result))

	for sampleIdx, stream := range data.Result {
		if len(stream.Samples) != 1 {
			panic(fmt.Sprintf("vector data stream should have 1 sample, but has %v", len(stream.Samples)))
		}

		metric := make([]string, len(stream.Labels)*2)

		for labelIdx, label := range stream.Labels {
			metric[2*labelIdx] = label.Name
			metric[2*labelIdx+1] = label.Value
		}

		samples[sampleIdx] = uninternedquerywithenumpb.VectorSample{
			Metric:                metric,
			Value:                 stream.Samples[0].Value,
			TimestampMilliseconds: stream.Samples[0].TimestampMs,
		}
	}

	return uninternedquerywithenumpb.VectorData{
		Samples: samples,
	}
}

func (c UninternedProtobufWithEnumCodec) encodePrometheusMatrix(data *querymiddleware.PrometheusData) uninternedquerywithenumpb.MatrixData {
	series := make([]uninternedquerywithenumpb.MatrixSeries, len(data.Result))

	for seriesIdx, stream := range data.Result {
		metric := make([]string, len(stream.Labels)*2)

		for labelIdx, label := range stream.Labels {
			metric[2*labelIdx] = label.Name
			metric[2*labelIdx+1] = label.Value
		}

		samples := make([]uninternedquerywithenumpb.MatrixSample, len(stream.Samples))

		for sampleIdx, sample := range stream.Samples {
			samples[sampleIdx] = uninternedquerywithenumpb.MatrixSample{
				Value:                 sample.Value,
				TimestampMilliseconds: sample.TimestampMs,
			}
		}

		series[seriesIdx] = uninternedquerywithenumpb.MatrixSeries{
			Metric:  metric,
			Samples: samples,
		}
	}

	return uninternedquerywithenumpb.MatrixData{
		Series: series,
	}
}
