// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"errors"
	"fmt"

	"github.com/prometheus/common/model"
	v1 "github.com/prometheus/prometheus/web/api/v1"

	"github.com/grafana/mimir/pkg/mimirpb"
)

type protobufFormatter struct{}

func (f protobufFormatter) Name() string {
	return formatProtobuf
}

func (f protobufFormatter) ContentType() v1.MIMEType {
	return v1.MIMEType{Type: mimirpb.QueryResponseMimeTypeType, SubType: mimirpb.QueryResponseMimeTypeSubType}
}

func (f protobufFormatter) EncodeQueryResponse(resp *PrometheusResponse) ([]byte, error) {
	status, err := mimirpb.StatusFromPrometheusString(resp.Status)
	if err != nil {
		return nil, err
	}

	errorType, err := mimirpb.ErrorTypeFromPrometheusString(resp.ErrorType)
	if err != nil {
		return nil, err
	}

	payload := mimirpb.QueryResponse{
		Status:    status,
		ErrorType: errorType,
		Error:     resp.Error,
		Warnings:  resp.Warnings,
		Infos:     resp.Infos,
	}

	if resp.Data != nil {
		switch resp.Data.ResultType {
		case model.ValString.String():
			data, err := f.encodeStringData(resp.Data.Result)
			if err != nil {
				return nil, err
			}

			payload.Data = &mimirpb.QueryResponse_String_{String_: &data}

		case model.ValScalar.String():
			data, err := f.encodeScalarData(resp.Data.Result)
			if err != nil {
				return nil, err
			}

			payload.Data = &mimirpb.QueryResponse_Scalar{Scalar: &data}

		case model.ValVector.String():
			data, err := f.encodeVectorData(resp.Data.Result)
			if err != nil {
				return nil, err
			}

			payload.Data = &mimirpb.QueryResponse_Vector{Vector: &data}

		case model.ValMatrix.String():
			data := f.encodeMatrixData(resp.Data.Result)
			payload.Data = &mimirpb.QueryResponse_Matrix{Matrix: &data}

		default:
			return nil, fmt.Errorf("unknown result type '%s'", resp.Data.ResultType)
		}
	}

	return payload.Marshal()
}

func (protobufFormatter) encodeStringData(data []SampleStream) (mimirpb.StringData, error) {
	if len(data) != 1 {
		return mimirpb.StringData{}, fmt.Errorf("expected string response to contain exactly one stream, but it has %d", len(data))
	}

	stream := data[0]

	if len(stream.Samples) != 1 {
		return mimirpb.StringData{}, fmt.Errorf("expected string response stream to contain exactly one sample, but it has %d", len(stream.Samples))
	}

	sample := stream.Samples[0]

	if len(stream.Labels) != 1 {
		return mimirpb.StringData{}, fmt.Errorf("expected string response stream to contain exactly one label, but it has %d", len(stream.Labels))
	}

	label := stream.Labels[0]

	if label.Name != "value" {
		return mimirpb.StringData{}, fmt.Errorf("expected string response stream label to have name 'value', but it has name '%s'", label.Name)
	}

	return mimirpb.StringData{
		TimestampMs: sample.TimestampMs,
		Value:       label.Value,
	}, nil
}

func (protobufFormatter) encodeScalarData(data []SampleStream) (mimirpb.ScalarData, error) {
	if len(data) != 1 {
		return mimirpb.ScalarData{}, fmt.Errorf("expected scalar response to contain exactly one stream, but it has %d", len(data))
	}

	stream := data[0]

	if len(stream.Samples) != 1 {
		return mimirpb.ScalarData{}, fmt.Errorf("expected scalar response stream to contain exactly one sample, but it has %d", len(stream.Samples))
	}

	sample := stream.Samples[0]

	return mimirpb.ScalarData{
		TimestampMs: sample.TimestampMs,
		Value:       sample.Value,
	}, nil
}

func (protobufFormatter) encodeVectorData(data []SampleStream) (mimirpb.VectorData, error) {
	floatCount := 0
	histogramCount := 0

	for _, stream := range data {
		if len(stream.Samples) == 1 {
			floatCount++
		} else if len(stream.Histograms) == 1 {
			histogramCount++
		} else {
			return mimirpb.VectorData{}, fmt.Errorf("expected vector response series to contain exactly one float sample or one histogram, but it contains %d float sample(s) and %d histogram(s)", len(stream.Samples), len(stream.Histograms))
		}
	}

	samples := make([]mimirpb.VectorSample, 0, floatCount)
	histograms := make([]mimirpb.VectorHistogram, 0, histogramCount)

	for _, stream := range data {
		metric := stringArrayFromLabels(stream.Labels)

		if len(stream.Samples) == 1 {
			sample := stream.Samples[0]

			samples = append(samples, mimirpb.VectorSample{
				Metric:      metric,
				Value:       sample.Value,
				TimestampMs: sample.TimestampMs,
			})
		} else {
			sample := stream.Histograms[0]

			histograms = append(histograms, mimirpb.VectorHistogram{
				Metric:      metric,
				Histogram:   *sample.Histogram,
				TimestampMs: sample.TimestampMs,
			})
		}
	}

	return mimirpb.VectorData{
		Samples:    samples,
		Histograms: histograms,
	}, nil
}

func (protobufFormatter) encodeMatrixData(data []SampleStream) mimirpb.MatrixData {
	series := make([]mimirpb.MatrixSeries, len(data))

	for i, stream := range data {
		series[i] = mimirpb.MatrixSeries{
			Metric:     stringArrayFromLabels(stream.Labels),
			Samples:    stream.Samples,
			Histograms: stream.Histograms,
		}
	}

	return mimirpb.MatrixData{Series: series}
}

func (f protobufFormatter) DecodeQueryResponse(buf []byte) (*PrometheusResponse, error) {
	var resp mimirpb.QueryResponse

	if err := resp.Unmarshal(buf); err != nil {
		return nil, err
	}

	status, err := resp.Status.ToPrometheusString()
	if err != nil {
		return nil, err
	}

	errorType, err := resp.ErrorType.ToPrometheusString()
	if err != nil {
		return nil, err
	}

	data, err := f.decodeData(resp)
	if err != nil {
		return nil, err
	}

	return &PrometheusResponse{
		Status:    status,
		ErrorType: errorType,
		Error:     resp.Error,
		Data:      data,
		Warnings:  resp.Warnings,
		Infos:     resp.Infos,
	}, nil
}

func (f protobufFormatter) decodeData(resp mimirpb.QueryResponse) (*PrometheusData, error) {
	if resp.Data == nil {
		if resp.Status != mimirpb.QueryResponse_SUCCESS {
			return nil, nil
		}

		return nil, errors.New("received unexpected nil query response data")
	}

	switch d := resp.Data.(type) {
	case *mimirpb.QueryResponse_String_:
		return f.decodeStringData(d.String_), nil
	case *mimirpb.QueryResponse_Scalar:
		return f.decodeScalarData(d.Scalar), nil
	case *mimirpb.QueryResponse_Vector:
		return f.decodeVectorData(d.Vector)
	case *mimirpb.QueryResponse_Matrix:
		return f.decodeMatrixData(d.Matrix)
	default:
		return nil, fmt.Errorf("unknown query response data type: %T", resp.Data)
	}
}

func (f protobufFormatter) decodeStringData(data *mimirpb.StringData) *PrometheusData {
	return &PrometheusData{
		ResultType: model.ValString.String(),
		Result: []SampleStream{
			{
				Labels:  []mimirpb.LabelAdapter{{Name: "value", Value: data.Value}},
				Samples: []mimirpb.Sample{{TimestampMs: data.TimestampMs}},
			},
		},
	}
}

func (f protobufFormatter) decodeScalarData(data *mimirpb.ScalarData) *PrometheusData {
	return &PrometheusData{
		ResultType: model.ValScalar.String(),
		Result: []SampleStream{
			{
				Samples: []mimirpb.Sample{{TimestampMs: data.TimestampMs, Value: data.Value}},
			},
		},
	}
}

func (f protobufFormatter) decodeVectorData(data *mimirpb.VectorData) (*PrometheusData, error) {
	streams := make([]SampleStream, len(data.Samples)+len(data.Histograms))

	for i, sample := range data.Samples {
		l, err := labelsFromStringArray(sample.Metric)
		if err != nil {
			return nil, err
		}

		streams[i] = SampleStream{
			Labels: l,
			Samples: []mimirpb.Sample{
				{TimestampMs: sample.TimestampMs, Value: sample.Value},
			},
		}
	}

	for i, sample := range data.Histograms {
		l, err := labelsFromStringArray(sample.Metric)
		if err != nil {
			return nil, err
		}

		streams[i+len(data.Samples)] = SampleStream{
			Labels: l,
			Histograms: []mimirpb.FloatHistogramPair{
				{
					TimestampMs: sample.TimestampMs,
					Histogram:   &data.Histograms[i].Histogram,
				},
			},
		}
	}

	return &PrometheusData{
		ResultType: model.ValVector.String(),
		Result:     streams,
	}, nil
}

func (f protobufFormatter) decodeMatrixData(data *mimirpb.MatrixData) (*PrometheusData, error) {
	streams := make([]SampleStream, len(data.Series))

	for seriesIdx, series := range data.Series {
		l, err := labelsFromStringArray(series.Metric)
		if err != nil {
			return nil, err
		}

		streams[seriesIdx] = SampleStream{
			Labels:     l,
			Samples:    series.Samples,
			Histograms: series.Histograms,
		}
	}

	return &PrometheusData{
		ResultType: model.ValMatrix.String(),
		Result:     streams,
	}, nil
}

func (f protobufFormatter) EncodeLabelsResponse(*PrometheusLabelsResponse) ([]byte, error) {
	return nil, errors.New("protobuf labels encoding is not supported")
}

func (f protobufFormatter) DecodeLabelsResponse([]byte) (*PrometheusLabelsResponse, error) {
	return nil, errors.New("protobuf labels decoding is not supported")
}

func (f protobufFormatter) EncodeSeriesResponse(*PrometheusSeriesResponse) ([]byte, error) {
	return nil, errors.New("protobuf series encoding is not supported")
}

func (f protobufFormatter) DecodeSeriesResponse([]byte) (*PrometheusSeriesResponse, error) {
	return nil, errors.New("protobuf series decoding is not supported")
}

func labelsFromStringArray(s []string) ([]mimirpb.LabelAdapter, error) {
	if len(s)%2 != 0 {
		return nil, fmt.Errorf("metric is malformed: expected even number of symbols, but got %v", len(s))
	}

	labelCount := len(s) / 2
	l := make([]mimirpb.LabelAdapter, labelCount)

	for i := 0; i < labelCount; i++ {
		l[i] = mimirpb.LabelAdapter{
			Name:  s[2*i],
			Value: s[2*i+1],
		}
	}

	return l, nil
}

func stringArrayFromLabels(labels []mimirpb.LabelAdapter) []string {
	s := make([]string, len(labels)*2)

	for i, l := range labels {
		s[2*i] = l.Name
		s[2*i+1] = l.Value
	}

	return s
}
