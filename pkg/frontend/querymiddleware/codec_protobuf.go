// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"errors"
	"fmt"

	"github.com/prometheus/common/model"

	"github.com/grafana/mimir/pkg/mimirpb"
)

type protobufFormat struct{}

func (f protobufFormat) Name() string {
	return formatProtobuf
}

func (f protobufFormat) EncodeResponse(resp *PrometheusResponse) ([]byte, error) {
	panic("not yet implemented")
}

func (f protobufFormat) DecodeResponse(buf []byte) (*PrometheusResponse, error) {
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
	}, nil
}

func (f protobufFormat) decodeData(resp mimirpb.QueryResponse) (*PrometheusData, error) {
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

func (f protobufFormat) decodeStringData(data *mimirpb.StringData) *PrometheusData {
	return &PrometheusData{
		ResultType: model.ValString.String(),
		Result: []SampleStream{
			{
				Labels:  []mimirpb.LabelAdapter{{Name: "value", Value: data.Value}},
				Samples: []mimirpb.Sample{{TimestampMs: data.TimestampMilliseconds}},
			},
		},
	}
}

func (f protobufFormat) decodeScalarData(data *mimirpb.ScalarData) *PrometheusData {
	return &PrometheusData{
		ResultType: model.ValScalar.String(),
		Result: []SampleStream{
			{
				Samples: []mimirpb.Sample{{TimestampMs: data.TimestampMilliseconds, Value: data.Value}},
			},
		},
	}
}

func (f protobufFormat) decodeVectorData(data *mimirpb.VectorData) (*PrometheusData, error) {
	streams := make([]SampleStream, len(data.Samples)+len(data.Histograms))

	for i, sample := range data.Samples {
		l, err := labelsFromStringArray(sample.Metric)
		if err != nil {
			return nil, err
		}

		streams[i] = SampleStream{
			Labels: l,
			Samples: []mimirpb.Sample{
				{TimestampMs: sample.TimestampMilliseconds, Value: sample.Value},
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
					Timestamp: sample.TimestampMilliseconds,
					Histogram: sample.Histogram,
				},
			},
		}
	}

	return &PrometheusData{
		ResultType: model.ValVector.String(),
		Result:     streams,
	}, nil
}

func (f protobufFormat) decodeMatrixData(data *mimirpb.MatrixData) (*PrometheusData, error) {
	streams := make([]SampleStream, len(data.Series))

	for seriesIdx, series := range data.Series {
		l, err := labelsFromStringArray(series.Metric)
		if err != nil {
			return nil, err
		}

		samples := make([]mimirpb.Sample, len(series.Samples))

		for sampleIdx, sample := range series.Samples {
			samples[sampleIdx] = mimirpb.Sample{
				TimestampMs: sample.TimestampMilliseconds,
				Value:       sample.Value,
			}
		}

		histograms := make([]mimirpb.FloatHistogramPair, len(series.Histograms))

		for histogramIdx, sample := range series.Histograms {
			histograms[histogramIdx] = mimirpb.FloatHistogramPair{
				Timestamp: sample.TimestampMilliseconds,
				Histogram: sample.Histogram,
			}
		}

		streams[seriesIdx] = SampleStream{
			Labels:     l,
			Samples:    samples,
			Histograms: histograms,
		}
	}

	return &PrometheusData{
		ResultType: model.ValMatrix.String(),
		Result:     streams,
	}, nil
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
