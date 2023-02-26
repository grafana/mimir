// SPDX-License-Identifier: AGPL-3.0-only

package api

import (
	"fmt"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	v1 "github.com/prometheus/prometheus/web/api/v1"

	"github.com/grafana/mimir/pkg/mimirpb"
)

type protobufCodec struct{}

func (c protobufCodec) ContentType() string {
	return mimirpb.QueryResponseMimeType
}

func (c protobufCodec) CanEncode(resp *v1.Response) bool {
	if resp.Status == "error" && resp.Data == nil {
		return true
	}

	_, ok := resp.Data.(*v1.QueryData)
	return ok
}

func (c protobufCodec) Encode(resp *v1.Response) ([]byte, error) {
	status, err := mimirpb.StatusFromPrometheusString(string(resp.Status))
	if err != nil {
		return nil, err
	}

	errorType, err := mimirpb.ErrorTypeFromPrometheusString(string(resp.ErrorType))
	if err != nil {
		return nil, err
	}

	p := mimirpb.QueryResponse{
		Status:    status,
		ErrorType: errorType,
		Error:     resp.Error,
	}

	if resp.Data != nil {
		data := resp.Data.(*v1.QueryData)

		switch data.ResultType {
		case parser.ValueTypeString:
			s := c.encodeString(data.Result.(promql.String))
			p.Data = &mimirpb.QueryResponse_String_{String_: &s}

		case parser.ValueTypeScalar:
			s := c.encodeScalar(data.Result.(promql.Scalar))
			p.Data = &mimirpb.QueryResponse_Scalar{Scalar: &s}

		case parser.ValueTypeVector:
			v, err := c.encodeVector(data.Result.(promql.Vector))
			if err != nil {
				return nil, err
			}

			p.Data = &mimirpb.QueryResponse_Vector{Vector: &v}

		case parser.ValueTypeMatrix:
			m, err := c.encodeMatrix(data.Result.(promql.Matrix))
			if err != nil {
				return nil, err
			}

			p.Data = &mimirpb.QueryResponse_Matrix{Matrix: &m}

		default:
			return nil, fmt.Errorf("unknown result type '%v'", data.ResultType)
		}
	}

	return p.Marshal()
}

func (c protobufCodec) encodeString(s promql.String) mimirpb.StringData {
	return mimirpb.StringData{
		TimestampMilliseconds: s.T,
		Value:                 s.V,
	}
}

func (c protobufCodec) encodeScalar(s promql.Scalar) mimirpb.ScalarData {
	return mimirpb.ScalarData{
		TimestampMilliseconds: s.T,
		Value:                 s.V,
	}
}

func (c protobufCodec) encodeVector(v promql.Vector) (mimirpb.VectorData, error) {
	histogramCount := 0

	for _, s := range v {
		if s.H != nil {
			histogramCount++
		}
	}

	samples := make([]mimirpb.VectorSample, 0, len(v)-histogramCount)
	histograms := make([]mimirpb.VectorHistogram, 0, histogramCount)

	for _, s := range v {
		metric := labelsToStringArray(s.Metric)

		if s.H == nil {
			samples = append(samples, mimirpb.VectorSample{
				Metric:                metric,
				TimestampMilliseconds: s.T,
				Value:                 s.V,
			})
		} else {
			h, err := protobufHistogramFromPoint(s.Point)
			if err != nil {
				return mimirpb.VectorData{}, err
			}

			histograms = append(histograms, mimirpb.VectorHistogram{
				Metric:                metric,
				TimestampMilliseconds: s.T,
				Histogram:             h,
			})
		}
	}

	return mimirpb.VectorData{
		Samples:    samples,
		Histograms: histograms,
	}, nil
}

func (c protobufCodec) encodeMatrix(m promql.Matrix) (mimirpb.MatrixData, error) {
	protobufSeries := make([]mimirpb.MatrixSeries, len(m))

	for i, s := range m {
		series, err := c.encodeMatrixSeries(s)
		if err != nil {
			return mimirpb.MatrixData{}, err
		}

		protobufSeries[i] = series
	}

	return mimirpb.MatrixData{
		Series: protobufSeries,
	}, nil
}

func (c protobufCodec) encodeMatrixSeries(s promql.Series) (mimirpb.MatrixSeries, error) {
	histogramCount := 0

	for _, p := range s.Points {
		if p.H != nil {
			histogramCount++
		}
	}

	samples := make([]mimirpb.MatrixSample, 0, len(s.Points)-histogramCount)
	histograms := make([]mimirpb.MatrixHistogram, 0, histogramCount)

	for _, p := range s.Points {
		if p.H == nil {
			samples = append(samples, mimirpb.MatrixSample{
				TimestampMilliseconds: p.T,
				Value:                 p.V,
			})
		} else {
			h, err := protobufHistogramFromPoint(p)
			if err != nil {
				return mimirpb.MatrixSeries{}, nil
			}

			histograms = append(histograms, mimirpb.MatrixHistogram{
				TimestampMilliseconds: p.T,
				Histogram:             h,
			})
		}
	}

	return mimirpb.MatrixSeries{
		Metric:     labelsToStringArray(s.Metric),
		Samples:    samples,
		Histograms: histograms,
	}, nil
}

func labelsToStringArray(l labels.Labels) []string {
	strings := make([]string, 2*l.Len())
	i := 0

	l.Range(func(l labels.Label) {
		strings[2*i] = l.Name
		strings[2*i+1] = l.Value
		i++
	})

	return strings
}

func protobufHistogramFromPoint(p promql.Point) (mimirpb.FloatHistogram, error) {
	return *mimirpb.FloatHistogramFromPrometheusModel(p.H), nil
}

func protobufSpansFromSpans(spans []histogram.Span) []mimirpb.BucketSpan {
	protobufSpans := make([]mimirpb.BucketSpan, len(spans))

	for i, s := range spans {
		protobufSpans[i] = mimirpb.BucketSpan{
			Offset: s.Offset,
			Length: s.Length,
		}
	}

	return protobufSpans
}
