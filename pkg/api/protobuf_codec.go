// SPDX-License-Identifier: AGPL-3.0-only

package api

import (
	"fmt"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	v1 "github.com/prometheus/prometheus/web/api/v1"

	"github.com/grafana/mimir/pkg/mimirpb"
)

type protobufCodec struct{}

func (c protobufCodec) ContentType() v1.MIMEType {
	return v1.MIMEType{Type: mimirpb.QueryResponseMimeTypeType, SubType: mimirpb.QueryResponseMimeTypeSubType}
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
		Warnings:  resp.Warnings,
		Infos:     resp.Infos,
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
			v := c.encodeVector(data.Result.(promql.Vector))
			p.Data = &mimirpb.QueryResponse_Vector{Vector: &v}

		case parser.ValueTypeMatrix:
			m := c.encodeMatrix(data.Result.(promql.Matrix))
			p.Data = &mimirpb.QueryResponse_Matrix{Matrix: &m}

		default:
			return nil, fmt.Errorf("unknown result type '%v'", data.ResultType)
		}
	}

	return p.Marshal()
}

func (c protobufCodec) encodeString(s promql.String) mimirpb.StringData {
	return mimirpb.StringData{
		TimestampMs: s.T,
		Value:       s.V,
	}
}

func (c protobufCodec) encodeScalar(s promql.Scalar) mimirpb.ScalarData {
	return mimirpb.ScalarData{
		TimestampMs: s.T,
		Value:       s.V,
	}
}

func (c protobufCodec) encodeVector(v promql.Vector) mimirpb.VectorData {
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
				Metric:      metric,
				TimestampMs: s.T,
				Value:       s.F,
			})
		} else {
			histograms = append(histograms, mimirpb.VectorHistogram{
				Metric:      metric,
				TimestampMs: s.T,
				Histogram:   *mimirpb.FloatHistogramFromPrometheusModel(s.H),
			})
		}
	}

	return mimirpb.VectorData{
		Samples:    samples,
		Histograms: histograms,
	}
}

func (c protobufCodec) encodeMatrix(m promql.Matrix) mimirpb.MatrixData {
	protobufSeries := make([]mimirpb.MatrixSeries, len(m))

	for i, s := range m {
		protobufSeries[i] = c.encodeMatrixSeries(s)
	}

	return mimirpb.MatrixData{
		Series: protobufSeries,
	}
}

func (c protobufCodec) encodeMatrixSeries(s promql.Series) mimirpb.MatrixSeries {
	samples := make([]mimirpb.Sample, 0, len(s.Floats))
	for _, p := range s.Floats {
		samples = append(samples, mimirpb.Sample{
			TimestampMs: p.T,
			Value:       p.F,
		})
	}

	histograms := make([]mimirpb.FloatHistogramPair, 0, len(s.Histograms))
	for _, p := range s.Histograms {
		histograms = append(histograms, mimirpb.FloatHistogramPair{
			TimestampMs: p.T,
			Histogram:   mimirpb.FloatHistogramFromPrometheusModel(p.H),
		})
	}

	return mimirpb.MatrixSeries{
		Metric:     labelsToStringArray(s.Metric),
		Samples:    samples,
		Histograms: histograms,
	}
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
