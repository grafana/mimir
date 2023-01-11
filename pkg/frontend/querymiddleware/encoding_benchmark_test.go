// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/internedquerypb"
	"github.com/grafana/mimir/pkg/querier/uninternedquerypb"
)

func TestEncodingRoundtrip(t *testing.T) {
	rootDir := "/Users/charleskorn/Desktop/queries/querier"
	originalFormatDir := path.Join(rootDir, "original-format")
	uninternedProtoFormatDir := path.Join(rootDir, "uninterned-protobuf")
	internedProtoFormatDir := path.Join(rootDir, "interned-protobuf")

	originalFileNames, err := filepath.Glob(path.Join(originalFormatDir, "*.json"))
	require.NoError(t, err)

	for _, originalFileName := range originalFileNames {
		relativeName, err := filepath.Rel(originalFormatDir, originalFileName)
		require.NoError(t, err)

		t.Run(relativeName, func(t *testing.T) {
			originalBytes, err := os.ReadFile(originalFileName)
			require.NoError(t, err)

			original, err := originalDecode(originalBytes)
			require.NoError(t, err)

			t.Run("uninterned-protobuf", func(t *testing.T) {
				protoBytesOnDisk, err := os.ReadFile(path.Join(uninternedProtoFormatDir, relativeName+".pb"))
				require.NoError(t, err)

				actualProto, err := uninternedProtoDecode(protoBytesOnDisk)
				require.NoError(t, err)
				requireEqual(t, original, actualProto)

				protoBytesInMemory, err := uninternedProtoEncode(original)
				require.NoError(t, err)
				require.Equal(t, protoBytesOnDisk, protoBytesInMemory)
			})

			t.Run("interned-protobuf", func(t *testing.T) {
				protoBytesOnDisk, err := os.ReadFile(path.Join(internedProtoFormatDir, relativeName+".pb"))
				require.NoError(t, err)

				actualProto, err := internedProtoDecode(protoBytesOnDisk)
				require.NoError(t, err)
				requireEqual(t, original, actualProto)

				protoBytesInMemory, err := internedProtoEncode(original)
				require.NoError(t, err)
				require.Equal(t, protoBytesOnDisk, protoBytesInMemory)
			})
		})
	}
}

func requireEqual(t *testing.T, expected PrometheusResponse, actual PrometheusResponse) {
	require.Equal(t, expected.Status, actual.Status)
	require.Equal(t, expected.ErrorType, actual.ErrorType)
	require.Equal(t, expected.Error, actual.Error)
	require.Equal(t, expected.Headers, actual.Headers)
	require.Equal(t, expected.Data.ResultType, actual.Data.ResultType)
	require.Len(t, actual.Data.Result, len(expected.Data.Result))

	for streamIdx, actualStream := range actual.Data.Result {
		expectedStream := expected.Data.Result[streamIdx]

		require.ElementsMatch(t, expectedStream.Labels, actualStream.Labels)
		require.Len(t, actualStream.Samples, len(expectedStream.Samples))

		for sampleIdx, actualSample := range actualStream.Samples {
			expectedSample := expectedStream.Samples[sampleIdx]

			if math.IsNaN(expectedSample.Value) && math.IsNaN(actualSample.Value) {
				// NaN != NaN, so we can't assert that the two points are the same if both have NaN values.
				// So we have to check the timestamp separately.
				require.Equal(t, expectedSample.TimestampMs, actualSample.TimestampMs)
			} else {
				require.Equal(t, expectedSample, actualSample)
			}
		}
	}
}

func BenchmarkDecode(b *testing.B) {
	filePattern := "/Users/charleskorn/Desktop/queries/querier/original-format/*.json"
	files, err := filepath.Glob(filePattern)
	require.NoError(b, err)
	require.NotEmpty(b, files)

	for _, file := range files {
		body, err := os.ReadFile(file)
		require.NoError(b, err)

		fileName, _, _ := strings.Cut(filepath.Base(file), ".")

		b.Run(fileName, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := originalDecode(body)

				if err != nil {
					require.NoError(b, err)
				}
			}
		})
	}
}

// FIXME: this isn't a perfect comparison - real PrometheusResponse values also include the response headers from Prometheus
func originalDecode(b []byte) (PrometheusResponse, error) {
	var resp PrometheusResponse

	if err := json.Unmarshal(b, &resp); err != nil {
		return PrometheusResponse{}, err
	}

	return resp, nil
}

func uninternedProtoDecode(b []byte) (PrometheusResponse, error) {
	var resp uninternedquerypb.QueryResponse

	if err := resp.Unmarshal(b); err != nil {
		return PrometheusResponse{}, err
	}

	var prometheusData PrometheusData

	switch d := resp.Data.(type) {
	case *uninternedquerypb.QueryResponse_Scalar:
		prometheusData = decodeUninternedScalar(d.Scalar)
	case *uninternedquerypb.QueryResponse_Vector:
		prometheusData = decodeUninternedVector(d.Vector)
	case *uninternedquerypb.QueryResponse_Matrix:
		prometheusData = decodeUninternedMatrix(d.Matrix)
	default:
		return PrometheusResponse{}, fmt.Errorf("unknown data type %T", resp.Data)
	}

	return PrometheusResponse{
		Status:    resp.Status,
		Data:      &prometheusData,
		ErrorType: resp.ErrorType,
		Error:     resp.Error,
	}, nil
}

func decodeUninternedScalar(d *uninternedquerypb.ScalarData) PrometheusData {
	return PrometheusData{
		ResultType: model.ValScalar.String(),
		Result: []SampleStream{
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

func decodeUninternedVector(d *uninternedquerypb.VectorData) PrometheusData {
	result := make([]SampleStream, len(d.Samples))

	for sampleIdx, sample := range d.Samples {
		labelCount := len(sample.Metric) / 2
		labels := make([]mimirpb.LabelAdapter, labelCount)

		for labelIdx := 0; labelIdx < labelCount; labelIdx++ {
			labels[labelIdx] = mimirpb.LabelAdapter{
				Name:  sample.Metric[2*labelIdx],
				Value: sample.Metric[2*labelIdx+1],
			}
		}

		result[sampleIdx] = SampleStream{
			Labels: labels,
			Samples: []mimirpb.Sample{
				{
					Value:       sample.Value,
					TimestampMs: sample.Timestamp,
				},
			},
		}
	}

	return PrometheusData{
		ResultType: model.ValVector.String(),
		Result:     result,
	}
}

func decodeUninternedMatrix(d *uninternedquerypb.MatrixData) PrometheusData {
	result := make([]SampleStream, len(d.Series))

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

		result[seriesIdx] = SampleStream{
			Labels:  labels,
			Samples: samples,
		}
	}

	return PrometheusData{
		ResultType: model.ValMatrix.String(),
		Result:     result,
	}
}

func uninternedProtoEncode(prometheusResponse PrometheusResponse) ([]byte, error) {
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

func encodeUninternedPrometheusScalar(data *PrometheusData) uninternedquerypb.ScalarData {
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

func encodeUninternedPrometheusVector(data *PrometheusData) uninternedquerypb.VectorData {
	samples := make([]*uninternedquerypb.VectorSample, len(data.Result))

	for sampleIdx, stream := range data.Result {
		if len(stream.Samples) != 1 {
			panic(fmt.Sprintf("vector data stream should have 1 sample, but has %v", len(stream.Samples)))
		}

		metric := make([]string, len(stream.Labels)*2)

		for labelIdx, label := range stream.Labels {
			metric[2*labelIdx] = label.Name
			metric[2*labelIdx+1] = label.Value
		}

		samples[sampleIdx] = &uninternedquerypb.VectorSample{
			Metric:    metric,
			Value:     stream.Samples[0].Value,
			Timestamp: stream.Samples[0].TimestampMs,
		}
	}

	return uninternedquerypb.VectorData{
		Samples: samples,
	}
}

func encodeUninternedPrometheusMatrix(data *PrometheusData) uninternedquerypb.MatrixData {
	series := make([]*uninternedquerypb.MatrixSeries, len(data.Result))

	for seriesIdx, stream := range data.Result {
		metric := make([]string, len(stream.Labels)*2)

		for labelIdx, label := range stream.Labels {
			metric[2*labelIdx] = label.Name
			metric[2*labelIdx+1] = label.Value
		}

		samples := make([]*uninternedquerypb.MatrixSample, len(stream.Samples))

		for sampleIdx, sample := range stream.Samples {
			samples[sampleIdx] = &uninternedquerypb.MatrixSample{
				Value:     sample.Value,
				Timestamp: sample.TimestampMs,
			}
		}

		series[seriesIdx] = &uninternedquerypb.MatrixSeries{
			Metric:  metric,
			Samples: samples,
		}
	}

	return uninternedquerypb.MatrixData{
		Series: series,
	}
}

func internedProtoDecode(b []byte) (PrometheusResponse, error) {
	var resp internedquerypb.QueryResponse

	if err := resp.Unmarshal(b); err != nil {
		return PrometheusResponse{}, err
	}

	var prometheusData PrometheusData

	switch d := resp.Data.(type) {
	case *internedquerypb.QueryResponse_Scalar:
		prometheusData = decodeInternedScalar(d.Scalar)
	case *internedquerypb.QueryResponse_Vector:
		prometheusData = decodeInternedVector(d.Vector)
	case *internedquerypb.QueryResponse_Matrix:
		prometheusData = decodeInternedMatrix(d.Matrix)
	default:
		return PrometheusResponse{}, fmt.Errorf("unknown data type %T", resp.Data)
	}

	return PrometheusResponse{
		Status:    resp.Status,
		Data:      &prometheusData,
		ErrorType: resp.ErrorType,
		Error:     resp.Error,
	}, nil
}

func decodeInternedScalar(d *internedquerypb.ScalarData) PrometheusData {
	return PrometheusData{
		ResultType: model.ValScalar.String(),
		Result: []SampleStream{
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

func decodeInternedVector(d *internedquerypb.VectorData) PrometheusData {
	result := make([]SampleStream, len(d.Samples))

	for sampleIdx, sample := range d.Samples {
		labelCount := len(sample.MetricSymbols) / 2
		labels := make([]mimirpb.LabelAdapter, labelCount)

		for labelIdx := 0; labelIdx < labelCount; labelIdx++ {
			labels[labelIdx] = mimirpb.LabelAdapter{
				Name:  d.Symbols[sample.MetricSymbols[2*labelIdx]],
				Value: d.Symbols[sample.MetricSymbols[2*labelIdx+1]],
			}
		}

		result[sampleIdx] = SampleStream{
			Labels: labels,
			Samples: []mimirpb.Sample{
				{
					Value:       sample.Value,
					TimestampMs: sample.Timestamp,
				},
			},
		}
	}

	return PrometheusData{
		ResultType: model.ValVector.String(),
		Result:     result,
	}
}

func decodeInternedMatrix(d *internedquerypb.MatrixData) PrometheusData {
	result := make([]SampleStream, len(d.Series))

	for seriesIdx, series := range d.Series {
		labelCount := len(series.MetricSymbols) / 2
		labels := make([]mimirpb.LabelAdapter, labelCount)

		for labelIdx := 0; labelIdx < labelCount; labelIdx++ {
			labels[labelIdx] = mimirpb.LabelAdapter{
				Name:  d.Symbols[series.MetricSymbols[2*labelIdx]],
				Value: d.Symbols[series.MetricSymbols[2*labelIdx+1]],
			}
		}

		samples := make([]mimirpb.Sample, len(series.Samples))

		for sampleIdx, sample := range series.Samples {
			samples[sampleIdx] = mimirpb.Sample{
				Value:       sample.Value,
				TimestampMs: sample.Timestamp,
			}
		}

		result[seriesIdx] = SampleStream{
			Labels:  labels,
			Samples: samples,
		}
	}

	return PrometheusData{
		ResultType: model.ValMatrix.String(),
		Result:     result,
	}
}

func internedProtoEncode(prometheusResponse PrometheusResponse) ([]byte, error) {
	resp := internedquerypb.QueryResponse{
		Status:    prometheusResponse.Status,
		ErrorType: prometheusResponse.ErrorType,
		Error:     prometheusResponse.Error,
	}

	switch prometheusResponse.Data.ResultType {
	case model.ValScalar.String():
		scalar := encodeInternedPrometheusScalar(prometheusResponse.Data)
		resp.Data = &internedquerypb.QueryResponse_Scalar{Scalar: &scalar}
	case model.ValVector.String():
		vector := encodeInternedPrometheusVector(prometheusResponse.Data)
		resp.Data = &internedquerypb.QueryResponse_Vector{Vector: &vector}
	case model.ValMatrix.String():
		matrix := encodeInternedPrometheusMatrix(prometheusResponse.Data)
		resp.Data = &internedquerypb.QueryResponse_Matrix{Matrix: &matrix}
	default:
		return nil, fmt.Errorf("unknown result type %v", prometheusResponse.Data.ResultType)
	}

	return resp.Marshal()
}

func encodeInternedPrometheusScalar(data *PrometheusData) internedquerypb.ScalarData {
	if len(data.Result) != 1 {
		panic(fmt.Sprintf("scalar data should have 1 stream, but has %v", len(data.Result)))
	}

	stream := data.Result[0]

	if len(stream.Samples) != 1 {
		panic(fmt.Sprintf("scalar data stream should have 1 sample, but has %v", len(stream.Samples)))
	}

	sample := stream.Samples[0]

	return internedquerypb.ScalarData{
		Value:     sample.Value,
		Timestamp: sample.TimestampMs,
	}
}

func encodeInternedPrometheusVector(data *PrometheusData) internedquerypb.VectorData {
	samples := make([]*internedquerypb.VectorSample, len(data.Result))
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

		samples[sampleIdx] = &internedquerypb.VectorSample{
			MetricSymbols: metricSymbols,
			Value:         stream.Samples[0].Value,
			Timestamp:     stream.Samples[0].TimestampMs,
		}
	}

	symbols := make([]string, len(invertedSymbols))

	for s, i := range invertedSymbols {
		symbols[i] = s
	}

	return internedquerypb.VectorData{
		Symbols: symbols,
		Samples: samples,
	}
}

func encodeInternedPrometheusMatrix(data *PrometheusData) internedquerypb.MatrixData {
	series := make([]*internedquerypb.MatrixSeries, len(data.Result))
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

		samples := make([]*internedquerypb.MatrixSample, len(stream.Samples))

		for sampleIdx, sample := range stream.Samples {
			samples[sampleIdx] = &internedquerypb.MatrixSample{
				Value:     sample.Value,
				Timestamp: sample.TimestampMs,
			}
		}

		series[seriesIdx] = &internedquerypb.MatrixSeries{
			MetricSymbols: metricSymbols,
			Samples:       samples,
		}
	}

	symbols := make([]string, len(invertedSymbols))

	for s, i := range invertedSymbols {
		symbols[i] = s
	}

	return internedquerypb.MatrixData{
		Symbols: symbols,
		Series:  series,
	}
}
