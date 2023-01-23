// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"fmt"

	"github.com/prometheus/common/model"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware/encoding/packedinterneddeltatquerypb"
	"github.com/grafana/mimir/pkg/mimirpb"
)

type PackedInternedProtobufWithRelativeTimestampsCodec struct{}

func (c PackedInternedProtobufWithRelativeTimestampsCodec) Decode(b []byte) (querymiddleware.PrometheusResponse, error) {
	var resp packedinterneddeltatquerypb.QueryResponse

	if err := resp.Unmarshal(b); err != nil {
		return querymiddleware.PrometheusResponse{}, err
	}

	var prometheusData querymiddleware.PrometheusData

	switch d := resp.Data.(type) {
	case *packedinterneddeltatquerypb.QueryResponse_Scalar:
		prometheusData = c.decodeScalar(d.Scalar)
	case *packedinterneddeltatquerypb.QueryResponse_Vector:
		prometheusData = c.decodeVector(d.Vector)
	case *packedinterneddeltatquerypb.QueryResponse_Matrix:
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

func (c PackedInternedProtobufWithRelativeTimestampsCodec) decodeScalar(d *packedinterneddeltatquerypb.ScalarData) querymiddleware.PrometheusData {
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

func (c PackedInternedProtobufWithRelativeTimestampsCodec) decodeVector(d *packedinterneddeltatquerypb.VectorData) querymiddleware.PrometheusData {
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
					TimestampMs: d.Timestamp,
				},
			},
		}
	}

	return querymiddleware.PrometheusData{
		ResultType: model.ValVector.String(),
		Result:     result,
	}
}

func (c PackedInternedProtobufWithRelativeTimestampsCodec) decodeMatrix(d *packedinterneddeltatquerypb.MatrixData) querymiddleware.PrometheusData {
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
		sampleCount := len(series.TimestampDeltas)
		samples := make([]mimirpb.Sample, sampleCount)
		timestamp := d.BaseTimestamp
		currentDelta := int64(0)

		for sampleIdx := 0; sampleIdx < sampleCount; sampleIdx++ {
			currentDelta += series.TimestampDeltas[sampleIdx]
			timestamp += currentDelta

			samples[sampleIdx] = mimirpb.Sample{
				Value:       series.Values[sampleIdx],
				TimestampMs: timestamp,
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

func (c PackedInternedProtobufWithRelativeTimestampsCodec) Encode(prometheusResponse querymiddleware.PrometheusResponse) ([]byte, error) {
	resp := packedinterneddeltatquerypb.QueryResponse{
		Status:    prometheusResponse.Status,
		ErrorType: prometheusResponse.ErrorType,
		Error:     prometheusResponse.Error,
	}

	switch prometheusResponse.Data.ResultType {
	case model.ValScalar.String():
		scalar := c.encodeScalar(prometheusResponse.Data)
		resp.Data = &packedinterneddeltatquerypb.QueryResponse_Scalar{Scalar: &scalar}
	case model.ValVector.String():
		vector := c.encodeVector(prometheusResponse.Data)
		resp.Data = &packedinterneddeltatquerypb.QueryResponse_Vector{Vector: &vector}
	case model.ValMatrix.String():
		matrix := c.encodeMatrix(prometheusResponse.Data)
		resp.Data = &packedinterneddeltatquerypb.QueryResponse_Matrix{Matrix: &matrix}
	default:
		return nil, fmt.Errorf("unknown result type %v", prometheusResponse.Data.ResultType)
	}

	return resp.Marshal()
}

func (c PackedInternedProtobufWithRelativeTimestampsCodec) encodeScalar(data *querymiddleware.PrometheusData) packedinterneddeltatquerypb.ScalarData {
	if len(data.Result) != 1 {
		panic(fmt.Sprintf("scalar data should have 1 stream, but has %v", len(data.Result)))
	}

	stream := data.Result[0]

	if len(stream.Samples) != 1 {
		panic(fmt.Sprintf("scalar data stream should have 1 sample, but has %v", len(stream.Samples)))
	}

	sample := stream.Samples[0]

	return packedinterneddeltatquerypb.ScalarData{
		Value:     sample.Value,
		Timestamp: sample.TimestampMs,
	}
}

func (c PackedInternedProtobufWithRelativeTimestampsCodec) encodeVector(data *querymiddleware.PrometheusData) packedinterneddeltatquerypb.VectorData {
	metrics := make([]packedinterneddeltatquerypb.Metric, len(data.Result))
	values := make([]float64, len(data.Result))
	invertedSymbols := map[string]uint64{} // TODO: might be able to save resizing this by scanning through response once and allocating a map big enough to hold all symbols (ie. not just unique symbols)

	timestamp := int64(0)

	if len(data.Result) > 0 && len(data.Result[0].Samples) > 0 {
		timestamp = data.Result[0].Samples[0].TimestampMs
	}

	for sampleIdx, stream := range data.Result {
		if len(stream.Samples) != 1 {
			panic(fmt.Sprintf("vector data stream should have 1 sample, but has %v", len(stream.Samples)))
		}

		sample := stream.Samples[0]

		if sample.TimestampMs != timestamp {
			panic(fmt.Sprintf("vector data stream should have all points with same timestamp %v, but sample index %v has timestamp %v", timestamp, sampleIdx, sample.TimestampMs))
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
	}

	symbols := make([]string, len(invertedSymbols))

	for s, i := range invertedSymbols {
		symbols[i] = s
	}

	return packedinterneddeltatquerypb.VectorData{
		Symbols:   symbols,
		Metrics:   metrics,
		Values:    values,
		Timestamp: timestamp,
	}
}

func (c PackedInternedProtobufWithRelativeTimestampsCodec) encodeMatrix(data *querymiddleware.PrometheusData) packedinterneddeltatquerypb.MatrixData {
	series := make([]packedinterneddeltatquerypb.MatrixSeries, len(data.Result))
	invertedSymbols := map[string]uint64{} // TODO: might be able to save resizing this by scanning through response once and allocating a map big enough to hold all symbols (ie. not just unique symbols)
	baseTimestamp := int64(0)
	haveBaseTimestamp := false

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
		timestampDeltas := make([]int64, len(stream.Samples))
		currentDelta := int64(0)

		for sampleIdx, sample := range stream.Samples {
			if !haveBaseTimestamp {
				baseTimestamp = sample.TimestampMs
				haveBaseTimestamp = true
			}

			values[sampleIdx] = sample.Value

			if sampleIdx == 0 {
				timestampDeltas[sampleIdx] = sample.TimestampMs - baseTimestamp
				currentDelta = timestampDeltas[sampleIdx]
			} else {
				timestampDeltas[sampleIdx] = sample.TimestampMs - stream.Samples[sampleIdx-1].TimestampMs - currentDelta
				currentDelta += timestampDeltas[sampleIdx]
			}
		}

		series[seriesIdx] = packedinterneddeltatquerypb.MatrixSeries{
			MetricSymbols:   metricSymbols,
			Values:          values,
			TimestampDeltas: timestampDeltas,
		}
	}

	symbols := make([]string, len(invertedSymbols))

	for s, i := range invertedSymbols {
		symbols[i] = s
	}

	return packedinterneddeltatquerypb.MatrixData{
		Symbols:       symbols,
		BaseTimestamp: baseTimestamp,
		Series:        series,
	}
}
