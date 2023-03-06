// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/chunkcompat/compat.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package chunkcompat

import (
	"bytes"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/chunk"
	"github.com/grafana/mimir/pkg/util/modelutil"
)

// StreamsToMatrix converts a slice of QueryStreamResponse to a model.Matrix.
func StreamsToMatrix(from, through model.Time, responses []*client.QueryStreamResponse) (model.Matrix, error) {
	result := model.Matrix{}
	for _, response := range responses {
		series, err := SeriesChunksToMatrix(from, through, response.Chunkseries)
		if err != nil {
			return nil, err
		}

		result = append(result, series...)

		series, err = TimeseriesToMatrix(from, through, response.Timeseries)
		if err != nil {
			return nil, err
		}
		result = append(result, series...)
	}
	return result, nil
}

// SeriesChunksToMatrix converts slice of []client.TimeSeriesChunk to a model.Matrix.
func SeriesChunksToMatrix(from, through model.Time, serieses []client.TimeSeriesChunk) (model.Matrix, error) {
	if serieses == nil {
		return nil, nil
	}

	result := model.Matrix{}
	for _, series := range serieses {
		metric := mimirpb.FromLabelAdaptersToMetric(series.Labels)
		chunks, err := FromChunks(mimirpb.FromLabelAdaptersToLabels(series.Labels), series.Chunks)
		if err != nil {
			return nil, err
		}

		samples := []model.SamplePair{}
		histograms := []mimirpb.Histogram{}
		for _, chunk := range chunks {
			sf, sh, err := chunk.Samples(from, through)
			if err != nil {
				return nil, err
			}
			samples = modelutil.MergeSampleSets(samples, sf)
			histograms = modelutil.MergeHistogramSets(histograms, sh)
		}

		stream := &model.SampleStream{
			Metric: metric,
		}
		if len(samples) > 0 {
			stream.Values = samples
		}
		if len(histograms) > 0 {
			histogramsDecoded := make([]model.SampleHistogramPair, 0, len(histograms))
			for _, h := range histograms {
				histogramsDecoded = append(histogramsDecoded, model.SampleHistogramPair{
					Timestamp: model.Time(h.Timestamp),
					Histogram: mimirpb.FromHistogramProtoToPromHistogram(&h),
				})
			}
			stream.Histograms = histogramsDecoded
		}
		result = append(result, stream)
	}
	return result, nil
}

func TimeseriesToMatrix(from, through model.Time, series []mimirpb.TimeSeries) (model.Matrix, error) {
	if series == nil {
		return nil, nil
	}

	result := model.Matrix{}
	for _, ser := range series {
		metric := mimirpb.FromLabelAdaptersToMetric(ser.Labels)

		var samples []model.SamplePair
		for _, sam := range ser.Samples {
			if sam.TimestampMs < int64(from) || sam.TimestampMs > int64(through) {
				continue
			}

			samples = append(samples, model.SamplePair{
				Timestamp: model.Time(sam.TimestampMs),
				Value:     model.SampleValue(sam.Value),
			})

			// Only used in tests. Add native histogram support later: https://github.com/grafana/mimir/issues/4378
		}

		result = append(result, &model.SampleStream{
			Metric: metric,
			Values: samples,
		})
	}
	return result, nil
}

// FromChunks converts []client.Chunk to []chunk.Chunk.
func FromChunks(metric labels.Labels, in []client.Chunk) ([]chunk.Chunk, error) {
	out := make([]chunk.Chunk, 0, len(in))
	for _, i := range in {
		o, err := chunk.NewForEncoding(chunk.Encoding(byte(i.Encoding)))
		if err != nil {
			return nil, err
		}

		if err := o.UnmarshalFromBuf(i.Data); err != nil {
			return nil, err
		}

		firstTime, lastTime := model.Time(i.StartTimestampMs), model.Time(i.EndTimestampMs)
		// As the lifetime of this chunk is scopes to this request, we don't need
		// to supply a fingerprint.
		out = append(out, chunk.NewChunk(metric, o, firstTime, lastTime))
	}
	return out, nil
}

// ToChunks converts []chunk.Chunk to []client.Chunk.
func ToChunks(in []chunk.Chunk) ([]client.Chunk, error) {
	out := make([]client.Chunk, 0, len(in))
	for _, i := range in {
		wireChunk := client.Chunk{
			StartTimestampMs: int64(i.From),
			EndTimestampMs:   int64(i.Through),
			Encoding:         int32(i.Data.Encoding()),
		}

		buf := bytes.NewBuffer(make([]byte, 0, chunk.ChunkLen))
		if err := i.Data.Marshal(buf); err != nil {
			return nil, err
		}

		wireChunk.Data = buf.Bytes()
		out = append(out, wireChunk)
	}
	return out, nil
}
