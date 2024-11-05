// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/chunkcompat/compat.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package client

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/chunk"
	"github.com/grafana/mimir/pkg/util/modelutil"
)

// StreamsToMatrix converts a slice of QueryStreamResponse to a model.Matrix.
func StreamsToMatrix(from, through model.Time, responses []*QueryStreamResponse) (model.Matrix, error) {
	result := model.Matrix{}
	streamingSeries := [][]mimirpb.LabelAdapter{}
	haveReachedEndOfStreamingSeriesLabels := false

	for _, response := range responses {
		series, err := TimeSeriesChunksToMatrix(from, through, response.Chunkseries)
		if err != nil {
			return nil, err
		}

		result = append(result, series...)

		series, err = TimeseriesToMatrix(from, through, response.Timeseries)
		if err != nil {
			return nil, err
		}
		result = append(result, series...)

		for _, s := range response.StreamingSeries {
			if haveReachedEndOfStreamingSeriesLabels {
				return nil, errors.New("received series labels after IsEndOfSeriesStream=true")
			}

			streamingSeries = append(streamingSeries, s.Labels)
		}

		if response.IsEndOfSeriesStream {
			haveReachedEndOfStreamingSeriesLabels = true
		}

		for _, s := range response.StreamingSeriesChunks {
			if !haveReachedEndOfStreamingSeriesLabels {
				return nil, errors.New("received series chunks before IsEndOfSeriesStream=true")
			}

			series, err := seriesChunksToMatrix(from, through, mimirpb.FromLabelAdaptersToLabels(streamingSeries[s.SeriesIndex]), mimirpb.FromLabelAdaptersToMetric(streamingSeries[s.SeriesIndex]), s.Chunks)
			if err != nil {
				return nil, err
			}
			result = append(result, series)
		}
	}
	return result, nil
}

// StreamingSeriesToMatrix converts slice of []client.TimeSeriesChunk to a model.Matrix.
func StreamingSeriesToMatrix(from, through model.Time, sSeries []StreamingSeries) (model.Matrix, error) {
	if sSeries == nil {
		return nil, nil
	}

	result := model.Matrix{}
	var chunks []Chunk
	for _, series := range sSeries {
		chunks = chunks[:0]
		for sourceIdx, source := range series.Sources {
			sourceChunks, err := source.StreamReader.GetChunks(source.SeriesIndex)
			if err != nil {
				return nil, fmt.Errorf("GetChunks() from stream reader for series %d from source %d: %w", source.SeriesIndex, sourceIdx, err)
			}
			chunks = append(chunks, sourceChunks...)
		}
		stream, err := seriesChunksToMatrix(from, through, series.Labels, fromLabelsToMetric(series.Labels), chunks)
		if err != nil {
			return nil, err
		}
		result = append(result, stream)
	}
	return result, nil
}

func fromLabelsToMetric(ls labels.Labels) model.Metric {
	m := make(model.Metric, 16)
	ls.Range(func(l labels.Label) {
		m[model.LabelName(l.Name)] = model.LabelValue(l.Value)
	})
	return m
}

// TimeSeriesChunksToMatrix converts slice of []client.TimeSeriesChunk to a model.Matrix.
func TimeSeriesChunksToMatrix(from, through model.Time, serieses []TimeSeriesChunk) (model.Matrix, error) {
	if serieses == nil {
		return nil, nil
	}

	result := model.Matrix{}
	for _, series := range serieses {
		stream, err := seriesChunksToMatrix(from, through, mimirpb.FromLabelAdaptersToLabels(series.Labels), mimirpb.FromLabelAdaptersToMetric(series.Labels), series.Chunks)
		if err != nil {
			return nil, err
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
		}

		var histograms []model.SampleHistogramPair
		for _, h := range ser.Histograms {
			if h.Timestamp < int64(from) || h.Timestamp > int64(through) {
				continue
			}
			histograms = append(histograms, model.SampleHistogramPair{
				Timestamp: model.Time(h.Timestamp),
				Histogram: mimirpb.FromHistogramProtoToPromHistogram(&h),
			})
		}

		result = append(result, &model.SampleStream{
			Metric:     metric,
			Values:     samples,
			Histograms: histograms,
		})
	}
	return result, nil
}

func seriesChunksToMatrix(from, through model.Time, lbls labels.Labels, metric model.Metric, c []Chunk) (*model.SampleStream, error) {
	chunks, err := FromChunks(lbls, c)
	if err != nil {
		return nil, err
	}

	var samples []model.SamplePair
	var histograms []mimirpb.Histogram
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

	return stream, nil
}

// FromChunks converts []client.Chunk to []chunk.Chunk.
func FromChunks(metric labels.Labels, in []Chunk) ([]chunk.Chunk, error) {
	out := make([]chunk.Chunk, 0, len(in))
	prevMaxTime := int64(0)
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
		out = append(out, chunk.NewChunk(metric, o, firstTime, lastTime, prevMaxTime))
		prevMaxTime = int64(lastTime)
	}
	return out, nil
}

// ToChunks converts []chunk.Chunk to []client.Chunk.
func ToChunks(in []chunk.Chunk) ([]Chunk, error) {
	out := make([]Chunk, 0, len(in))
	for _, i := range in {
		wireChunk := Chunk{
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
