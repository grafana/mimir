// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/merger.go
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/strutil/merge.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
// Provenance-includes-copyright: The Thanos Authors.

package client

import (
	"errors"
	"fmt"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/chunk"
)

// StreamsToMatrixForTests converts a slice of QueryStreamResponse to a model.Matrix.
//
// NOTE: This should only be called from test code.
func StreamsToMatrixForTests(from, through model.Time, responses []*QueryStreamResponse) (model.Matrix, error) {
	result := model.Matrix{}
	streamingSeries := [][]mimirpb.LabelAdapter{}
	haveReachedEndOfStreamingSeriesLabels := false

	for _, response := range responses {
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

// StreamingSeriesToMatrixForTests converts slice of []client.TimeSeriesChunk to a model.Matrix.
//
// NOTE: This should only be called from test code.
func StreamingSeriesToMatrixForTests(from, through model.Time, sSeries []StreamingSeries) (model.Matrix, error) {
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

func seriesChunksToMatrix(from, through model.Time, lbls labels.Labels, metric model.Metric, c []Chunk) (*model.SampleStream, error) {
	chunks, err := FromChunks(lbls, c)
	if err != nil {
		return nil, err
	}

	var samples []model.SamplePair
	var histograms []mimirpb.Histogram
	for _, chk := range chunks {
		sf, sh, err := rangeValues(chk.Data.NewIterator(nil), from, through)
		if err != nil {
			return nil, err
		}
		samples = mergeSampleSets(samples, sf)
		histograms = mergeHistogramSets(histograms, sh)
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

// rangeValues is a utility function that retrieves all values within the given
// range from an Iterator.
func rangeValues(it chunk.Iterator, oldestInclusive, newestInclusive model.Time) ([]model.SamplePair, []mimirpb.Histogram, error) {
	resultFloat := []model.SamplePair{}
	resultHist := []mimirpb.Histogram{}
	currValType := it.FindAtOrAfter(oldestInclusive)
	if currValType == chunkenc.ValNone {
		return resultFloat, resultHist, it.Err()
	}
	for !model.Time(it.Timestamp()).After(newestInclusive) {
		switch currValType {
		case chunkenc.ValFloat:
			resultFloat = append(resultFloat, it.Value())
		case chunkenc.ValHistogram:
			t, h := it.AtHistogram(nil) // Nil argument as we pass the data to the protobuf as-is without copy.
			resultHist = append(resultHist, mimirpb.FromHistogramToHistogramProto(t, h))
		case chunkenc.ValFloatHistogram:
			t, h := it.AtFloatHistogram(nil) // Nil argument as we pass the data to the protobuf as-is without copy.
			resultHist = append(resultHist, mimirpb.FromFloatHistogramToHistogramProto(t, h))
		default:
			return nil, nil, fmt.Errorf("unknown value type %v in iterator", currValType)
		}
		currValType = it.Scan()
		if currValType == chunkenc.ValNone {
			break
		}
	}
	return resultFloat, resultHist, it.Err()
}

// mergeSampleSets merges and dedupes two sets of already sorted sample pairs.
func mergeSampleSets(a, b []model.SamplePair) []model.SamplePair {
	result := make([]model.SamplePair, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i].Timestamp < b[j].Timestamp {
			result = append(result, a[i])
			i++
		} else if a[i].Timestamp > b[j].Timestamp {
			result = append(result, b[j])
			j++
		} else {
			result = append(result, a[i])
			i++
			j++
		}
	}
	// Add the rest of a or b. One of them is empty now.
	result = append(result, a[i:]...)
	result = append(result, b[j:]...)
	return result
}

// mergeHistogramSets merges and dedupes two sets of already sorted histograms.
func mergeHistogramSets(a, b []mimirpb.Histogram) []mimirpb.Histogram {
	result := make([]mimirpb.Histogram, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i].GetTimestamp() < b[j].GetTimestamp() {
			result = append(result, a[i])
			i++
		} else if a[i].GetTimestamp() > b[j].GetTimestamp() {
			result = append(result, b[j])
			j++
		} else {
			result = append(result, a[i])
			i++
			j++
		}
	}
	// Add the rest of a or b. One of them is empty now.
	result = append(result, a[i:]...)
	result = append(result, b[j:]...)
	return result
}
