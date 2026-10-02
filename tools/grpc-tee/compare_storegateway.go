// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"fmt"
	"slices"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

// storeGatewayComparator compares the responses of the gatewaypb.StoreGateway service.
// It compares the data of the responses, not the messages: the same data in different batches is a match.
type storeGatewayComparator struct{}

func (storeGatewayComparator) Compare(fullMethod string, primary, secondary []proxiedMessage) (ComparisonResult, error) {
	switch fullMethod {
	case "/gatewaypb.StoreGateway/Series":
		return compareFlattened(primary, secondary, flattenSeries, compareSeries)
	case "/gatewaypb.StoreGateway/LabelNames":
		return compareFlattened(primary, secondary, flattenLabelNames, compareStrings)
	case "/gatewaypb.StoreGateway/LabelValues":
		return compareFlattened(primary, secondary, flattenLabelValues, compareStrings)
	case "/gatewaypb.StoreGateway/SearchLabelNames", "/gatewaypb.StoreGateway/SearchLabelValues":
		return compareFlattened(primary, secondary, flattenSearchResults, compareSearchResults)
	default:
		return ComparisonSkipped, fmt.Errorf("no comparison for method %s", fullMethod)
	}
}

// compareFlattened flattens the responses of each backend, then compares the flattened data.
func compareFlattened[T any](primary, secondary []proxiedMessage, flatten func([]proxiedMessage) (T, error), compare func(primary, secondary T) error) (ComparisonResult, error) {
	primaryData, err := flatten(primary)
	if err != nil {
		return ComparisonSkipped, fmt.Errorf("primary responses: %w", err)
	}
	secondaryData, err := flatten(secondary)
	if err != nil {
		return ComparisonSkipped, fmt.Errorf("secondary responses: %w", err)
	}
	if err := compare(primaryData, secondaryData); err != nil {
		return ComparisonMismatch, err
	}
	return ComparisonMatch, nil
}

// seriesData is the data of all the responses of one Series call.
// It does not hold the stats, the chunks estimate, and the response hints, because they can differ between backends.
type seriesData struct {
	labels   [][]mimirpb.LabelAdapter
	chunks   map[uint64][]storepb.AggrChunk
	warnings []string
}

func flattenSeries(responses []proxiedMessage) (seriesData, error) {
	data := seriesData{chunks: map[uint64][]storepb.AggrChunk{}}
	for _, msg := range responses {
		resp, ok := msg.Decoded.(*storepb.SeriesResponse)
		if !ok {
			return seriesData{}, fmt.Errorf("unexpected message type %T", msg.Decoded)
		}
		if batch := resp.GetStreamingSeries(); batch != nil {
			for _, s := range batch.Series {
				data.labels = append(data.labels, s.Labels)
			}
		}
		if batch := resp.GetStreamingChunks(); batch != nil {
			for _, s := range batch.Series {
				data.chunks[s.SeriesIndex] = append(data.chunks[s.SeriesIndex], s.Chunks...)
			}
		}
		if warning := resp.GetWarning(); warning != "" {
			data.warnings = append(data.warnings, warning)
		}
	}
	return data, nil
}

func compareSeries(primary, secondary seriesData) error {
	if len(primary.labels) != len(secondary.labels) {
		return fmt.Errorf("the primary backend returned %d series but the secondary backend returned %d", len(primary.labels), len(secondary.labels))
	}
	for i := range primary.labels {
		if mimirpb.CompareLabelAdapters(primary.labels[i], secondary.labels[i]) != 0 {
			return fmt.Errorf("series %d: the primary backend returned %s but the secondary backend returned %s", i,
				mimirpb.FromLabelAdaptersToLabels(primary.labels[i]).String(), mimirpb.FromLabelAdaptersToLabels(secondary.labels[i]).String())
		}
	}

	if len(primary.chunks) != len(secondary.chunks) {
		return fmt.Errorf("the primary backend returned chunks for %d series but the secondary backend returned chunks for %d", len(primary.chunks), len(secondary.chunks))
	}
	for seriesIndex, primaryChunks := range primary.chunks {
		secondaryChunks := secondary.chunks[seriesIndex]
		if !slices.EqualFunc(primaryChunks, secondaryChunks, func(a, b storepb.AggrChunk) bool { return a.Equal(b) }) {
			return fmt.Errorf("series %d: the primary backend returned %d chunks but the secondary backend returned %d different chunks", seriesIndex, len(primaryChunks), len(secondaryChunks))
		}
	}

	return compareWarnings(primary.warnings, secondary.warnings)
}

// stringsData is the data of all the responses of one LabelNames or LabelValues call.
type stringsData struct {
	values   []string
	warnings []string
}

func flattenLabelNames(responses []proxiedMessage) (stringsData, error) {
	var data stringsData
	for _, msg := range responses {
		resp, ok := msg.Decoded.(*storepb.LabelNamesResponse)
		if !ok {
			return stringsData{}, fmt.Errorf("unexpected message type %T", msg.Decoded)
		}
		data.values = append(data.values, resp.Names...)
		data.warnings = append(data.warnings, resp.Warnings...)
	}
	return data, nil
}

func flattenLabelValues(responses []proxiedMessage) (stringsData, error) {
	var data stringsData
	for _, msg := range responses {
		resp, ok := msg.Decoded.(*storepb.LabelValuesResponse)
		if !ok {
			return stringsData{}, fmt.Errorf("unexpected message type %T", msg.Decoded)
		}
		data.values = append(data.values, resp.Values...)
		data.warnings = append(data.warnings, resp.Warnings...)
	}
	return data, nil
}

func compareStrings(primary, secondary stringsData) error {
	if len(primary.values) != len(secondary.values) {
		return fmt.Errorf("the primary backend returned %d values but the secondary backend returned %d", len(primary.values), len(secondary.values))
	}
	for i := range primary.values {
		if primary.values[i] != secondary.values[i] {
			return fmt.Errorf("value %d: the primary backend returned %q but the secondary backend returned %q", i, primary.values[i], secondary.values[i])
		}
	}
	return compareWarnings(primary.warnings, secondary.warnings)
}

// searchData is the data of all the responses of one SearchLabelNames or SearchLabelValues call.
type searchData struct {
	results  []storepb.SearchResultBatch_Result
	warnings []string
}

func flattenSearchResults(responses []proxiedMessage) (searchData, error) {
	var data searchData
	for _, msg := range responses {
		resp, ok := msg.Decoded.(*storepb.SearchResultBatch)
		if !ok {
			return searchData{}, fmt.Errorf("unexpected message type %T", msg.Decoded)
		}
		data.results = append(data.results, resp.Results...)
		data.warnings = append(data.warnings, resp.Warnings...)
	}
	return data, nil
}

func compareSearchResults(primary, secondary searchData) error {
	if len(primary.results) != len(secondary.results) {
		return fmt.Errorf("the primary backend returned %d results but the secondary backend returned %d", len(primary.results), len(secondary.results))
	}
	for i := range primary.results {
		if primary.results[i] != secondary.results[i] {
			return fmt.Errorf("result %d: the primary backend returned %v but the secondary backend returned %v", i, primary.results[i], secondary.results[i])
		}
	}
	return compareWarnings(primary.warnings, secondary.warnings)
}

func compareWarnings(primary, secondary []string) error {
	if !slices.Equal(primary, secondary) {
		return fmt.Errorf("the primary backend returned warnings %q but the secondary backend returned %q", primary, secondary)
	}
	return nil
}
