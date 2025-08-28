// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"fmt"

	"github.com/prometheus/prometheus/model/labels"
)

func ReuseRW2(req *WriteRequest) {
	reuseSymbolsSlice(req.SymbolsRW2)
}

// FromWriteRequestToRW2Request converts a write request with RW1 fields populated to a write request with RW2 fields populated.
// It makes a new RW2 request, leaving the original request alone - it is still up to the caller to free the provided request.
// It might retain references in the RW1 request. It's not safe to free the RW1 request until the RW2 request is no longer used.
func FromWriteRequestToRW2Request(rw1 *WriteRequest, commonSymbols []string, offset uint32) (*WriteRequest, error) {
	if rw1 == nil {
		return nil, nil
	}
	if len(rw1.SymbolsRW2) > 0 || len(rw1.TimeseriesRW2) > 0 {
		return nil, fmt.Errorf("the provided request is already rw2")
	}

	rw2 := &WriteRequest{
		Source:                          rw1.Source,
		SkipLabelValidation:             rw1.SkipLabelValidation,
		SkipLabelCountValidation:        rw1.SkipLabelCountValidation,
		skipUnmarshalingExemplars:       rw1.skipUnmarshalingExemplars,
		skipNormalizeMetadataMetricName: rw1.skipNormalizeMetadataMetricName,
	}

	symbols := symbolsTableFromPool()
	defer reuseSymbolsTable(symbols)
	symbols.ConfigureCommonSymbols(offset, commonSymbols)

	var metadataMap map[string]*MetricMetadata
	var extraMetadata []*MetricMetadata
	if len(rw1.Metadata) > 0 && len(rw1.Timeseries) > 0 {
		metadataMap = make(map[string]*MetricMetadata, len(rw1.Metadata))
		// Very rarely, a request might send multiple conflicting metadatas for the same metric.
		// For example, in Prometheus this can happen on the border of a metric churning,
		// if new and old versions of the same metadata are scraped from different processes and end up in the same batch.
		//
		// The Prometheus behavior for RW1.0 is to store *both* metadatas and not take any preference.
		// extraMetadata is used to carry duplicates of existing metadata entries that don't match, so we can propagate them at the end.
		// We don't preallocate any size here, because this should be extremely rare, most metadata requests won't have anything here.
		extraMetadata = make([]*MetricMetadata, 0)
		for _, meta := range rw1.Metadata {
			if seen, ok := metadataMap[meta.MetricFamilyName]; !ok {
				metadataMap[meta.MetricFamilyName] = meta
			} else {
				if !meta.Equal(seen) {
					extraMetadata = append(extraMetadata, meta)
				}
			}
		}
	}

	rw2Timeseries := make([]TimeSeriesRW2, 0, len(rw1.Timeseries)+len(rw1.Metadata)) // TODO: Pool-ify this allocation
	for _, ts := range rw1.Timeseries {
		refs := make([]uint32, 0, len(ts.Labels)*2) // TODO: Pool-ify this allocation
		metricName := ""
		for i := range ts.Labels {
			if ts.Labels[i].Name == labels.MetricName {
				metricName = ts.Labels[i].Value
			}
			refs = append(refs, symbols.Symbolize(ts.Labels[i].Name), symbols.Symbolize(ts.Labels[i].Value))
		}

		var metadata MetadataRW2
		if len(metadataMap) > 0 {
			if rw1Metadata, ok := metadataMap[metricName]; ok {
				metadata = FromMetricMetadataToMetadataRW2(rw1Metadata, symbols)
				delete(metadataMap, metricName)
			}
		}

		rw2Timeseries = append(rw2Timeseries, TimeSeriesRW2{
			LabelsRefs:       refs,
			Samples:          ts.Samples,
			Histograms:       ts.Histograms,
			Exemplars:        FromExemplarsToExemplarsRW2(ts.Exemplars, symbols),
			Metadata:         metadata,
			CreatedTimestamp: ts.CreatedTimestamp,
		})
	}

	// If there are extra metadata not associated with any timeseries, we fabricate an empty timeseries to carry it.
	for _, meta := range rw1.Metadata {
		// If we are matching metadata to series, and we already matched this metadata, we can skip it.
		if metadataMap != nil {
			if _, ok := metadataMap[meta.MetricFamilyName]; !ok {
				continue
			}
		}

		labelsRefs := []uint32{symbols.Symbolize(labels.MetricName), symbols.Symbolize(meta.MetricFamilyName)}
		rw2meta := FromMetricMetadataToMetadataRW2(meta, symbols)
		rw2Timeseries = append(rw2Timeseries, TimeSeriesRW2{
			LabelsRefs: labelsRefs,
			Metadata:   rw2meta,
		})
	}

	rw2.TimeseriesRW2 = rw2Timeseries
	syms := symbolsSliceFromPool()
	rw2.SymbolsRW2 = symbols.SymbolsPrealloc(syms)

	return rw2, nil
}

func FromExemplarsToExemplarsRW2(exemplars []Exemplar, symbols StringSymbolizer) []ExemplarRW2 {
	if exemplars == nil {
		return nil
	}

	result := make([]ExemplarRW2, 0, len(exemplars)) // TODO: Pool-ify this allocation?
	for _, ex := range exemplars {
		refs := make([]uint32, 0, len(ex.Labels)*2)
		for i := range ex.Labels {
			refs = append(refs, symbols.Symbolize(ex.Labels[i].Name), symbols.Symbolize(ex.Labels[i].Value))
		}

		exv2 := ExemplarRW2{
			LabelsRefs: refs,
			Value:      ex.Value,
			Timestamp:  ex.TimestampMs,
		}
		result = append(result, exv2)
	}
	return result
}

func FromMetricMetadataToMetadataRW2(metadata *MetricMetadata, symbols StringSymbolizer) MetadataRW2 {
	if metadata == nil {
		return MetadataRW2{}
	}

	result := MetadataRW2{
		Type: MetadataRW2_MetricType(metadata.Type),
	}
	result.HelpRef = symbols.Symbolize(metadata.Help)
	result.UnitRef = symbols.Symbolize(metadata.Unit)
	return result
}
