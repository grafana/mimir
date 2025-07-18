// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import "fmt"

func ReuseRW2(req *WriteRequest) {
	reuseSymbolsSlice(req.SymbolsRW2)
}

// FromWriteRequestToRW2Request converts a write request with RW1 fields populated to a write request with RW2 fields populated.
// It makes a new RW2 request, leaving the original request alone - it is still up to the caller to free the provided request.
func FromWriteRequestToRW2Request(rw1 *WriteRequest, commonSymbols []string, offset uint32) (*WriteRequest, error) {
	if rw1 == nil {
		return nil, nil
	}
	if len(rw1.SymbolsRW2) > 0 || len(rw1.TimeseriesRW2) > 0 {
		return nil, fmt.Errorf("the provided request is already rw2")
	}

	rw2 := &WriteRequest{
		Source:                    rw1.Source,
		SkipLabelValidation:       rw1.SkipLabelValidation,
		SkipLabelCountValidation:  rw1.SkipLabelCountValidation,
		skipUnmarshalingExemplars: rw1.skipUnmarshalingExemplars,
	}

	symbols := symbolsTableFromPool()
	defer reuseSymbolsTable(symbols)
	symbols.ConfigureCommonSymbols(offset, commonSymbols)

	rw2Timeseries := make([]TimeSeriesRW2, 0, len(rw1.Timeseries)+len(rw1.Metadata)) // TODO: Pool-ify this allocation
	for _, ts := range rw1.Timeseries {
		refs := make([]uint32, 0, len(ts.Labels)*2) // TODO: Pool-ify this allocation
		for i := range ts.Labels {
			refs = append(refs, symbols.Symbolize(ts.Labels[i].Name), symbols.Symbolize(ts.Labels[i].Value))
		}

		rw2Timeseries = append(rw2Timeseries, TimeSeriesRW2{
			LabelsRefs:       refs,
			Samples:          ts.Samples,
			Histograms:       ts.Histograms,
			Exemplars:        FromExemplarsToExemplarsRW2(ts.Exemplars, symbols),
			Metadata:         MetadataRW2{},
			CreatedTimestamp: ts.CreatedTimestamp,
		})
	}

	// Represent metadata as extra, empty timeseries rather than attaching it to existing series.
	// It prevents duplication, and removes the need to match metadata to series, with very low actual size overhead.
	for _, meta := range rw1.Metadata {
		rw2meta := FromMetricMetadataToMetadataRW2(meta, symbols)
		rw2Timeseries = append(rw2Timeseries, TimeSeriesRW2{
			LabelsRefs: []uint32{symbols.Symbolize("__name__"), symbols.Symbolize(meta.MetricFamilyName)},
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

	if metadata.Help != "" {
		result.HelpRef = symbols.Symbolize(metadata.Help)
	}
	if metadata.Unit != "" {
		result.UnitRef = symbols.Symbolize(metadata.Unit)
	}

	return result
}
