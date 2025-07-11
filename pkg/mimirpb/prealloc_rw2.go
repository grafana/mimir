package mimirpb

import "fmt"

// FromWriteRequestToRW2Request converts a write request with RW1 fields populated to a write request with RW2 fields populated.
// TODO: It destroys the given write request?
func FromWriteRequestToRW2Request(rw1 *PreallocWriteRequest) (*PreallocWriteRequest, error) {
	// Source - automatic
	if rw1 == nil {
		return nil, nil
	}
	if len(rw1.SymbolsRW2) > 0 || len(rw1.TimeseriesRW2) > 0 {
		return nil, fmt.Errorf("the provided request is already rw2")
	}
	symbols := symbolsTableFromPool()
	defer reuseSymbolsTable(symbols) // TODO: is this safe because we leak the symbols slice by returning it? but this puts it back in a pool too early?

	rw2Timeseries := make([]TimeSeriesRW2, 0, len(rw1.Timeseries)) // TODO: Pool-ify this allocation
	for _, ts := range rw1.Timeseries {
		refs := make([]uint32, 0, len(ts.Labels)*2) // TODO: Pool-ify this allocation
		for i := range ts.Labels {
			refs = append(refs, symbols.Symbolize(ts.Labels[i].Name), symbols.Symbolize(ts.Labels[i].Value))
		}

		rw2Timeseries = append(rw2Timeseries, TimeSeriesRW2{
			LabelsRefs: refs,
			Samples:    ts.Samples,
			Histograms: ts.Histograms,
			Exemplars:  FromExemplarsToExemplarsV2(ts.Exemplars, symbols),
			// TODO: Metadata
			CreatedTimestamp: ts.CreatedTimestamp,
		})
	}

	// TODO: Metadata
	rw1.Timeseries = nil // TODO: return to pool
	rw1.TimeseriesRW2 = rw2Timeseries
	rw1.SymbolsRW2 = symbols.Symbols() // TODO: I think we leak this because reuse puts it back in a pool but we dont want to

	rw1.UnmarshalFromRW2 = true
	// TODO: Common symbols not yet supported.
	rw1.RW2SymbolOffset = 0
	rw1.RW2CommonSymbols = nil

	return rw1, nil
}

func FromExemplarsToExemplarsV2(exemplars []Exemplar, symbols StringSymbolizer) []ExemplarRW2 {
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
