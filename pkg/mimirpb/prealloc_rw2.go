// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"fmt"
	"sync"

	"github.com/prometheus/prometheus/util/zeropool"
)

const (
	minPreallocatedTimeseriesRW2 = 100
	maxPreallocatedTimeseriesRW2 = 10000
	minPreallocatedLabelsRefs    = 20
	maxPreallocatedLabelsRefs    = 200
)

var (
	preallocTimeseriesRW2SlicePool = sync.Pool{
		New: func() interface{} {
			return make([]TimeSeriesRW2, 0, minPreallocatedTimeseriesRW2)
		},
	}

	// preallocLabelsRefsSlicePool pools a `*[]uint32` under the hood.
	// Zeropool is a thin wrapper that encapsulates the extra pointer ref/deref for us.
	// We can't just pool a []uint32 directly, as Go seems to try too hard to optimize around it and injects unexpected copies/allocations,
	// in a way we don't see with slices of less primitive types.
	preallocLabelsRefsSlicePool = zeropool.New(func() []uint32 {
		return make([]uint32, 0, minPreallocatedLabelsRefs)
	})
)

func labelsRefsSliceFromPool() []uint32 {
	return preallocLabelsRefsSlicePool.Get()
}

func reuseLabelsRefsSlice(s []uint32) {
	if cap(s) == 0 {
		return
	}

	if cap(s) > maxPreallocatedLabelsRefs {
		return
	}

	for i := range s {
		s[i] = 0
	}
	preallocLabelsRefsSlicePool.Put(s[:0])
}

func timeSeriesRW2SliceFromPool() []TimeSeriesRW2 {
	return preallocTimeseriesRW2SlicePool.Get().([]TimeSeriesRW2)
}

func reuseTimeSeriesRW2Slice(s []TimeSeriesRW2) {
	if cap(s) == 0 {
		return
	}

	if cap(s) > maxPreallocatedTimeseriesRW2 {
		return
	}

	for i := range s {
		reuseLabelsRefsSlice(s[i].LabelsRefs)
		s[i] = TimeSeriesRW2{}
	}
	preallocTimeseriesRW2SlicePool.Put(s[:0])
}

func ReuseRW2(req *WriteRequest) {
	reuseSymbolsSlice(req.SymbolsRW2)
	reuseTimeSeriesRW2Slice(req.TimeseriesRW2)
}

// FromWriteRequestToRW2Request converts a write request with RW1 fields populated to a write request with RW2 fields populated.
// It makes a new RW2 request, leaving the original request alone - it is still up to the caller to free the provided request.
// It might retain references in the RW1 request. It's not safe to free the RW1 request until the RW2 request is no longer used.
func FromWriteRequestToRW2Request(rw1 *WriteRequest, commonSymbols *CommonSymbols, offset uint32) (*WriteRequest, error) {
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

	// rw2Timeseries := make([]TimeSeriesRW2, 0, len(rw1.Timeseries)+len(rw1.Metadata)) // TODO: Pool-ify this allocation
	expTimeseriesCount := len(rw1.Timeseries) + len(rw1.Metadata)
	rw2Timeseries := timeSeriesRW2SliceFromPool()
	if cap(rw2Timeseries) < expTimeseriesCount {
		rw2Timeseries = make([]TimeSeriesRW2, 0, expTimeseriesCount)
	}

	for _, ts := range rw1.Timeseries {
		const stringsPerLabel = 2
		expLabelsCount := len(ts.Labels) * stringsPerLabel
		refs := labelsRefsSliceFromPool()
		if cap(refs) < stringsPerLabel {
			refs = make([]uint32, 0, expLabelsCount)
		}

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
	// We never replicate metadata if there are multiple series with the same name, and it removes the requirement to match up the metadata to the right series.
	for _, meta := range rw1.Metadata {
		labelsRefs := []uint32{symbols.Symbolize("__name__"), symbols.Symbolize(meta.MetricFamilyName)}
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
