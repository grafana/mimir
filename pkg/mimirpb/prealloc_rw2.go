// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"fmt"
	"sync"

	"github.com/prometheus/common/model"
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
	//nolint:staticcheck // SA6002: safe to ignore and actually fixing it has some performance penalty.
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
		Source:                          rw1.Source,
		SkipLabelValidation:             rw1.SkipLabelValidation,
		SkipLabelCountValidation:        rw1.SkipLabelCountValidation,
		skipUnmarshalingExemplars:       rw1.skipUnmarshalingExemplars,
		skipNormalizeMetadataMetricName: rw1.skipNormalizeMetadataMetricName,
		skipDeduplicateMetadata:         rw1.skipDeduplicateMetadata,
	}

	symbols := symbolsTableFromPool()
	defer reuseSymbolsTable(symbols)
	symbols.ConfigureCommonSymbols(offset, commonSymbols)

	// If our request has both metadata and timeseries, we can try to match.
	var metadataMap map[string]int // Metric name -> index of the first metadata, in the Metadata slice.
	var matchedMetadataMask []bool
	if len(rw1.Metadata) > 0 && len(rw1.Timeseries) > 0 {
		metadataMap = make(map[string]int, len(rw1.Metadata))
		matchedMetadataMask = make([]bool, len(rw1.Metadata))
		// Very rarely, a request might send multiple conflicting metadatas for the same metric.
		// For example, in Prometheus this can happen on the border of a metric churning,
		// if new and old versions of the same metadata are scraped from different processes and end up in the same batch.
		//
		// The Prometheus behavior for RW1.0 is to store *both* metadatas and not take any preference.
		// So, we only pick the first metadata matching a name as a candidate to be matched.
		// Any other metadata for the same name will be picked up later.
		for i := range rw1.Metadata {
			if seenIdx, ok := metadataMap[rw1.Metadata[i].MetricFamilyName]; !ok {
				metadataMap[rw1.Metadata[i].MetricFamilyName] = i
			} else {
				// If we see a duplicate metadata, skip sending it to save on some space.
				if rw1.Metadata[i].Equal(rw1.Metadata[seenIdx]) {
					matchedMetadataMask[i] = true
				}
			}
		}
	}

	expTimeseriesCount := len(rw1.Timeseries) + len(rw1.Metadata)
	rw2Timeseries := timeSeriesRW2SliceFromPool()
	if cap(rw2Timeseries) < expTimeseriesCount {
		rw2Timeseries = make([]TimeSeriesRW2, 0, expTimeseriesCount)
	}

	for _, ts := range rw1.Timeseries {
		metricName := ""
		const stringsPerLabel = 2
		expLabelsCount := len(ts.Labels) * stringsPerLabel
		refs := labelsRefsSliceFromPool()
		if cap(refs) < stringsPerLabel {
			refs = make([]uint32, 0, expLabelsCount)
		}

		for i := range ts.Labels {
			if ts.Labels[i].Name == model.MetricNameLabel {
				metricName = ts.Labels[i].Value
			}
			refs = append(refs, symbols.Symbolize(ts.Labels[i].Name), symbols.Symbolize(ts.Labels[i].Value))
		}

		var metadata MetadataRW2
		if len(metadataMap) > 0 {
			if rw1MetadataIdx, ok := metadataMap[metricName]; ok {
				metadata = FromMetricMetadataToMetadataRW2(rw1.Metadata[rw1MetadataIdx], symbols)
				matchedMetadataMask[rw1MetadataIdx] = true
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
	for i := range rw1.Metadata {
		// If we are matching metadata to series, and we already matched this metadata, we can skip it.
		if matchedMetadataMask != nil && matchedMetadataMask[i] {
			continue
		}

		labelsRefs := []uint32{symbols.Symbolize(model.MetricNameLabel), symbols.Symbolize(rw1.Metadata[i].MetricFamilyName)}
		rw2meta := FromMetricMetadataToMetadataRW2(rw1.Metadata[i], symbols)
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
