// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
)

// mergeRequests coalesces multiple WriteRequests destined for the same (partition, tenant) into
// a single RW2 request with a unified symbol table.
//
// Input requests may be in RW1 (classic Timeseries) or RW2 (TimeseriesRW2) format. They are
// converted to RW2 as needed, then all per-request symbol tables are merged into one, and every
// symbol reference is remapped to the unified table.
//
// The caller retains ownership of the input requests; mergeRequests may mutate the LabelsRefs
// slices of any RW2 timeseries it encounters (remapping them in-place).
func mergeRequests(reqs []*mimirpb.WriteRequest) (*mimirpb.WriteRequest, error) {
	if len(reqs) == 0 {
		return &mimirpb.WriteRequest{}, nil
	}

	const offset = uint32(ingest.V2RecordSymbolOffset)

	// Convert all requests to RW2.
	rw2Reqs := make([]*mimirpb.WriteRequest, 0, len(reqs))
	for _, req := range reqs {
		if len(req.TimeseriesRW2) > 0 || len(req.SymbolsRW2) > 0 {
			// Already RW2; use as-is.
			rw2Reqs = append(rw2Reqs, req)
			continue
		}
		rw2, err := mimirpb.FromWriteRequestToRW2Request(req, ingest.V2CommonSymbols, offset)
		if err != nil {
			return nil, err
		}
		rw2Reqs = append(rw2Reqs, rw2)
	}

	// Fast path: single request, already in RW2 — return it directly.
	if len(rw2Reqs) == 1 {
		return rw2Reqs[0], nil
	}

	// Build a unified symbol table seeded with the same common symbols that the ingest writer
	// uses, so that common-symbol refs pass through unchanged.
	unified := mimirpb.NewFastSymbolsTable(256)
	unified.ConfigureCommonSymbols(offset, ingest.V2CommonSymbols)

	// Pre-size the output timeseries slice.
	totalTS := 0
	for _, r := range rw2Reqs {
		totalTS += len(r.TimeseriesRW2)
	}
	allTimeseries := make([]mimirpb.TimeSeriesRW2, 0, totalTS)

	for _, rw2 := range rw2Reqs {
		// Build a remap table that translates every ref in this request to a ref in the
		// unified table. Size covers 0..offset+len(SymbolsRW2)-1.
		remapLen := int(offset) + len(rw2.SymbolsRW2)
		remap := make([]uint32, remapLen)

		// Refs 0..offset-1 are common-symbol refs. Since the unified table uses the same
		// common symbols, they are identity-mapped.
		for r := uint32(1); r < offset; r++ {
			remap[r] = r
		}

		// Refs offset..offset+len(SymbolsRW2)-1 are per-request symbols.
		for k, sym := range rw2.SymbolsRW2 {
			remap[uint32(k)+offset] = unified.Symbolize(sym)
		}

		// Remap all symbol references in every timeseries and append to the output.
		for i := range rw2.TimeseriesRW2 {
			ts := &rw2.TimeseriesRW2[i]

			remapRefs(ts.LabelsRefs, remap)

			for j := range ts.Exemplars {
				remapRefs(ts.Exemplars[j].LabelsRefs, remap)
			}

			if ts.Metadata.HelpRef > 0 && int(ts.Metadata.HelpRef) < remapLen {
				ts.Metadata.HelpRef = remap[ts.Metadata.HelpRef]
			}
			if ts.Metadata.UnitRef > 0 && int(ts.Metadata.UnitRef) < remapLen {
				ts.Metadata.UnitRef = remap[ts.Metadata.UnitRef]
			}

			allTimeseries = append(allTimeseries, *ts)
		}
	}

	return &mimirpb.WriteRequest{
		SymbolsRW2:    unified.Symbols(),
		TimeseriesRW2: allTimeseries,
	}, nil
}

// remapRefs rewrites every element of refs in-place using remap.
// Elements whose value exceeds len(remap) are left unchanged (defensive).
func remapRefs(refs []uint32, remap []uint32) {
	for i, r := range refs {
		if int(r) < len(remap) {
			refs[i] = remap[r]
		}
	}
}
