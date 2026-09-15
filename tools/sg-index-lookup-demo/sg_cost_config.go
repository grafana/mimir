// SPDX-License-Identifier: AGPL-3.0-only

package main

import "github.com/grafana/mimir/pkg/ingester/lookupplan"

// ingestCostConfig returns the default ingester cost config, for comparison.
func ingestCostConfig() lookupplan.CostConfig {
	return lookupplan.CostConfig{
		RetrievedPostingCost:              lookupplan.DefaultRetrievedPostingCost,
		RetrievedPostingListCost:          lookupplan.DefaultRetrievedPostingListCost,
		RetrievedSeriesCost:               lookupplan.DefaultRetrievedSeriesCost,
		MinSeriesPerBlockForQueryPlanning: 0,
		LabelCardinalityForLargerSketch:   lookupplan.DefaultLabelCardinalityForLargerSketch,
		LabelCardinalityForSmallerSketch:  lookupplan.DefaultLabelCardinalityForSmallerSketch,
	}
}

// sgCostConfig returns a CostConfig tuned for the store-gateway's cost profile.
//
// The ingester defaults model in-memory costs: a fixed overhead per posting list
// (RetrievedPostingListCost=10) and a cheap per-series load (RetrievedSeriesCost=15).
// Both are wrong for the store-gateway, where the bottleneck is bytes transferred
// from object storage, not per-list or per-series overhead.
//
// The two key corrections:
//
//  1. RetrievedPostingListCost — the ingester charges a flat fee per posting list
//     fetched (10 units × number of matched lists). In the store-gateway, posting lists
//     for a given label are laid out contiguously in the index file and the partitioner
//     coalesces them into a small number of range reads. The actual cost is proportional
//     to posting bytes transferred (cardinality × 4 bytes/ref), not number of lists.
//     Setting RetrievedPostingListCost to a per-byte equivalent: 4 bytes/ref ×
//     RetrievedPostingCost gives exactly cardinality × RetrievedPostingCost, so we
//     set RetrievedPostingListCost = 4 × RetrievedPostingCost.
//     But the existing formula multiplies by uniqueVals × selectivity, which
//     approximates cardinality (uniqueVals × selectivity ≈ fraction of values matched,
//     not series count). To make the per-byte model work within the existing formula
//     we set RetrievedPostingListCost low enough that per-list overhead is negligible
//     and the RetrievedPostingCost term (which does multiply by cardinality) dominates.
//
//  2. RetrievedSeriesCost — the ingester uses 15 units/series (cheap in-memory access).
//     In the store-gateway, loading a series entry from the index means a range read
//     from object storage. Series refs after intersection are scattered across the
//     512-byte-aligned series section; the partitioner uses a 17 KB window per ref,
//     so coalescing is poor compared to the dense posting section. Each series read
//     transfers ~512 bytes on average (EstimatedSeriesP99Size), with low coalescing
//     efficiency. We want this cost to dominate over posting fetch when numSelected
//     is large — and to be proportional to bytes transferred.
//
//     We express everything relative to RetrievedPostingCost=1 (one unit per posting
//     ref iterated). A posting ref is 4 bytes. A series entry is ~512 bytes and is
//     read with poor locality. The byte ratio is 512/4 = 128, with an additional
//     scatter penalty of ~4×, giving RetrievedSeriesCost ≈ 512.
//
// With these values the planner compares posting I/O bytes vs series I/O bytes:
//   - posting fetch:    cardinality × 4 bytes  (via RetrievedPostingCost, 4 bytes/ref)
//   - series fetch:     numSelected × 512 bytes (EstimatedSeriesP99Size, scattered reads)
//
// indexScanCost (matcher against offset table values) is in-memory work on the
// sparse index-header and is cheap relative to object-store I/O. It is zeroed
// out by setting singleMatchCost's contribution to near-zero via the per-byte
// framing — the existing formula still computes it but it is negligible compared
// to the I/O terms.
func sgCostConfig() lookupplan.CostConfig {
	return lookupplan.CostConfig{
		// 1 unit = 4 bytes (one posting ref). indexScanCost uses singleMatchCost
		// which is in absolute units; we accept it contributes a small amount.
		RetrievedPostingCost: 4.0,
		// Posting list fetch cost is already captured by RetrievedPostingCost × cardinality.
		// Per-list overhead (seeking, checksumming) is negligible thanks to coalescing.
		RetrievedPostingListCost: 0.0,
		// One series entry = ~512 bytes read from scattered locations in the index.
		// Coalescing is poor (17 KB window, refs are non-contiguous after intersection),
		// so the effective per-series I/O cost is high relative to posting bytes.
		RetrievedSeriesCost:               512.0,
		MinSeriesPerBlockForQueryPlanning: 0,
		LabelCardinalityForLargerSketch:   lookupplan.DefaultLabelCardinalityForLargerSketch,
		LabelCardinalityForSmallerSketch:  lookupplan.DefaultLabelCardinalityForSmallerSketch,
	}
}
