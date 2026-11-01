// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
)

// CardinalityEstimateRequest asks for series counts over [MinT, MaxT), see
// storegatewaypb.SeriesCountsRequest for the fields.
type CardinalityEstimateRequest struct {
	Matchers   []*labels.Matcher
	MinT, MaxT int64
	Step       int64
	// Window is each bucket's width with Step, 0 for Step; see
	// cardinality.Buckets. [MinT, MaxT) is then the data read, and the
	// first bucket ends at MinT+Window.
	Window  int64
	GroupBy string
	// MaxIndexBytes caps the index bytes the whole request reads, 0 for
	// none. It is split evenly across the store-gateway calls.
	MaxIndexBytes int64
	// MaxHashes caps the label-set hashes a deduplicating request may hold,
	// 0 for none. Each source gets a share and refuses past it. Estimate sets
	// it from the tenant's memory limit, at estimateHashBytes per hash.
	MaxHashes int64
	// MaxGroups caps the distinct group_by values the answer may hold, 0 for
	// none. Estimate sets it from the tenant's limit; each source applies it
	// to its own answer, and the querier to the answers merged.
	MaxGroups int64
	// SketchAbove has each store-gateway send an HLL sketch instead of
	// hashes for a group with more series than this, when deduplicating.
	// 0 keeps every group exact.
	SketchAbove int64
	// Snap widens a window inside one block range to that range.
	Snap bool
	// StreamNames has the groups of a request over one block range, for any
	// GroupBy, come back as an open table merged from the store-gateways'
	// SeriesCounts streams, in the result's names, instead of in Counts, so
	// the caller can merge it without holding it. The caller must close it.
	StreamNames bool
}

// windowMs is the window_ms to send: 0 when the buckets are the step wide,
// so a source that doesn't know the field counts them the same way.
func (r CardinalityEstimateRequest) windowMs() int64 {
	if r.Step == 0 || r.Window == r.Step {
		return 0
	}
	return r.Window
}

// expectedBuckets is how many counts each group of the answer carries: the
// buckets of the step and window, or 0 without a step. It fails when the step
// doesn't cut the window into whole buckets, which the handler doesn't check
// with snap, since a snapped window's buckets are known only once it is
// widened to a block range.
func (r CardinalityEstimateRequest) expectedBuckets() (int, error) {
	if r.Step <= 0 {
		return 0, nil
	}
	n, err := cardinality.Buckets(r.MinT, r.MaxT, r.Step, r.Window)
	if err != nil {
		return 0, badEstimateRequest("step: %v", err)
	}
	return n, nil
}

// CardinalityEstimateResult holds one count per bucket for each group.
type CardinalityEstimateResult struct {
	// Read is one of the Read values: how the sources were read.
	Read string
	// Source is one of the Source values: where the counts come from.
	Source string
	// HeadWhole is set when the counts are every series in the ingesters'
	// heads, since head postings can't be filtered by time.
	HeadWhole bool
	// HeadFallback, when set, is one of the HeadFallback values: why the
	// head walk was refused and the head was counted as one bucket from its
	// postings instead; headFallbackErr is that refusal, kept for the log
	// line and for what its status detail says.
	HeadFallback    string
	headFallbackErr error
	// CoverageGap is set when neither the blocks nor the ingesters hold part
	// of the window, so the counts are a lower bound.
	CoverageGap bool
	// HeadMinTime, when set, is the time from which every ingester still
	// holds data, after the window's start, so the counts are a lower bound.
	HeadMinTime int64
	Snapped     bool
	Counts      map[string][]int64
	// Dedup is set when the blocks may share series, so the counts come
	// from a union of label-set hashes rather than a sum.
	Dedup  bool
	Blocks []ulid.ULID
	// LowerBound is set when some blocks were not counted by any
	// store-gateway after every retry, which MissingBlocks lists, or when a
	// block range lacks some of its shard blocks, which MissingShards
	// lists. The counts then leave out those blocks' series.
	LowerBound    bool
	MissingBlocks []ulid.ULID
	MissingShards []missingShard
	// missingReason is the last store-gateway failure that left blocks
	// uncounted, for the request's log line; nil when MissingBlocks is empty
	// or no store-gateway failed.
	missingReason error
	// Retries is the number of attempts after the first.
	Retries              int
	StoreGateways        int
	SeriesCounted        int64
	PostingsFetchedBytes int64
	SeriesFetchedBytes   int64
	// IndexBytes is measured the way the Series call measures it.
	IndexBytes int64
	// PostingsOffsetsTableBytes is the postings offsets table the
	// store-gateways walked, which max_index_bytes bounds with IndexBytes.
	// With index-header version 2 it is read from object storage.
	PostingsOffsetsTableBytes int64
	// BlocksFromPostings is how many blocks were counted from the group-by
	// label's postings without reading series entries, and
	// BlocksFromOffsetsTable how many of those from the postings offsets
	// table alone, reading no postings either.
	BlocksFromPostings     int64
	BlocksFromOffsetsTable int64
	// Estimated holds the groups counted from an HLL sketch, not exactly.
	Estimated map[string]bool
	// HashBytes and SketchBytes are the dedup payload the store-gateways sent.
	HashBytes, SketchBytes int64
	// DedupHashes is the distinct label-set hashes the querier held to
	// deduplicate, the most of any bucket answered on its own.
	DedupHashes int64
	// names, when set, holds the counts instead of Counts: see
	// CardinalityEstimateRequest.StreamNames.
	names *nameTable
}

// setCoverage records which of the blocks in all were counted. A missing
// block, or a block range missing some of its shard blocks, makes the
// counts a lower bound; when no block was counted at all there is nothing
// to bound, and the request fails.
func (r *CardinalityEstimateResult) setCoverage(all bucketindex.Blocks, cov estimateCoverage) error {
	missing := make(map[ulid.ULID]bool, len(cov.Missing))
	for _, id := range cov.Missing {
		missing[id] = true
	}
	if len(all) > 0 && len(missing) == len(all) {
		return errNothingCounted(len(all), cov)
	}
	for _, b := range all {
		if !missing[b.ID] {
			r.Blocks = append(r.Blocks, b.ID)
		}
	}
	r.MissingBlocks = cov.Missing
	if len(cov.Missing) > 0 {
		r.missingReason = cov.Unavailable
	}
	r.MissingShards = missingShards(all)
	r.LowerBound = len(cov.Missing) > 0 || len(r.MissingShards) > 0
	r.StoreGateways = cov.StoreGateways
	r.Retries = cov.Retries
	return nil
}
