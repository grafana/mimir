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
	// Window is each bucket's width, 0 for Step.
	Window  int64
	GroupBy string
	// MaxIndexBytes caps the index bytes the whole request reads, 0 for none.
	MaxIndexBytes int64
	// MaxHashes caps the label-set hashes a deduplicating request may hold, 0 for none.
	MaxHashes int64
	// MaxGroups caps the distinct group_by values in the answer, 0 for none.
	MaxGroups int64
	// SketchAbove sends an HLL sketch instead of hashes for groups with more series than this. 0 keeps every group exact.
	SketchAbove int64
	// Snap widens a window inside one block range to that range.
	Snap bool
	// StreamNames returns the groups in names instead of Counts. The caller must close it.
	StreamNames bool
}

// windowMs is the window_ms to send: 0 when the buckets are step wide.
func (r CardinalityEstimateRequest) windowMs() int64 {
	if r.Step == 0 || r.Window == r.Step {
		return 0
	}
	return r.Window
}

// expectedBuckets is how many counts each group carries, 0 without a step.
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
	// Read is how the sources were read, one of the Read values.
	Read string
	// Source is where the counts come from, one of the Source values.
	Source string
	// HeadWhole is set when the counts are every series in the ingesters' heads.
	HeadWhole bool
	// HeadFallback is why the head was counted as one bucket from its postings,
	// headFallbackErr is the refusal.
	HeadFallback    string
	headFallbackErr error
	// CoverageGap is set when neither blocks nor ingesters hold part of the window.
	CoverageGap bool
	// HeadMinTime, when set, is when every ingester starts to hold data, after the window's start.
	HeadMinTime int64
	Snapped     bool
	Counts      map[string][]int64
	// Dedup is set when the counts come from a union of label-set hashes.
	Dedup  bool
	Blocks []ulid.ULID
	// LowerBound is set when MissingBlocks or MissingShards left out series.
	LowerBound    bool
	MissingBlocks []ulid.ULID
	MissingShards []missingShard
	// missingReason is the last store-gateway failure, for the log line.
	missingReason error
	// Retries is the number of attempts after the first.
	Retries              int
	StoreGateways        int
	SeriesCounted        int64
	PostingsFetchedBytes int64
	SeriesFetchedBytes   int64
	// IndexBytes is measured the way the Series call measures it.
	IndexBytes int64
	// PostingsOffsetsTableBytes is the postings offsets table the store-gateways walked.
	PostingsOffsetsTableBytes int64
	// BlocksFromPostings counts blocks answered from postings alone,
	// BlocksFromOffsetsTable from the offsets table alone.
	BlocksFromPostings     int64
	BlocksFromOffsetsTable int64
	// Estimated holds the groups counted from an HLL sketch, not exactly.
	Estimated map[string]bool
	// HashBytes and SketchBytes are the dedup payload received.
	HashBytes, SketchBytes int64
	// DedupHashes is the distinct label-set hashes held to deduplicate.
	DedupHashes int64
	// names holds the counts instead of Counts, see StreamNames.
	names *nameTable
}

// setCoverage records which of the blocks in all were counted. It fails when none was.
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
