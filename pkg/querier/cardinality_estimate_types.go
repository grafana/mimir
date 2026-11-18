// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
)

// CardinalityEstimateRequest asks for series counts over [MinT, MaxT), see
// storegatewaypb.SeriesCountsRequest for the fields.
type CardinalityEstimateRequest struct {
	Matchers   []*labels.Matcher
	MinT, MaxT int64
	GroupBy    string
	// MaxIndexBytes caps the index bytes the whole request reads, 0 for none.
	MaxIndexBytes int64
	// MaxHashes caps the label-set hashes a deduplicating request may hold, 0 for none.
	MaxHashes int64
	// MaxGroups caps the distinct group_by values in the answer, 0 for none.
	MaxGroups int64
}

// CardinalityEstimateResult holds the count of each group, as its one entry.
type CardinalityEstimateResult struct {
	// Read is how the sources were read, one of the Read values.
	Read string
	// Source is where the counts come from, one of the Source values.
	Source string
	// CoverageGap is set when neither blocks nor ingesters hold part of the window.
	CoverageGap bool
	// HeadMinTime, when set, is when every ingester starts to hold data, after the window's start.
	HeadMinTime int64
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
	// IndexBytes is measured the way the store-gateways charge max_index_bytes.
	IndexBytes int64
	// PostingsOffsetsTableBytes is the postings offsets table the store-gateways walked.
	PostingsOffsetsTableBytes int64
	// BlocksFromPostings counts blocks answered from postings alone,
	// BlocksFromOffsetsTable from the offsets table alone.
	BlocksFromPostings     int64
	BlocksFromOffsetsTable int64
	// HashBytes is the dedup payload received.
	HashBytes int64
	// DedupHashes is the distinct label-set hashes held to deduplicate.
	DedupHashes int64
	// PeakMemoryBytes is the most the querier's memory tracker held for the estimate's groups, hashes and held replies.
	PeakMemoryBytes uint64
}

// setCoverage records which of the blocks in all were counted. It fails when none was.
func (r *CardinalityEstimateResult) setCoverage(all bucketindex.Blocks, cov estimateCoverage) error {
	missing := make(map[ulid.ULID]bool, len(cov.Missing))
	for _, id := range cov.Missing {
		missing[id] = true
	}
	for _, id := range cov.Awaited {
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
		r.missingReason = cov.Failure
	}
	r.MissingShards = missingShards(all)
	r.LowerBound = len(cov.Missing) > 0 || len(r.MissingShards) > 0
	r.StoreGateways = cov.StoreGateways
	r.Retries = cov.Retries
	return nil
}
