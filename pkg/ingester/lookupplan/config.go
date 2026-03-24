// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"flag"
	"fmt"
)

const (
	DefaultRetrievedPostingCost              = 0.01
	DefaultRetrievedSeriesCost               = 15.0
	DefaultRetrievedPostingListCost          = 10.0
	DefaultMinSeriesPerBlockForQueryPlanning = 10_000
	DefaultLabelCardinalityForLargerSketch   = 1e6
	DefaultLabelCardinalityForSmallerSketch  = 1e3
)

var defaultCostConfig = CostConfig{
	RetrievedPostingCost:              DefaultRetrievedPostingCost,
	RetrievedSeriesCost:               DefaultRetrievedSeriesCost,
	RetrievedPostingListCost:          DefaultRetrievedPostingListCost,
	MinSeriesPerBlockForQueryPlanning: DefaultMinSeriesPerBlockForQueryPlanning,
}

type CostConfig struct {
	// RetrievedSeriesCost accounts for iterating postings that have been retrieved form the index.
	RetrievedPostingCost float64 `yaml:"retrieved_posting_cost" category:"advanced"`

	// RetrievedSeriesCost  accounts for retrieving series from the index and checking if a series belongs to the query's shard.
	// This is much cheaper for the head block, but for blocks on disk, we need to read from disk and sometimes do hashing on the hot path.
	// For comparison, you can see that vendor/github.com/prometheus/prometheus/model/labels/cost.go, estimatedStringEqualityCost=1.1.
	RetrievedSeriesCost float64 `yaml:"retrieved_series_cost" category:"advanced"`

	// RetrievedPostingListCost accounts for the cost of retrieving the posting list from disk or from memory.
	RetrievedPostingListCost float64 `yaml:"retrieved_posting_list_cost" category:"advanced"`

	// MinSeriesPerBlockForQueryPlanning is the minimum number of series a block must have for query planning to be used.
	MinSeriesPerBlockForQueryPlanning uint64 `yaml:"min_series_per_block_for_query_planning" category:"advanced"`

	// LabelCardinalityForLargerSketch is the number of series with a label for that label name to be allocated a larger count-min sketch
	LabelCardinalityForLargerSketch uint64 `yaml:"label_cardinality_for_larger_sketch" category:"advanced"`

	// LabelCardinalityForSmallerSketch is the number of series with a label for that label name to be allocated a smaller count-min sketch.
	LabelCardinalityForSmallerSketch uint64 `yaml:"label_cardinality_for_smaller_sketch" category:"advanced"`
}

func (cfg *CostConfig) RegisterFlags(f *flag.FlagSet, prefix string) {
	f.Float64Var(&cfg.RetrievedPostingCost, prefix+"retrieved-posting-cost", DefaultRetrievedPostingCost, "Cost for iterating postings that have been retrieved from the index.")
	f.Float64Var(&cfg.RetrievedSeriesCost, prefix+"retrieved-series-cost", DefaultRetrievedSeriesCost, "Cost for retrieving series from the index and checking if a series belongs to the query's shard.")
	f.Float64Var(&cfg.RetrievedPostingListCost, prefix+"retrieved-posting-list-cost", DefaultRetrievedPostingListCost, "Cost for retrieving the posting list from disk or from memory.")
	f.Uint64Var(&cfg.MinSeriesPerBlockForQueryPlanning, prefix+"min-series-per-block-for-query-planning", DefaultMinSeriesPerBlockForQueryPlanning, "Minimum number of series a block must have for query planning to be used.")
	f.Uint64Var(&cfg.LabelCardinalityForLargerSketch, prefix+"label-cardinality-for-larger-sketch", DefaultLabelCardinalityForLargerSketch, "Number of series for a label name above which larger count-min sketches are used for that label.")
	f.Uint64Var(&cfg.LabelCardinalityForSmallerSketch, prefix+"label-cardinality-for-smaller-sketch", DefaultLabelCardinalityForSmallerSketch, "Number of series for a label name above which smaller count-min sketches are used for that label.")
}

func (cfg *CostConfig) Validate() error {
	if cfg.LabelCardinalityForSmallerSketch > cfg.LabelCardinalityForLargerSketch {
		return fmt.Errorf("cardinality limit for smaller sketches cannot be larger than cardinality limit for larger sketches")
	}
	return nil
}
