// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"errors"
	"flag"
	"time"
)

// errTrustCompactedBlocksBeforeIgnored is returned when compacted blocks would be trusted before the store-gateways
// load them.
var errTrustCompactedBlocksBeforeIgnored = errors.New("-querier.cardinality-estimate.trust-compacted-blocks-after must not be shorter than -blocks-storage.bucket-store.ignore-blocks-within, or a window counted from compacted blocks alone can need blocks the store-gateways don't load yet")

// CardinalityEstimateConfig configures the experimental
// /api/v1/cardinality/estimate route.
type CardinalityEstimateConfig struct {
	MaxGroups int64 `yaml:"max_groups" category:"experimental"`

	TrustCompactedBlocksAfter time.Duration `yaml:"trust_compacted_blocks_after" category:"experimental"`

	// Timeout is -querier.timeout, which bounds an estimate as it bounds a
	// query. It is set from the querier's config, not a flag of its own.
	Timeout time.Duration `yaml:"-"`
}

func (cfg *CardinalityEstimateConfig) RegisterFlags(f *flag.FlagSet) {
	f.DurationVar(&cfg.TrustCompactedBlocksAfter, "querier.cardinality-estimate.trust-compacted-blocks-after", 0, "A window that ended at least this long ago and that compacted blocks cover is counted from those blocks alone, without the ingesters, so a count for a past day can use the index-headers before -querier.query-store-after has passed. Data written into that range after it was compacted, such as late out-of-order samples, is not counted until it is compacted too. With ingest storage, a compacted block can miss a partition whose block-builder lags, and the count misses that partition's series without being marked as a lower bound. It must not be shorter than -blocks-storage.bucket-store.ignore-blocks-within. 0 disables it, so windows inside -querier.query-ingesters-within are always counted from the ingesters.")
	f.Int64Var(&cfg.MaxGroups, "querier.cardinality-estimate.max-groups", 10_000_000, "Maximum number of distinct values of the group_by label, metric names by default, in one cardinality estimate: each store-gateway applies it to its own answer, the ingesters to their whole head when an estimate falls back to it, and the querier to the answers merged. A request over it fails. 0 means no limit.")
}

// errNegativeMaxGroups is returned for a negative max-groups, which every
// store-gateway would otherwise refuse at request time.
var errNegativeMaxGroups = errors.New("-querier.cardinality-estimate.max-groups must be 0 or greater")

// Validate checks the config against the store-gateways'
// -blocks-storage.bucket-store.ignore-blocks-within, as this process sees it.
// A window counted from compacted blocks alone ended at least
// TrustCompactedBlocksAfter ago, so its blocks start before that; the
// store-gateways load them only once they start before ignore-blocks-within.
func (cfg *CardinalityEstimateConfig) Validate(ignoreBlocksWithin time.Duration) error {
	if cfg.MaxGroups < 0 {
		return errNegativeMaxGroups
	}
	if cfg.TrustCompactedBlocksAfter > 0 && ignoreBlocksWithin > 0 && cfg.TrustCompactedBlocksAfter < ignoreBlocksWithin {
		return errTrustCompactedBlocksBeforeIgnored
	}
	return nil
}
