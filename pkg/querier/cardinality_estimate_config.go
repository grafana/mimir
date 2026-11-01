// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"errors"
	"flag"
	"time"
)

// errTrustCompactedBlocksBeforeIgnored is returned when compacted blocks would be trusted before the store-gateways load them.
var errTrustCompactedBlocksBeforeIgnored = errors.New("-querier.cardinality-estimate.trust-compacted-blocks-after must not be shorter than -blocks-storage.bucket-store.ignore-blocks-within, or a window counted from compacted blocks alone can need blocks the store-gateways don't load yet")

// CardinalityEstimateConfig configures the experimental /api/v1/cardinality/estimate route.
type CardinalityEstimateConfig struct {
	TrustCompactedBlocksAfter time.Duration `yaml:"trust_compacted_blocks_after" category:"experimental"`

	// Timeout is -querier.timeout. It is set from the querier's config.
	Timeout time.Duration `yaml:"-"`
}

func (cfg *CardinalityEstimateConfig) RegisterFlags(f *flag.FlagSet) {
	f.DurationVar(&cfg.TrustCompactedBlocksAfter, "querier.cardinality-estimate.trust-compacted-blocks-after", 0, "A window that ended at least this long ago and that compacted blocks cover is counted from those blocks alone, without the ingesters, so a count for a past day can use the index-headers before -querier.query-store-after has passed. Data written into that range after it was compacted, such as late out-of-order samples, is not counted until it is compacted too. With ingest storage, a compacted block can miss a partition whose block-builder lags, and the count misses that partition's series without being marked as a lower bound. It must not be shorter than -blocks-storage.bucket-store.ignore-blocks-within. 0 disables it, so windows inside -querier.query-ingesters-within are always counted from the ingesters.")
}

// Validate checks the config against -blocks-storage.bucket-store.ignore-blocks-within.
func (cfg *CardinalityEstimateConfig) Validate(ignoreBlocksWithin time.Duration) error {
	if cfg.TrustCompactedBlocksAfter > 0 && ignoreBlocksWithin > 0 && cfg.TrustCompactedBlocksAfter < ignoreBlocksWithin {
		return errTrustCompactedBlocksBeforeIgnored
	}
	return nil
}
