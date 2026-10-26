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
	CompareEnabled bool  `yaml:"compare_enabled" category:"experimental"`
	MaxDedupSeries int64 `yaml:"max_dedup_series" category:"experimental"`
	MaxMetricNames int64 `yaml:"max_metric_names" category:"experimental"`

	TrustCompactedBlocksAfter time.Duration `yaml:"trust_compacted_blocks_after" category:"experimental"`
}

func (cfg *CardinalityEstimateConfig) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.CompareEnabled, "querier.cardinality-estimate.compare-enabled", false, "If set to true, the experimental cardinality estimate route accepts compare=true, which also answers the request by loading chunks through the store-gateways, like a PromQL count, and reports whether the two agree. The comparison runs with the query limits of the request context only, so it is meant for testing, not for tenants.")
	f.Int64Var(&cfg.MaxDedupSeries, "querier.cardinality-estimate.max-dedup-series", 10_000_000, "Maximum number of series hashes a cardinality estimate may hold in the querier while deduplicating across block ranges, 8 bytes each. A request over it fails. 0 means no limit.")
	f.DurationVar(&cfg.TrustCompactedBlocksAfter, "querier.cardinality-estimate.trust-compacted-blocks-after", 0, "A window that ended at least this long ago and that compacted blocks cover is counted from those blocks alone, without the ingesters, so a count for a past day can use the index-headers before -querier.query-store-after has passed. Data written into that range after it was compacted, such as late out-of-order samples, is not counted until it is compacted too. With ingest storage, a compacted block can miss a partition whose block-builder lags, and the count misses that partition's series without being marked as a lower bound. It must not be shorter than -blocks-storage.bucket-store.ignore-blocks-within. 0 disables it, so windows inside -querier.query-ingesters-within are always counted from the ingesters.")
	f.Int64Var(&cfg.MaxMetricNames, "querier.cardinality-estimate.max-metric-names", 10_000_000, "Maximum number of distinct metric names a store-gateway may return for one cardinality estimate read from the index-headers. A request over it fails. 0 means no limit.")
}

// Validate checks the config against the store-gateways'
// -blocks-storage.bucket-store.ignore-blocks-within, as this process sees it.
// A window counted from compacted blocks alone ended at least
// TrustCompactedBlocksAfter ago, so its blocks start before that; the
// store-gateways load them only once they start before ignore-blocks-within.
func (cfg *CardinalityEstimateConfig) Validate(ignoreBlocksWithin time.Duration) error {
	if cfg.TrustCompactedBlocksAfter > 0 && ignoreBlocksWithin > 0 && cfg.TrustCompactedBlocksAfter < ignoreBlocksWithin {
		return errTrustCompactedBlocksBeforeIgnored
	}
	return nil
}
