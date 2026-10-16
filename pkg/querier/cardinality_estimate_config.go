// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"flag"
	"time"
)

// CardinalityEstimateConfig configures the experimental
// /api/v1/cardinality/estimate route.
type CardinalityEstimateConfig struct {
	Enabled        bool  `yaml:"enabled" category:"experimental"`
	CompareEnabled bool  `yaml:"compare_enabled" category:"experimental"`
	MaxDedupSeries int64 `yaml:"max_dedup_series" category:"experimental"`
	MaxMetricNames int64 `yaml:"max_metric_names" category:"experimental"`

	TrustCompactedBlocksAfter time.Duration `yaml:"trust_compacted_blocks_after" category:"experimental"`
}

func (cfg *CardinalityEstimateConfig) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, "querier.cardinality-estimate.enabled", false, "If set to true, enables the experimental /api/v1/cardinality/estimate route, which counts series per label value from the store-gateways' index without reading chunks.")
	f.BoolVar(&cfg.CompareEnabled, "querier.cardinality-estimate.compare-enabled", false, "If set to true, the experimental cardinality estimate route accepts compare=true, which also answers the request by loading chunks through the store-gateways, like a PromQL count, and reports whether the two agree. The comparison runs with the query limits of the request context only, so it is meant for testing, not for tenants.")
	f.Int64Var(&cfg.MaxDedupSeries, "querier.cardinality-estimate.max-dedup-series", 10_000_000, "Maximum number of series hashes a cardinality estimate may hold in the querier while deduplicating across block ranges, 8 bytes each. A request over it fails. 0 means no limit.")
	f.DurationVar(&cfg.TrustCompactedBlocksAfter, "querier.cardinality-estimate.trust-compacted-blocks-after", time.Hour, "A window that ended at least this long ago and that compacted blocks cover is counted from those blocks alone, without the ingesters, so a count for a past day can use the index-headers before -querier.query-store-after has passed. Data written into that range after it was compacted, such as late out-of-order samples, is not counted until it is compacted too. 0 disables it, and windows inside -querier.query-ingesters-within always ask the ingesters.")
	f.Int64Var(&cfg.MaxMetricNames, "querier.cardinality-estimate.max-metric-names", 10_000_000, "Maximum number of distinct metric names a store-gateway may return for one cardinality estimate read from the index-headers. A request over it fails. 0 means no limit.")
}
