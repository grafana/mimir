// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/distributor/distributor.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package mimirpb

import (
	"flag"

	"github.com/prometheus/prometheus/model/labels"
)

type ShardingConfig struct {
	ExcludeClassicHistogramBucketLabel bool `yaml:"exclude_classic_histogram_bucket_label" category:"experimental"`
}

func (l *ShardingConfig) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&l.ExcludeClassicHistogramBucketLabel, "distributor.sharding.exclude-classic-histogram-bucket-label", false, "When set to true, the distributor computes the series hash – used for sharding – excluding the 'le' label. This means that, when this configuration option is set to true, all buckets of a classic histogram metric are stored in the same shard. This configuration option should be set for distributors, rulers and ingesters.")
}

// ShardByMetricName returns the token for the given metric. The provided metricName
// is guaranteed to not be retained.
func ShardByMetricName(userID string, metricName string) uint32 {
	h := ShardByUser(userID)
	h = HashAdd32(h, metricName)
	return h
}

func ShardByUser(userID string) uint32 {
	h := HashNew32()
	h = HashAdd32(h, userID)
	return h
}

// ShardBySeriesLabels returns the token that must be used to shard a series across ingesters / partitions for given user.
//
// ShardBySeriesLabels generates different values for different order of same labels.
func ShardBySeriesLabels(userID string, ls labels.Labels, cfg ShardingConfig) uint32 {
	h := ShardByUser(userID)
	ls.Range(func(l labels.Label) {
		if cfg.ExcludeClassicHistogramBucketLabel && l.Name == labels.BucketLabel {
			return
		}

		h = HashAdd32(h, l.Name)
		h = HashAdd32(h, l.Value)
	})
	return h
}

// ShardBySeriesLabelAdapters is like ShardByAllLabel, but uses LabelAdapter type.
func ShardBySeriesLabelAdapters(userID string, ls []LabelAdapter, cfg ShardingConfig) uint32 {
	h := ShardByUser(userID)
	for _, l := range ls {
		if cfg.ExcludeClassicHistogramBucketLabel && l.Name == labels.BucketLabel {
			continue
		}

		h = HashAdd32(h, l.Name)
		h = HashAdd32(h, l.Value)
	}
	return h
}
