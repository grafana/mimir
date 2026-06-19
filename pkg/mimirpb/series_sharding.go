// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/distributor/distributor.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package mimirpb

import (
	"github.com/prometheus/prometheus/model/labels"
)

// ShardByMetricName returns the token for the given metric. The provided metricName
// is guaranteed to not be retained.
func ShardByMetricName(userID string, metricName string) uint32 {
	return ShardByMetricNameWithSeed(ShardByUser(userID), metricName)
}

// ShardByMetricNameWithSeed is like ShardByMetricName, but takes a precomputed user
// seed (from ShardByUser) instead of the userID. Callers that compute tokens for many
// metrics of the same user should compute the seed once and reuse it to avoid
// re-hashing the userID for every metric.
func ShardByMetricNameWithSeed(seed uint32, metricName string) uint32 {
	return HashAdd32(seed, metricName)
}

func ShardByUser(userID string) uint32 {
	h := HashNew32()
	h = HashAdd32(h, userID)
	return h
}

// ShardByAllLabels returns the token for given user and series.
//
// ShardByAllLabels generates different values for different order of same labels.
func ShardByAllLabels(userID string, ls labels.Labels) uint32 {
	return ShardByAllLabelsWithSeed(ShardByUser(userID), ls)
}

// ShardByAllLabelsWithSeed is like ShardByAllLabels, but takes a precomputed user seed
// (from ShardByUser) instead of the userID. Callers that compute tokens for many series
// of the same user should compute the seed once and reuse it to avoid re-hashing the
// userID for every series.
func ShardByAllLabelsWithSeed(seed uint32, ls labels.Labels) uint32 {
	h := seed
	ls.Range(func(l labels.Label) {
		h = HashAdd32(h, l.Name)
		h = HashAdd32(h, l.Value)
	})
	return h
}

// ShardByAllLabelAdapters is like ShardByAllLabel, but uses LabelAdapter type.
func ShardByAllLabelAdapters(userID string, ls []LabelAdapter) uint32 {
	return ShardByAllLabelAdaptersWithSeed(ShardByUser(userID), ls)
}

// ShardByAllLabelAdaptersWithSeed is like ShardByAllLabelAdapters, but takes a
// precomputed user seed (from ShardByUser) instead of the userID. Callers that compute
// tokens for many series of the same user should compute the seed once and reuse it to
// avoid re-hashing the userID for every series.
func ShardByAllLabelAdaptersWithSeed(seed uint32, ls []LabelAdapter) uint32 {
	h := seed
	for _, l := range ls {
		h = HashAdd32(h, l.Name)
		h = HashAdd32(h, l.Value)
	}
	return h
}
