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
	h := ShardByUser(userID)
	h = HashAdd32(h, metricName)
	return h
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
	h := ShardByUser(userID)
	ls.Range(func(l labels.Label) {
		h = HashAdd32(h, l.Name)
		h = HashAdd32(h, l.Value)
	})
	return h
}

// ShardByAllLabelAdapters is like ShardByAllLabel, but uses LabelAdapter type.
func ShardByAllLabelAdapters(userID string, ls []LabelAdapter) uint32 {
	h := ShardByUser(userID)
	for _, l := range ls {
		h = HashAdd32(h, l.Name)
		h = HashAdd32(h, l.Value)
	}
	return h
}

const (
	// MetricNameMask selects the top 16 bits of a 32-bit hash, used for metric name locality.
	MetricNameMask = uint32(0xFFFF0000)
	// LabelBitsMask selects the bottom 16 bits of a 32-bit hash, used for label differentiation.
	LabelBitsMask = uint32(0x0000FFFF)
)

// ShardByMetricNameLocality produces a hash where the most significant 16 bits encode
// the metric name (via ShardByMetricName) and the least significant 16 bits encode
// the full label set (via ShardByAllLabelAdapters). This ensures that all series for
// a given metric name hash to a contiguous range of ring tokens, enabling query-time
// partition pruning by metric name.
func ShardByMetricNameLocality(userID string, metricName string, ls []LabelAdapter) uint32 {
	metricHash := ShardByMetricName(userID, metricName)
	labelsHash := ShardByAllLabelAdapters(userID, ls)
	return (metricHash & MetricNameMask) | (labelsHash & LabelBitsMask)
}

// MetricNameHashRange returns the inclusive hash range [lo, hi] that covers all possible
// series hashes for a given user and metric name under locality-aware sharding.
func MetricNameHashRange(userID string, metricName string) (lo, hi uint32) {
	h := ShardByMetricName(userID, metricName)
	lo = h & MetricNameMask
	hi = lo | LabelBitsMask
	return
}
