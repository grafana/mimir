// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/distributor/distributor.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package mimirpb

import (
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/mimirpb_custom"
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
func ShardByAllLabelAdapters(userID string, ls []mimirpb_custom.LabelAdapter) uint32 {
	h := ShardByUser(userID)
	for _, l := range ls {
		h = HashAdd32(h, l.Name)
		h = HashAdd32(h, l.Value)
	}
	return h
}
