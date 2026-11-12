// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/tsdb/util.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package tsdb

import (
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/histogram"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// HashBlockID returns a 32-bit hash of the block ID useful for
// ring-based sharding.
func HashBlockID(id ulid.ULID) uint32 {
	h := mimirpb.HashNew32()
	for _, b := range id {
		h = mimirpb.HashAddByte32(h, b)
	}
	return h
}

// ZeroHistogramFor returns the zero sample to append for a created timestamp. It matches the type and layout of the
// given histogram so that appending it doesn't cut a new chunk.
func ZeroHistogramFor(h *histogram.Histogram, fh *histogram.FloatHistogram) (*histogram.Histogram, *histogram.FloatHistogram) {
	if fh != nil {
		return nil, &histogram.FloatHistogram{
			CounterResetHint: histogram.CounterReset,
			Schema:           fh.Schema,
			ZeroThreshold:    fh.ZeroThreshold,
			CustomValues:     fh.CustomValues,
		}
	}
	return &histogram.Histogram{
		CounterResetHint: histogram.CounterReset,
		Schema:           h.Schema,
		ZeroThreshold:    h.ZeroThreshold,
		CustomValues:     h.CustomValues,
	}, nil
}
