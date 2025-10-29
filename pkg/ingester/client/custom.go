// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/client/custom.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package client

import (
	"fmt"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"

	"github.com/grafana/mimir/pkg/storage/chunk"
)

func ChunkFromMeta(meta chunks.Meta) (Chunk, error) {
	ch := Chunk{
		StartTimestampMs: meta.MinTime,
		EndTimestampMs:   meta.MaxTime,
		Data:             meta.Chunk.Bytes(),
	}

	switch meta.Chunk.Encoding() {
	case chunkenc.EncXOR:
		ch.Encoding = int32(chunk.PrometheusXorChunk)
	case chunkenc.EncHistogram:
		ch.Encoding = int32(chunk.PrometheusHistogramChunk)
	case chunkenc.EncFloatHistogram:
		ch.Encoding = int32(chunk.PrometheusFloatHistogramChunk)
	default:
		return Chunk{}, fmt.Errorf("unknown chunk encoding from TSDB chunk querier: %v", meta.Chunk.Encoding())
	}

	return ch, nil
}

// DefaultMetricsMetadataRequest initialises MetricsMetadataRequest with default values
// equivalent to no limits and no filtering.
func DefaultMetricsMetadataRequest() *MetricsMetadataRequest {
	return &MetricsMetadataRequest{Limit: -1, LimitPerMetric: -1, Metric: ""}
}
