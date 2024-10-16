// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/client/custom.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package client

import (
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"google.golang.org/grpc/mem"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/chunk"
)

var _ mimirpb.BufferHolder = &QueryResponse{}

func (m *QueryResponse) SetBuffer(buf mem.Buffer) {
	m.buffer = buf
}

func (m *QueryResponse) FreeBuffer() {
	if m.buffer != nil {
		m.buffer.Free()
		m.buffer = nil
	}
}

var _ mimirpb.BufferHolder = &QueryStreamResponse{}

func (m *QueryStreamResponse) SetBuffer(buf mem.Buffer) {
	m.buffer = buf
}

func (m *QueryStreamResponse) FreeBuffer() {
	if m.buffer != nil {
		m.buffer.Free()
		m.buffer = nil
	}
}

var _ mimirpb.BufferHolder = &ExemplarQueryResponse{}

func (m *ExemplarQueryResponse) SetBuffer(buf mem.Buffer) {
	m.buffer = buf
}

func (m *ExemplarQueryResponse) FreeBuffer() {
	if m.buffer != nil {
		m.buffer.Free()
		m.buffer = nil
	}
}

func ChunksCount(series []TimeSeriesChunk) int {
	if len(series) == 0 {
		return 0
	}

	count := 0
	for _, entry := range series {
		count += len(entry.Chunks)
	}
	return count
}

func ChunksSize(series []TimeSeriesChunk) int {
	if len(series) == 0 {
		return 0
	}

	size := 0
	for _, entry := range series {
		for _, chunk := range entry.Chunks {
			size += chunk.Size()
		}
	}
	return size
}

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
		return Chunk{}, errors.Errorf("unknown chunk encoding from TSDB chunk querier: %v", meta.Chunk.Encoding())
	}

	return ch, nil
}

// DefaultMetricsMetadataRequest initialises MetricsMetadataRequest with default values
// equivalent to no limits and no filtering.
func DefaultMetricsMetadataRequest() *MetricsMetadataRequest {
	return &MetricsMetadataRequest{Limit: -1, LimitPerMetric: -1, Metric: ""}
}

var _ mimirpb.BufferHolder = &MetricsForLabelMatchersResponse{}

func (m *MetricsForLabelMatchersResponse) SetBuffer(buf mem.Buffer) {
	m.buffer = buf
}

func (m *MetricsForLabelMatchersResponse) FreeBuffer() {
	if m.buffer != nil {
		m.buffer.Free()
		m.buffer = nil
	}
}

var _ mimirpb.BufferHolder = &ActiveSeriesResponse{}

func (m *ActiveSeriesResponse) SetBuffer(buf mem.Buffer) {
	m.buffer = buf
}

func (m *ActiveSeriesResponse) FreeBuffer() {
	if m.buffer != nil {
		m.buffer.Free()
		m.buffer = nil
	}
}
