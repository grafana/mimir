// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/client/custom.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package client

import (
	"fmt"
	io "io"
	"slices"
	"sync"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/chunk"
)

var (
	chunkDataPool = sync.Pool{
		New: func() any {
			return mimirpb.UnsafeByteSlice{}
		},
	}
	timeSeriesChunkPool = sync.Pool{
		New: func() any {
			return &TimeSeriesChunk{}
		},
	}
	queryStreamSeriesPool = sync.Pool{
		New: func() any {
			return &QueryStreamSeries{}
		},
	}
	queryStreamSeriesChunksPool = sync.Pool{
		New: func() any {
			return &QueryStreamSeriesChunks{}
		},
	}
)

func ChunksCount(series []CustomTimeSeriesChunk) int {
	if len(series) == 0 {
		return 0
	}

	count := 0
	for _, entry := range series {
		count += len(entry.Chunks)
	}
	return count
}

func ChunksSize(series []CustomTimeSeriesChunk) int {
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

type CustomTimeSeriesChunk struct {
	*TimeSeriesChunk
}

// Release back to pool.
func (m *CustomTimeSeriesChunk) Release() {
	for _, chk := range m.Chunks {
		//nolint:staticcheck
		chunkDataPool.Put(chk.Data)
		chk.Data = nil
	}
	m.Labels = m.Labels[:0]
	m.Chunks = m.Chunks[:0]
	timeSeriesChunkPool.Put(m.TimeSeriesChunk)
	m.TimeSeriesChunk = nil
}

func (m *CustomTimeSeriesChunk) Unmarshal(data []byte) error {
	m.TimeSeriesChunk = timeSeriesChunkPool.Get().(*TimeSeriesChunk)
	m.Labels = m.Labels[:0]
	m.Chunks = m.Chunks[:0]

	l := len(data)
	index := 0
	for index < l {
		preIndex := index
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowIngester
			}
			if index >= l {
				return io.ErrUnexpectedEOF
			}
			b := data[index]
			index++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: TimeSeriesChunk: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: TimeSeriesChunk: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field FromIngesterId", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowIngester
				}
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthIngester
			}
			postIndex := index + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthIngester
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.FromIngesterId = string(data[index:postIndex])
			index = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field UserId", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowIngester
				}
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthIngester
			}
			postIndex := index + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthIngester
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.UserId = string(data[index:postIndex])
			index = postIndex
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Labels", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowIngester
				}
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthIngester
			}
			postIndex := index + msglen
			if postIndex < 0 {
				return ErrInvalidLengthIngester
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}

			var la mimirpb.LabelAdapter
			if err := unmarshalLabelAdapter(&la, data[index:postIndex]); err != nil {
				return err
			}
			m.Labels = append(m.Labels, la)
			index = postIndex
		case 4:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Chunks", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowIngester
				}
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthIngester
			}
			postIndex := index + msglen
			if postIndex < 0 {
				return ErrInvalidLengthIngester
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			var chk Chunk
			if err := unmarshalChunk(&chk, data[index:postIndex]); err != nil {
				return err
			}
			m.Chunks = append(m.Chunks, chk)
			if err := m.Chunks[len(m.Chunks)-1].Unmarshal(data[index:postIndex]); err != nil {
				return err
			}
			index = postIndex
		default:
			index = preIndex
			skippy, err := skipIngester(data[index:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthIngester
			}
			if (index + skippy) < 0 {
				return ErrInvalidLengthIngester
			}
			if (index + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			index += skippy
		}
	}

	if index > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func unmarshalLabelAdapter(la *mimirpb.LabelAdapter, data []byte) error {
	l := len(data)
	index := 0
	for index < l {
		preIndex := index
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return mimirpb.ErrIntOverflowMimir
			}
			if index >= l {
				return io.ErrUnexpectedEOF
			}
			b := data[index]
			index++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: LabelPair: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: LabelPair: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Name", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return mimirpb.ErrIntOverflowMimir
				}
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return mimirpb.ErrInvalidLengthMimir
			}
			postIndex := index + byteLen
			if postIndex < 0 {
				return mimirpb.ErrInvalidLengthMimir
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			// TODO: Consider using a pool: Get byte slice from pool, copy the data to it, and take a yoloString.
			la.Name = string(data[index:postIndex])
			index = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Value", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return mimirpb.ErrIntOverflowMimir
				}
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return mimirpb.ErrInvalidLengthMimir
			}
			postIndex := index + byteLen
			if postIndex < 0 {
				return mimirpb.ErrInvalidLengthMimir
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			// TODO: Consider using a pool: Get byte slice from pool, copy the data to it, and take a yoloString.
			la.Value = string(data[index:postIndex])
			index = postIndex
		default:
			index = preIndex
			skippy, err := skipMimir(data[index:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return mimirpb.ErrInvalidLengthMimir
			}
			if (index + skippy) < 0 {
				return mimirpb.ErrInvalidLengthMimir
			}
			if (index + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			index += skippy
		}
	}
	if index > l {
		return io.ErrUnexpectedEOF
	}

	return nil
}

func skipMimir(data []byte) (n int, err error) {
	l := len(data)
	index := 0
	for index < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, mimirpb.ErrIntOverflowMimir
			}
			if index >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := data[index]
			index++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, mimirpb.ErrIntOverflowMimir
				}
				if index >= l {
					return 0, io.ErrUnexpectedEOF
				}
				index++
				if data[index-1] < 0x80 {
					break
				}
			}
			return index, nil
		case 1:
			index += 8
			return index, nil
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, mimirpb.ErrIntOverflowMimir
				}
				if index >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if length < 0 {
				return 0, mimirpb.ErrInvalidLengthMimir
			}
			index += length
			if index < 0 {
				return 0, mimirpb.ErrInvalidLengthMimir
			}
			return index, nil
		case 3:
			for {
				var innerWire uint64
				start := index
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return 0, mimirpb.ErrIntOverflowMimir
					}
					if index >= l {
						return 0, io.ErrUnexpectedEOF
					}
					b := data[index]
					index++
					innerWire |= (uint64(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				innerWireType := int(innerWire & 0x7)
				if innerWireType == 4 {
					break
				}
				next, err := skipMimir(data[start:])
				if err != nil {
					return 0, err
				}
				index = start + next
				if index < 0 {
					return 0, mimirpb.ErrInvalidLengthMimir
				}
			}
			return index, nil
		case 4:
			return index, nil
		case 5:
			index += 4
			return index, nil
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
	}
	panic("unreachable")
}

func unmarshalChunk(chk *Chunk, data []byte) error {
	l := len(data)
	index := 0
	for index < l {
		preIndex := index
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowIngester
			}
			if index >= l {
				return io.ErrUnexpectedEOF
			}
			b := data[index]
			index++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Chunk: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Chunk: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field StartTimestampMs", wireType)
			}
			chk.StartTimestampMs = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowIngester
				}
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				chk.StartTimestampMs |= int64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field EndTimestampMs", wireType)
			}
			chk.EndTimestampMs = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowIngester
				}
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				chk.EndTimestampMs |= int64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 3:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Encoding", wireType)
			}
			chk.Encoding = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowIngester
				}
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				chk.Encoding |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 4:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Data", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowIngester
				}
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthIngester
			}
			postIndex := index + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthIngester
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			chk.Data = slices.Grow(chunkDataPool.Get().(mimirpb.UnsafeByteSlice)[:0], l)[0:l]
			copy(chk.Data, data[index:postIndex])
			index = postIndex
		default:
			index = preIndex
			skippy, err := skipIngester(data[index:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthIngester
			}
			if (index + skippy) < 0 {
				return ErrInvalidLengthIngester
			}
			if (index + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			index += skippy
		}
	}

	if index > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}

type CustomQueryStreamSeries struct {
	*QueryStreamSeries
}

// Release back to pool.
func (m *CustomQueryStreamSeries) Release() {
	m.Labels = m.Labels[:0]
	queryStreamSeriesPool.Put(m.QueryStreamSeries)
	m.QueryStreamSeries = nil
}

func (m *CustomQueryStreamSeries) Unmarshal(data []byte) error {
	m.QueryStreamSeries = queryStreamSeriesPool.Get().(*QueryStreamSeries)
	m.Labels = m.Labels[:0]

	l := len(data)
	index := 0
	for index < l {
		preIndex := index
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowIngester
			}
			if index >= l {
				return io.ErrUnexpectedEOF
			}
			b := data[index]
			index++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: QueryStreamSeries: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: QueryStreamSeries: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Labels", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowIngester
				}
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthIngester
			}
			postIndex := index + msglen
			if postIndex < 0 {
				return ErrInvalidLengthIngester
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			var la mimirpb.LabelAdapter
			if err := unmarshalLabelAdapter(&la, data[index:postIndex]); err != nil {
				return err
			}
			m.Labels = append(m.Labels, la)
			index = postIndex
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field ChunkCount", wireType)
			}
			m.ChunkCount = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowIngester
				}
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				m.ChunkCount |= int64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			index = preIndex
			skippy, err := skipIngester(data[index:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthIngester
			}
			if (index + skippy) < 0 {
				return ErrInvalidLengthIngester
			}
			if (index + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			index += skippy
		}
	}

	if index > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}

type CustomQueryStreamSeriesChunks struct {
	*QueryStreamSeriesChunks
}

// Release back to pool.
func (m *CustomQueryStreamSeriesChunks) Release() {
	for _, chk := range m.Chunks {
		//nolint:staticcheck
		chunkDataPool.Put(chk.Data)
		chk.Data = nil
	}
	m.Chunks = m.Chunks[:0]
	queryStreamSeriesChunksPool.Put(m.QueryStreamSeriesChunks)
	m.QueryStreamSeriesChunks = nil
}

func (m *CustomQueryStreamSeriesChunks) Unmarshal(data []byte) error {
	m.QueryStreamSeriesChunks = queryStreamSeriesChunksPool.Get().(*QueryStreamSeriesChunks)
	m.Chunks = m.Chunks[:0]

	l := len(data)
	index := 0
	for index < l {
		preIndex := index
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowIngester
			}
			if index >= l {
				return io.ErrUnexpectedEOF
			}
			b := data[index]
			index++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: QueryStreamSeriesChunks: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: QueryStreamSeriesChunks: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field SeriesIndex", wireType)
			}
			m.SeriesIndex = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowIngester
				}
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				m.SeriesIndex |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Chunks", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowIngester
				}
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthIngester
			}
			postIndex := index + msglen
			if postIndex < 0 {
				return ErrInvalidLengthIngester
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			var chk Chunk
			if err := unmarshalChunk(&chk, data[index:postIndex]); err != nil {
				return err
			}
			m.Chunks = append(m.Chunks, chk)
			index = postIndex
		default:
			index = preIndex
			skippy, err := skipIngester(data[index:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthIngester
			}
			if (index + skippy) < 0 {
				return ErrInvalidLengthIngester
			}
			if (index + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			index += skippy
		}
	}

	if index > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}

// Release held resources to pool.
func (m *QueryStreamResponse) Release() {
	for _, s := range m.Chunkseries {
		s.Release()
	}
	m.Chunkseries = nil
	for _, s := range m.Timeseries {
		s.Release()
	}
	m.Timeseries = nil
	for _, s := range m.StreamingSeries {
		s.Release()
	}
	m.StreamingSeriesChunks = nil
	for _, s := range m.StreamingSeriesChunks {
		s.Release()
	}
}

// Release back to pool.
func (m *MetricsForLabelMatchersResponse) Release() {
	for _, me := range m.Metric {
		me.Release()
	}
	m.Metric = nil
}

// Release back to pool.
func (m *ExemplarQueryResponse) Release() {
	for _, s := range m.Timeseries {
		s.Release()
	}
	m.Timeseries = nil
}

// ReleaseChunk puts chk back in pool.
func ReleaseChunk(chk Chunk) {
	//nolint:staticcheck
	chunkDataPool.Put(chk.Data)
}
