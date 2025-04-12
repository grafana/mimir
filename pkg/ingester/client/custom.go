// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/client/custom.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package client

import (
	"fmt"
	io "io"
	"sync"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/chunk"
	"github.com/grafana/mimir/pkg/util/protobuf"
)

var (
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

	labelBuf protobuf.LabelBuffer
	chunkBuf chunkBuffer
}

// Release back to pool.
func (m *CustomTimeSeriesChunk) Release() {
	m.Labels = m.Labels[:0]
	m.Chunks = m.Chunks[:0]
	timeSeriesChunkPool.Put(m.TimeSeriesChunk)
	m.TimeSeriesChunk = nil
	m.labelBuf.Release()
	m.chunkBuf.Release()
}

func (m *CustomTimeSeriesChunk) Unmarshal(data []byte) error {
	m.TimeSeriesChunk = timeSeriesChunkPool.Get().(*TimeSeriesChunk)
	m.Labels = m.Labels[:0]
	m.Chunks = m.Chunks[:0]
	m.labelBuf = protobuf.NewLabelBuffer()
	m.chunkBuf = newChunkBuffer()

	numLabels, labelBufSz, numChunks, chunkBufSz, err := m.unmarshal(data, true)
	if err != nil {
		return err
	}

	if cap(m.Labels) < numLabels {
		m.Labels = make([]mimirpb.LabelAdapter, 0, numLabels)
	}
	m.labelBuf.Reserve(labelBufSz)
	if cap(m.Chunks) < numChunks {
		m.Chunks = make([]Chunk, 0, numChunks)
	}
	m.chunkBuf.Reserve(chunkBufSz)

	_, _, _, _, err = m.unmarshal(data, false)
	return err
}

func (m *CustomTimeSeriesChunk) unmarshal(data []byte, calcSizes bool) (int, int, int, int, error) {
	var chk Chunk
	var la mimirpb.LabelAdapter

	numLabels := 0
	labelBufSize := 0
	numChunks := 0
	chunkBufSize := 0
	l := len(data)
	index := 0
	for index < l {
		preIndex := index
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return numLabels, labelBufSize, numChunks, chunkBufSize, ErrIntOverflowIngester
			}
			if index >= l {
				return numLabels, labelBufSize, numChunks, chunkBufSize, io.ErrUnexpectedEOF
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
			return numLabels, labelBufSize, numChunks, chunkBufSize, fmt.Errorf("proto: TimeSeriesChunk: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return numLabels, labelBufSize, numChunks, chunkBufSize, fmt.Errorf("proto: TimeSeriesChunk: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return numLabels, labelBufSize, numChunks, chunkBufSize, fmt.Errorf("proto: wrong wireType = %d for field FromIngesterId", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return numLabels, labelBufSize, numChunks, chunkBufSize, ErrIntOverflowIngester
				}
				if index >= l {
					return numLabels, labelBufSize, numChunks, chunkBufSize, io.ErrUnexpectedEOF
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
				return numLabels, labelBufSize, numChunks, chunkBufSize, ErrInvalidLengthIngester
			}
			postIndex := index + intStringLen
			if postIndex < 0 {
				return numLabels, labelBufSize, numChunks, chunkBufSize, ErrInvalidLengthIngester
			}
			if postIndex > l {
				return numLabels, labelBufSize, numChunks, chunkBufSize, io.ErrUnexpectedEOF
			}
			m.FromIngesterId = string(data[index:postIndex])
			index = postIndex
		case 2:
			if wireType != 2 {
				return numLabels, labelBufSize, numChunks, chunkBufSize, fmt.Errorf("proto: wrong wireType = %d for field UserId", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return numLabels, labelBufSize, numChunks, chunkBufSize, ErrIntOverflowIngester
				}
				if index >= l {
					return numLabels, labelBufSize, numChunks, chunkBufSize, io.ErrUnexpectedEOF
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
				return numLabels, labelBufSize, numChunks, chunkBufSize, ErrInvalidLengthIngester
			}
			postIndex := index + intStringLen
			if postIndex < 0 {
				return numLabels, labelBufSize, numChunks, chunkBufSize, ErrInvalidLengthIngester
			}
			if postIndex > l {
				return numLabels, labelBufSize, numChunks, chunkBufSize, io.ErrUnexpectedEOF
			}
			m.UserId = string(data[index:postIndex])
			index = postIndex
		case 3:
			if wireType != 2 {
				return numLabels, labelBufSize, numChunks, chunkBufSize, fmt.Errorf("proto: wrong wireType = %d for field Labels", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return numLabels, labelBufSize, numChunks, chunkBufSize, ErrIntOverflowIngester
				}
				if index >= l {
					return numLabels, labelBufSize, numChunks, chunkBufSize, io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return numLabels, labelBufSize, numChunks, chunkBufSize, ErrInvalidLengthIngester
			}
			postIndex := index + msglen
			if postIndex < 0 {
				return numLabels, labelBufSize, numChunks, chunkBufSize, ErrInvalidLengthIngester
			}
			if postIndex > l {
				return numLabels, labelBufSize, numChunks, chunkBufSize, io.ErrUnexpectedEOF
			}

			if calcSizes {
				numLabels++
				sz, err := protobuf.UnmarshalLabelAdapter(&la, data[index:postIndex], &m.labelBuf, true)
				if err != nil {
					return numLabels, labelBufSize, numChunks, chunkBufSize, err
				}
				labelBufSize += sz
			} else {
				if _, err := protobuf.UnmarshalLabelAdapter(&la, data[index:postIndex], &m.labelBuf, false); err != nil {
					return numLabels, labelBufSize, numChunks, chunkBufSize, err
				}
				m.Labels = append(m.Labels, la)
			}
			index = postIndex
		case 4:
			if wireType != 2 {
				return numLabels, labelBufSize, numChunks, chunkBufSize, fmt.Errorf("proto: wrong wireType = %d for field Chunks", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return numLabels, labelBufSize, numChunks, chunkBufSize, ErrIntOverflowIngester
				}
				if index >= l {
					return numLabels, labelBufSize, numChunks, chunkBufSize, io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return numLabels, labelBufSize, numChunks, chunkBufSize, ErrInvalidLengthIngester
			}
			postIndex := index + msglen
			if postIndex < 0 {
				return numLabels, labelBufSize, numChunks, chunkBufSize, ErrInvalidLengthIngester
			}
			if postIndex > l {
				return numLabels, labelBufSize, numChunks, chunkBufSize, io.ErrUnexpectedEOF
			}
			if calcSizes {
				numChunks++
				sz, err := unmarshalChunk(&chk, data[index:postIndex], &m.chunkBuf, true)
				if err != nil {
					return numLabels, labelBufSize, numChunks, chunkBufSize, err
				}
				chunkBufSize += sz
			} else {
				if _, err := unmarshalChunk(&chk, data[index:postIndex], &m.chunkBuf, false); err != nil {
					return numLabels, labelBufSize, numChunks, chunkBufSize, err
				}
				m.Chunks = append(m.Chunks, chk)
			}
			index = postIndex
		default:
			index = preIndex
			skippy, err := skipIngester(data[index:])
			if err != nil {
				return numLabels, labelBufSize, numChunks, chunkBufSize, err
			}
			if skippy < 0 {
				return numLabels, labelBufSize, numChunks, chunkBufSize, ErrInvalidLengthIngester
			}
			if (index + skippy) < 0 {
				return numLabels, labelBufSize, numChunks, chunkBufSize, ErrInvalidLengthIngester
			}
			if (index + skippy) > l {
				return numLabels, labelBufSize, numChunks, chunkBufSize, io.ErrUnexpectedEOF
			}
			index += skippy
		}
	}

	if index > l {
		return numLabels, labelBufSize, numChunks, chunkBufSize, io.ErrUnexpectedEOF
	}
	return numLabels, labelBufSize, numChunks, chunkBufSize, nil
}

func unmarshalChunk(chk *Chunk, data []byte, buf *chunkBuffer, calcSize bool) (int, error) {
	bufSize := 0
	l := len(data)
	var byteLen int
	index := 0
	for index < l {
		preIndex := index
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return bufSize, ErrIntOverflowIngester
			}
			if index >= l {
				return bufSize, io.ErrUnexpectedEOF
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
			return bufSize, fmt.Errorf("proto: Chunk: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return bufSize, fmt.Errorf("proto: Chunk: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return bufSize, fmt.Errorf("proto: wrong wireType = %d for field StartTimestampMs", wireType)
			}
			chk.StartTimestampMs = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return bufSize, ErrIntOverflowIngester
				}
				if index >= l {
					return bufSize, io.ErrUnexpectedEOF
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
				return bufSize, fmt.Errorf("proto: wrong wireType = %d for field EndTimestampMs", wireType)
			}
			chk.EndTimestampMs = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return bufSize, ErrIntOverflowIngester
				}
				if index >= l {
					return bufSize, io.ErrUnexpectedEOF
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
				return bufSize, fmt.Errorf("proto: wrong wireType = %d for field Encoding", wireType)
			}
			chk.Encoding = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return bufSize, ErrIntOverflowIngester
				}
				if index >= l {
					return bufSize, io.ErrUnexpectedEOF
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
				return bufSize, fmt.Errorf("proto: wrong wireType = %d for field Data", wireType)
			}

			byteLen = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return bufSize, ErrIntOverflowIngester
				}
				if index >= l {
					return bufSize, io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return bufSize, ErrInvalidLengthIngester
			}
			postIndex := index + byteLen
			if postIndex < 0 {
				return bufSize, ErrInvalidLengthIngester
			}
			if postIndex > l {
				return bufSize, io.ErrUnexpectedEOF
			}
			if calcSize {
				bufSize += postIndex - index
			} else {
				chk.Data = buf.Add(data[index:postIndex])
			}
			index = postIndex
		default:
			index = preIndex
			skippy, err := skipIngester(data[index:])
			if err != nil {
				return bufSize, err
			}
			if skippy < 0 {
				return bufSize, ErrInvalidLengthIngester
			}
			if (index + skippy) < 0 {
				return bufSize, ErrInvalidLengthIngester
			}
			if (index + skippy) > l {
				return bufSize, io.ErrUnexpectedEOF
			}
			index += skippy
		}
	}

	if index > l {
		return bufSize, io.ErrUnexpectedEOF
	}
	return bufSize, nil
}

type CustomQueryStreamSeries struct {
	*QueryStreamSeries

	labelBuf protobuf.LabelBuffer
}

// Release back to pool.
func (m *CustomQueryStreamSeries) Release() {
	m.Labels = m.Labels[:0]
	queryStreamSeriesPool.Put(m.QueryStreamSeries)
	m.QueryStreamSeries = nil
	m.labelBuf.Release()
}

func (m *CustomQueryStreamSeries) Unmarshal(data []byte) error {
	m.QueryStreamSeries = queryStreamSeriesPool.Get().(*QueryStreamSeries)
	m.Labels = m.Labels[:0]
	m.labelBuf = protobuf.NewLabelBuffer()

	numLabels, bufSz, err := m.unmarshal(data, true)
	if err != nil {
		return err
	}

	if cap(m.Labels) < numLabels {
		m.Labels = make([]mimirpb.LabelAdapter, 0, numLabels)
	}
	m.labelBuf.Reserve(bufSz)
	_, _, err = m.unmarshal(data, false)
	return err
}

func (m *CustomQueryStreamSeries) unmarshal(data []byte, calcCounts bool) (int, int, error) {
	bufSz := 0
	numLabels := 0
	l := len(data)
	var la mimirpb.LabelAdapter
	index := 0
	for index < l {
		preIndex := index
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return numLabels, bufSz, ErrIntOverflowIngester
			}
			if index >= l {
				return numLabels, bufSz, io.ErrUnexpectedEOF
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
			return numLabels, bufSz, fmt.Errorf("proto: QueryStreamSeries: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return numLabels, bufSz, fmt.Errorf("proto: QueryStreamSeries: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return numLabels, bufSz, fmt.Errorf("proto: wrong wireType = %d for field Labels", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return numLabels, bufSz, ErrIntOverflowIngester
				}
				if index >= l {
					return numLabels, bufSz, io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return numLabels, bufSz, ErrInvalidLengthIngester
			}
			postIndex := index + msglen
			if postIndex < 0 {
				return numLabels, bufSz, ErrInvalidLengthIngester
			}
			if postIndex > l {
				return numLabels, bufSz, io.ErrUnexpectedEOF
			}
			if calcCounts {
				numLabels++
				sz, err := protobuf.UnmarshalLabelAdapter(&la, data[index:postIndex], &m.labelBuf, true)
				if err != nil {
					return numLabels, bufSz, err
				}
				bufSz += sz
			} else {
				if _, err := protobuf.UnmarshalLabelAdapter(&la, data[index:postIndex], &m.labelBuf, false); err != nil {
					return numLabels, bufSz, err
				}
				m.Labels = append(m.Labels, la)
			}
			index = postIndex
		case 2:
			if wireType != 0 {
				return numLabels, bufSz, fmt.Errorf("proto: wrong wireType = %d for field ChunkCount", wireType)
			}
			m.ChunkCount = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return numLabels, bufSz, ErrIntOverflowIngester
				}
				if index >= l {
					return numLabels, bufSz, io.ErrUnexpectedEOF
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
				return numLabels, bufSz, err
			}
			if skippy < 0 {
				return numLabels, bufSz, ErrInvalidLengthIngester
			}
			if (index + skippy) < 0 {
				return numLabels, bufSz, ErrInvalidLengthIngester
			}
			if (index + skippy) > l {
				return numLabels, bufSz, io.ErrUnexpectedEOF
			}
			index += skippy
		}
	}

	if index > l {
		return numLabels, bufSz, io.ErrUnexpectedEOF
	}
	return numLabels, bufSz, nil
}

type CustomQueryStreamSeriesChunks struct {
	*QueryStreamSeriesChunks

	chunkBuf chunkBuffer
}

// Release back to pool.
func (m *CustomQueryStreamSeriesChunks) Release() {
	m.Chunks = m.Chunks[:0]
	m.chunkBuf.Release()
	queryStreamSeriesChunksPool.Put(m.QueryStreamSeriesChunks)
	m.QueryStreamSeriesChunks = nil
}

func (m *CustomQueryStreamSeriesChunks) Unmarshal(data []byte) error {
	m.QueryStreamSeriesChunks = queryStreamSeriesChunksPool.Get().(*QueryStreamSeriesChunks)
	m.Chunks = m.Chunks[:0]
	m.chunkBuf = newChunkBuffer()

	numChunks, chunkBufSz, err := m.unmarshal(data, true)
	if err != nil {
		return err
	}
	if cap(m.Chunks) < numChunks {
		m.Chunks = make([]Chunk, 0, numChunks)
	}
	m.chunkBuf.Reserve(chunkBufSz)

	_, _, err = m.unmarshal(data, false)
	return err
}

func (m *CustomQueryStreamSeriesChunks) unmarshal(data []byte, calcSizes bool) (int, int, error) {
	numChunks := 0
	bufSize := 0
	var chk Chunk
	l := len(data)
	index := 0
	for index < l {
		preIndex := index
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return numChunks, bufSize, ErrIntOverflowIngester
			}
			if index >= l {
				return numChunks, bufSize, io.ErrUnexpectedEOF
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
			return numChunks, bufSize, fmt.Errorf("proto: QueryStreamSeriesChunks: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return numChunks, bufSize, fmt.Errorf("proto: QueryStreamSeriesChunks: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return numChunks, bufSize, fmt.Errorf("proto: wrong wireType = %d for field SeriesIndex", wireType)
			}
			m.SeriesIndex = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return numChunks, bufSize, ErrIntOverflowIngester
				}
				if index >= l {
					return numChunks, bufSize, io.ErrUnexpectedEOF
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
				return numChunks, bufSize, fmt.Errorf("proto: wrong wireType = %d for field Chunks", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return numChunks, bufSize, ErrIntOverflowIngester
				}
				if index >= l {
					return numChunks, bufSize, io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return numChunks, bufSize, ErrInvalidLengthIngester
			}
			postIndex := index + msglen
			if postIndex < 0 {
				return numChunks, bufSize, ErrInvalidLengthIngester
			}
			if postIndex > l {
				return numChunks, bufSize, io.ErrUnexpectedEOF
			}

			if calcSizes {
				numChunks++
				sz, err := unmarshalChunk(&chk, data[index:postIndex], &m.chunkBuf, true)
				if err != nil {
					return numChunks, bufSize, err
				}
				bufSize += sz
			} else {
				m.Chunks = m.Chunks[0 : len(m.Chunks)+1]
				if _, err := unmarshalChunk(&m.Chunks[len(m.Chunks)-1], data[index:postIndex], &m.chunkBuf, false); err != nil {
					return numChunks, bufSize, err
				}
			}
			index = postIndex
		default:
			index = preIndex
			skippy, err := skipIngester(data[index:])
			if err != nil {
				return numChunks, bufSize, err
			}
			if skippy < 0 {
				return numChunks, bufSize, ErrInvalidLengthIngester
			}
			if (index + skippy) < 0 {
				return numChunks, bufSize, ErrInvalidLengthIngester
			}
			if (index + skippy) > l {
				return numChunks, bufSize, io.ErrUnexpectedEOF
			}
			index += skippy
		}
	}

	if index > l {
		return numChunks, bufSize, io.ErrUnexpectedEOF
	}
	return numChunks, bufSize, nil
}

// Release held resources to pool.
func (m *QueryStreamResponse) Release() {
	for i := range m.Chunkseries {
		m.Chunkseries[i].Release()
	}
	m.Chunkseries = nil
	for i := range m.Timeseries {
		m.Timeseries[i].Release()
	}
	m.Timeseries = nil
	for i := range m.StreamingSeries {
		m.StreamingSeries[i].Release()
	}
	m.StreamingSeries = nil
	/* TODO: Seems broken
	for _, s := range m.StreamingSeriesChunks {
		s.Release()
	}
	m.StreamingSeriesChunks = nil
	*/
}

// Release back to pool.
func (m *MetricsForLabelMatchersResponse) Release() {
	for i := range m.Metric {
		m.Metric[i].Release()
	}
	m.Metric = nil
}

// Release back to pool.
func (m *ExemplarQueryResponse) Release() {
	for i := range m.Timeseries {
		m.Timeseries[i].Release()
	}
	m.Timeseries = nil
}

var (
	chunkBufferPool = sync.Pool{
		New: func() any {
			return []byte{}
		},
	}
)

type chunkBuffer struct {
	buf []byte
}

func newChunkBuffer() chunkBuffer {
	return chunkBuffer{
		buf: chunkBufferPool.Get().([]byte)[:0],
	}
}

func (b *chunkBuffer) Reserve(size int) {
	if cap(b.buf) < size {
		b.buf = make([]byte, 0, size)
	}
}

func (b *chunkBuffer) Add(data []byte) mimirpb.UnsafeByteSlice {
	oldLen := len(b.buf)
	newLen := oldLen + len(data)
	if cap(b.buf)-len(b.buf) < len(data) {
		panic(fmt.Errorf("only %d chunk bytes allocated, need %d", cap(b.buf), newLen))
	}
	b.buf = b.buf[0:newLen]
	copy(b.buf[oldLen:newLen], data)

	return b.buf[oldLen:newLen]
}

func (b *chunkBuffer) Release() {
	//nolint:staticcheck
	chunkBufferPool.Put(b.buf[:0])
	b.buf = nil
}
