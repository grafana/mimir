// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/storepb/custom.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package storepb

import (
	"fmt"
	"io"
	"slices"
	"sync"

	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/chunk"
)

func NewSeriesResponse(series CustomSeries) *SeriesResponse {
	return &SeriesResponse{
		Result: &SeriesResponse_Series{
			Series: &series,
		},
	}
}

func NewHintsSeriesResponse(hints *types.Any) *SeriesResponse {
	return &SeriesResponse{
		Result: &SeriesResponse_Hints{
			Hints: hints,
		},
	}
}

func NewStatsResponse(indexBytesFetched int) *SeriesResponse {
	return &SeriesResponse{
		Result: &SeriesResponse_Stats{
			Stats: &Stats{FetchedIndexBytes: uint64(indexBytesFetched)},
		},
	}
}

func NewStreamingSeriesResponse(series CustomStreamingSeriesBatch) *SeriesResponse {
	return &SeriesResponse{
		Result: &SeriesResponse_StreamingSeries{
			StreamingSeries: &series,
		},
	}
}

func NewStreamingChunksResponse(series CustomStreamingChunksBatch) *SeriesResponse {
	return &SeriesResponse{
		Result: &SeriesResponse_StreamingChunks{
			StreamingChunks: &series,
		},
	}
}

func NewStreamingChunksEstimate(estimatedChunks uint64) *SeriesResponse {
	return &SeriesResponse{
		Result: &SeriesResponse_StreamingChunksEstimate{
			StreamingChunksEstimate: &StreamingChunksEstimate{
				EstimatedChunkCount: estimatedChunks,
			},
		},
	}
}

type emptySeriesSet struct{}

func (emptySeriesSet) Next() bool                       { return false }
func (emptySeriesSet) At() (labels.Labels, []AggrChunk) { return labels.EmptyLabels(), nil }
func (emptySeriesSet) Err() error                       { return nil }

// EmptySeriesSet returns a new series set that contains no series.
func EmptySeriesSet() SeriesSet {
	return emptySeriesSet{}
}

// SeriesSet is a set of series and their corresponding chunks.
// The set is sorted by the label sets. Chunks may be overlapping or expected of order.
type SeriesSet interface {
	Next() bool
	At() (labels.Labels, []AggrChunk)
	Err() error
}

// PromMatchersToMatchers returns proto matchers from Prometheus matchers.
// NOTE: It allocates memory.
func PromMatchersToMatchers(ms ...*labels.Matcher) ([]LabelMatcher, error) {
	res := make([]LabelMatcher, 0, len(ms))
	for _, m := range ms {
		var t LabelMatcher_Type

		switch m.Type {
		case labels.MatchEqual:
			t = LabelMatcher_EQ
		case labels.MatchNotEqual:
			t = LabelMatcher_NEQ
		case labels.MatchRegexp:
			t = LabelMatcher_RE
		case labels.MatchNotRegexp:
			t = LabelMatcher_NRE
		default:
			return nil, errors.Errorf("unrecognized matcher type %d", m.Type)
		}
		res = append(res, LabelMatcher{Type: t, Name: m.Name, Value: m.Value})
	}
	return res, nil
}

// MatchersToPromMatchers returns Prometheus matchers from proto matchers.
// NOTE: It allocates memory.
func MatchersToPromMatchers(ms ...LabelMatcher) ([]*labels.Matcher, error) {
	res := make([]*labels.Matcher, 0, len(ms))
	for _, m := range ms {
		var t labels.MatchType

		switch m.Type {
		case LabelMatcher_EQ:
			t = labels.MatchEqual
		case LabelMatcher_NEQ:
			t = labels.MatchNotEqual
		case LabelMatcher_RE:
			t = labels.MatchRegexp
		case LabelMatcher_NRE:
			t = labels.MatchNotRegexp
		default:
			return nil, errors.Errorf("unrecognized label matcher type %d", m.Type)
		}
		m, err := labels.NewMatcher(t, m.Name, m.Value)
		if err != nil {
			return nil, err
		}
		res = append(res, m)
	}
	return res, nil
}

func (c AggrChunk) GetChunkEncoding() (chunk.Encoding, bool) {
	switch c.Raw.Type {
	case Chunk_XOR:
		return chunk.PrometheusXorChunk, true
	case Chunk_Histogram:
		return chunk.PrometheusHistogramChunk, true
	case Chunk_FloatHistogram:
		return chunk.PrometheusFloatHistogramChunk, true
	default:
		return 0, false
	}
}

var (
	seriesPool = sync.Pool{
		New: func() any {
			return &Series{}
		},
	}
	chunkDataPool = sync.Pool{
		New: func() any {
			return mimirpb.UnsafeByteSlice{}
		},
	}
	streamingSeriesBatchPool = sync.Pool{
		New: func() any {
			return &StreamingSeriesBatch{}
		},
	}
	streamingChunksBatchPool = sync.Pool{
		New: func() any {
			return &StreamingChunksBatch{}
		},
	}
	labelAdaptersPool = sync.Pool{
		New: func() any {
			return []mimirpb.LabelAdapter{}
		},
	}
)

type CustomSeries struct {
	*Series
}

// Release back to pool.
func (m *CustomSeries) Release() {
	for _, chk := range m.Chunks {
		//nolint:staticcheck
		chunkDataPool.Put(chk.Raw.Data)
		chk.Raw.Data = nil
	}
	m.Labels = m.Labels[:0]
	m.Chunks = m.Chunks[:0]
	seriesPool.Put(m.Series)
	m.Series = nil
}

func (m *CustomSeries) Unmarshal(data []byte) error {
	m.Series = seriesPool.Get().(*Series)
	m.Labels = m.Labels[:0]
	m.Chunks = m.Chunks[:0]
	l := len(data)
	index := 0
	for index < l {
		preIndex := index
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowTypes
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
			return fmt.Errorf("proto: Series: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Series: illegal tag %d (wire type %d)", fieldNum, wire)
		}

		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Labels", wireType)
			}

			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
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
				return ErrInvalidLengthTypes
			}

			postIndex := index + msglen
			if postIndex < 0 {
				return ErrInvalidLengthTypes
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
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Chunks", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
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
				return ErrInvalidLengthTypes
			}
			postIndex := index + msglen
			if postIndex < 0 {
				return ErrInvalidLengthTypes
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			var chk AggrChunk
			if err := unmarshalAggrChunk(&chk, data[index:postIndex]); err != nil {
				return err
			}
			m.Chunks = append(m.Chunks, chk)
			index = postIndex
		default:
			index = preIndex
			skippy, err := skipTypes(data[index:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthTypes
			}
			if (index + skippy) < 0 {
				return ErrInvalidLengthTypes
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

func (m *SeriesResponse) GetSeries() *CustomSeries {
	if x, ok := m.GetResult().(*SeriesResponse_Series); ok {
		return x.Series
	}
	return nil
}

func (m *SeriesResponse) GetStreamingChunks() *CustomStreamingChunksBatch {
	if x, ok := m.GetResult().(*SeriesResponse_StreamingChunks); ok {
		return x.StreamingChunks
	}
	return nil
}

func (m *SeriesResponse) GetStreamingSeries() *CustomStreamingSeriesBatch {
	if x, ok := m.GetResult().(*SeriesResponse_StreamingSeries); ok {
		return x.StreamingSeries
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

func unmarshalAggrChunk(chk *AggrChunk, data []byte) error {
	l := len(data)
	index := 0
	for index < l {
		preIndex := index
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowTypes
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
			return fmt.Errorf("proto: AggrChunk: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: AggrChunk: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field MinTime", wireType)
			}
			chk.MinTime = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
				}
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				chk.MinTime |= int64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field MaxTime", wireType)
			}
			chk.MaxTime = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
				}
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				chk.MaxTime |= int64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Raw", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
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
				return ErrInvalidLengthTypes
			}
			postIndex := index + msglen
			if postIndex < 0 {
				return ErrInvalidLengthTypes
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if err := unmarshalChunk(&chk.Raw, data[index:postIndex]); err != nil {
				return err
			}
			index = postIndex
		default:
			index = preIndex
			skippy, err := skipTypes(data[index:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthTypes
			}
			if (index + skippy) < 0 {
				return ErrInvalidLengthTypes
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

func unmarshalChunk(chk *Chunk, data []byte) error {
	l := len(data)
	index := 0
	for index < l {
		preIndex := index
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowTypes
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
				return fmt.Errorf("proto: wrong wireType = %d for field Type", wireType)
			}
			chk.Type = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
				}
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				chk.Type |= Chunk_Encoding(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Data", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
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
				return ErrInvalidLengthTypes
			}
			postIndex := index + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthTypes
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			l := postIndex - index
			chk.Data = slices.Grow(chunkDataPool.Get().(mimirpb.UnsafeByteSlice)[:0], l)[0:l]
			copy(chk.Data, data[index:postIndex])
			index = postIndex
		default:
			index = preIndex
			skippy, err := skipTypes(data[index:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthTypes
			}
			if (index + skippy) < 0 {
				return ErrInvalidLengthTypes
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

type CustomStreamingSeriesBatch struct {
	*StreamingSeriesBatch
}

// Release back to pool.
func (m *CustomStreamingSeriesBatch) Release() {
	for _, s := range m.Series {
		//nolint:staticcheck
		labelAdaptersPool.Put(s.Labels)
	}
	streamingSeriesBatchPool.Put(m.StreamingSeriesBatch)
	m.StreamingSeriesBatch = nil
}

func (m *CustomStreamingSeriesBatch) Unmarshal(data []byte) error {
	m.StreamingSeriesBatch = streamingSeriesBatchPool.Get().(*StreamingSeriesBatch)
	m.Series = m.Series[:0]

	l := len(data)
	index := 0
	for index < l {
		preIndex := index
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowTypes
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
			return fmt.Errorf("proto: StreamingSeriesBatch: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: StreamingSeriesBatch: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Series", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
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
				return ErrInvalidLengthTypes
			}
			postIndex := index + msglen
			if postIndex < 0 {
				return ErrInvalidLengthTypes
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			var ss StreamingSeries
			if err := unmarshalStreamingSeries(&ss, data[index:postIndex]); err != nil {
				return err
			}
			m.Series = append(m.Series, &ss)
			index = postIndex
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field IsEndOfSeriesStream", wireType)
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
				}
				if index >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[index]
				index++
				v |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.IsEndOfSeriesStream = bool(v != 0)
		default:
			index = preIndex
			skippy, err := skipTypes(data[index:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthTypes
			}
			if (index + skippy) < 0 {
				return ErrInvalidLengthTypes
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

func unmarshalStreamingSeries(m *StreamingSeries, data []byte) error {
	m.Labels = labelAdaptersPool.Get().([]mimirpb.LabelAdapter)[:0]

	l := len(data)
	index := 0
	for index < l {
		preIndex := index
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowTypes
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
			return fmt.Errorf("proto: StreamingSeries: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: StreamingSeries: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Labels", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
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
				return ErrInvalidLengthTypes
			}
			postIndex := index + msglen
			if postIndex < 0 {
				return ErrInvalidLengthTypes
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
		default:
			index = preIndex
			skippy, err := skipTypes(data[index:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthTypes
			}
			if (index + skippy) < 0 {
				return ErrInvalidLengthTypes
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

type CustomStreamingChunksBatch struct {
	*StreamingChunksBatch
}

// Release batch back to pool.
// Contained chunks are also released back to pool.
func (m *CustomStreamingChunksBatch) Release() {
	for _, chks := range m.Series {
		for _, chk := range chks.Chunks {
			ReleaseChunk(&chk.Raw)
		}
		chks.Chunks = chks.Chunks[:0]
	}

	m.Series = m.Series[:0]
	streamingChunksBatchPool.Put(m.StreamingChunksBatch)
	m.StreamingChunksBatch = nil
}

// ReleaseChunk releases chk back to the pool.
func ReleaseChunk(chk *Chunk) {
	chk.Data = chk.Data[:0]
	//nolint:staticcheck
	chunkDataPool.Put(chk.Data)
	chk.Data = nil
}

func (m *CustomStreamingChunksBatch) Unmarshal(data []byte) error {
	m.StreamingChunksBatch = streamingChunksBatchPool.Get().(*StreamingChunksBatch)
	m.Series = m.Series[:0]

	l := len(data)
	index := 0
	for index < l {
		preIndex := index
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowTypes
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
			return fmt.Errorf("proto: StreamingChunksBatch: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: StreamingChunksBatch: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Series", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
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
				return ErrInvalidLengthTypes
			}
			postIndex := index + msglen
			if postIndex < 0 {
				return ErrInvalidLengthTypes
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			var ss StreamingChunks
			if err := unmarshalStreamingChunks(&ss, data[index:postIndex]); err != nil {
				return err
			}
			m.Series = append(m.Series, &ss)
			index = postIndex
		default:
			index = preIndex
			skippy, err := skipTypes(data[index:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthTypes
			}
			if (index + skippy) < 0 {
				return ErrInvalidLengthTypes
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

func unmarshalStreamingChunks(m *StreamingChunks, data []byte) error {
	l := len(data)
	index := 0
	for index < l {
		preIndex := index
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowTypes
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
			return fmt.Errorf("proto: StreamingChunks: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: StreamingChunks: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field SeriesIndex", wireType)
			}
			m.SeriesIndex = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTypes
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
					return ErrIntOverflowTypes
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
				return ErrInvalidLengthTypes
			}
			postIndex := index + msglen
			if postIndex < 0 {
				return ErrInvalidLengthTypes
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			var chk AggrChunk
			if err := unmarshalAggrChunk(&chk, data[index:postIndex]); err != nil {
				return nil
			}
			m.Chunks = append(m.Chunks, chk)
			index = postIndex
		default:
			index = preIndex
			skippy, err := skipTypes(data[index:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthTypes
			}
			if (index + skippy) < 0 {
				return ErrInvalidLengthTypes
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
