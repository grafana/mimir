// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"encoding/binary"
	"errors"
	"math"
	"slices"
	"sync"
	"unsafe"

	"github.com/grafana/mimir/pkg/mimirpb"
)

var errMalformedRW2 = errors.New("malformed remote write 2.0 record")

// rw2Series is a series of a record, by where its parts are in the source's buffers.
type rw2Series struct {
	refsAt, refsTo       int32
	samplesAt, samplesTo int32
	// Whether the series has more than floats: histograms, exemplars, metadata or a created timestamp.
	other bool
}

// rw2Source is the series of a remote write 2.0 record, read without decoding it into a request: the labels are
// symbol references, which resolve to strings into the record's bytes.
type rw2Source struct {
	symbols []string
	// The references below offset are to common, the ones from it to symbols.
	common []string
	offset uint32
	refs   []uint32
	floats []mimirpb.Sample
	series []rw2Series
	bodies [][]byte
	source mimirpb.WriteRequest_SourceEnum
	// Whether the record has more than series of floats, which only decoding it reads: a series with more, or
	// metadata of the record's own.
	other        bool
	minTimestamp int64
}

var rw2Sources = sync.Pool{New: func() any { return &rw2Source{} }}

func (s *rw2Source) symbol(ref uint32) (string, bool) {
	if ref < s.offset {
		if int(ref) >= len(s.common) {
			return "", false
		}
		return s.common[ref], true
	}
	ref -= s.offset
	if int(ref) >= len(s.symbols) {
		return "", false
	}
	return s.symbols[ref], true
}

func (s *rw2Source) pairs(index int, into [][2]string) ([][2]string, string) {
	series := &s.series[index]
	var name string
	refs := s.refs[series.refsAt:series.refsTo]
	for at := 0; at+1 < len(refs); at += 2 {
		// References were checked when the record was read.
		labelName, _ := s.symbol(refs[at])
		value, _ := s.symbol(refs[at+1])
		into = append(into, [2]string{labelName, value})
		if labelName == metricNameLabel {
			name = value
		}
	}
	return into, name
}

func (s *rw2Source) samples(index int) []mimirpb.Sample {
	series := &s.series[index]
	return s.floats[series.samplesAt:series.samplesTo]
}

// read reads the record's symbols and series. Strings refer to content.
func (s *rw2Source) read(content []byte, common []string, offset uint32) error {
	s.symbols, s.refs, s.floats, s.series, s.bodies = s.symbols[:0], s.refs[:0], s.floats[:0], s.series[:0], s.bodies[:0]
	s.common, s.offset, s.source, s.other, s.minTimestamp = common, offset, mimirpb.API, false, math.MaxInt64
	// The series reference symbols, which a writer may put after them: they are read first.
	rest := content
	for len(rest) > 0 {
		field, wire, body, next, ok := protoField(rest)
		if !ok {
			return errMalformedRW2
		}
		rest = next
		switch {
		case field == 4 && wire == 2:
			s.symbols = append(s.symbols, unsafe.String(unsafe.SliceData(body), len(body)))
		case field == 5 && wire == 2:
			s.bodies = append(s.bodies, body)
		case field == 2 && wire == 0:
			source, _ := binary.Uvarint(body)
			s.source = mimirpb.WriteRequest_SourceEnum(source)
		case field == 1:
			// Series of a version 1 request, which this doesn't read.
			return errMalformedRW2
		case field == 1000 || field == 1001:
			// Validation the distributor did.
		default:
			s.other = true
		}
	}
	for _, body := range s.bodies {
		if err := s.readSeries(body); err != nil {
			return err
		}
	}
	clear(s.bodies)
	return nil
}

func (s *rw2Source) readSeries(body []byte) error {
	series := rw2Series{refsAt: int32(len(s.refs)), samplesAt: int32(len(s.floats))}
	for len(body) > 0 {
		field, wire, value, next, ok := protoField(body)
		if !ok {
			return errMalformedRW2
		}
		body = next
		switch {
		case field == 1 && wire == 2:
			for len(value) > 0 {
				ref, size := binary.Uvarint(value)
				if size <= 0 {
					return errMalformedRW2
				}
				value = value[size:]
				if _, ok := s.symbol(uint32(ref)); !ok {
					return errMalformedRW2
				}
				s.refs = append(s.refs, uint32(ref))
			}
		case field == 2 && wire == 2:
			var sample mimirpb.Sample
			for len(value) > 0 {
				sampleField, sampleWire, sampleValue, sampleNext, ok := protoField(value)
				if !ok {
					return errMalformedRW2
				}
				value = sampleNext
				switch {
				case sampleField == 1 && sampleWire == 1:
					sample.Value = math.Float64frombits(binary.LittleEndian.Uint64(sampleValue))
				case sampleField == 2 && sampleWire == 0:
					timestamp, _ := binary.Uvarint(sampleValue)
					sample.TimestampMs = int64(timestamp)
				}
			}
			s.floats = append(s.floats, sample)
			s.minTimestamp = min(s.minTimestamp, sample.TimestampMs)
		case field == 5 && len(value) == 0:
			// The empty metadata writers put in every series.
		default:
			series.other = true
		}
	}
	series.refsTo, series.samplesTo = int32(len(s.refs)), int32(len(s.floats))
	if (series.refsTo-series.refsAt)%2 != 0 {
		return errMalformedRW2
	}
	s.other = s.other || series.other
	s.series = append(s.series, series)
	return nil
}

// protoField reads one field of a protobuf message: for a varint, value holds its encoding, for a fixed64 its eight
// bytes, and for a length-delimited field its bytes.
func protoField(data []byte) (field uint64, wire uint64, value, rest []byte, ok bool) {
	tag, size := binary.Uvarint(data)
	if size <= 0 {
		return 0, 0, nil, nil, false
	}
	data = data[size:]
	field, wire = tag>>3, tag&7
	switch wire {
	case 0:
		_, size := binary.Uvarint(data)
		if size <= 0 {
			return 0, 0, nil, nil, false
		}
		return field, wire, data[:size], data[size:], true
	case 1:
		if len(data) < 8 {
			return 0, 0, nil, nil, false
		}
		return field, wire, data[:8], data[8:], true
	case 2:
		length, size := binary.Uvarint(data)
		if size <= 0 || uint64(len(data)-size) < length {
			return 0, 0, nil, nil, false
		}
		data = data[size:]
		return field, wire, data[:length], data[length:], true
	case 5:
		if len(data) < 4 {
			return 0, 0, nil, nil, false
		}
		return field, wire, data[:4], data[4:], true
	}
	return 0, 0, nil, nil, false
}

// RW2Record is the content of a remote write 2.0 record read in place, which AppendRW2 appends the floats of. Its
// strings refer to the content, which it must not outlive.
type RW2Record struct {
	source *rw2Source
}

// ReadRW2 reads a record's content, whose symbol references below symbolOffset are to commonSymbols.
func ReadRW2(content []byte, commonSymbols []string, symbolOffset uint32) (*RW2Record, error) {
	source := rw2Sources.Get().(*rw2Source)
	if err := source.read(content, commonSymbols, symbolOffset); err != nil {
		releaseRW2Source(source)
		return nil, err
	}
	return &RW2Record{source: source}, nil
}

func releaseRW2Source(source *rw2Source) {
	// The symbols refer to the content.
	clear(source.symbols)
	source.common = nil
	rw2Sources.Put(source)
}

// Release gives back what the record holds; it can't be used after.
func (r *RW2Record) Release() {
	releaseRW2Source(r.source)
	r.source = nil
}

// Len is how many series the record has.
func (r *RW2Record) Len() int { return len(r.source.series) }

// Source is where the request the record has came from.
func (r *RW2Record) Source() mimirpb.WriteRequest_SourceEnum { return r.source.source }

// OnlyFloats reports whether the record has only series of floats, without metadata: what else it has is only read
// by decoding it.
func (r *RW2Record) OnlyFloats() bool { return !r.source.other }

// MinTimestamp is the oldest timestamp of the record's floats, math.MaxInt64 without any.
func (r *RW2Record) MinTimestamp() int64 { return r.source.minTimestamp }

// AppendRW2 is AppendFloats for the series of a record. Series are identified by their index in the record; leftover
// has the ones it didn't handle, which the caller decodes and appends another way, those with more than floats among
// them.
func (e *Engine) AppendRW2(record *RW2Record, minTimestampMs, maxTimestampMs, ingestedAtMs int64, otlp bool, sink FloatSink) (leftover []int, ingested int, err error) {
	source := record.source
	fast := make([]int, 0, len(source.series))
	for index := range source.series {
		if series := &source.series[index]; series.other || series.samplesTo == series.samplesAt {
			leftover = append(leftover, index)
		} else {
			fast = append(fast, index)
		}
	}
	rest, ingested, err := e.appendFloats(source, fast, minTimestampMs, maxTimestampMs, ingestedAtMs, otlp, sink)
	if len(leftover) == 0 {
		return rest, ingested, err
	}
	leftover = append(leftover, rest...)
	slices.Sort(leftover)
	return leftover, ingested, err
}
