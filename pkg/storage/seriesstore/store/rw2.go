// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"encoding/binary"
	"errors"
	"math"
	"slices"
	"strings"
	"sync"
	"unsafe"

	"github.com/grafana/mimir/pkg/mimirpb"
)

var errMalformedRW2 = errors.New("malformed remote write 2.0 record")

// rw2Series is a series of a record, by where its parts are in the source's buffers.
type rw2Series struct {
	refsAt, refsTo       int32
	samplesAt, samplesTo int32
	// The position of the series' metadata in the source's, or -1.
	metadata int32
	// Whether the series has more than floats, which only decoding it reads: histograms, exemplars or a created
	// timestamp.
	other bool
	// Whether the series has histograms or exemplars, which make it one with data like floats do.
	moreData bool
	// The series' resource, 1-based into the source's, or 0 for none.
	resource uint32
}

// rw2Metadata is a series' metadata, with the symbol references of its help and unit, which identify it within the
// record.
type rw2Metadata struct {
	series           int32
	metricType       mimirpb.MetricMetadata_MetricType
	help, unit       string
	helpRef, unitRef uint32
}

// rw2Source is the series of a remote write 2.0 record, read without decoding it into a request: the labels are
// symbol references, which resolve to strings into the record's bytes.
type rw2Source struct {
	symbols []string
	// The references below offset are to common, the ones from it to symbols.
	common   []string
	offset   uint32
	refs     []uint32
	floats   []mimirpb.Sample
	series   []rw2Series
	metadata []rw2Metadata
	bodies   [][]byte
	// The encodings of the record's resource table's entries, which refer to content.
	resources [][]byte
	content   []byte
	source    mimirpb.WriteRequest_SourceEnum
	// Whether the record has fields of its own this doesn't read, which only decoding it does.
	partial bool
	// The oldest timestamp of the record's samples, histograms and exemplars.
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
	s.symbols, s.refs, s.floats, s.series, s.metadata, s.bodies, s.resources = s.symbols[:0], s.refs[:0], s.floats[:0], s.series[:0], s.metadata[:0], s.bodies[:0], s.resources[:0]
	s.common, s.offset, s.content, s.source, s.partial, s.minTimestamp = common, offset, content, mimirpb.API, false, math.MaxInt64
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
		case field == 1002 && wire == 2:
			s.resources = append(s.resources, body)
		default:
			s.partial = true
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
	series := rw2Series{refsAt: int32(len(s.refs)), samplesAt: int32(len(s.floats)), metadata: -1}
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
		case field == 5 && wire == 2:
			if len(value) == 0 {
				// The empty metadata writers put in every series.
				continue
			}
			metadata, err := s.readMetadata(value)
			if err != nil {
				return err
			}
			metadata.series = int32(len(s.series))
			series.metadata = int32(len(s.metadata))
			s.metadata = append(s.metadata, metadata)
		case field == 3 && wire == 2:
			// A histogram, whose timestamp is its field 15.
			series.other, series.moreData = true, true
			if err := s.observeTimestamp(value, 15); err != nil {
				return err
			}
		case field == 4 && wire == 2:
			// An exemplar, whose timestamp is its field 3.
			series.other, series.moreData = true, true
			if err := s.observeTimestamp(value, 3); err != nil {
				return err
			}
		case field == 1000 && wire == 0:
			// The table is read before the series.
			ref, _ := binary.Uvarint(value)
			if ref > uint64(len(s.resources)) {
				return errMalformedRW2
			}
			series.resource = uint32(ref)
		default:
			series.other = true
		}
	}
	series.refsTo, series.samplesTo = int32(len(s.refs)), int32(len(s.floats))
	if (series.refsTo-series.refsAt)%2 != 0 {
		return errMalformedRW2
	}
	s.series = append(s.series, series)
	return nil
}

// observeTimestamp takes the varint field of message into the record's oldest timestamp.
func (s *rw2Source) observeTimestamp(message []byte, timestampField uint64) error {
	for len(message) > 0 {
		field, wire, value, next, ok := protoField(message)
		if !ok {
			return errMalformedRW2
		}
		message = next
		if field == timestampField && wire == 0 {
			timestamp, _ := binary.Uvarint(value)
			s.minTimestamp = min(s.minTimestamp, int64(timestamp))
		}
	}
	return nil
}

// metricMetadataID implements floatSource: a record's series with the same metadata have the same symbol references.
func (s *rw2Source) metricMetadataID(index int, memo *metadataMemo) uint32 {
	at := s.series[index].metadata
	if at < 0 {
		return 0
	}
	m := &s.metadata[at]
	return memo.byRefs([3]uint32{uint32(m.metricType), m.helpRef, m.unitRef}, mimirpb.SeriesMetadata{Type: m.metricType, Help: m.help, Unit: m.unit})
}

// resource implements floatSource: a record's series with the same resource have the same ref.
func (s *rw2Source) resource(index int, memo *resourceMemo, t int64) *resourceObservation {
	ref := s.series[index].resource
	if ref == 0 {
		return nil
	}
	return memo.byRecordRef(ref, s.resources[ref-1], t)
}

func (s *rw2Source) readMetadata(message []byte) (rw2Metadata, error) {
	var metadata rw2Metadata
	for len(message) > 0 {
		field, wire, value, next, ok := protoField(message)
		if !ok {
			return metadata, errMalformedRW2
		}
		message = next
		if wire != 0 {
			continue
		}
		number, _ := binary.Uvarint(value)
		switch field {
		case 1:
			metadata.metricType = mimirpb.MetricMetadata_MetricType(number)
		case 3, 4:
			symbol, ok := s.symbol(uint32(number))
			if !ok {
				return metadata, errMalformedRW2
			}
			if field == 3 {
				metadata.help, metadata.helpRef = symbol, uint32(number)
			} else {
				metadata.unit, metadata.unitRef = symbol, uint32(number)
			}
		}
	}
	return metadata, nil
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
	// The strings refer to the content.
	clear(source.symbols)
	clear(source.metadata)
	clear(source.resources)
	source.common, source.content = nil, nil
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

// Complete reports whether the record was read whole: one with fields of its own this doesn't read is only read by
// decoding it.
func (r *RW2Record) Complete() bool { return !r.source.partial }

// MinTimestamp is the oldest timestamp of the record's samples, histograms and exemplars, math.MaxInt64 without any.
func (r *RW2Record) MinTimestamp() int64 { return r.source.minTimestamp }

// Metadata is the metric families' metadata of the record, as decoding it without deduplicating nor normalizing the
// metric names gives: one for each series without data with any, in order. Series with data carry their own.
func (r *RW2Record) Metadata() []*mimirpb.MetricMetadata {
	source := r.source
	if len(source.metadata) == 0 {
		return nil
	}
	var pairs [][2]string
	out := make([]*mimirpb.MetricMetadata, 0, len(source.metadata))
	for _, metadata := range source.metadata {
		if series := &source.series[metadata.series]; series.samplesTo > series.samplesAt || series.moreData {
			continue
		}
		var name string
		pairs, name = source.pairs(int(metadata.series), pairs[:0])
		if name == "" || metadata.help == "" && metadata.unit == "" && metadata.metricType == 0 {
			continue
		}
		out = append(out, &mimirpb.MetricMetadata{
			MetricFamilyName: strings.Clone(name),
			Help:             strings.Clone(metadata.help),
			Unit:             strings.Clone(metadata.unit),
			Type:             metadata.metricType,
		})
	}
	return out
}

// Series returns the labels and floats of the series at index, which refer to the record's content.
func (r *RW2Record) Series(index int) ([]mimirpb.LabelAdapter, []mimirpb.Sample) {
	pairs, _ := r.source.pairs(index, nil)
	adapters := make([]mimirpb.LabelAdapter, len(pairs))
	for i, pair := range pairs {
		adapters[i] = mimirpb.LabelAdapter{Name: pair[0], Value: pair[1]}
	}
	return adapters, r.source.samples(index)
}

// Subset appends to into the content of a record with the series at indices, which are in order, and the record's
// symbols and fields of its own: what decoding only those series needs.
func (r *RW2Record) Subset(indices []int, into []byte) []byte {
	// The exact size first: the symbols are most of a record, which growing the copy would copy again.
	size := 0
	r.subsetFields(indices, func(raw []byte) { size += len(raw) })
	into = slices.Grow(into[:0], size)
	r.subsetFields(indices, func(raw []byte) { into = append(into, raw...) })
	return into
}

// subsetFields calls visit with the encoding of each of the record's fields Subset keeps.
func (r *RW2Record) subsetFields(indices []int, visit func(raw []byte)) {
	rest, series := r.source.content, 0
	for len(rest) > 0 {
		field, _, _, next, _ := protoField(rest)
		raw := rest[:len(rest)-len(next)]
		rest = next
		if field == 5 {
			if len(indices) > 0 && indices[0] == series {
				visit(raw)
				indices = indices[1:]
			}
			series++
			continue
		}
		visit(raw)
	}
}

// AppendRW2 is AppendFloats for the series of a record. Series are identified by their index in the record; leftover
// has the ones it didn't handle, which the caller decodes and appends another way, those with more than floats among
// them. Series with nothing to append are neither.
func (e *Engine) AppendRW2(record *RW2Record, minTimestampMs, maxTimestampMs, ingestedAtMs int64, otlp bool, sink FloatSink) (leftover []int, ingested int, err error) {
	source := record.source
	fast := make([]int, 0, len(source.series))
	for index := range source.series {
		switch series := &source.series[index]; {
		case series.other:
			leftover = append(leftover, index)
		case series.samplesTo > series.samplesAt:
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
