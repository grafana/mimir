// SPDX-License-Identifier: AGPL-3.0-only

// Package record decodes Mimir's ingest-storage Kafka records into what the store applies.
package record

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
	"unicode/utf8"
	"unsafe"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/grafana/mimir/pkg/mimirpb"
)

const symbolOffset = 64

var commonSymbols = []string{
	"",
	"__name__",
	"__aggregation__",
	"<aggregated>",
	"le",
	"component",
	"cortex_request_duration_seconds_bucket",
	"storage_operation_duration_seconds_bucket",
	"grafana",
	"asserts_env",
	"asserts_request_context",
	"asserts_source",
	"asserts_entity_type",
	"asserts_request_type",
	"name",
	"image",
	"cluster",
	"namespace",
	"pod",
	"job",
	"instance",
	"container",
	"replicaset",
	"interface",
	"status_code",
	"resource",
	"operation",
	"method",
	"kube-system",
	"kube-system/cadvisor",
	"node-exporter",
	"node-exporter/node-exporter",
	"kube-system/kubelet",
	"kube-system/node-local-dns",
	"kube-state-metrics/kube-state-metrics",
	"default/kubernetes",
}

// DecodedSeries is one series of a record. Its label names and values share the record's buffer:
// records carry every label of every series, and most belong to series the store already has, so
// copying each one into its own allocation was most of the decoding cost.
type DecodedSeries struct {
	Labels           [][2]string
	Samples          []mimirpb.Sample
	Histograms       []mimirpb.Histogram
	Exemplars        []mimirpb.Exemplar
	CreatedTimestamp int64
}

type DecodedRequest struct {
	Source   int32
	Series   []DecodedSeries
	Metadata []mimirpb.MetricMetadata
}

// Span is where a series' encoded labels are in its record, when they are one run of fields.
type Span struct {
	Start, End int
	OK         bool
}

var errUnderflow = errors.New("buffer underflow")

// DecodeRecord decodes a record of the given ingest-storage version, copying it first so the
// result doesn't keep the caller's buffer.
func DecodeRecord(version uint32, bytes []byte) (DecodedRequest, error) {
	return DecodeRecordBytes(version, append([]byte(nil), bytes...))
}

// DecodeRecordBytes is like DecodeRecord, with label fields as views of bytes, which must not
// change afterwards.
func DecodeRecordBytes(version uint32, bytes []byte) (DecodedRequest, error) {
	if version > 2 {
		return DecodedRequest{}, fmt.Errorf("unsupported ingest-storage record version %d", version)
	}
	if version < 2 {
		request, err := decodeV1(bytes, nil)
		if err != nil {
			return DecodedRequest{}, fmt.Errorf("decode WriteRequest: %w", err)
		}
		return request, nil
	}
	request, err := decodeV2(bytes)
	if err != nil {
		return DecodedRequest{}, fmt.Errorf("decode WriteRequest: %w", err)
	}
	return request, nil
}

// DecodeRecordWithLabelSpans is like DecodeRecordBytes, with where each series' encoded labels
// are in bytes, when they are one run of fields. The same bytes always decode to the same labels.
func DecodeRecordWithLabelSpans(version uint32, bytes []byte) (DecodedRequest, []Span, error) {
	if version != 1 && version != 0 {
		request, err := DecodeRecordBytes(version, bytes)
		if err != nil {
			return DecodedRequest{}, nil, err
		}
		return request, make([]Span, len(request.Series)), nil
	}
	spans := make([]Span, 0, 8)
	request, err := decodeV1(bytes, &spans)
	if err != nil {
		return DecodedRequest{}, nil, fmt.Errorf("decode WriteRequest: %w", err)
	}
	return request, spans, nil
}

// Records are mostly series labels, and building protobuf messages for them and converting those
// cost several times reading the fields directly. Decodes like the generated WriteRequest
// decoding, with the rarer messages left to it.
func decodeV1(record []byte, spans *[]Span) (DecodedRequest, error) {
	var request DecodedRequest
	var metadata []mimirpb.MetricMetadata
	// A record's series share its label and sample arrays, and the series slice is sized up
	// front: allocating each per series was most of what ingestion left for the GC.
	count := countFields(record)
	arena := newArena(count.labels, count.samples)
	if count.series > 0 {
		request.Series = make([]DecodedSeries, 0, count.series)
		if spans != nil && cap(*spans) < count.series {
			*spans = make([]Span, 0, count.series)
		}
	}
	buf := record
	for len(buf) > 0 {
		tag, wireType, rest, err := key(buf)
		if err != nil {
			return DecodedRequest{}, err
		}
		buf = rest
		switch tag {
		case 1:
			field, rest, err := lengthDelimited(wireType, buf)
			if err != nil {
				return DecodedRequest{}, err
			}
			buf = rest
			series, span, err := decodeSeries(record, field, &arena)
			if err != nil {
				return DecodedRequest{}, err
			}
			request.Series = append(request.Series, series)
			if spans != nil {
				*spans = append(*spans, span)
			}
		case 2:
			value, rest, err := varint(wireType, buf)
			if err != nil {
				return DecodedRequest{}, err
			}
			buf = rest
			request.Source = int32(value)
		case 3:
			field, rest, err := lengthDelimited(wireType, buf)
			if err != nil {
				return DecodedRequest{}, err
			}
			buf = rest
			item, err := decodeMetadata(field)
			if err != nil {
				return DecodedRequest{}, err
			}
			metadata = append(metadata, item)
		// Remote write 2 fields, which a version 1 record doesn't use but must still be valid.
		case 4:
			field, rest, err := lengthDelimited(wireType, buf)
			if err != nil {
				return DecodedRequest{}, err
			}
			buf = rest
			if !utf8.Valid(field) {
				return DecodedRequest{}, errors.New("invalid string value: data is not UTF-8 encoded")
			}
		case 5:
			field, rest, err := lengthDelimited(wireType, buf)
			if err != nil {
				return DecodedRequest{}, err
			}
			buf = rest
			if _, err := decodeSeriesRW2(field); err != nil {
				return DecodedRequest{}, err
			}
		default:
			rest, err := skip(tag, wireType, buf)
			if err != nil {
				return DecodedRequest{}, err
			}
			buf = rest
		}
	}
	request.Metadata = v1Metadata(metadata)
	return request, nil
}

func v1Metadata(metadata []mimirpb.MetricMetadata) []mimirpb.MetricMetadata {
	kept := metadata[:0]
	for _, item := range metadata {
		item.MetricFamilyName = normalizeMetadataName(item.MetricFamilyName, int32(item.Type))
		if item.MetricFamilyName != "" && (item.Type != 0 || item.Help != "" || item.Unit != "") {
			kept = append(kept, item)
		}
	}
	if len(kept) == 0 {
		return nil
	}
	return kept
}

func normalizeMetadataName(name string, metricType int32) string {
	var suffixes []string
	switch metricType {
	case 5:
		suffixes = []string{"_count", "_sum"}
	case 3:
		suffixes = []string{"_bucket", "_count", "_sum"}
	case 4:
		suffixes = []string{"_bucket", "_gcount", "_gsum"}
	}
	for _, suffix := range suffixes {
		if trimmed, ok := strings.CutSuffix(name, suffix); ok {
			return trimmed
		}
	}
	return name
}

// The parts of an RW2 series the store uses.
type seriesRW2 struct {
	labelsRefs       []uint32
	samples          []mimirpb.Sample
	histograms       []mimirpb.Histogram
	exemplars        []exemplarRW2
	metadata         *metadataRW2
	createdTimestamp int64
}

type exemplarRW2 struct {
	labelsRefs []uint32
	value      float64
	timestamp  int64
}

type metadataRW2 struct {
	metricType int32
	helpRef    uint32
	unitRef    uint32
}

func decodeV2(record []byte) (DecodedRequest, error) {
	var (
		source  int32
		symbols []string
		series  []seriesRW2
	)
	buf := record
	for len(buf) > 0 {
		tag, wireType, rest, err := key(buf)
		if err != nil {
			return DecodedRequest{}, err
		}
		buf = rest
		switch tag {
		case 1:
			field, rest, err := lengthDelimited(wireType, buf)
			if err != nil {
				return DecodedRequest{}, err
			}
			buf = rest
			scratch := newArena(0, 0)
			if _, _, err := decodeSeries(record, field, &scratch); err != nil {
				return DecodedRequest{}, err
			}
		case 2:
			value, rest, err := varint(wireType, buf)
			if err != nil {
				return DecodedRequest{}, err
			}
			buf = rest
			source = int32(value)
		case 3:
			field, rest, err := lengthDelimited(wireType, buf)
			if err != nil {
				return DecodedRequest{}, err
			}
			buf = rest
			if _, err := decodeMetadata(field); err != nil {
				return DecodedRequest{}, err
			}
		case 4:
			field, rest, err := lengthDelimited(wireType, buf)
			if err != nil {
				return DecodedRequest{}, err
			}
			buf = rest
			if !utf8.Valid(field) {
				return DecodedRequest{}, errors.New("invalid string value: data is not UTF-8 encoded")
			}
			// Each symbol is converted once, and labels share it.
			symbols = append(symbols, view(field))
		case 5:
			field, rest, err := lengthDelimited(wireType, buf)
			if err != nil {
				return DecodedRequest{}, err
			}
			buf = rest
			decoded, err := decodeSeriesRW2(field)
			if err != nil {
				return DecodedRequest{}, err
			}
			series = append(series, decoded)
		default:
			rest, err := skip(tag, wireType, buf)
			if err != nil {
				return DecodedRequest{}, err
			}
			buf = rest
		}
	}
	symbol := func(reference uint32) (string, error) {
		if int(reference) < len(commonSymbols) {
			return commonSymbols[reference], nil
		}
		if reference < symbolOffset {
			return "", fmt.Errorf("reserved RW2 symbol reference %d", reference)
		}
		index := int(reference - symbolOffset)
		if index >= len(symbols) {
			return "", fmt.Errorf("RW2 symbol reference %d out of range", reference)
		}
		return symbols[index], nil
	}
	request := DecodedRequest{Source: source, Series: make([]DecodedSeries, 0, len(series))}
	for _, ts := range series {
		if len(ts.labelsRefs)%2 != 0 {
			return DecodedRequest{}, errors.New("odd number of RW2 label references")
		}
		labels := make([][2]string, 0, len(ts.labelsRefs)/2)
		for i := 0; i < len(ts.labelsRefs); i += 2 {
			name, err := symbol(ts.labelsRefs[i])
			if err != nil {
				return DecodedRequest{}, err
			}
			value, err := symbol(ts.labelsRefs[i+1])
			if err != nil {
				return DecodedRequest{}, err
			}
			labels = append(labels, [2]string{name, value})
		}
		var exemplars []mimirpb.Exemplar
		for _, e := range ts.exemplars {
			if len(e.labelsRefs)%2 != 0 {
				return DecodedRequest{}, errors.New("odd number of RW2 exemplar label references")
			}
			pairs := make([]mimirpb.LabelAdapter, 0, len(e.labelsRefs)/2)
			for i := 0; i < len(e.labelsRefs); i += 2 {
				name, err := symbol(e.labelsRefs[i])
				if err != nil {
					return DecodedRequest{}, err
				}
				value, err := symbol(e.labelsRefs[i+1])
				if err != nil {
					return DecodedRequest{}, err
				}
				pairs = append(pairs, mimirpb.LabelAdapter{Name: name, Value: value})
			}
			exemplars = append(exemplars, mimirpb.Exemplar{Labels: pairs, Value: e.value, TimestampMs: e.timestamp})
		}
		if meta := ts.metadata; meta != nil {
			var metricName string
			for _, pair := range labels {
				if pair[0] == "__name__" {
					metricName = pair[1]
					break
				}
			}
			help, err := symbol(meta.helpRef)
			if err != nil {
				return DecodedRequest{}, err
			}
			unit, err := symbol(meta.unitRef)
			if err != nil {
				return DecodedRequest{}, err
			}
			// Like Mimir's RW2 unmarshalling, every series carries a metadata field, and only one
			// that says something about a named metric becomes metadata.
			if metricName != "" && (meta.metricType != 0 || help != "" || unit != "") {
				request.Metadata = append(request.Metadata, mimirpb.MetricMetadata{
					Type:             mimirpb.MetricMetadata_MetricType(meta.metricType),
					MetricFamilyName: metricName,
					Help:             help,
					Unit:             unit,
				})
			}
		}
		request.Series = append(request.Series, DecodedSeries{
			Labels:           labels,
			Samples:          ts.samples,
			Histograms:       ts.histograms,
			Exemplars:        exemplars,
			CreatedTimestamp: ts.createdTimestamp,
		})
	}
	return request, nil
}

func decodeSeriesRW2(buf []byte) (seriesRW2, error) {
	var series seriesRW2
	for len(buf) > 0 {
		tag, wireType, rest, err := key(buf)
		if err != nil {
			return seriesRW2{}, err
		}
		buf = rest
		switch tag {
		case 1:
			series.labelsRefs, buf, err = uint32s(series.labelsRefs, wireType, buf)
		case 2:
			var field []byte
			if field, buf, err = lengthDelimited(wireType, buf); err == nil {
				var sample mimirpb.Sample
				if sample, err = decodeSample(field); err == nil {
					series.samples = append(series.samples, sample)
				}
			}
		case 3:
			var field []byte
			if field, buf, err = lengthDelimited(wireType, buf); err == nil {
				var histogram mimirpb.Histogram
				if err = histogram.Unmarshal(field); err == nil {
					series.histograms = append(series.histograms, histogram)
				}
			}
		case 4:
			var field []byte
			if field, buf, err = lengthDelimited(wireType, buf); err == nil {
				var exemplar exemplarRW2
				if exemplar, err = decodeExemplarRW2(field); err == nil {
					series.exemplars = append(series.exemplars, exemplar)
				}
			}
		case 5:
			var field []byte
			if field, buf, err = lengthDelimited(wireType, buf); err == nil {
				// Like protobuf, a repeated message field merges into the one before.
				if series.metadata == nil {
					series.metadata = &metadataRW2{}
				}
				err = decodeMetadataRW2(field, series.metadata)
			}
		case 6:
			var value uint64
			if value, buf, err = varint(wireType, buf); err == nil {
				series.createdTimestamp = int64(value)
			}
		default:
			buf, err = skip(tag, wireType, buf)
		}
		if err != nil {
			return seriesRW2{}, err
		}
	}
	return series, nil
}

func decodeExemplarRW2(buf []byte) (exemplarRW2, error) {
	var exemplar exemplarRW2
	for len(buf) > 0 {
		tag, wireType, rest, err := key(buf)
		if err != nil {
			return exemplarRW2{}, err
		}
		buf = rest
		switch tag {
		case 1:
			exemplar.labelsRefs, buf, err = uint32s(exemplar.labelsRefs, wireType, buf)
		case 2:
			exemplar.value, buf, err = double(wireType, buf)
		case 3:
			var value uint64
			if value, buf, err = varint(wireType, buf); err == nil {
				exemplar.timestamp = int64(value)
			}
		default:
			buf, err = skip(tag, wireType, buf)
		}
		if err != nil {
			return exemplarRW2{}, err
		}
	}
	return exemplar, nil
}

func decodeMetadataRW2(buf []byte, metadata *metadataRW2) error {
	for len(buf) > 0 {
		tag, wireType, rest, err := key(buf)
		if err != nil {
			return err
		}
		buf = rest
		var value uint64
		switch tag {
		case 1:
			if value, buf, err = varint(wireType, buf); err == nil {
				metadata.metricType = int32(value)
			}
		case 3:
			if value, buf, err = varint(wireType, buf); err == nil {
				metadata.helpRef = uint32(value)
			}
		case 4:
			if value, buf, err = varint(wireType, buf); err == nil {
				metadata.unitRef = uint32(value)
			}
		default:
			buf, err = skip(tag, wireType, buf)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func decodeMetadata(buf []byte) (mimirpb.MetricMetadata, error) {
	var metadata mimirpb.MetricMetadata
	for len(buf) > 0 {
		tag, wireType, rest, err := key(buf)
		if err != nil {
			return mimirpb.MetricMetadata{}, err
		}
		buf = rest
		var field []byte
		switch tag {
		case 1:
			var value uint64
			if value, buf, err = varint(wireType, buf); err == nil {
				metadata.Type = mimirpb.MetricMetadata_MetricType(int32(value))
			}
		case 2, 4, 5:
			if field, buf, err = lengthDelimited(wireType, buf); err == nil {
				if !utf8.Valid(field) {
					err = errors.New("invalid string value: data is not UTF-8 encoded")
					break
				}
				value := string(field)
				switch tag {
				case 2:
					metadata.MetricFamilyName = value
				case 4:
					metadata.Help = value
				case 5:
					metadata.Unit = value
				}
			}
		default:
			buf, err = skip(tag, wireType, buf)
		}
		if err != nil {
			return mimirpb.MetricMetadata{}, err
		}
	}
	return metadata, nil
}

func decodeSeries(record, buf []byte, arena *arena) (DecodedSeries, Span, error) {
	// Positions in the record, from what is left of the series: an empty slice has no address.
	end := offset(record, buf) + len(buf)
	position := func(rest []byte) int { return end - len(rest) }
	// The labels' fields, while no other field came between them.
	var span Span
	contiguous, afterLabels := true, false
	var series DecodedSeries
	arena.begin()
	for len(buf) > 0 {
		fieldStart := position(buf)
		tag, wireType, rest, err := key(buf)
		if err != nil {
			return DecodedSeries{}, Span{}, err
		}
		buf = rest
		if tag == 1 {
			contiguous = contiguous && !afterLabels
		} else if span.OK {
			afterLabels = true
		}
		switch tag {
		case 1:
			pair, rest, err := lengthDelimited(wireType, buf)
			if err != nil {
				return DecodedSeries{}, Span{}, err
			}
			buf = rest
			if !span.OK {
				span = Span{Start: fieldStart, OK: true}
			}
			span.End = position(buf)
			var name, value []byte
			// The generated encoding writes a pair as its name then its value, each with a
			// one-byte length when short: read that directly, and anything else field by field.
			if n := len(pair); n >= 4 && pair[0] == 0x0a && pair[1] < 0x80 && int(pair[1])+4 <= n {
				nameEnd := 2 + int(pair[1])
				if pair[nameEnd] == 0x12 && pair[nameEnd+1] < 0x80 && nameEnd+2+int(pair[nameEnd+1]) == n {
					name, value, pair = pair[2:nameEnd], pair[nameEnd+2:], nil
				}
			}
			for len(pair) > 0 {
				tag, wireType, rest, err := key(pair)
				if err != nil {
					return DecodedSeries{}, Span{}, err
				}
				pair = rest
				switch tag {
				case 1:
					name, pair, err = lengthDelimited(wireType, pair)
				case 2:
					value, pair, err = lengthDelimited(wireType, pair)
				default:
					pair, err = skip(tag, wireType, pair)
				}
				if err != nil {
					return DecodedSeries{}, Span{}, err
				}
			}
			arena.addLabel([2]string{Label(name), Label(value)})
		case 2:
			field, rest, err := lengthDelimited(wireType, buf)
			if err != nil {
				return DecodedSeries{}, Span{}, err
			}
			buf = rest
			sample, err := decodeSample(field)
			if err != nil {
				return DecodedSeries{}, Span{}, err
			}
			arena.addSample(sample)
		case 3:
			field, rest, err := lengthDelimited(wireType, buf)
			if err != nil {
				return DecodedSeries{}, Span{}, err
			}
			buf = rest
			exemplar, err := decodeExemplar(field)
			if err != nil {
				return DecodedSeries{}, Span{}, err
			}
			series.Exemplars = append(series.Exemplars, exemplar)
		case 4:
			field, rest, err := lengthDelimited(wireType, buf)
			if err != nil {
				return DecodedSeries{}, Span{}, err
			}
			buf = rest
			var histogram mimirpb.Histogram
			if err := histogram.Unmarshal(field); err != nil {
				return DecodedSeries{}, Span{}, err
			}
			series.Histograms = append(series.Histograms, histogram)
		case 6:
			value, rest, err := varint(wireType, buf)
			if err != nil {
				return DecodedSeries{}, Span{}, err
			}
			buf = rest
			series.CreatedTimestamp = int64(value)
		default:
			rest, err := skip(tag, wireType, buf)
			if err != nil {
				return DecodedSeries{}, Span{}, err
			}
			buf = rest
		}
	}
	if !contiguous {
		span = Span{}
	}
	series.Labels, series.Samples = arena.end()
	return series, span, nil
}

type fieldCounts struct{ series, labels, samples int }

// countFields counts a record's series and their labels and samples, skipping everything else,
// so decoding allocates each once. Counts stop at anything unexpected: decoding reports it.
func countFields(record []byte) fieldCounts {
	var count fieldCounts
	for len(record) > 0 {
		tag, wireType, rest, err := key(record)
		if err != nil {
			return count
		}
		if tag != 1 {
			if record, err = skip(tag, wireType, rest); err != nil {
				return count
			}
			continue
		}
		series, rest, err := lengthDelimited(wireType, rest)
		if err != nil {
			return count
		}
		record = rest
		count.series++
		for len(series) > 0 {
			tag, wireType, rest, err := key(series)
			if err != nil {
				break
			}
			switch tag {
			case 1:
				count.labels++
			case 2:
				count.samples++
			}
			if series, err = skip(tag, wireType, rest); err != nil {
				break
			}
		}
	}
	return count
}

// arena holds a record's decoded labels and samples in shared arrays. A full array is replaced
// rather than grown in place, with the current series' part moved over, so the series already
// decoded keep theirs; each series' slices are capped at their length, so appending to one never
// writes into the next.
type arena struct {
	labels      [][2]string
	labelStart  int
	samples     []mimirpb.Sample
	sampleStart int
}

func newArena(labels, samples int) arena {
	return arena{
		labels:  make([][2]string, 0, max(labels, 1)),
		samples: make([]mimirpb.Sample, 0, max(samples, 1)),
	}
}

func (a *arena) begin() {
	a.labelStart, a.sampleStart = len(a.labels), len(a.samples)
}

func (a *arena) addLabel(pair [2]string) {
	if len(a.labels) == cap(a.labels) {
		a.labels = append(make([][2]string, 0, 2*cap(a.labels)), a.labels[a.labelStart:]...)
		a.labelStart = 0
	}
	a.labels = append(a.labels, pair)
}

func (a *arena) addSample(sample mimirpb.Sample) {
	if len(a.samples) == cap(a.samples) {
		a.samples = append(make([]mimirpb.Sample, 0, 2*cap(a.samples)), a.samples[a.sampleStart:]...)
		a.sampleStart = 0
	}
	a.samples = append(a.samples, sample)
}

// end returns the current series' labels and samples.
func (a *arena) end() ([][2]string, []mimirpb.Sample) {
	labels := a.labels[a.labelStart:len(a.labels):len(a.labels)]
	var samples []mimirpb.Sample
	if len(a.samples) > a.sampleStart {
		samples = a.samples[a.sampleStart:len(a.samples):len(a.samples)]
	}
	return labels, samples
}

func decodeSample(buf []byte) (mimirpb.Sample, error) {
	var sample mimirpb.Sample
	for len(buf) > 0 {
		tag, wireType, rest, err := key(buf)
		if err != nil {
			return mimirpb.Sample{}, err
		}
		buf = rest
		switch tag {
		case 1:
			sample.Value, buf, err = double(wireType, buf)
		case 2:
			var value uint64
			if value, buf, err = varint(wireType, buf); err == nil {
				sample.TimestampMs = int64(value)
			}
		default:
			buf, err = skip(tag, wireType, buf)
		}
		if err != nil {
			return mimirpb.Sample{}, err
		}
	}
	return sample, nil
}

// Exemplar labels keep their bytes as they are, like the Go ingester's.
func decodeExemplar(buf []byte) (mimirpb.Exemplar, error) {
	var exemplar mimirpb.Exemplar
	for len(buf) > 0 {
		tag, wireType, rest, err := key(buf)
		if err != nil {
			return mimirpb.Exemplar{}, err
		}
		buf = rest
		switch tag {
		case 1:
			var pair []byte
			if pair, buf, err = lengthDelimited(wireType, buf); err == nil {
				var label mimirpb.LabelAdapter
				for len(pair) > 0 && err == nil {
					var tag protowire.Number
					var wireType protowire.Type
					if tag, wireType, pair, err = key(pair); err != nil {
						break
					}
					var field []byte
					switch tag {
					case 1:
						if field, pair, err = lengthDelimited(wireType, pair); err == nil {
							label.Name = view(field)
						}
					case 2:
						if field, pair, err = lengthDelimited(wireType, pair); err == nil {
							label.Value = view(field)
						}
					default:
						pair, err = skip(tag, wireType, pair)
					}
				}
				exemplar.Labels = append(exemplar.Labels, label)
			}
		case 2:
			exemplar.Value, buf, err = double(wireType, buf)
		case 3:
			var value uint64
			if value, buf, err = varint(wireType, buf); err == nil {
				exemplar.TimestampMs = int64(value)
			}
		default:
			buf, err = skip(tag, wireType, buf)
		}
		if err != nil {
			return mimirpb.Exemplar{}, err
		}
	}
	return exemplar, nil
}

// Label is a label name or value of a record, as a view of its bytes. Invalid UTF-8 is replaced
// like Rust's `String::from_utf8_lossy`, so the store keys series the same way whatever wrote
// them.
func Label(bytes []byte) string {
	if len(bytes) == 0 {
		return ""
	}
	// Label names and values are nearly always ASCII.
	if isASCII(bytes) || utf8.Valid(bytes) {
		return view(bytes)
	}
	return Lossy(bytes)
}

func isASCII(bytes []byte) bool {
	for len(bytes) >= 8 {
		if binary.LittleEndian.Uint64(bytes)&0x8080808080808080 != 0 {
			return false
		}
		bytes = bytes[8:]
	}
	for _, b := range bytes {
		if b >= 0x80 {
			return false
		}
	}
	return true
}

// Lossy replaces each maximal ill-formed subsequence of bytes with U+FFFD, like Rust's
// `String::from_utf8_lossy`: Go's strings.ToValidUTF8 replaces runs of them with one.
func Lossy(bytes []byte) string {
	var out strings.Builder
	out.Grow(len(bytes) + 8)
	for i := 0; i < len(bytes); {
		b := bytes[i]
		if b < 0x80 {
			out.WriteByte(b)
			i++
			continue
		}
		width, lower, upper := 0, byte(0x80), byte(0xbf)
		switch {
		case b >= 0xc2 && b <= 0xdf:
			width = 2
		case b == 0xe0:
			width, lower = 3, 0xa0
		case b >= 0xe1 && b <= 0xec, b == 0xee, b == 0xef:
			width = 3
		case b == 0xed:
			width, upper = 3, 0x9f
		case b == 0xf0:
			width, lower = 4, 0x90
		case b >= 0xf1 && b <= 0xf3:
			width = 4
		case b == 0xf4:
			width, upper = 4, 0x8f
		}
		if width == 0 {
			out.WriteString("�")
			i++
			continue
		}
		// The bytes after the lead that continue it; only the second has the lead's own range.
		valid := 1
		for valid < width && i+valid < len(bytes) {
			next := bytes[i+valid]
			low, high := byte(0x80), byte(0xbf)
			if valid == 1 {
				low, high = lower, upper
			}
			if next < low || next > high {
				break
			}
			valid++
		}
		if valid == width {
			out.Write(bytes[i : i+width])
		} else {
			out.WriteString("�")
		}
		i += valid
	}
	return out.String()
}

func view(bytes []byte) string {
	if len(bytes) == 0 {
		return ""
	}
	return unsafe.String(&bytes[0], len(bytes))
}

// Where buf starts in record, which it must be a part of.
func offset(record, buf []byte) int {
	if len(buf) == 0 {
		return 0
	}
	return int(uintptr(unsafe.Pointer(&buf[0])) - uintptr(unsafe.Pointer(&record[0])))
}

func key(buf []byte) (protowire.Number, protowire.Type, []byte, error) {
	// Records only use field numbers below 16, whose keys take one byte.
	if len(buf) > 0 && buf[0] < 0x80 && buf[0]>>3 != 0 && buf[0]&7 <= 5 {
		return protowire.Number(buf[0] >> 3), protowire.Type(buf[0] & 7), buf[1:], nil
	}
	value, n := protowire.ConsumeVarint(buf)
	if n < 0 {
		return 0, 0, nil, protowire.ParseError(n)
	}
	if value>>3 > math.MaxInt32 || value>>3 == 0 {
		return 0, 0, nil, fmt.Errorf("invalid tag value: %d", value>>3)
	}
	wireType := protowire.Type(value & 7)
	if wireType > 5 {
		return 0, 0, nil, fmt.Errorf("invalid wire type value: %d", wireType)
	}
	return protowire.Number(value >> 3), wireType, buf[n:], nil
}

func checkWireType(expected, actual protowire.Type) error {
	if expected != actual {
		return fmt.Errorf("invalid wire type: %d (expected %d)", actual, expected)
	}
	return nil
}

func varint(wireType protowire.Type, buf []byte) (uint64, []byte, error) {
	if err := checkWireType(protowire.VarintType, wireType); err != nil {
		return 0, nil, err
	}
	value, n := protowire.ConsumeVarint(buf)
	if n < 0 {
		return 0, nil, protowire.ParseError(n)
	}
	return value, buf[n:], nil
}

func double(wireType protowire.Type, buf []byte) (float64, []byte, error) {
	if err := checkWireType(protowire.Fixed64Type, wireType); err != nil {
		return 0, nil, err
	}
	value, n := protowire.ConsumeFixed64(buf)
	if n < 0 {
		return 0, nil, errUnderflow
	}
	return math.Float64frombits(value), buf[n:], nil
}

func lengthDelimited(wireType protowire.Type, buf []byte) ([]byte, []byte, error) {
	// Label names and values are nearly always shorter than 128 bytes, whose length takes one byte.
	if wireType == protowire.BytesType && len(buf) > 0 && buf[0] < 0x80 && int(buf[0]) < len(buf) {
		return buf[1 : 1+int(buf[0])], buf[1+int(buf[0]):], nil
	}
	if err := checkWireType(protowire.BytesType, wireType); err != nil {
		return nil, nil, err
	}
	length, n := protowire.ConsumeVarint(buf)
	if n < 0 {
		return nil, nil, protowire.ParseError(n)
	}
	buf = buf[n:]
	if length > uint64(len(buf)) {
		return nil, nil, errUnderflow
	}
	return buf[:length], buf[length:], nil
}

// uint32s appends a repeated uint32 field, packed or not, like protobuf decoders accept either.
func uint32s(values []uint32, wireType protowire.Type, buf []byte) ([]uint32, []byte, error) {
	if wireType == protowire.BytesType {
		packed, rest, err := lengthDelimited(wireType, buf)
		if err != nil {
			return nil, nil, err
		}
		for len(packed) > 0 {
			value, n := protowire.ConsumeVarint(packed)
			if n < 0 {
				return nil, nil, protowire.ParseError(n)
			}
			values = append(values, uint32(value))
			packed = packed[n:]
		}
		return values, rest, nil
	}
	value, rest, err := varint(wireType, buf)
	if err != nil {
		return nil, nil, err
	}
	return append(values, uint32(value)), rest, nil
}

func skip(tag protowire.Number, wireType protowire.Type, buf []byte) ([]byte, error) {
	if wireType == protowire.EndGroupType {
		return nil, errors.New("unexpected end group")
	}
	n := protowire.ConsumeFieldValue(tag, wireType, buf)
	if n < 0 {
		return nil, protowire.ParseError(n)
	}
	return buf[n:], nil
}
