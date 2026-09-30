// SPDX-License-Identifier: AGPL-3.0-only

package chunks

import (
	"errors"
	"fmt"

	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// Chunk encodings as the wire's cortex.Chunk numbers them, Mimir's `chunk.Encoding`.
const (
	EncodingXOR            int32 = 4
	EncodingHistogram      int32 = 5
	EncodingFloatHistogram int32 = 6
)

// Header is a histogram chunk's counter reset header, in the top two bits of its third byte.
type Header = chunkenc.CounterResetHeader

const (
	HeaderUnknown         = chunkenc.UnknownCounterReset
	HeaderCounterReset    = chunkenc.CounterReset
	HeaderNotCounterReset = chunkenc.NotCounterReset
	HeaderGauge           = chunkenc.GaugeType
)

// HeaderFromHint is the header a chunk starting with a sample of this reset hint gets.
func HeaderFromHint(hint mimirpb.Histogram_ResetHint) Header {
	switch hint {
	case mimirpb.Histogram_YES:
		return HeaderCounterReset
	case mimirpb.Histogram_NO:
		return HeaderNotCounterReset
	case mimirpb.Histogram_GAUGE:
		return HeaderGauge
	default:
		return HeaderUnknown
	}
}

// HintOf is the reset hint of a chunk's first sample that HeaderFromHint turns back into header.
func HintOf(header Header) mimirpb.Histogram_ResetHint {
	switch header {
	case HeaderCounterReset:
		return mimirpb.Histogram_YES
	case HeaderNotCounterReset:
		return mimirpb.Histogram_NO
	case HeaderGauge:
		return mimirpb.Histogram_GAUGE
	default:
		return mimirpb.Histogram_UNKNOWN
	}
}

// EncodedHistogram is a histogram chunk's wire encoding and bytes.
type EncodedHistogram struct {
	Encoding int32
	Data     []byte
}

// Appended is what HistogramAppender.Append did, like Prometheus's AppendHistogram results.
type Appended uint8

const (
	AppendedInChunk Appended = iota
	// AppendedRecoded means the chunk was re-encoded with a wider bucket layout; the appender now
	// holds it.
	AppendedRecoded
	// AppendedNewChunk means the sample needs a new chunk, returned with the sample in it.
	AppendedNewChunk
)

// HistogramAppender is an open histogram or float histogram chunk, appended to with Prometheus's
// own histogram appenders so the bytes and the rules are the Go head's: bucket layouts that only
// gain buckets recode the chunk, counter resets and incompatible layouts start a new one.
type HistogramAppender struct {
	chunk          chunkenc.Chunk
	app            chunkenc.Appender
	float          bool
	firstTimestamp int64
	last           *mimirpb.Histogram
}

// NewEmptyHistogramAppender returns an empty chunk's appender with the given header.
func NewEmptyHistogramAppender(float bool, header Header) *HistogramAppender {
	var chunk chunkenc.Chunk
	if float {
		chunk = chunkenc.NewFloatHistogramChunk()
	} else {
		chunk = chunkenc.NewHistogramChunk()
	}
	app, err := chunk.Appender()
	if err != nil {
		panic(err) // An empty histogram chunk always has an appender.
	}
	// What chunkenc's unexported setCounterResetHeader does.
	chunk.Bytes()[2] = chunk.Bytes()[2]&0x3f | byte(header)
	return &HistogramAppender{chunk: chunk, app: app, float: float}
}

// NewHistogramAppender returns a chunk with h as its first sample. previous is the chunk it
// follows, which decides whether the new chunk starts with a counter reset.
func NewHistogramAppender(h *mimirpb.Histogram, previous *HistogramAppender) *HistogramAppender {
	a := NewEmptyHistogramAppender(h.IsFloatHistogram(), HeaderUnknown)
	var prev chunkenc.Appender
	if previous != nil && previous.float == a.float {
		prev = previous.app
	}
	if _, _, err := a.append(prev, h, false); err != nil {
		panic(err) // Appending to an empty chunk never fails.
	}
	return a
}

// NewHistogramAppenderFromSamples rebuilds an open chunk from its samples, the first carrying the
// chunk header as its hint, as HistogramAppender.Samples returns them.
func NewHistogramAppenderFromSamples(samples []mimirpb.Histogram) (*HistogramAppender, error) {
	if len(samples) == 0 {
		return nil, nil
	}
	a := NewEmptyHistogramAppender(samples[0].IsFloatHistogram(), HeaderFromHint(samples[0].ResetHint))
	for index := range samples {
		if err := a.RawAppend(&samples[index]); err != nil {
			return nil, err
		}
	}
	return a, nil
}

func (a *HistogramAppender) Len() int {
	return a.chunk.NumSamples()
}

func (a *HistogramAppender) IsEmpty() bool {
	return a.chunk.NumSamples() == 0
}

func (a *HistogramAppender) Header() Header {
	return Header(a.chunk.Bytes()[2] & 0xc0)
}

func (a *HistogramAppender) FirstTimestamp() int64 {
	return a.firstTimestamp
}

// Last is the newest sample as appended, with the buckets a wider chunk layout added to it.
func (a *HistogramAppender) Last() *mimirpb.Histogram {
	return a.last
}

// Encoded is the chunk, whose bytes stay valid until the next append.
func (a *HistogramAppender) Encoded() EncodedHistogram {
	encoding := EncodingHistogram
	if a.float {
		encoding = EncodingFloatHistogram
	}
	return EncodedHistogram{Encoding: encoding, Data: a.chunk.Bytes()}
}

func (a *HistogramAppender) EncodedLen() int {
	return len(a.chunk.Bytes())
}

// RawAppend appends a sample that fits the chunk, like one decoded from it.
func (a *HistogramAppender) RawAppend(h *mimirpb.Histogram) error {
	if h.IsFloatHistogram() != a.float {
		return errors.New("histogram type differs from its chunk's")
	}
	next, _, err := a.append(nil, h, true)
	if err != nil {
		return err
	}
	if next != nil {
		return errors.New("histogram does not fit its chunk")
	}
	return nil
}

// Append is Prometheus's AppendHistogram/AppendFloatHistogram with appendOnly false. With
// AppendedNewChunk, the returned appender holds h and this one is complete.
func (a *HistogramAppender) Append(h *mimirpb.Histogram) (Appended, *HistogramAppender, error) {
	if h.IsFloatHistogram() != a.float && !a.IsEmpty() {
		return AppendedNewChunk, NewHistogramAppender(h, nil), nil
	}
	newChunk, recoded, err := a.append(nil, h, false)
	switch {
	case err != nil:
		return AppendedInChunk, nil, err
	case recoded:
		return AppendedRecoded, nil, nil
	case newChunk != nil:
		return AppendedNewChunk, newChunk, nil
	default:
		return AppendedInChunk, nil, nil
	}
}

// append appends h through chunkenc. A chunk chunkenc recodes into replaces this one's; one it
// cuts is returned with h in it, leaving this one as it was.
func (a *HistogramAppender) append(prev chunkenc.Appender, h *mimirpb.Histogram, appendOnly bool) (*HistogramAppender, bool, error) {
	var (
		newChunk chunkenc.Chunk
		recoded  bool
		app      chunkenc.Appender
		err      error
		last     mimirpb.Histogram
	)
	// Copied: chunkenc widens the layout of the histogram it's given in place.
	if a.float {
		model := mimirpb.FromFloatHistogramProtoToFloatHistogram(h).Copy()
		newChunk, recoded, app, err = a.app.AppendFloatHistogram(prev, 0, h.Timestamp, model, appendOnly)
		last = mimirpb.FromFloatHistogramToHistogramProto(h.Timestamp, model)
	} else {
		model := mimirpb.FromHistogramProtoToHistogram(h).Copy()
		newChunk, recoded, app, err = a.app.AppendHistogram(prev, 0, h.Timestamp, model, appendOnly)
		last = mimirpb.FromHistogramToHistogramProto(h.Timestamp, model)
	}
	if err != nil {
		return nil, false, fmt.Errorf("append histogram: %w", err)
	}
	if newChunk != nil && !recoded {
		return &HistogramAppender{
			chunk:          newChunk,
			app:            app,
			float:          a.float,
			firstTimestamp: h.Timestamp,
			last:           &last,
		}, false, nil
	}
	if a.last == nil {
		a.firstTimestamp = h.Timestamp
	}
	if newChunk != nil {
		a.chunk = newChunk
	}
	a.app = app
	a.last = &last
	return nil, recoded, nil
}

// Samples returns the open chunk's samples for NewHistogramAppenderFromSamples: the first one
// carries the chunk header as its reset hint.
func (a *HistogramAppender) Samples() ([]mimirpb.Histogram, error) {
	encoded := a.Encoded()
	samples, err := DecodeHistograms(encoded.Encoding, encoded.Data)
	if err != nil {
		return nil, err
	}
	if len(samples) > 0 {
		samples[0].ResetHint = HintOf(a.Header())
	}
	return samples, nil
}

// EncodeHistogram encodes one histogram into a chunk.
func EncodeHistogram(h *mimirpb.Histogram) (EncodedHistogram, error) {
	return EncodeHistogramSequence([]mimirpb.Histogram{*h})
}

// EncodeHistogramSequence encodes histograms of one layout into one chunk, with the header of the
// first one's reset hint.
func EncodeHistogramSequence(histograms []mimirpb.Histogram) (EncodedHistogram, error) {
	a, err := NewHistogramAppenderFromSamples(histograms)
	if err != nil {
		return EncodedHistogram{}, err
	}
	return a.Encoded(), nil
}

// IsValidHistogram is Prometheus's Histogram.Validate and FloatHistogram.Validate, which its head
// appender runs before appending.
func IsValidHistogram(h *mimirpb.Histogram) bool {
	if h.IsFloatHistogram() {
		return mimirpb.FromFloatHistogramProtoToFloatHistogram(h).Validate() == nil
	}
	return mimirpb.FromHistogramProtoToHistogram(h).Validate() == nil
}

// EqualHistogramValues is Prometheus's Histogram.Equals and FloatHistogram.Equals: the same values
// and bucket layout, whatever the reset hint or zero-length spans.
func EqualHistogramValues(a, b *mimirpb.Histogram) bool {
	if a.IsFloatHistogram() != b.IsFloatHistogram() {
		return false
	}
	if a.IsFloatHistogram() {
		return mimirpb.FromFloatHistogramProtoToFloatHistogram(a).Equals(mimirpb.FromFloatHistogramProtoToFloatHistogram(b))
	}
	return mimirpb.FromHistogramProtoToHistogram(a).Equals(mimirpb.FromHistogramProtoToHistogram(b))
}

// DecodeHistograms decodes a histogram (encoding 5) or float histogram (6) chunk, with the reset
// hints and stale markers the Prometheus iterator reports: gauge chunks mark every sample as a
// gauge, other chunks leave the first sample's hint unknown and mark the others as not a counter
// reset.
func DecodeHistograms(encoding int32, data []byte) ([]mimirpb.Histogram, error) {
	var chunkEncoding chunkenc.Encoding
	switch encoding {
	case EncodingHistogram:
		chunkEncoding = chunkenc.EncHistogram
	case EncodingFloatHistogram:
		chunkEncoding = chunkenc.EncFloatHistogram
	default:
		return nil, fmt.Errorf("not a histogram chunk encoding: %d", encoding)
	}
	if len(data) < 3 {
		return nil, errors.New("histogram chunk too short")
	}
	chunk, err := chunkenc.FromData(chunkEncoding, data)
	if err != nil {
		return nil, err
	}
	result := make([]mimirpb.Histogram, 0, chunk.NumSamples())
	iterator := chunk.Iterator(nil)
	for {
		switch iterator.Next() {
		case chunkenc.ValHistogram:
			t, h := iterator.AtHistogram(nil)
			result = append(result, mimirpb.FromHistogramToHistogramProto(t, h))
		case chunkenc.ValFloatHistogram:
			t, h := iterator.AtFloatHistogram(nil)
			result = append(result, mimirpb.FromFloatHistogramToHistogramProto(t, h))
		case chunkenc.ValNone:
			if err := iterator.Err(); err != nil {
				return nil, fmt.Errorf("decode histogram chunk: %w", err)
			}
			if len(result) != chunk.NumSamples() {
				return nil, errors.New("histogram chunk truncated")
			}
			return result, nil
		default:
			return nil, errors.New("unexpected sample type in histogram chunk")
		}
	}
}
