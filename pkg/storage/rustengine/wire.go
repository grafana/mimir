// SPDX-License-Identifier: AGPL-3.0-only
//go:build rustengine && cgo

package rustengine

import (
	"encoding/binary"
	"errors"
	"math"
	"unsafe"

	"github.com/grafana/mimir/pkg/mimirpb"
)

const wireMagic = 0x334d494d

type wireWriter []byte

func (w *wireWriter) u8(v uint8)         { *w = append(*w, v) }
func (w *wireWriter) u64(v uint64)       { *w = binary.LittleEndian.AppendUint64(*w, v) }
func (w *wireWriter) i64(v int64)        { w.u64(uint64(v)) }
func (w *wireWriter) int(v int)          { w.u64(uint64(v)) }
func (w *wireWriter) int64usize(v int64) { w.i64(v) }
func (w *wireWriter) u64usize(v uint64)  { w.u64(v) }
func (w *wireWriter) bool(v bool) {
	if v {
		w.u8(1)
	} else {
		w.u8(0)
	}
}
func (w *wireWriter) count(v int) {
	if uint64(v) > math.MaxUint32 {
		panic("Rust frame too large")
	}
	*w = binary.LittleEndian.AppendUint32(*w, uint32(v))
}
func (w *wireWriter) string(v string) { w.count(len(v)); *w = append(*w, v...) }

type wireReader struct {
	data []byte
	err  error
}

func (r *wireReader) take(n int) []byte {
	if r.err != nil {
		return nil
	}
	if n < 0 || n > len(r.data) {
		r.err = errors.New("truncated Rust frame")
		return nil
	}
	b := r.data[:n:n]
	r.data = r.data[n:]
	return b
}
func (r *wireReader) u8() uint8 {
	b := r.take(1)
	if b == nil {
		return 0
	}
	return b[0]
}
func (r *wireReader) u64() uint64 {
	b := r.take(8)
	if b == nil {
		return 0
	}
	return binary.LittleEndian.Uint64(b)
}
func (r *wireReader) i64() int64 { return int64(r.u64()) }
func (r *wireReader) int() int {
	v := r.u64()
	if v > uint64(^uint(0)>>1) {
		r.err = errors.New("Rust integer overflow")
		return 0
	}
	return int(v)
}
func (r *wireReader) int64usize() int64 {
	v := r.u64()
	if v > math.MaxInt64 {
		r.err = errors.New("Rust integer overflow")
		return 0
	}
	return int64(v)
}
func (r *wireReader) u64usize() uint64 { return r.u64() }
func (r *wireReader) bool() bool {
	v := r.u8()
	if v > 1 {
		r.err = errors.New("invalid Rust boolean")
	}
	return v == 1
}
func (r *wireReader) count() int {
	b := r.take(4)
	if b == nil {
		return 0
	}
	n := uint64(binary.LittleEndian.Uint32(b))
	if n > uint64(len(r.data)) {
		r.err = errors.New("invalid Rust collection length")
		return 0
	}
	return int(n)
}
func (r *wireReader) string() string {
	b := r.take(r.count())
	// Responses have an unpooled Go-owned frame, so strings retain stable memory after native free.
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func (w *wireWriter) Label(v Label) {
	w.string(v.Name)
	w.string(v.Value)
}
func (r *wireReader) Label() (v Label) {
	v.Name = r.string()
	v.Value = r.string()
	return
}

func (w *wireWriter) Sample(v Sample) {
	w.i64(v.Timestamp)
	w.u8(v.Kind)
	w.u64(v.Bits)
	w.string(v.Data)
}
func (r *wireReader) Sample() (v Sample) {
	v.Timestamp = r.i64()
	v.Kind = r.u8()
	v.Bits = r.u64()
	v.Data = r.string()
	return
}

func (w *wireWriter) Exemplar(v Exemplar) {
	w.i64(v.Timestamp)
	w.u64(v.Bits)
	w.count(len(v.Labels))
	for _, v := range v.Labels {
		w.Label(v)
	}
}
func (r *wireReader) Exemplar() (v Exemplar) {
	v.Timestamp = r.i64()
	v.Bits = r.u64()
	v.Labels = make([]Label, r.count())
	for i := range v.Labels {
		v.Labels[i] = r.Label()
	}
	return
}

func (w *wireWriter) Series(v Series) {
	w.u64(v.Ref)
	w.count(len(v.Labels))
	for _, v := range v.Labels {
		w.Label(v)
	}
	w.count(len(v.Samples))
	for k, v := range v.Samples {
		w.i64(k)
		w.Sample(v)
	}
	w.count(len(v.Exemplars))
	for _, v := range v.Exemplars {
		w.Exemplar(v)
	}
}
func (r *wireReader) Series() (v Series) {
	v.Ref = r.u64()
	v.Labels = make([]Label, r.count())
	for i := range v.Labels {
		v.Labels[i] = r.Label()
	}
	n := r.count()
	if n > 0 {
		v.Samples = make(map[int64]Sample)
	}
	for ; n > 0 && r.err == nil; n-- {
		k := r.i64()
		v.Samples[k] = r.Sample()
	}
	v.Exemplars = make([]Exemplar, r.count())
	for i := range v.Exemplars {
		v.Exemplars[i] = r.Exemplar()
	}
	return
}

func (w *wireWriter) FloatSeries(v FloatSeries) {
	w.int(v.Index)
	size := 4
	for _, l := range v.Labels {
		size += 8 + len(l.Name) + len(l.Value)
	}
	w.count(size)
	w.count(len(v.Labels))
	for _, l := range v.Labels {
		w.string(l.Name)
		w.string(l.Value)
	}
	w.count(len(v.Samples))
	for _, sample := range v.Samples {
		w.i64(sample.TimestampMs)
		w.u8(1)
		w.u64(math.Float64bits(sample.Value))
		w.string("")
	}
}
func (r *wireReader) FloatSeries() (v FloatSeries) {
	v.Index = r.int()
	key := wireReader{data: r.take(r.count())}
	v.Labels = make([]mimirpb.LabelAdapter, key.count())
	for i := range v.Labels {
		v.Labels[i] = mimirpb.LabelAdapter{Name: key.string(), Value: key.string()}
	}
	if key.err != nil {
		r.err = key.err
	} else if len(key.data) != 0 {
		r.err = errors.New("trailing Rust label key bytes")
	}
	v.Samples = make([]mimirpb.Sample, r.count())
	for i := range v.Samples {
		sample := r.Sample()
		if sample.Kind != 1 || sample.Data != "" {
			r.err = errors.New("invalid Rust float frame sample")
		}
		v.Samples[i] = mimirpb.Sample{TimestampMs: sample.Timestamp, Value: math.Float64frombits(sample.Bits)}
	}
	return
}

func (w *wireWriter) SampleError(v SampleError) {
	w.i64(v.Timestamp)
	w.string(v.Error)
}
func (r *wireReader) SampleError() (v SampleError) {
	v.Timestamp = r.i64()
	v.Error = r.string()
	return
}

func (w *wireWriter) FloatResult(v FloatResult) {
	w.int(v.Index)
	w.u64(v.Ref)
	w.int(v.Accepted)
	w.count(len(v.Errors))
	for _, v := range v.Errors {
		w.SampleError(v)
	}
}
func (r *wireReader) FloatResult() (v FloatResult) {
	v.Index = r.int()
	v.Ref = r.u64()
	v.Accepted = r.int()
	v.Errors = make([]SampleError, r.count())
	for i := range v.Errors {
		v.Errors[i] = r.SampleError()
	}
	return
}

func (w *wireWriter) QueryChunk(v QueryChunk) {
	w.i64(v.Mint)
	w.i64(v.Maxt)
	w.count(len(v.Data))
	for _, v := range v.Data {
		w.u8(v)
	}
	w.count(len(v.Samples))
	for _, v := range v.Samples {
		w.Sample(v)
	}
}
func (r *wireReader) QueryChunk() (v QueryChunk) {
	v.Mint = r.i64()
	v.Maxt = r.i64()
	v.Data = r.take(r.count())
	v.Samples = make([]Sample, r.count())
	for i := range v.Samples {
		v.Samples[i] = r.Sample()
	}
	return
}

func (w *wireWriter) Request(v Request) {
	w.string(v.Op)
	w.u64(v.Ref)
	w.count(len(v.Labels))
	for _, v := range v.Labels {
		w.Label(v)
	}
	w.bool(v.Sample != nil)
	if v.Sample != nil {
		w.Sample((*v.Sample))
	}
	w.bool(v.Exemplar != nil)
	if v.Exemplar != nil {
		w.Exemplar((*v.Exemplar))
	}
	w.bool(v.RejectOOO)
	w.i64(v.Window)
	w.int64usize(v.MaxExemplars)
	w.count(len(v.Refs))
	for _, v := range v.Refs {
		w.u64(v)
	}
	w.i64(v.UpTo)
	w.i64(v.Mint)
	w.i64(v.Maxt)
	w.string(v.Name)
	w.count(len(v.Values))
	for _, v := range v.Values {
		w.string(v)
	}
	w.bool(v.IncludeCold)
	w.bool(v.Filter)
	w.count(len(v.Batch))
	for _, v := range v.Batch {
		w.FloatSeries(v)
	}
	w.int(v.Limit)
	w.u64(v.Cursor)
	w.bool(v.Sorted)
	w.bool(v.Unordered)
}
func (r *wireReader) Request() (v Request) {
	v.Op = r.string()
	v.Ref = r.u64()
	v.Labels = make([]Label, r.count())
	for i := range v.Labels {
		v.Labels[i] = r.Label()
	}
	if r.bool() {
		value := r.Sample()
		v.Sample = &value
	}
	if r.bool() {
		value := r.Exemplar()
		v.Exemplar = &value
	}
	v.RejectOOO = r.bool()
	v.Window = r.i64()
	v.MaxExemplars = r.int64usize()
	v.Refs = make([]uint64, r.count())
	for i := range v.Refs {
		v.Refs[i] = r.u64()
	}
	v.UpTo = r.i64()
	v.Mint = r.i64()
	v.Maxt = r.i64()
	v.Name = r.string()
	v.Values = make([]string, r.count())
	for i := range v.Values {
		v.Values[i] = r.string()
	}
	v.IncludeCold = r.bool()
	v.Filter = r.bool()
	v.Batch = make([]FloatSeries, r.count())
	for i := range v.Batch {
		v.Batch[i] = r.FloatSeries()
	}
	v.Limit = r.int()
	v.Cursor = r.u64()
	v.Sorted = r.bool()
	v.Unordered = r.bool()
	return
}

func (w *wireWriter) Response(v Response) {
	w.string(v.Error)
	w.u64(v.Ref)
	w.count(len(v.Series))
	for _, v := range v.Series {
		w.Series(v)
	}
	w.bool(v.Active)
	w.count(len(v.Refs))
	for _, v := range v.Refs {
		w.u64(v)
	}
	w.count(len(v.Values))
	for _, v := range v.Values {
		w.string(v)
	}
	w.count(len(v.Results))
	for _, v := range v.Results {
		w.FloatResult(v)
	}
	w.count(len(v.Leftovers))
	for _, v := range v.Leftovers {
		w.int(v)
	}
	w.bool(v.Sample != nil)
	if v.Sample != nil {
		w.Sample((*v.Sample))
	}
	w.u64usize(v.Count)
	w.i64(v.Mint)
	w.i64(v.Maxt)
	w.i64(v.MinValid)
	w.count(len(v.Bytes))
	for k, v := range v.Bytes {
		w.string(k)
		w.u64(v)
	}
	w.u64(v.Cursor)
	w.bool(v.Done)
	w.count(len(v.Chunks))
	for _, v := range v.Chunks {
		w.QueryChunk(v)
	}
}
func (r *wireReader) Response() (v Response) {
	v.Error = r.string()
	v.Ref = r.u64()
	v.Series = make([]Series, r.count())
	for i := range v.Series {
		v.Series[i] = r.Series()
	}
	v.Active = r.bool()
	v.Refs = make([]uint64, r.count())
	for i := range v.Refs {
		v.Refs[i] = r.u64()
	}
	v.Values = make([]string, r.count())
	for i := range v.Values {
		v.Values[i] = r.string()
	}
	v.Results = make([]FloatResult, r.count())
	for i := range v.Results {
		v.Results[i] = r.FloatResult()
	}
	v.Leftovers = make([]int, r.count())
	for i := range v.Leftovers {
		v.Leftovers[i] = r.int()
	}
	if r.bool() {
		value := r.Sample()
		v.Sample = &value
	}
	v.Count = r.u64usize()
	v.Mint = r.i64()
	v.Maxt = r.i64()
	v.MinValid = r.i64()
	n := r.count()
	if n > 0 {
		v.Bytes = make(map[string]uint64)
	}
	for ; n > 0 && r.err == nil; n-- {
		k := r.string()
		v.Bytes[k] = r.u64()
	}
	v.Cursor = r.u64()
	v.Done = r.bool()
	v.Chunks = make([]QueryChunk, r.count())
	for i := range v.Chunks {
		v.Chunks[i] = r.QueryChunk()
	}
	return
}

func encodeRequest(dst []byte, req Request) []byte {
	w := wireWriter(binary.LittleEndian.AppendUint32(dst[:0], wireMagic))
	w.Request(req)
	return w
}
func decodeResponse(data []byte) (Response, error) {
	r := wireReader{data: append([]byte(nil), data...)}
	b := r.take(4)
	if b == nil || binary.LittleEndian.Uint32(b) != wireMagic {
		return Response{}, errors.New("invalid Rust wire version")
	}
	out := r.Response()
	if r.err == nil && len(r.data) != 0 {
		r.err = errors.New("trailing Rust frame bytes")
	}
	return out, r.err
}
