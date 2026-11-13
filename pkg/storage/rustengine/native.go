// SPDX-License-Identifier: AGPL-3.0-only
//go:build rustengine && cgo

// Package rustengine calls a tenant storage engine written in Rust. Each call sends an operation and a frame, and
// copies the response into Go memory before Rust frees it, so nothing Rust owns escapes a call and Rust keeps no Go
// memory after one.
package rustengine

/*
#cgo LDFLAGS: -L${SRCDIR}/native/target/release -lmimir_engine
#cgo linux LDFLAGS: -ldl -lpthread -lm
#cgo darwin LDFLAGS: -framework Security -framework CoreFoundation
#include "native/engine.h"
*/
import "C"

import (
	"encoding/binary"
	"errors"
	"math"
	"runtime"
	"sync"
	"unsafe"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// Operations, as native/src/lib.rs numbers them.
const (
	opParse           = 1
	opIngest          = 2
	opSeriesOf        = 3
	opSubset          = 4
	opRelease         = 5
	opLookup          = 10
	opCreate          = 11
	opAppend          = 12
	opExemplar        = 13
	opCommit          = 14
	opRollback        = 15
	opLast            = 16
	opStats           = 20
	opSelect          = 21
	opCursorSeries    = 22
	opCursorChunks    = 23
	opCursorClose     = 24
	opLabelNames      = 30
	opLabelValues     = 31
	opPostings        = 32
	opSeriesLabels    = 33
	opValues          = 34
	opValuesIfChanged = 37
	opHeadSeries      = 35
	opLabelBytes      = 36
	opFlush           = 40
	opEvict           = 41
	opEvictIdle       = 42
	opRetain          = 43
	opConfigure       = 44
	opCloseCheckpoint = 45
	opRecovery        = 46
	opCompactOOO      = 47
	opExemplars       = 50
	opActive          = 60
	opDeactivate      = 61
	opSetTrackers     = 62
	opIsActive        = 63
	opMemory          = 70
	opPurge           = 71
)

// Engine is an open Rust engine. Its methods may be called concurrently; Close waits for the calls in flight.
type Engine struct {
	mu     sync.RWMutex
	handle unsafe.Pointer
}

var requests = sync.Pool{New: func() any { return &Writer{} }}

// Open opens the engine in dir. Records' symbol references below offset are to common.
func Open(dir string, blockRange int64, offset uint32, common []string) (*Engine, error) {
	w := Writer{}
	w.String(dir)
	w.I64(blockRange)
	w.U32(offset)
	w.U32(uint32(len(common)))
	for _, s := range common {
		w.String(s)
	}
	var failure C.mimir_buffer
	handle := C.mimir_open((*C.uint8_t)(unsafe.Pointer(unsafe.SliceData(w.buf))), C.size_t(len(w.buf)), &failure)
	runtime.KeepAlive(w.buf)
	if handle == nil {
		defer C.mimir_free(failure)
		return nil, errors.New(C.GoStringN((*C.char)(unsafe.Pointer(failure.data)), C.int(failure.len)))
	}
	return &Engine{handle: unsafe.Pointer(handle)}, nil
}

// Close closes the engine, once the calls in flight return.
func (e *Engine) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.handle != nil {
		C.mimir_close(e.handle)
		e.handle = nil
	}
}

var errClosed = errors.New("Rust engine is closed")

// call runs the operation with the input, and data borrowed for the call, and returns the response in Go memory.
func (e *Engine) call(op uint32, in *Writer, data []byte) (Reader, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.handle == nil {
		return Reader{}, errClosed
	}
	var inPtr, dataPtr *C.uint8_t
	var inLen int
	if in != nil && len(in.buf) > 0 {
		inPtr, inLen = (*C.uint8_t)(unsafe.Pointer(unsafe.SliceData(in.buf))), len(in.buf)
	}
	if len(data) > 0 {
		dataPtr = (*C.uint8_t)(unsafe.Pointer(unsafe.SliceData(data)))
	}
	out := C.mimir_call(e.handle, C.uint32_t(op), inPtr, C.size_t(inLen), dataPtr, C.size_t(len(data)))
	if in != nil {
		runtime.KeepAlive(in.buf)
	}
	runtime.KeepAlive(data)
	raw := unsafe.Slice((*byte)(unsafe.Pointer(out.data)), int(out.len))
	if len(raw) == 0 {
		C.mimir_free(out)
		return Reader{}, errors.New("empty Rust response")
	}
	if raw[0] != 0 {
		err := errors.New(string(raw[1:]))
		C.mimir_free(out)
		return Reader{}, err
	}
	resp := make([]byte, len(raw)-1)
	copy(resp, raw[1:])
	C.mimir_free(out)
	return Reader{data: resp}, nil
}

// callBorrowed runs the operation like call, but returns the response in Rust's memory, without copying it: what is
// read from it is valid until free is called, which must be once.
func (e *Engine) callBorrowed(op uint32, in *Writer) (r Reader, free func(), err error) {
	e.mu.RLock()
	if e.handle == nil {
		e.mu.RUnlock()
		return Reader{}, nil, errClosed
	}
	var inPtr *C.uint8_t
	if in != nil && len(in.buf) > 0 {
		inPtr = (*C.uint8_t)(unsafe.Pointer(unsafe.SliceData(in.buf)))
	}
	out := C.mimir_call(e.handle, C.uint32_t(op), inPtr, C.size_t(len(in.buf)), nil, 0)
	runtime.KeepAlive(in.buf)
	e.mu.RUnlock()
	raw := unsafe.Slice((*byte)(unsafe.Pointer(out.data)), int(out.len))
	free = func() { C.mimir_free(out) }
	if len(raw) == 0 {
		free()
		return Reader{}, nil, errors.New("empty Rust response")
	}
	if raw[0] != 0 {
		err := errors.New(string(raw[1:]))
		free()
		return Reader{}, nil, err
	}
	return Reader{data: raw[1:]}, free, nil
}

func request() *Writer {
	w := requests.Get().(*Writer)
	w.buf = w.buf[:0]
	return w
}

func done(w *Writer) {
	if cap(w.buf) <= 1<<20 {
		requests.Put(w)
	}
}

// Writer builds a request frame.
type Writer struct{ buf []byte }

func (w *Writer) U8(v uint8)   { w.buf = append(w.buf, v) }
func (w *Writer) U32(v uint32) { w.buf = binary.LittleEndian.AppendUint32(w.buf, v) }
func (w *Writer) U64(v uint64) { w.buf = binary.LittleEndian.AppendUint64(w.buf, v) }
func (w *Writer) I64(v int64)  { w.U64(uint64(v)) }
func (w *Writer) Bool(v bool) {
	if v {
		w.U8(1)
	} else {
		w.U8(0)
	}
}
func (w *Writer) Bytes(v []byte) {
	w.U32(uint32(len(v)))
	w.buf = append(w.buf, v...)
}
func (w *Writer) String(v string) {
	w.U32(uint32(len(v)))
	w.buf = append(w.buf, v...)
}

// Reader reads a response frame, which is Go memory: strings and bytes from it may be kept.
type Reader struct {
	data []byte
	err  error
}

func (r *Reader) Err() error { return r.err }

func (r *Reader) take(n int) []byte {
	if r.err != nil {
		return nil
	}
	if n < 0 || n > len(r.data) {
		r.err = errors.New("truncated Rust response")
		return nil
	}
	b := r.data[:n:n]
	r.data = r.data[n:]
	return b
}
func (r *Reader) U8() uint8 {
	if b := r.take(1); b != nil {
		return b[0]
	}
	return 0
}
func (r *Reader) Bool() bool { return r.U8() != 0 }
func (r *Reader) U32() uint32 {
	if b := r.take(4); b != nil {
		return binary.LittleEndian.Uint32(b)
	}
	return 0
}
func (r *Reader) U64() uint64 {
	if b := r.take(8); b != nil {
		return binary.LittleEndian.Uint64(b)
	}
	return 0
}
func (r *Reader) I64() int64 { return int64(r.U64()) }
func (r *Reader) Count() int {
	n := int(r.U32())
	if n > len(r.data) {
		r.err = errors.New("invalid count in Rust response")
		return 0
	}
	return n
}
func (r *Reader) Bytes() []byte { return r.take(int(r.U32())) }
func (r *Reader) String() string {
	b := r.Bytes()
	return unsafe.String(unsafe.SliceData(b), len(b))
}
func (r *Reader) Rest() []byte { return r.data }

// Label is a label of a series; its strings refer to the response they were read from.
type Label = mimirpb.LabelAdapter

// Labels appends the count and label pairs read to into.
func (r *Reader) Labels(into []Label) []Label {
	n := r.Count()
	for range n {
		into = append(into, Label{Name: r.String(), Value: r.String()})
	}
	return into
}

func writeLabels(w *Writer, ls []Label) {
	w.U32(uint32(len(ls)))
	for _, l := range ls {
		w.String(l.Name)
		w.String(l.Value)
	}
}

// Record is a remote write 2.0 record the engine read.
type Record struct {
	Handle       uint32
	Len          int
	Source       uint8
	Complete     bool
	MinTimestamp int64
	Metadata     []RecordMetadata
}

type RecordMetadata struct {
	Family     string
	Type       uint8
	Help, Unit string
}

// Parse reads a record, whose content must be passed again, unchanged, with every call about it until Release.
func (e *Engine) Parse(content []byte) (Record, error) {
	r, err := e.call(opParse, nil, content)
	if err != nil {
		return Record{}, err
	}
	rec := Record{Handle: r.U32(), Len: int(r.U32()), Source: r.U8(), Complete: r.Bool(), MinTimestamp: r.I64()}
	if n := r.Count(); n > 0 {
		rec.Metadata = make([]RecordMetadata, n)
		for i := range rec.Metadata {
			rec.Metadata[i] = RecordMetadata{Family: r.String(), Type: r.U8(), Help: r.String(), Unit: r.String()}
		}
	}
	return rec, r.Err()
}

func (e *Engine) Release(handle uint32) {
	w := request()
	defer done(w)
	w.U32(handle)
	_, _ = e.call(opRelease, w, nil)
}

// SampleError is a sample the engine rejected.
type SampleError struct {
	Series    uint32
	Timestamp int64
	Code      uint8
}

// IngestResult is what ingesting a record did.
type IngestResult struct {
	Accepted uint64
	// The series that got samples, by index, and their refs, when tracked.
	Ingested  []IngestedSeries
	Leftovers []int
	Skipped   []int
	Errors    []SampleError
}

type IngestedSeries struct {
	Index int
	Ref   uint64
}

// Ingest ingests the floats of the record's series the engine has in its head. With skip, series whose samples are
// all before skipBefore are skipped instead.
func (e *Engine) Ingest(handle uint32, content []byte, minTs, maxTs, ingestedMs int64, otlp, skip bool, skipBefore int64, track bool) (IngestResult, error) {
	w := request()
	defer done(w)
	w.U32(handle)
	w.I64(minTs)
	w.I64(maxTs)
	w.I64(ingestedMs)
	w.Bool(otlp)
	w.Bool(skip)
	w.I64(skipBefore)
	w.Bool(track)
	r, err := e.call(opIngest, w, content)
	if err != nil {
		return IngestResult{}, err
	}
	var out IngestResult
	out.Accepted = r.U64()
	if n := r.Count(); n > 0 {
		out.Ingested = make([]IngestedSeries, n)
		for i := range out.Ingested {
			out.Ingested[i] = IngestedSeries{Index: int(r.U32()), Ref: r.U64()}
		}
	}
	if n := r.Count(); n > 0 {
		out.Leftovers = make([]int, n)
		for i := range out.Leftovers {
			out.Leftovers[i] = int(r.U32())
		}
	}
	if n := r.Count(); n > 0 {
		out.Skipped = make([]int, n)
		for i := range out.Skipped {
			out.Skipped[i] = int(r.U32())
		}
	}
	if n := r.Count(); n > 0 {
		out.Errors = make([]SampleError, n)
		for i := range out.Errors {
			out.Errors[i] = SampleError{Series: r.U32(), Timestamp: r.I64(), Code: r.U8()}
		}
	}
	return out, r.Err()
}

// SeriesOf returns the labels and floats, as time and value bits, of a series of the record.
func (e *Engine) SeriesOf(handle uint32, index int, content []byte) ([]Label, [][2]uint64, error) {
	w := request()
	defer done(w)
	w.U32(handle)
	w.U32(uint32(index))
	r, err := e.call(opSeriesOf, w, content)
	if err != nil {
		return nil, nil, err
	}
	ls := r.Labels(nil)
	samples := make([][2]uint64, r.Count())
	for i := range samples {
		samples[i] = [2]uint64{r.U64(), r.U64()}
	}
	return ls, samples, r.Err()
}

// Subset appends to into the content of a record with only the series at indices.
func (e *Engine) Subset(handle uint32, content []byte, indices []int, into []byte) ([]byte, error) {
	w := request()
	defer done(w)
	w.U32(handle)
	w.U32(uint32(len(indices)))
	for _, i := range indices {
		w.U32(uint32(i))
	}
	r, err := e.call(opSubset, w, content)
	if err != nil {
		return into, err
	}
	return append(into, r.Rest()...), nil
}

func (e *Engine) Lookup(ls []Label) (ref uint64, inHead bool, err error) {
	w := request()
	defer done(w)
	writeLabels(w, ls)
	r, err := e.call(opLookup, w, nil)
	if err != nil {
		return 0, false, err
	}
	return r.U64(), r.Bool(), r.Err()
}

func (e *Engine) Create(ls []Label, trackers uint64, otlp bool) (uint64, error) {
	w := request()
	defer done(w)
	writeLabels(w, ls)
	w.U64(trackers)
	w.Bool(otlp)
	r, err := e.call(opCreate, w, nil)
	if err != nil {
		return 0, err
	}
	return r.U64(), r.Err()
}

// Sample kinds.
const (
	KindFloat          = 1
	KindHistogram      = 2
	KindFloatHistogram = 3
)

// Append stages a sample of the transaction, and returns the code of why it was rejected, or 0.
func (e *Engine) Append(ref uint64, t int64, kind uint8, v float64, payload []byte, buckets int, rejectOOO bool) (uint8, error) {
	w := request()
	defer done(w)
	w.U64(ref)
	w.I64(t)
	w.U8(kind)
	w.U64(math.Float64bits(v))
	w.Bytes(payload)
	w.U32(uint32(int32(buckets)))
	w.Bool(rejectOOO)
	r, err := e.call(opAppend, w, nil)
	if err != nil {
		return 0, err
	}
	return r.U8(), r.Err()
}

func (e *Engine) AppendExemplar(ref uint64, t int64, v float64, ls []Label) (uint8, error) {
	w := request()
	defer done(w)
	w.U64(ref)
	w.I64(t)
	w.U64(math.Float64bits(v))
	writeLabels(w, ls)
	r, err := e.call(opExemplar, w, nil)
	if err != nil {
		return 0, err
	}
	return r.U8(), r.Err()
}

func (e *Engine) Commit(ingestedMs int64, otlp bool) (uint64, error) {
	w := request()
	defer done(w)
	w.I64(ingestedMs)
	w.Bool(otlp)
	r, err := e.call(opCommit, w, nil)
	if err != nil {
		return 0, err
	}
	return r.U64(), r.Err()
}

// RemovedSeries is a series that left the head.
type RemovedSeries struct {
	Ref    uint64
	Labels []Label
}

func readRemoved(r Reader, err error) ([]RemovedSeries, error) {
	if err != nil {
		return nil, err
	}
	out := make([]RemovedSeries, r.Count())
	for i := range out {
		out[i].Ref = r.U64()
		out[i].Labels = r.Labels(nil)
	}
	return out, r.Err()
}

func (e *Engine) Rollback() ([]RemovedSeries, error) {
	return readRemoved(e.call(opRollback, nil, nil))
}

// Last returns the time and kind of the series' newest in-order sample.
func (e *Engine) Last(ref uint64) (int64, uint8, bool, error) {
	w := request()
	defer done(w)
	w.U64(ref)
	r, err := e.call(opLast, w, nil)
	if err != nil || !r.Bool() {
		return 0, 0, false, err
	}
	return r.I64(), r.U8(), true, r.Err()
}

type Stats struct {
	InHead             uint64
	HeadMint, HeadMaxt int64
	MinValid           int64
	OOOMint, OOOMaxt   int64
	Live, Generation   uint64
}

// NoTime is the time of what has none.
const NoTime = math.MinInt64

func (e *Engine) Stats() (Stats, error) {
	r, err := e.call(opStats, nil, nil)
	if err != nil {
		return Stats{}, err
	}
	return Stats{InHead: r.U64(), HeadMint: r.I64(), HeadMaxt: r.I64(), MinValid: r.I64(), OOOMint: r.I64(), OOOMaxt: r.I64(), Live: r.U64(), Generation: r.U64()}, r.Err()
}

// Filter selects series by a label: those with one of the values, or with Exclude one that isn't, or without the
// label when MatchMissing.
type Filter struct {
	Name         string
	Values       []string
	Exclude      bool
	MatchMissing bool
}

func writeFilters(w *Writer, fs []Filter) {
	w.U32(uint32(len(fs)))
	for _, f := range fs {
		w.String(f.Name)
		w.U32(uint32(len(f.Values)))
		for _, v := range f.Values {
			w.String(v)
		}
		w.Bool(f.Exclude)
		w.Bool(f.MatchMissing)
	}
}

// Selection is what a query selects: all series, those of the refs, or those of the filters.
type Selection struct {
	All     bool
	Filters []Filter
	Refs    []uint64
	ByRefs  bool
}

// SelectResult is a cursor of a select, with its first series and the chunks of the first of those.
type SelectResult struct {
	Cursor uint64
	Count  int
	Series []CursorSeries
	Chunks [][]Chunk
}

// Select snapshots the chunks of the selected series in [mint, maxt], and returns the cursor to read them through,
// with the first inlineSeries series, and the chunks of the first ones, of at most inlineChunkSeries series and
// about inlineBytes bytes.
func (e *Engine) Select(sel Selection, mint, maxt int64, sorted, ordered, labelsOnly bool, inlineSeries, inlineChunkSeries, inlineBytes int) (SelectResult, error) {
	w := request()
	defer done(w)
	w.I64(mint)
	w.I64(maxt)
	w.Bool(sorted)
	w.Bool(ordered)
	w.Bool(labelsOnly)
	switch {
	case sel.All:
		w.U8(0)
	case sel.ByRefs:
		w.U8(2)
		w.U32(uint32(len(sel.Refs)))
		for _, ref := range sel.Refs {
			w.U64(ref)
		}
	default:
		w.U8(1)
		writeFilters(w, sel.Filters)
	}
	w.U32(uint32(inlineSeries))
	w.U32(uint32(inlineChunkSeries))
	w.U32(uint32(inlineBytes))
	r, err := e.call(opSelect, w, nil)
	if err != nil {
		return SelectResult{}, err
	}
	out := SelectResult{Cursor: r.U64(), Count: int(r.U32())}
	out.Series = readSeriesPage(&r, nil)
	out.Chunks = readChunksPage(&r, nil)
	return out, r.Err()
}

// CursorSeries is a series of a cursor.
type CursorSeries struct {
	Ref    uint64
	Labels []Label
	Chunks int
	// Whether some of its chunks have histograms, which may take more chunks to encode.
	Histograms bool
}

func readSeriesPage(r *Reader, into []CursorSeries) []CursorSeries {
	n := r.Count()
	if n == 0 {
		return into
	}
	start := len(into)
	into = append(into, make([]CursorSeries, n)...)
	// One array for the labels of the page.
	var arena []Label
	for i := range n {
		s := &into[start+i]
		s.Ref = r.U64()
		m := r.Count()
		if cap(arena)-len(arena) < m {
			arena = make([]Label, 0, max(m, (n-i)*m))
		}
		from := len(arena)
		for range m {
			arena = append(arena, Label{Name: r.String(), Value: r.String()})
		}
		s.Labels = arena[from:len(arena):len(arena)]
		s.Chunks = int(r.U32())
		s.Histograms = r.Bool()
	}
	return into
}

func (e *Engine) CursorSeries(cursor uint64, from, max int, into []CursorSeries) ([]CursorSeries, error) {
	w := request()
	defer done(w)
	w.U64(cursor)
	w.U32(uint32(from))
	w.U32(uint32(max))
	r, err := e.call(opCursorSeries, w, nil)
	if err != nil {
		return into, err
	}
	into = readSeriesPage(&r, into)
	return into, r.Err()
}

// Chunk is a chunk of a series: XOR, or HistogramEncoding for the histogram samples Go decodes.
type Chunk struct {
	Mint, Maxt int64
	Encoding   uint8
	Data       []byte
}

// HistogramEncoding is the encoding of a chunk of histogram samples: count, then time, kind and protobuf of each.
const HistogramEncoding = 0x80

func readChunksPage(r *Reader, into [][]Chunk) [][]Chunk {
	n := r.Count()
	var arena []Chunk
	for range n {
		m := r.Count()
		if cap(arena)-len(arena) < m {
			arena = make([]Chunk, 0, max(m, 256))
		}
		from := len(arena)
		for range m {
			arena = append(arena, Chunk{Mint: r.I64(), Maxt: r.I64(), Encoding: r.U8(), Data: r.Bytes()})
		}
		into = append(into, arena[from:len(arena):len(arena)])
	}
	return into
}

// CursorSeriesBorrowed is CursorSeries with the labels in Rust's memory, valid until free is called.
func (e *Engine) CursorSeriesBorrowed(cursor uint64, from, max int, into []CursorSeries) ([]CursorSeries, func(), error) {
	w := request()
	defer done(w)
	w.U64(cursor)
	w.U32(uint32(from))
	w.U32(uint32(max))
	r, free, err := e.callBorrowed(opCursorSeries, w)
	if err != nil {
		return into, nil, err
	}
	into = readSeriesPage(&r, into)
	if r.err != nil {
		free()
		return into, nil, r.err
	}
	return into, free, nil
}

// CursorChunksBorrowed is CursorChunks with the chunks in Rust's memory, valid until free is called.
func (e *Engine) CursorChunksBorrowed(cursor uint64, from, max, maxBytes int, into [][]Chunk) ([][]Chunk, func(), error) {
	w := request()
	defer done(w)
	w.U64(cursor)
	w.U32(uint32(from))
	w.U32(uint32(max))
	w.U32(uint32(maxBytes))
	r, free, err := e.callBorrowed(opCursorChunks, w)
	if err != nil {
		return into, nil, err
	}
	into = readChunksPage(&r, into)
	if r.err != nil {
		free()
		return into, nil, r.err
	}
	return into, free, nil
}

// CursorChunks returns the chunks of the series from `from` on, of at most max series and about maxBytes, by series.
func (e *Engine) CursorChunks(cursor uint64, from, max, maxBytes int, into [][]Chunk) ([][]Chunk, error) {
	w := request()
	defer done(w)
	w.U64(cursor)
	w.U32(uint32(from))
	w.U32(uint32(max))
	w.U32(uint32(maxBytes))
	r, err := e.call(opCursorChunks, w, nil)
	if err != nil {
		return into, err
	}
	into = readChunksPage(&r, into)
	return into, r.Err()
}

func (e *Engine) CursorClose(cursor uint64) {
	w := request()
	defer done(w)
	w.U64(cursor)
	_, _ = e.call(opCursorClose, w, nil)
}

func readStrings(r Reader, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}
	out := make([]string, r.Count())
	for i := range out {
		out[i] = r.String()
	}
	return out, r.Err()
}

func (e *Engine) LabelNames(mint, maxt int64, limit int, filters []Filter) ([]string, error) {
	w := request()
	defer done(w)
	w.I64(mint)
	w.I64(maxt)
	w.U32(uint32(limit))
	writeFilters(w, filters)
	return readStrings(e.call(opLabelNames, w, nil))
}

func (e *Engine) LabelValues(name string, mint, maxt int64, limit int, filters []Filter) ([]string, error) {
	w := request()
	defer done(w)
	w.String(name)
	w.I64(mint)
	w.I64(maxt)
	w.U32(uint32(limit))
	writeFilters(w, filters)
	return readStrings(e.call(opLabelValues, w, nil))
}

// Postings returns the refs of the selected series, in order, of those in the head only with headOnly.
func (e *Engine) Postings(headOnly, all bool, filters []Filter) ([]uint64, error) {
	w := request()
	defer done(w)
	w.Bool(headOnly)
	w.Bool(all)
	if !all {
		writeFilters(w, filters)
	}
	r, err := e.call(opPostings, w, nil)
	if err != nil {
		return nil, err
	}
	out := make([]uint64, r.Count())
	for i := range out {
		out[i] = r.U64()
	}
	return out, r.Err()
}

// SeriesLabels visits the labels of the series, by their index in refs, skipping those that don't exist.
func (e *Engine) SeriesLabels(refs []uint64, visit func(i int, ls []Label) error) error {
	w := request()
	defer done(w)
	w.U32(uint32(len(refs)))
	for _, ref := range refs {
		w.U64(ref)
	}
	r, err := e.call(opSeriesLabels, w, nil)
	if err != nil {
		return err
	}
	var ls []Label
	for i := range refs {
		if !r.Bool() {
			continue
		}
		ls = r.Labels(ls[:0])
		if r.err != nil {
			return r.err
		}
		if err := visit(i, ls); err != nil {
			return err
		}
	}
	return r.Err()
}

// Values returns all the values of the label, of the head's series only with headOnly.
func (e *Engine) Values(name string, headOnly bool) ([]string, error) {
	w := request()
	defer done(w)
	w.String(name)
	w.Bool(headOnly)
	return readStrings(e.call(opValues, w, nil))
}

// ValuesIfChanged returns the values of the label, of all series, unless their generation is still known.
func (e *Engine) ValuesIfChanged(name string, known uint64) (generation uint64, values []string, changed bool, err error) {
	w := request()
	defer done(w)
	w.String(name)
	w.U64(known)
	r, err := e.call(opValuesIfChanged, w, nil)
	if err != nil {
		return 0, nil, false, err
	}
	generation = r.U64()
	if changed = r.Bool(); changed {
		values = make([]string, r.Count())
		for i := range values {
			values[i] = r.String()
		}
	}
	return generation, values, changed, r.Err()
}

// HeadSeries visits the series in the head with their labels, a page at a time.
func (e *Engine) HeadSeries(visit func(ref uint64, ls []Label) error) error {
	from := 0
	var ls []Label
	for {
		w := request()
		w.U32(uint32(from))
		w.U32(4096)
		r, err := e.call(opHeadSeries, w, nil)
		done(w)
		if err != nil {
			return err
		}
		next := int(r.U32())
		n := r.Count()
		for range n {
			ref := r.U64()
			ls = r.Labels(ls[:0])
			if r.err != nil {
				return r.err
			}
			if err := visit(ref, ls); err != nil {
				return err
			}
		}
		if r.err != nil {
			return r.err
		}
		if n == 0 || next <= from {
			return nil
		}
		from = next
	}
}

func (e *Engine) LabelBytes() (map[string]uint64, error) {
	r, err := e.call(opLabelBytes, nil, nil)
	if err != nil {
		return nil, err
	}
	n := r.Count()
	out := make(map[string]uint64, n)
	for range n {
		name := r.String()
		out[name] = r.U64()
	}
	return out, r.Err()
}

func (e *Engine) Flush(upTo int64) ([]RemovedSeries, error) {
	w := request()
	defer done(w)
	w.I64(upTo)
	return readRemoved(e.call(opFlush, w, nil))
}

func (e *Engine) Evict(refs []uint64) ([]RemovedSeries, error) {
	w := request()
	defer done(w)
	w.U32(uint32(len(refs)))
	for _, ref := range refs {
		w.U64(ref)
	}
	return readRemoved(e.call(opEvict, w, nil))
}

func (e *Engine) EvictIdle(age int64) ([]RemovedSeries, error) {
	w := request()
	defer done(w)
	w.I64(age)
	return readRemoved(e.call(opEvictIdle, w, nil))
}

func (e *Engine) Retain(cutoff int64) error {
	w := request()
	defer done(w)
	w.I64(cutoff)
	_, err := e.call(opRetain, w, nil)
	return err
}

func (e *Engine) CompactOOO() error {
	_, err := e.call(opCompactOOO, nil, nil)
	return err
}

func (e *Engine) Configure(window int64, maxExemplars int64) error {
	w := request()
	defer done(w)
	w.I64(window)
	w.U64(uint64(max(0, maxExemplars)))
	_, err := e.call(opConfigure, w, nil)
	return err
}

// CloseCheckpoint writes the head, so the next open restores it; the engine takes no more calls but Close.
func (e *Engine) CloseCheckpoint() error {
	_, err := e.call(opCloseCheckpoint, nil, nil)
	return err
}

// RestoredClean reports whether opening restored the head of a clean shutdown.
func (e *Engine) RestoredClean() (bool, error) {
	r, err := e.call(opRecovery, nil, nil)
	if err != nil {
		return false, err
	}
	return r.Bool(), r.Err()
}

// SeriesExemplars are the exemplars of a series.
type SeriesExemplars struct {
	Ref       uint64
	Labels    []Label
	Exemplars []Exemplar
}

type Exemplar struct {
	Timestamp int64
	Value     float64
	Labels    []Label
}

func (e *Engine) Exemplars(mint, maxt int64, all bool, refs []uint64) ([]SeriesExemplars, error) {
	w := request()
	defer done(w)
	w.I64(mint)
	w.I64(maxt)
	w.Bool(all)
	w.U32(uint32(len(refs)))
	for _, ref := range refs {
		w.U64(ref)
	}
	r, err := e.call(opExemplars, w, nil)
	if err != nil {
		return nil, err
	}
	out := make([]SeriesExemplars, r.Count())
	for i := range out {
		out[i].Ref = r.U64()
		out[i].Labels = r.Labels(nil)
		out[i].Exemplars = make([]Exemplar, r.Count())
		for j := range out[i].Exemplars {
			out[i].Exemplars[j] = Exemplar{Timestamp: r.I64(), Value: math.Float64frombits(r.U64()), Labels: r.Labels(nil)}
		}
	}
	return out, r.Err()
}

// Memory returns the bytes the allocator has allocated and resident, then the bytes of the series, symbols, postings,
// and slots and table.
func (e *Engine) Memory() ([6]uint64, error) {
	r, err := e.call(opMemory, nil, nil)
	if err != nil {
		return [6]uint64{}, err
	}
	return [6]uint64{r.U64(), r.U64(), r.U64(), r.U64(), r.U64(), r.U64()}, r.Err()
}

// Purge gives the memory Rust freed back to the system.
func (e *Engine) Purge() {
	_, _ = e.call(opPurge, nil, nil)
}

// ActiveCounts are the active series, in all and by tracker: total, native histograms, and their buckets.
type ActiveCounts struct {
	Total, OTLP, Histograms, Buckets uint64
	Trackers                         [][3]uint64
}

// ActiveChange is a series that became active, or stopped being, since cost attribution was last told.
type ActiveChange struct {
	Labels  []Label
	Active  bool
	Buckets int
}

// Active counts the head's series ingested at or after cutoffMs, by trackers custom trackers, and with report what
// changed for cost attribution.
func (e *Engine) Active(cutoffMs int64, trackers int, report bool) (ActiveCounts, []ActiveChange, error) {
	w := request()
	defer done(w)
	w.I64(cutoffMs)
	w.U32(uint32(trackers))
	w.Bool(report)
	r, err := e.call(opActive, w, nil)
	if err != nil {
		return ActiveCounts{}, nil, err
	}
	out := ActiveCounts{Total: r.U64(), OTLP: r.U64(), Histograms: r.U64(), Buckets: r.U64(), Trackers: make([][3]uint64, trackers)}
	for i := range out.Trackers {
		out.Trackers[i] = [3]uint64{r.U64(), r.U64(), r.U64()}
	}
	var changes []ActiveChange
	if n := r.Count(); n > 0 {
		changes = make([]ActiveChange, n)
		for i := range changes {
			changes[i].Active = r.Bool()
			changes[i].Buckets = int(int32(r.U32()))
			changes[i].Labels = r.Labels(nil)
		}
	}
	return out, changes, r.Err()
}

// TrackerMask is the custom trackers a series matches, a bit each.
type TrackerMask struct {
	Ref, Mask uint64
}

func (e *Engine) SetTrackers(masks []TrackerMask) error {
	w := request()
	defer done(w)
	w.U32(uint32(len(masks)))
	for _, m := range masks {
		w.U64(m.Ref)
		w.U64(m.Mask)
	}
	_, err := e.call(opSetTrackers, w, nil)
	return err
}

// Deactivate makes the series inactive until their next samples, or all of them.
func (e *Engine) Deactivate(refs []uint64, all bool) error {
	w := request()
	defer done(w)
	w.Bool(all)
	w.U32(uint32(len(refs)))
	for _, ref := range refs {
		w.U64(ref)
	}
	_, err := e.call(opDeactivate, w, nil)
	return err
}

// IsActive reports whether the series was ingested at or after cutoffMs, with the bucket count of its last native
// histogram, or -1.
func (e *Engine) IsActive(ref uint64, cutoffMs int64) (bool, int, error) {
	w := request()
	defer done(w)
	w.U64(ref)
	w.I64(cutoffMs)
	r, err := e.call(opIsActive, w, nil)
	if err != nil {
		return false, -1, err
	}
	return r.Bool(), int(int32(r.U32())), r.Err()
}
