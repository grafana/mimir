package main

import (
	"fmt"
	"strings"

	roaring "github.com/RoaringBitmap/roaring/v2"
)

// Label is a single name=value pair on a series.
type Label struct {
	Name  string
	Value string
}

func (l Label) String() string {
	return l.Name + "=" + l.Value
}

// FormatLabels renders a label set as {n1="v1", n2="v2", ...}.
func FormatLabels(ls []Label) string {
	parts := make([]string, len(ls))
	for i, l := range ls {
		parts[i] = fmt.Sprintf(`%s="%s"`, l.Name, l.Value)
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

// SymbolTable provides bidirectional string <-> uint32 mapping.
// It is append-only and shared across all segments in an Index.
type SymbolTable struct {
	strToID map[string]uint32
	idToStr []string
}

func NewSymbolTable() *SymbolTable {
	return &SymbolTable{
		strToID: make(map[string]uint32),
	}
}

// Encode returns the ID for s, allocating a new one if needed.
func (st *SymbolTable) Encode(s string) uint32 {
	if id, ok := st.strToID[s]; ok {
		return id
	}
	id := uint32(len(st.idToStr))
	st.strToID[s] = id
	st.idToStr = append(st.idToStr, s)
	return id
}

// Decode returns the string for id, or "" if unknown.
func (st *SymbolTable) Decode(id uint32) string {
	if int(id) < len(st.idToStr) {
		return st.idToStr[id]
	}
	return ""
}

// Lookup returns the ID for s and whether it exists.
func (st *SymbolTable) Lookup(s string) (uint32, bool) {
	id, ok := st.strToID[s]
	return id, ok
}

// invertedKey is the composite key for the inverted index: (nameID, valueID).
type invertedKey = [2]uint32

// Segment holds one time-window of ingested series.
type Segment struct {
	inverted  map[invertedKey]*roaring.Bitmap
	allSeries *roaring.Bitmap
	forward   map[uint32][]Label
	frozen    bool
}

func newSegment() *Segment {
	return &Segment{
		inverted:  make(map[invertedKey]*roaring.Bitmap),
		allSeries: roaring.New(),
		forward:   make(map[uint32][]Label),
	}
}

// add inserts a series into this segment. Panics if frozen.
func (seg *Segment) add(symbols *SymbolTable, ref uint32, ls []Label) {
	if seg.frozen {
		panic("cannot add to a frozen segment")
	}
	seg.allSeries.Add(ref)
	seg.forward[ref] = ls
	for _, l := range ls {
		nameID := symbols.Encode(l.Name)
		valID := symbols.Encode(l.Value)
		key := invertedKey{nameID, valID}
		bm, ok := seg.inverted[key]
		if !ok {
			bm = roaring.New()
			seg.inverted[key] = bm
		}
		bm.Add(ref)
	}
}

// Index is the segment-stack inverted index backed by Roaring Bitmaps.
type Index struct {
	symbols  *SymbolTable
	buf      *Segment
	segments []*Segment
}

func NewIndex() *Index {
	return &Index{
		symbols: NewSymbolTable(),
		buf:     newSegment(),
	}
}

// Add inserts a series into the active write buffer.
func (idx *Index) Add(ref uint32, ls []Label) {
	idx.buf.add(idx.symbols, ref, ls)
}

// Flush freezes the current write buffer into an immutable segment
// and creates a fresh buffer.
func (idx *Index) Flush() {
	idx.buf.frozen = true
	idx.segments = append(idx.segments, idx.buf)
	idx.buf = newSegment()
}

// DropOldestSegment removes the oldest frozen segment.
func (idx *Index) DropOldestSegment() {
	if len(idx.segments) == 0 {
		return
	}
	idx.segments = idx.segments[1:]
}

// allSegments returns every segment that should be queried:
// all frozen segments followed by the active buffer.
func (idx *Index) allSegments() []*Segment {
	result := make([]*Segment, 0, len(idx.segments)+1)
	result = append(result, idx.segments...)
	result = append(result, idx.buf)
	return result
}

// Series returns the labels for a given series ref, searching the
// buffer first and then frozen segments newest-to-oldest.
func (idx *Index) Series(ref uint32) ([]Label, bool) {
	if ls, ok := idx.buf.forward[ref]; ok {
		return ls, true
	}
	for i := len(idx.segments) - 1; i >= 0; i-- {
		if ls, ok := idx.segments[i].forward[ref]; ok {
			return ls, true
		}
	}
	return nil, false
}
