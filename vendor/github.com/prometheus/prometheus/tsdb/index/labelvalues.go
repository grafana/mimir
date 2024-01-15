package index

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"

	"golang.org/x/exp/slices"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/util/annotations"
)

type labelValuesV2 struct {
	name      string
	cur       string
	dec       encoding.Decbuf
	matchers  []*labels.Matcher
	skip      int
	lastVal   string
	exhausted bool
	err       error
}

// newLabelValuesV2 returns an iterator over label values in a v2 index.
func (r *Reader) newLabelValuesV2(name string, matchers []*labels.Matcher) storage.LabelValues {
	p := r.postings[name]
	if len(p) == 0 {
		return storage.EmptyLabelValues()
	}

	d := encoding.NewDecbufAt(r.b, int(r.toc.PostingsTable), nil)
	d.Skip(p[0].off)
	// These are always the same number of bytes, and it's faster to skip than to parse
	skip := d.Len()
	// Key count
	d.Uvarint()
	// Label name
	d.UvarintBytes()
	skip -= d.Len()

	return &labelValuesV2{
		name:     name,
		matchers: matchers,
		dec:      d,
		lastVal:  p[len(p)-1].value,
		skip:     skip,
	}
}

func (l *labelValuesV2) Next() bool {
	if l.err != nil || l.exhausted {
		return false
	}

	// Pick the first matching label value
	for l.dec.Err() == nil {
		// Label value
		val := yoloString(l.dec.UvarintBytes())
		isMatch := true
		for _, m := range l.matchers {
			if m.Name != l.name {
				// This should not happen
				continue
			}

			if !m.Matches(val) {
				isMatch = false
				break
			}
		}

		if isMatch {
			l.cur = val
		}
		if val == l.lastVal {
			l.exhausted = true
			return isMatch
		}

		// Offset
		l.dec.Uvarint64()
		// Skip forward to next entry
		l.dec.Skip(l.skip)

		if isMatch {
			break
		}
	}
	if l.dec.Err() != nil {
		// An error occurred decoding
		l.err = fmt.Errorf("get postings offset entry: %w", l.dec.Err())
		return false
	}

	return true
}

func (l *labelValuesV2) At() string {
	return l.cur
}

func (l *labelValuesV2) Err() error {
	return l.err
}

func (l *labelValuesV2) Warnings() annotations.Annotations {
	return nil
}

func (l *labelValuesV2) Close() error {
	return nil
}

type labelValuesV1 struct {
	it       *reflect.MapIter
	matchers []*labels.Matcher
	name     string
}

func (l *labelValuesV1) Next() bool {
loop:
	for l.it.Next() {
		for _, m := range l.matchers {
			if m.Name != l.name {
				// This should not happen
				continue
			}

			if !m.Matches(l.At()) {
				continue loop
			}
		}

		// This entry satisfies all matchers
		return true
	}

	return false
}

func (l *labelValuesV1) At() string {
	return yoloString(l.it.Value().Bytes())
}

func (*labelValuesV1) Err() error {
	return nil
}

func (*labelValuesV1) Warnings() annotations.Annotations {
	return nil
}

func (*labelValuesV1) Close() error {
	return nil
}

// LabelValuesFor returns LabelValues for the given label name in the series referred to by postings.
func (r *Reader) LabelValuesFor(postings Postings, name string) storage.LabelValues {
	return r.labelValuesFor(postings, name, true)
}

// LabelValuesExcluding returns LabelValues for the given label name in all other series than those referred to by postings.
// This is useful for obtaining label values for other postings than the ones you wish to exclude.
func (r *Reader) LabelValuesExcluding(postings Postings, name string) storage.LabelValues {
	return r.labelValuesFor(postings, name, false)
}

func (r *Reader) labelValuesFor(postings Postings, name string, includeMatches bool) storage.LabelValues {
	if r.version == FormatV1 {
		return r.labelValuesForV1(postings, name, includeMatches)
	}

	e := r.postings[name]
	if len(e) == 0 {
		return storage.EmptyLabelValues()
	}

	d := encoding.NewDecbufAt(r.b, int(r.toc.PostingsTable), nil)
	// Skip to start
	d.Skip(e[0].off)
	lastVal := e[len(e)-1].value

	return &intersectLabelValues{
		d:              &d,
		b:              r.b,
		dec:            r.dec,
		lastVal:        lastVal,
		postings:       postings,
		includeMatches: includeMatches,
	}
}

func (r *Reader) labelValuesForV1(postings Postings, name string, includeMatches bool) storage.LabelValues {
	e := r.postingsV1[name]
	if len(e) == 0 {
		return storage.EmptyLabelValues()
	}
	vals := make([]string, 0, len(e))
	for v := range e {
		vals = append(vals, v)
	}
	slices.Sort(vals)
	return &intersectLabelValuesV1{
		postingOffsets: e,
		values:         storage.NewListLabelValues(vals, nil),
		postings:       postings,
		b:              r.b,
		dec:            r.dec,
		includeMatches: includeMatches,
	}
}

type intersectLabelValuesV1 struct {
	postingOffsets map[string]uint64
	values         *storage.ListLabelValues
	postings       Postings
	curPostings    bigEndianPostings
	b              ByteSlice
	dec            *Decoder
	cur            string
	err            error
	includeMatches bool
}

func (it *intersectLabelValuesV1) Next() bool {
	if it.err != nil {
		return false
	}

	// Look for a value with intersecting postings
	for it.values.Next() {
		val := it.values.At()

		postingsOff := it.postingOffsets[val]
		// Read from the postings table.
		d := encoding.NewDecbufAt(it.b, int(postingsOff), castagnoliTable)
		if _, err := it.dec.PostingsInPlace(d.Get(), &it.curPostings); err != nil {
			it.err = fmt.Errorf("decode postings: %w", err)
			return false
		}

		it.postings.Reset()

		isMatch := false
		if it.includeMatches {
			isMatch = intersect(it.postings, &it.curPostings)
		} else {
			// We only want to include this value if curPostings is not fully contained
			// by the postings iterator (which is to be excluded).
			isMatch = !contains(it.postings, &it.curPostings)
		}
		if isMatch {
			it.cur = val
			return true
		}
	}

	return false
}

func (it *intersectLabelValuesV1) At() string {
	return it.cur
}

func (it *intersectLabelValuesV1) Err() error {
	return it.err
}

func (it *intersectLabelValuesV1) Warnings() annotations.Annotations {
	return nil
}

func (it *intersectLabelValuesV1) Close() error {
	return nil
}

type intersectLabelValues struct {
	d              *encoding.Decbuf
	d2             encoding.Decbuf
	b              ByteSlice
	dec            *Decoder
	postings       Postings
	curPostings    bigEndianPostings
	lastVal        string
	skip           int
	cur            string
	exhausted      bool
	err            error
	includeMatches bool
}

func (it *intersectLabelValues) Next() bool {
	if it.exhausted || it.err != nil {
		return false
	}

	for !it.exhausted && it.d.Err() == nil {
		if it.skip == 0 {
			// These are always the same number of bytes,
			// and it's faster to skip than to parse.
			it.skip = it.d.Len()
			// Key count
			it.d.Uvarint()
			// Label name
			it.d.UvarintBytes()
			it.skip -= it.d.Len()
		} else {
			it.d.Skip(it.skip)
		}

		// Label value
		val := it.d.UvarintBytes()
		postingsOff := int(it.d.Uvarint64())
		// Read from the postings table
		b := it.b.Range(postingsOff, postingsOff+4)
		l := int(binary.BigEndian.Uint32(b))
		b = it.b.Range(postingsOff+4, postingsOff+4+l+4)
		it.d2.B = b[:len(b)-4]
		if exp := binary.BigEndian.Uint32(b[len(b)-4:]); it.d2.Crc32(castagnoliTable) != exp {
			it.d2.E = encoding.ErrInvalidChecksum
		}
		if _, err := it.dec.PostingsInPlace(it.d2.Get(), &it.curPostings); err != nil {
			it.err = fmt.Errorf("decode postings: %w", err)
			return false
		}
		it.exhausted = string(val) == it.lastVal
		it.postings.Reset()
		isMatch := false
		if it.includeMatches {
			isMatch = intersect(it.postings, &it.curPostings)
		} else {
			// We only want to include this value if curPostings is not fully contained
			// by the postings iterator (which is to be excluded).
			isMatch = !contains(it.postings, &it.curPostings)
		}
		if isMatch {
			// Make sure to allocate a new string
			it.cur = string(val)
			return true
		}
	}
	if it.d.Err() != nil {
		it.err = fmt.Errorf("get postings offset entry: %w", it.d.Err())
	}

	return false
}

func (it *intersectLabelValues) At() string {
	return it.cur
}

func (it *intersectLabelValues) Err() error {
	return it.err
}

func (it *intersectLabelValues) Warnings() annotations.Annotations {
	return nil
}

func (it *intersectLabelValues) Close() error {
	return nil
}

type LabelsGetter interface {
	// Labels reads the series with the given ref and writes its labels into builder.
	Labels(ref storage.SeriesRef, builder *labels.ScratchBuilder) error
}

// LabelValuesFor returns LabelValues for the given label name in the series referred to by postings.
// lg is used to get labels from series in case the ratio of postings vs label values is sufficiently low,
// as an optimization.
func (p *MemPostings) LabelValuesFor(postings Postings, name string, lg LabelsGetter) storage.LabelValues {
	p.mtx.RLock()

	e := p.m[name]
	if len(e) == 0 {
		p.mtx.RUnlock()
		return storage.EmptyLabelValues()
	}

	// With thread safety in mind and due to random key ordering in map, we have to construct the array in memory
	vals := make([]string, 0, len(e))
	for val := range e {
		vals = append(vals, val)
	}

	// Let's see if expanded postings for matchers have smaller cardinality than label values.
	// Since computing label values from series is expensive, we apply a limit on number of expanded
	// postings (and series).
	const maxExpandedPostingsFactor = 100 // Division factor for maximum number of matched series.
	maxExpandedPostings := len(vals) / maxExpandedPostingsFactor
	if maxExpandedPostings > 0 {
		// Add space for one extra posting when checking if we expanded all postings.
		expanded := make([]storage.SeriesRef, 0, maxExpandedPostings+1)

		// Call postings.Next() even if len(expanded) == maxExpandedPostings. This tells us if there are more postings or not.
		for len(expanded) <= maxExpandedPostings && postings.Next() {
			expanded = append(expanded, postings.At())
		}

		if len(expanded) <= maxExpandedPostings {
			// When we're here, postings.Next() must have returned false, so we need to check for errors.
			p.mtx.RUnlock()

			if err := postings.Err(); err != nil {
				return storage.ErrLabelValues(fmt.Errorf("expanding postings for matchers: %w", err))
			}

			// We have expanded all the postings -- all returned label values will be from these series only.
			// (We supply vals as a buffer for storing results. It should be big enough already, since it holds all possible label values.)
			vals, err := LabelValuesFromSeries(lg, name, expanded, vals)
			if err != nil {
				return storage.ErrLabelValues(err)
			}

			slices.Sort(vals)
			return storage.NewListLabelValues(vals, nil)
		}

		// If we haven't reached end of postings, we prepend our expanded postings to "postings", and continue.
		postings = NewPrependPostings(expanded, postings)
	}

	candidates := make([]Postings, 0, len(e))
	vals = vals[:0]
	for val, srs := range e {
		vals = append(vals, val)
		candidates = append(candidates, NewListPostings(srs))
	}
	indexes, err := FindIntersectingPostings(postings, candidates)
	p.mtx.RUnlock()
	if err != nil {
		return storage.ErrLabelValues(err)
	}

	// Filter the values, keeping only those with intersecting postings
	if len(vals) != len(indexes) {
		slices.Sort(indexes)
		for i, index := range indexes {
			vals[i] = vals[index]
		}
		vals = vals[:len(indexes)]
	}

	slices.Sort(vals)
	return storage.NewListLabelValues(vals, nil)
}

// LabelValuesExcluding returns LabelValues for the given label name in all other series than those referred to by postings.
// This is useful for obtaining label values for other postings than the ones you wish to exclude.
func (p *MemPostings) LabelValuesExcluding(postings Postings, name string) storage.LabelValues {
	return p.labelValuesFor(postings, name, false)
}

func (p *MemPostings) labelValuesFor(postings Postings, name string, includeMatches bool) storage.LabelValues {
	p.mtx.RLock()

	e := p.m[name]
	if len(e) == 0 {
		p.mtx.RUnlock()
		return storage.EmptyLabelValues()
	}

	// With thread safety in mind and due to random key ordering in map, we have to construct the array in memory
	vals := make([]string, 0, len(e))
	candidates := make([]Postings, 0, len(e))
	// Allocate a slice for all needed ListPostings, so no need to allocate them one by one.
	lps := make([]ListPostings, 0, len(e))
	for val, srs := range e {
		vals = append(vals, val)
		lps = append(lps, ListPostings{list: srs})
		candidates = append(candidates, &lps[len(lps)-1])
	}

	var (
		indexes []int
		err     error
	)
	if includeMatches {
		indexes, err = FindIntersectingPostings(postings, candidates)
	} else {
		// We wish to exclude the postings when finding label values, meaning that
		// we want to filter down candidates to only those not fully contained
		// by postings (a fully contained postings iterator should be excluded).
		indexes, err = findNonContainedPostings(postings, candidates)
	}
	p.mtx.RUnlock()
	if err != nil {
		return storage.ErrLabelValues(err)
	}

	// Filter the values
	if len(vals) != len(indexes) {
		slices.Sort(indexes)
		for i, index := range indexes {
			vals[i] = vals[index]
		}
		vals = vals[:len(indexes)]
	}

	slices.Sort(vals)
	return storage.NewListLabelValues(vals, nil)
}

// LabelValuesFromSeries returns all unique label values from r for given label name from supplied series. Values are not sorted.
// buf is space for holding the result (if it isn't big enough, it will be ignored), may be nil.
func LabelValuesFromSeries(lg LabelsGetter, labelName string, refs []storage.SeriesRef, buf []string) ([]string, error) {
	values := make(map[string]struct{}, len(buf))

	var builder labels.ScratchBuilder
	for _, ref := range refs {
		err := lg.Labels(ref, &builder)
		// Postings may be stale. Skip if no underlying series exists.
		if errors.Is(err, storage.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("label values for label %s: %w", labelName, err)
		}

		v := builder.Labels().Get(labelName)
		if v != "" {
			values[v] = struct{}{}
		}
	}

	if cap(buf) >= len(values) {
		buf = buf[:0]
	} else {
		buf = make([]string, 0, len(values))
	}
	for v := range values {
		buf = append(buf, v)
	}
	return buf, nil
}

// intersect returns whether p1 and p2 have at least one series in common.
func intersect(p1, p2 Postings) bool {
	if !p1.Next() || !p2.Next() {
		return false
	}

	cur := p1.At()
	if p2.At() > cur {
		cur = p2.At()
	}

	for {
		if !p1.Seek(cur) {
			break
		}
		if p1.At() > cur {
			cur = p1.At()
		}
		if !p2.Seek(cur) {
			break
		}
		if p2.At() > cur {
			cur = p2.At()
			continue
		}

		return true
	}

	return false
}

// contains returns whether subp is contained in p.
func contains(p, subp Postings) bool {
	for subp.Next() {
		if needle := subp.At(); !p.Seek(needle) || p.At() != needle {
			return false
		}
	}

	// Couldn't find any value in subp which is not in p.
	return true
}

// LabelValuesStream returns an iterator over sorted label values for the given name.
// The matchers should only be for the name in question.
// LabelValues iterators need to be sorted, to enable merging of them.
func (p *MemPostings) LabelValuesStream(_ context.Context, name string, matchers ...*labels.Matcher) storage.LabelValues {
	p.mtx.RLock()

	values := make([]string, 0, len(p.m[name]))
loop:
	for v := range p.m[name] {
		for _, m := range matchers {
			if m.Name != name {
				// This should not happen
				continue
			}

			if !m.Matches(v) {
				continue loop
			}
		}
		values = append(values, v)
	}
	p.mtx.RUnlock()

	slices.Sort(values)
	return storage.NewListLabelValues(values, nil)
}
