package main

import roaring "github.com/RoaringBitmap/roaring/v2"

// MatchType distinguishes equality from negation.
type MatchType int

const (
	MatchEqual    MatchType = iota // name="value"
	MatchNotEqual                  // name!="value"
)

// Matcher is a simplified label matcher.
type Matcher struct {
	Name  string
	Value string
	Type  MatchType
}

func Equal(name, value string) Matcher    { return Matcher{Name: name, Value: value, Type: MatchEqual} }
func NotEqual(name, value string) Matcher { return Matcher{Name: name, Value: value, Type: MatchNotEqual} }

// isSubtracting returns true for matchers whose result should be
// subtracted from the working set rather than intersected into it.
func (m Matcher) isSubtracting() bool {
	return m.Type == MatchNotEqual
}

// postingsForKey collects the bitmap for a single (name, value) pair
// across all active segments, OR-ing them together.
func (idx *Index) postingsForKey(name, value string) *roaring.Bitmap {
	nameID, nameOK := idx.symbols.Lookup(name)
	valID, valOK := idx.symbols.Lookup(value)
	if !nameOK || !valOK {
		return roaring.New()
	}
	key := invertedKey{nameID, valID}

	result := roaring.New()
	for _, seg := range idx.allSegments() {
		if bm, ok := seg.inverted[key]; ok {
			result.Or(bm)
		}
	}
	return result
}

// allPostingsBitmap returns the union of allSeries across every segment.
func (idx *Index) allPostingsBitmap() *roaring.Bitmap {
	result := roaring.New()
	for _, seg := range idx.allSegments() {
		result.Or(seg.allSeries)
	}
	return result
}

// Postings returns the bitmap of series refs matching name=value
// across all segments.
func (idx *Index) Postings(name, value string) *roaring.Bitmap {
	return idx.postingsForKey(name, value)
}

// PostingsForMatchers resolves a set of matchers against the index,
// following the same intersect-then-subtract strategy as Prometheus.
//
//  1. Classify each matcher as intersecting (positive) or subtracting (negative).
//  2. For positive: lookup bitmap, collect into "its".
//  3. For negative: lookup bitmap of the inverse value, collect into "notIts".
//  4. If only negatives exist, start from allPostings.
//  5. AND all "its" together.
//  6. ANDNOT each "notIts" from the result.
func (idx *Index) PostingsForMatchers(matchers ...Matcher) *roaring.Bitmap {
	if len(matchers) == 0 {
		return roaring.New()
	}

	var its []*roaring.Bitmap
	var notIts []*roaring.Bitmap

	hasPositive := false
	for _, m := range matchers {
		if !m.isSubtracting() {
			hasPositive = true
			break
		}
	}

	if !hasPositive {
		its = append(its, idx.allPostingsBitmap())
	}

	for _, m := range matchers {
		bm := idx.postingsForKey(m.Name, m.Value)
		if m.isSubtracting() {
			notIts = append(notIts, bm)
		} else {
			its = append(its, bm)
		}
	}

	// Intersect all positive bitmaps.
	var result *roaring.Bitmap
	switch len(its) {
	case 0:
		result = roaring.New()
	case 1:
		result = its[0].Clone()
	default:
		result = its[0].Clone()
		for _, bm := range its[1:] {
			result.And(bm)
		}
	}

	// Subtract all negative bitmaps.
	for _, bm := range notIts {
		result.AndNot(bm)
	}

	return result
}
