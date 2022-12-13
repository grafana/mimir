// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/tsdb/index/postings.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package index

import (
	"fmt"
	"hash/crc32"
	"sort"
	"strings"

	"github.com/grafana/dskit/runutil"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb/index"
	"golang.org/x/exp/slices"

	streamencoding "github.com/grafana/mimir/pkg/storegateway/indexheader/encoding"
)

const postingLengthFieldSize = 4

type PostingOffsetTable interface {
	// PostingsOffset returns a list of offsets for labels named name with value in values.
	// TODO: can this be simplified to only handle a single value?
	// StreamBinaryReader.PostingsOffset only ever calls this method with a single value, although there's a comment
	// in there about taking advantage of retrieving multiple values at once.
	PostingsOffset(name string, values ...string) ([]index.Range, error)

	// LabelValues returns a list of values for the label named name that match filter.
	LabelValues(name string, filter func(string) bool) ([]string, error)

	// LabelNames returns a sorted list of all label names in this table.
	LabelNames() ([]string, error)
}

type PostingOffsetTableV1 struct {
	// For the v1 format, labelname -> labelvalue -> offset.
	postings map[string]map[string]index.Range
}

func NewPostingOffsetTable(factory *streamencoding.DecbufFactory, tableOffset int, indexVersion int, indexLastPostingEnd uint64, postingOffsetsInMemSampling int) (PostingOffsetTable, error) {
	if indexVersion == index.FormatV1 {
		return newV1PostingOffsetTable(factory, tableOffset, indexLastPostingEnd)
	} else if indexVersion == index.FormatV2 {
		return newV2PostingOffsetTable(factory, tableOffset, indexLastPostingEnd, postingOffsetsInMemSampling)
	}

	return nil, fmt.Errorf("unknown index version %v", indexVersion)
}

func newV1PostingOffsetTable(factory *streamencoding.DecbufFactory, tableOffset int, indexLastPostingEnd uint64) (*PostingOffsetTableV1, error) {
	t := PostingOffsetTableV1{
		postings: map[string]map[string]index.Range{},
	}

	// Earlier V1 formats don't have a sorted postings offset table, so
	// load the whole offset table into memory.
	var lastKey []string
	var prevRng index.Range

	if err := readOffsetTable(factory, tableOffset, func(key []string, off uint64, _ int) error {
		if len(key) != 2 {
			return errors.Errorf("unexpected key length for posting table %d", len(key))
		}

		if lastKey != nil {
			prevRng.End = int64(off - crc32.Size)
			t.postings[lastKey[0]][lastKey[1]] = prevRng
		}

		if _, ok := t.postings[key[0]]; !ok {
			t.postings[key[0]] = map[string]index.Range{}
		}

		lastKey = key
		prevRng = index.Range{Start: int64(off + postingLengthFieldSize)}
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "read postings table")
	}

	if lastKey != nil {
		prevRng.End = int64(indexLastPostingEnd) - crc32.Size // Each posting offset table ends with a CRC32 checksum.
		t.postings[lastKey[0]][lastKey[1]] = prevRng
	}

	return &t, nil
}

func newV2PostingOffsetTable(factory *streamencoding.DecbufFactory, tableOffset int, indexLastPostingEnd uint64, postingOffsetsInMemSampling int) (*PostingOffsetTableV2, error) {
	t := PostingOffsetTableV2{
		factory:                     factory,
		tableOffset:                 tableOffset,
		postings:                    map[string]*postingValueOffsets{},
		postingOffsetsInMemSampling: postingOffsetsInMemSampling,
	}

	lastTableOff := 0
	valueCount := 0
	var lastKey []string

	// For the postings offset table we keep every label name but only every nth
	// label value (plus the first and last one), to save memory.
	if err := readOffsetTable(factory, tableOffset, func(key []string, off uint64, tableOff int) error {
		if len(key) != 2 {
			return errors.Errorf("unexpected key length for posting table %d", len(key))
		}

		if _, ok := t.postings[key[0]]; !ok {
			// Not seen before label name.
			t.postings[key[0]] = &postingValueOffsets{}
			if lastKey != nil {
				// Always include last value for each label name, unless it was just added in previous iteration based
				// on valueCount.
				if (valueCount-1)%postingOffsetsInMemSampling != 0 {
					t.postings[lastKey[0]].offsets = append(t.postings[lastKey[0]].offsets, postingOffset{value: lastKey[1], tableOff: lastTableOff})
				}
				t.postings[lastKey[0]].lastValOffset = int64(off - crc32.Size)
				lastKey = nil
			}
			valueCount = 0
		}

		lastKey = key
		lastTableOff = tableOff
		valueCount++

		if (valueCount-1)%postingOffsetsInMemSampling == 0 {
			t.postings[key[0]].offsets = append(t.postings[key[0]].offsets, postingOffset{value: key[1], tableOff: tableOff})
		}

		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "read postings table")
	}
	if lastKey != nil {
		if (valueCount-1)%postingOffsetsInMemSampling != 0 {
			// Always include last value for each label name if not included already based on valueCount.
			t.postings[lastKey[0]].offsets = append(t.postings[lastKey[0]].offsets, postingOffset{value: lastKey[1], tableOff: lastTableOff})
		}
		// In any case lastValOffset is unknown as don't have next posting anymore. Guess from TOC table.
		// In worst case we will overfetch a few bytes.
		t.postings[lastKey[0]].lastValOffset = int64(indexLastPostingEnd) - crc32.Size // Each posting offset table ends with a CRC32 checksum.
	}
	// Trim any extra space in the slices.
	for k, v := range t.postings {
		if len(v.offsets) == cap(v.offsets) {
			continue
		}

		l := make([]postingOffset, len(v.offsets))
		copy(l, v.offsets)
		t.postings[k].offsets = l
	}

	return &t, nil
}

// readOffsetTable reads an offset table and at the given position calls f for each
// found entry. If f returns an error it stops decoding and returns the received error.
func readOffsetTable(factory *streamencoding.DecbufFactory, tableOffset int, f func([]string, uint64, int) error) (err error) {
	d := factory.NewDecbufAtChecked(tableOffset, castagnoliTable)
	defer runutil.CloseWithErrCapture(&err, &d, "read offset table")

	startLen := d.Len()
	cnt := d.Be32()

	for d.Err() == nil && d.Len() > 0 && cnt > 0 {
		offsetPos := startLen - d.Len()
		keyCount := d.Uvarint()
		// The Postings offset table takes only 2 keys per entry (name and value of label),
		// and the LabelIndices offset table takes only 1 key per entry (a label name).
		// Hence setting the size to max of both, i.e. 2.

		keys := make([]string, 0, 2)

		for i := 0; i < keyCount; i++ {
			keys = append(keys, d.UvarintStr())
		}
		o := d.Uvarint64()
		if d.Err() != nil {
			break
		}
		if err := f(keys, o, offsetPos); err != nil {
			return err
		}
		cnt--
	}
	return d.Err()
}

func (t *PostingOffsetTableV1) PostingsOffset(name string, values ...string) ([]index.Range, error) {
	rngs := make([]index.Range, 0, len(values))
	e, ok := t.postings[name]
	if !ok {
		return nil, nil
	}
	for _, v := range values {
		rng, ok := e[v]
		if !ok {
			continue
		}
		rngs = append(rngs, rng)
	}
	return rngs, nil
}

func (t *PostingOffsetTableV1) LabelValues(name string, filter func(string) bool) ([]string, error) {
	e, ok := t.postings[name]
	if !ok {
		return nil, nil
	}
	values := make([]string, 0, len(e))
	for k := range e {
		if filter == nil || filter(k) {
			values = append(values, k)
		}
	}
	sort.Strings(values)
	return values, nil
}

func (t *PostingOffsetTableV1) LabelNames() ([]string, error) {
	labelNames := make([]string, 0, len(t.postings))
	allPostingsKeyName, _ := index.AllPostingsKey()

	for name := range t.postings {
		if name == allPostingsKeyName {
			continue
		}

		labelNames = append(labelNames, name)
	}

	slices.Sort(labelNames)

	return labelNames, nil
}

type PostingOffsetTableV2 struct {
	// Map of LabelName to a list of some LabelValues's position in the offset table.
	// The first and last values for each name are always present, we keep only 1/postingOffsetsInMemSampling of the rest.
	postings map[string]*postingValueOffsets

	postingOffsetsInMemSampling int

	factory     *streamencoding.DecbufFactory
	tableOffset int
}

type postingValueOffsets struct {
	offsets       []postingOffset
	lastValOffset int64
}

type postingOffset struct {
	// label value.
	value string
	// offset of this entry in posting offset table in index-header file.
	tableOff int
}

func (t *PostingOffsetTableV2) PostingsOffset(name string, values ...string) (r []index.Range, err error) {
	e, ok := t.postings[name]
	if !ok {
		return nil, nil
	}

	if len(values) == 0 {
		return nil, nil
	}

	rngs := make([]index.Range, 0, len(values))
	buf := 0
	valueIndex := 0
	for valueIndex < len(values) && values[valueIndex] < e.offsets[0].value {
		// Discard values before the start.
		valueIndex++
	}

	d := t.factory.NewDecbufAtUnchecked(t.tableOffset)
	defer runutil.CloseWithErrCapture(&err, &d, "get postings offsets")
	if err := d.Err(); err != nil {
		return nil, err
	}

	var newSameRngs []index.Range // The start, end offsets in the postings table in the original index file.
	for valueIndex < len(values) {
		wantedValue := values[valueIndex]

		i := sort.Search(len(e.offsets), func(i int) bool { return e.offsets[i].value >= wantedValue })
		if i == len(e.offsets) {
			// We're past the end.
			break
		}
		if i > 0 && e.offsets[i].value != wantedValue {
			// Need to look from previous entry.
			i--
		}

		// Reusing the same decoding buffer on each iteration so make sure it's reset to
		// the beginning of the posting offset table before we search
		d.ResetAt(e.offsets[i].tableOff + 4) // 4 byte length of the offset table

		// Iterate on the offset table.
		newSameRngs = newSameRngs[:0]
		for d.Err() == nil {
			// Posting format entry is as follows:
			// │ ┌────────────────────────────────────────┐ │
			// │ │  n = 2 <1b>                            │ │
			// │ ├──────────────────────┬─────────────────┤ │
			// │ │ len(name) <uvarint>  │ name <bytes>    │ │
			// │ ├──────────────────────┼─────────────────┤ │
			// │ │ len(value) <uvarint> │ value <bytes>   │ │
			// │ ├──────────────────────┴─────────────────┤ │
			// │ │  offset <uvarint64>                    │ │
			// │ └────────────────────────────────────────┘ │
			// First, let's skip n and name.
			skipNAndName(&d, &buf)
			value := d.UvarintStr() // Label value.
			postingOffset := int64(d.Uvarint64())

			if len(newSameRngs) > 0 {
				// We added some ranges in previous iteration. Use next posting offset as end of all our new ranges.
				for j := range newSameRngs {
					newSameRngs[j].End = postingOffset - crc32.Size
				}
				rngs = append(rngs, newSameRngs...)
				newSameRngs = newSameRngs[:0]
			}

			for value >= wantedValue {
				// If wantedValue is equals of greater than current value, loop over all given wanted values in the values until
				// this is no longer true or there are no more values wanted.
				// This ensures we cover case when someone asks for postingsOffset(name, value1, value1, value1).

				// Record on the way if wanted value is equal to the current value.
				if value == wantedValue {
					newSameRngs = append(newSameRngs, index.Range{Start: postingOffset + postingLengthFieldSize})
				}
				valueIndex++
				if valueIndex == len(values) {
					break
				}
				wantedValue = values[valueIndex]
			}

			if i+1 == len(e.offsets) {
				// No more offsets for this name.
				// Break this loop and record lastOffset on the way for ranges we just added if any.
				for j := range newSameRngs {
					newSameRngs[j].End = e.lastValOffset
				}
				rngs = append(rngs, newSameRngs...)
				break
			}

			if valueIndex != len(values) && wantedValue <= e.offsets[i+1].value {
				// wantedValue is smaller or same as the next offset we know about, let's iterate further to add those.
				continue
			}

			// Nothing wanted or wantedValue is larger than next offset we know about.
			// Let's exit and do binary search again / exit if nothing wanted.

			if len(newSameRngs) > 0 {
				// We added some ranges in this iteration. Use next posting offset as the end of our ranges.
				// We know it exists as we never go further in this loop than e.offsets[i, i+1].

				skipNAndName(&d, &buf)
				d.UvarintBytes() // Label value.
				postingOffset := int64(d.Uvarint64())

				for j := range newSameRngs {
					newSameRngs[j].End = postingOffset - crc32.Size
				}
				rngs = append(rngs, newSameRngs...)
			}
			break
		}
		if d.Err() != nil {
			return nil, errors.Wrap(d.Err(), "get postings offset entry")
		}
	}

	return rngs, nil
}

func (t *PostingOffsetTableV2) LabelValues(name string, filter func(string) bool) (v []string, err error) {
	e, ok := t.postings[name]
	if !ok {
		return nil, nil
	}
	if len(e.offsets) == 0 {
		return nil, nil
	}
	values := make([]string, 0, len(e.offsets)*t.postingOffsetsInMemSampling)

	// Don't Crc32 the entire postings offset table, this is very slow
	// so hope any issues were caught at startup.
	d := t.factory.NewDecbufAtUnchecked(t.tableOffset)
	defer runutil.CloseWithErrCapture(&err, &d, "get label values")

	d.Skip(e.offsets[0].tableOff)
	lastVal := e.offsets[len(e.offsets)-1].value

	skip := 0
	for d.Err() == nil {
		if skip == 0 {
			// These are always the same number of bytes,
			// and it's faster to skip than parse.
			skip = d.Len()
			d.Uvarint()      // Keycount.
			d.UvarintBytes() // Label name.
			skip -= d.Len()
		} else {
			d.Skip(skip)
		}
		s := yoloString(d.UvarintBytes()) // Label value.
		if filter == nil || filter(s) {
			// Clone the yolo string since its bytes will be invalidated as soon as
			// any other reads against the decoding buffer are performed.
			values = append(values, strings.Clone(s))
		}
		if s == lastVal {
			break
		}
		d.Uvarint64() // Offset.
	}
	if d.Err() != nil {
		return nil, errors.Wrap(d.Err(), "get postings offset entry")
	}
	return values, nil
}

func (t *PostingOffsetTableV2) LabelNames() ([]string, error) {
	labelNames := make([]string, 0, len(t.postings))
	allPostingsKeyName, _ := index.AllPostingsKey()

	for name := range t.postings {
		if name == allPostingsKeyName {
			continue
		}

		labelNames = append(labelNames, name)
	}

	slices.Sort(labelNames)

	return labelNames, nil
}

func skipNAndName(d *streamencoding.Decbuf, buf *int) {
	if *buf == 0 {
		// Keycount+LabelName are always the same number of bytes,
		// and it's faster to skip than parse.
		*buf = d.Len()
		d.Uvarint()      // Keycount.
		d.UvarintBytes() // Label name.
		*buf -= d.Len()
		return
	}
	d.Skip(*buf)
}
