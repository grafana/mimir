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
	// PostingsOffset returns the byte range of the postings section for the label with the given name and value.
	// The Start is inclusive and the End is exclusive.
	// The end offset might be bigger than the actual posting ending, but not larger than the whole index file.
	PostingsOffset(name string, value string) (rng index.Range, found bool, err error)

	// LabelValues returns a list of values for the label named name that match filter and have the prefix provided.
	LabelValues(name string, prefix string, filter func(string) bool) ([]string, error)

	// LabelNames returns a sorted list of all label names in this table.
	LabelNames() ([]string, error)
}

type PostingOffsetTableV1 struct {
	// For the v1 format, labelname -> labelvalue -> offset.
	postings map[string]map[string]index.Range
}

func NewPostingOffsetTable(factory *streamencoding.DecbufFactory, tableOffset int, indexVersion int, indexLastPostingListEndBound uint64, postingOffsetsInMemSampling int) (PostingOffsetTable, error) {
	if indexVersion == index.FormatV1 {
		return newV1PostingOffsetTable(factory, tableOffset, indexLastPostingListEndBound)
	} else if indexVersion == index.FormatV2 {
		return newV2PostingOffsetTable(factory, tableOffset, indexLastPostingListEndBound, postingOffsetsInMemSampling)
	}

	return nil, fmt.Errorf("unknown index version %v", indexVersion)
}

func newV1PostingOffsetTable(factory *streamencoding.DecbufFactory, tableOffset int, indexLastPostingListEndBound uint64) (*PostingOffsetTableV1, error) {
	t := PostingOffsetTableV1{
		postings: map[string]map[string]index.Range{},
	}

	// Earlier V1 formats don't have a sorted postings offset table, so
	// load the whole offset table into memory.
	var lastKey string
	var lastValue string
	var prevRng index.Range

	if err := readOffsetTable(factory, tableOffset, func(key string, value string, off uint64) error {
		if len(t.postings) > 0 {
			prevRng.End = int64(off - crc32.Size)
			t.postings[lastKey][lastValue] = prevRng
		}

		if _, ok := t.postings[key]; !ok {
			t.postings[key] = map[string]index.Range{}
		}

		lastKey = key
		lastValue = value
		prevRng = index.Range{Start: int64(off + postingLengthFieldSize)}
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "read postings table")
	}

	if len(t.postings) > 0 {
		// In case lastValOffset is unknown as we don't have next posting anymore. Guess from the index table of contents.
		// The last posting list ends before the label offset table.
		// In worst case we will overfetch a few bytes.
		prevRng.End = int64(indexLastPostingListEndBound) - crc32.Size
		t.postings[lastKey][lastValue] = prevRng
	}

	return &t, nil
}

func newV2PostingOffsetTable(factory *streamencoding.DecbufFactory, tableOffset int, indexLastPostingListEndBound uint64, postingOffsetsInMemSampling int) (table *PostingOffsetTableV2, err error) {
	t := PostingOffsetTableV2{
		factory:                     factory,
		tableOffset:                 tableOffset,
		postings:                    map[string]*postingValueOffsets{},
		postingOffsetsInMemSampling: postingOffsetsInMemSampling,
	}

	d := factory.NewDecbufAtChecked(tableOffset, castagnoliTable)
	defer runutil.CloseWithErrCapture(&err, &d, "read offset table")

	remainingCount := d.Be32()
	currentName := ""
	valuesForCurrentKey := 0
	lastEntryOffsetInTable := -1

	for d.Err() == nil && remainingCount > 0 {
		lastName := currentName
		offsetInTable := d.Position()
		keyCount := d.Uvarint()

		// The Postings offset table takes only 2 keys per entry (name and value of label).
		if keyCount != 2 {
			return nil, errors.Errorf("unexpected key length for posting table %d", keyCount)
		}

		// Important: this value is only valid as long as we don't perform any further reads from d.
		// If we need to retain its value, we must copy it before performing another read.
		if unsafeName := d.UnsafeUvarintBytes(); len(t.postings) == 0 || lastName != string(unsafeName) {
			newName := string(unsafeName)

			if lastEntryOffsetInTable != -1 {
				// We haven't recorded the last offset for the last value of the previous name.
				// Go back and read the last value for the previous name.
				newValueOffsetInTable := d.Position()
				d.ResetAt(lastEntryOffsetInTable)
				d.Uvarint()          // Skip the key count
				d.SkipUvarintBytes() // Skip the name
				value := d.UvarintStr()
				t.postings[lastName].offsets = append(t.postings[lastName].offsets, postingOffset{value: value, tableOff: lastEntryOffsetInTable})

				// Skip ahead to where we were before we called ResetAt() above.
				d.Skip(newValueOffsetInTable - d.Position())
			}

			currentName = newName
			t.postings[currentName] = &postingValueOffsets{}
			valuesForCurrentKey = 0
		}

		// Retain every 1-in-postingOffsetsInMemSampling entries, starting with the first one.
		if valuesForCurrentKey%postingOffsetsInMemSampling == 0 {
			value := d.UvarintStr()
			off := d.Uvarint64()
			t.postings[currentName].offsets = append(t.postings[currentName].offsets, postingOffset{value: value, tableOff: offsetInTable})

			if lastName != currentName {
				t.postings[lastName].lastValOffset = int64(off - crc32.Size)
			}

			// If the current value is the last one for this name, we don't need to record it again.
			lastEntryOffsetInTable = -1
		} else {
			// We only need to store this value if it's the last one for this name.
			// Record our current position in the table and come back to it if it turns out this is the last value.
			lastEntryOffsetInTable = offsetInTable

			// Skip over the value and offset.
			d.SkipUvarintBytes()
			d.Uvarint64()
		}

		valuesForCurrentKey++
		remainingCount--
	}

	if lastEntryOffsetInTable != -1 {
		// We haven't recorded the last offset for the last value of the last key
		// Go back and read the last value for the last key.
		d.ResetAt(lastEntryOffsetInTable)
		d.Uvarint()          // Skip the key count
		d.SkipUvarintBytes() // Skip the key
		value := d.UvarintStr()
		t.postings[currentName].offsets = append(t.postings[currentName].offsets, postingOffset{value: value, tableOff: lastEntryOffsetInTable})
	}

	if d.Err() != nil {
		return nil, errors.Wrap(d.Err(), "read postings table")
	}

	if len(t.postings) > 0 {
		// In case lastValOffset is unknown as we don't have next posting anymore. Guess from the index table of contents.
		// The last posting list ends before the label offset table.
		// In worst case we will overfetch a few bytes.
		t.postings[currentName].lastValOffset = int64(indexLastPostingListEndBound) - crc32.Size
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
func readOffsetTable(factory *streamencoding.DecbufFactory, tableOffset int, f func(string, string, uint64) error) (err error) {
	d := factory.NewDecbufAtChecked(tableOffset, castagnoliTable)
	defer runutil.CloseWithErrCapture(&err, &d, "read offset table")

	cnt := d.Be32()

	for d.Err() == nil && d.Len() > 0 && cnt > 0 {
		keyCount := d.Uvarint()

		// The Postings offset table takes only 2 keys per entry (name and value of label).
		if keyCount != 2 {
			return errors.Errorf("unexpected key length for posting table %d", keyCount)
		}

		key := d.UvarintStr()
		value := d.UvarintStr()
		o := d.Uvarint64()
		if d.Err() != nil {
			break
		}
		if err := f(key, value, o); err != nil {
			return err
		}
		cnt--
	}
	return d.Err()
}

func (t *PostingOffsetTableV1) PostingsOffset(name string, value string) (index.Range, bool, error) {
	e, ok := t.postings[name]
	if !ok {
		return index.Range{}, false, nil
	}
	rng, ok := e[value]
	if !ok {
		return index.Range{}, false, nil
	}
	return rng, true, nil
}

func (t *PostingOffsetTableV1) LabelValues(name string, prefix string, filter func(string) bool) ([]string, error) {
	e, ok := t.postings[name]
	if !ok {
		return nil, nil
	}
	values := make([]string, 0, len(e))
	for k := range e {
		if strings.HasPrefix(k, prefix) && (filter == nil || filter(k)) {
			values = append(values, k)
		}
	}
	slices.Sort(values)
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

func (e *postingValueOffsets) prefixOffset(prefix string) (int, bool) {
	// Find the first offset that is greater or equal to the value.
	offsetIdx := sort.Search(len(e.offsets), func(i int) bool {
		return prefix <= e.offsets[i].value
	})

	// We always include the last value in the offsets,
	// and given that prefix is always less or equal than the value,
	// we can conclude that there are no values with this prefix.
	if offsetIdx == len(e.offsets) {
		return 0, false
	}

	// Prefix is lower than the first value in the offsets, and that first value doesn't have this prefix.
	// Next values won't have the prefix, so we can return early.
	if offsetIdx == 0 && prefix < e.offsets[0].value && !strings.HasPrefix(e.offsets[0].value, prefix) {
		return 0, false
	}

	// If the value is not equal to the prefix, this value might have the prefix.
	// But maybe the values in the previous offset also had the prefix,
	// so we need to step back one offset to find all values with this prefix.
	// Unless, of course, we are at the first offset.
	if offsetIdx > 0 && e.offsets[offsetIdx].value != prefix {
		offsetIdx--
	}

	return offsetIdx, true
}

type postingOffset struct {
	// label value.
	value string
	// offset of this entry in posting offset table in index-header file.
	tableOff int
}

func (t *PostingOffsetTableV2) PostingsOffset(name string, value string) (r index.Range, found bool, err error) {
	e, ok := t.postings[name]
	if !ok {
		return index.Range{}, false, nil
	}

	if value < e.offsets[0].value {
		// The desired value sorts before the first known value.
		return index.Range{}, false, nil
	}

	d := t.factory.NewDecbufAtUnchecked(t.tableOffset)
	defer runutil.CloseWithErrCapture(&err, &d, "get postings offsets")
	if err := d.Err(); err != nil {
		return index.Range{}, false, err
	}

	i := sort.Search(len(e.offsets), func(i int) bool { return e.offsets[i].value >= value })

	if i == len(e.offsets) {
		// The desired value sorts after the last known value.
		return index.Range{}, false, nil
	}

	if i > 0 && e.offsets[i].value != value {
		// Need to look from previous entry.
		i--
	}

	d.ResetAt(e.offsets[i].tableOff)
	nAndNameSize := 0

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
		skipNAndName(&d, &nAndNameSize)
		currentValue := d.UvarintStr()
		postingOffset := int64(d.Uvarint64())

		if currentValue > value {
			// There is no entry for value.
			return index.Range{}, false, nil
		}

		if currentValue == value {
			rng := index.Range{Start: postingOffset + postingLengthFieldSize}

			if i+1 == len(e.offsets) {
				// No more offsets for this name.
				rng.End = e.lastValOffset
			} else {
				// There's at least one more value for this name, use that as the end of the range.
				skipNAndName(&d, &nAndNameSize)
				d.SkipUvarintBytes() // Label value.
				postingOffset := int64(d.Uvarint64())
				rng.End = postingOffset - crc32.Size
			}

			if d.Err() != nil {
				return index.Range{}, false, errors.Wrap(d.Err(), "get postings offset entry")
			}

			return rng, true, nil
		}
	}

	if d.Err() != nil {
		return index.Range{}, false, errors.Wrap(d.Err(), "get postings offset entry")
	}

	// If we get to here, there is no entry for value.
	return index.Range{}, false, nil
}

func (t *PostingOffsetTableV2) LabelValues(name string, prefix string, filter func(string) bool) (v []string, err error) {
	e, ok := t.postings[name]
	if !ok {
		return nil, nil
	}
	if len(e.offsets) == 0 {
		return nil, nil
	}

	offsetIdx := 0
	if prefix != "" {
		offsetIdx, ok = e.prefixOffset(prefix)
		if !ok {
			return nil, nil
		}
	}

	// Don't Crc32 the entire postings offset table, this is very slow
	// so hope any issues were caught at startup.
	d := t.factory.NewDecbufAtUnchecked(t.tableOffset)
	defer runutil.CloseWithErrCapture(&err, &d, "get label values")

	d.ResetAt(e.offsets[offsetIdx].tableOff)
	lastVal := e.offsets[len(e.offsets)-1].value

	skip := 0
	values := make([]string, 0, (len(e.offsets)-offsetIdx)*t.postingOffsetsInMemSampling)
	for d.Err() == nil {
		if skip == 0 {
			// These are always the same number of bytes,
			// and it's faster to skip than parse.
			skip = d.Len()
			d.Uvarint()          // Keycount.
			d.SkipUvarintBytes() // Label name.
			skip -= d.Len()
		} else {
			d.Skip(skip)
		}

		unsafeValue := yoloString(d.UnsafeUvarintBytes())
		if prefix == "" {
			// Quick path for no prefix matching.
			if filter == nil || filter(unsafeValue) {
				// Clone the yolo string since its bytes will be invalidated as soon as
				// any other reads against the decoding buffer are performed.
				values = append(values, strings.Clone(unsafeValue))
			}
		} else {
			if strings.HasPrefix(unsafeValue, prefix) {
				if filter == nil || filter(unsafeValue) {
					// Clone the yolo string since its bytes will be invalidated as soon as
					// any other reads against the decoding buffer are performed.
					values = append(values, strings.Clone(unsafeValue))
				}
			} else if prefix < unsafeValue {
				// There will be no more values with the prefix.
				break
			}
		}

		if unsafeValue == lastVal {
			break
		}
		d.Uvarint64() // Offset.
	}
	if d.Err() != nil {
		return nil, errors.Wrap(d.Err(), "get label values")
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
		d.Uvarint()          // Keycount.
		d.SkipUvarintBytes() // Label name.
		*buf -= d.Len()
		return
	}
	d.Skip(*buf)
}
