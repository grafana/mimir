// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/tsdb/index/postings.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package index

import (
	"context"
	"fmt"
	"hash/crc32"
	"slices"
	"sort"
	"strings"

	"github.com/grafana/dskit/runutil"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb/index"

	streamencoding "github.com/grafana/mimir/pkg/storage/indexheader/encoding"
	"github.com/grafana/mimir/pkg/storage/indexheader/indexheaderpb"
)

const (
	postingLengthFieldSize = 4
	// CheckContextEveryNIterations is used in some tight loops to check if the context is done.
	CheckContextEveryNIterations = 1024
)

// PostingListOffset contains the start and end offset of a posting list.
// The Start is inclusive and is the byte offset of the number_of_entries field of a posting list.
// The End is exclusive and is typically the byte offset of the CRC32 field.
// The End might be bigger than the actual posting ending, but not larger than the whole index file.
type PostingListOffset struct {
	LabelValue string
	Off        index.Range
}

type PostingOffsetsTableReader interface {
	// PostingsOffset returns the byte range of the postings section for the label with the given name and value.
	// The Start is inclusive and is the byte offset of the number_of_entries field of a posting list.
	// The End is exclusive and is typically the byte offset of the CRC32 field.
	// The End might be bigger than the actual posting ending, but not larger than the whole index file.
	PostingsOffset(name string, value string) (rng index.Range, found bool, err error)

	// LabelValuesOffsets returns all postings lists for the label named name that match filter and have the prefix provided.
	// The ranges of each posting list are the same as returned by PostingsOffset.
	// The returned label values are sorted lexicographically (which the same as sorted by posting offset).
	LabelValuesOffsets(ctx context.Context, name, prefix string, filter func(string) bool) ([]PostingListOffset, error)

	// LabelNames returns a sorted list of all label names in this table.
	LabelNames() ([]string, error)

	ToSparsePostingOffsetTable() (table *indexheaderpb.PostingOffsetTable)

	// PostingOffsetInMemSampling returns the inverse of the fraction of postings held in memory. A lower value indicates
	// postings are sample more frequently.
	PostingOffsetInMemSampling() int
}

// LabelSparsePostingsOffsets contains the offsets within the Postings Offsets table
// for a sparse set of Postings Offsets table entries with the same label name.
// This enables a fast in-memory binary search of a subset of table entries
// in order to bound the size of the actual table scan.
//
// Postings Offsets table entries contain a "key": a label (name, value) pair
// and a "value": the start offset in the Postings List for that label (name, value).
// This type does *not* capture those values pointing to the Postings list,
// only the location of the label (name, value) entries within the Postings Offsets table.
type LabelSparsePostingsOffsets struct {
	SparseOffsets []LabelValuePostingsOffset
	LastValOffset int64
}

type LabelValuePostingsOffset struct {
	// label value.
	value string
	// offset of this entry in posting offset table in index-header file.
	tableOff int
}

// prefixOffsets returns the index of the first matching offset (start) and the index of the first non-matching (end).
// If all SparseOffsets match the prefix, then end will equal the length of SparseOffsets.
// prefixOffsets returns false when no SparseOffsets match this prefix.
func (e *LabelSparsePostingsOffsets) prefixOffsets(prefix string) (start, end int, found bool) {
	// Find the first offset that is greater or equal to the value.
	start = sort.Search(len(e.SparseOffsets), func(i int) bool {
		return prefix <= e.SparseOffsets[i].value
	})

	// We always include the last value in the SparseOffsets,
	// and given that prefix is always less or equal than the value,
	// we can conclude that there are no values with this prefix.
	if start == len(e.SparseOffsets) {
		return 0, 0, false
	}

	// Prefix is lower than the first value in the SparseOffsets, and that first value doesn't have this prefix.
	// Next values won't have the prefix, so we can return early.
	if start == 0 && prefix < e.SparseOffsets[0].value && !strings.HasPrefix(e.SparseOffsets[0].value, prefix) {
		return 0, 0, false
	}

	// If the value is not equal to the prefix, this value might have the prefix.
	// But maybe the values in the previous offset also had the prefix,
	// so we need to step back one offset to find all values with this prefix.
	// Unless, of course, we are at the first offset.
	if start > 0 && e.SparseOffsets[start].value != prefix {
		start--
	}

	// Find the first offset which is larger than the prefix and doesn't have the prefix.
	// All values at and after that offset will not match the prefix.
	end = sort.Search(len(e.SparseOffsets)-start, func(i int) bool {
		return prefix < e.SparseOffsets[i+start].value && !strings.HasPrefix(e.SparseOffsets[i+start].value, prefix)
	})
	end += start
	return start, end, true
}

func NewPostingOffsetTableFromSparseHeader(
	factory streamencoding.DecbufFactory,
	postingsOffsetTable *indexheaderpb.PostingOffsetTable,
	tableOffset int,
	sparseSampleFactor int,
) (table *PostingOffsetsTableV2, err error) {
	t := PostingOffsetsTableV2{
		factory:               factory,
		tableOffset:           tableOffset,
		sparsePostingsOffsets: make(map[string]*LabelSparsePostingsOffsets, len(postingsOffsetTable.Postings)),
		sparseSampleFactor:    sparseSampleFactor,
	}

	pbSampling := int(postingsOffsetTable.GetPostingOffsetInMemorySampling())
	if pbSampling == 0 {
		return nil, fmt.Errorf("sparse index-header sampling rate not set")
	}

	if pbSampling > sparseSampleFactor {
		return nil, fmt.Errorf("sparse index-header sampling rate exceeds in-mem-sampling rate")
	}

	// if the sampling rate in the sparse index-header is set lower (more frequent) than
	// the configured sparseSampleFactor we downsample to the configured rate
	step, ok := stepSize(pbSampling, sparseSampleFactor)
	if !ok {
		return nil, fmt.Errorf("sparse index-header sampling rate not compatible with in-mem-sampling rate")
	}

	for sName, sOffsets := range postingsOffsetTable.Postings {

		olen := len(sOffsets.Offsets)
		downsampledLen := (olen + step - 1) / step
		if (olen > 1) && (downsampledLen == 1) {
			downsampledLen++
		}

		t.sparsePostingsOffsets[sName] = &LabelSparsePostingsOffsets{SparseOffsets: make([]LabelValuePostingsOffset, downsampledLen)}
		for i, sPostingOff := range sOffsets.Offsets {
			if i%step == 0 {
				t.sparsePostingsOffsets[sName].SparseOffsets[i/step] = LabelValuePostingsOffset{value: sPostingOff.Value, tableOff: int(sPostingOff.TableOff)}
			}

			if i == olen-1 {
				t.sparsePostingsOffsets[sName].SparseOffsets[downsampledLen-1] = LabelValuePostingsOffset{value: sPostingOff.Value, tableOff: int(sPostingOff.TableOff)}
			}
		}
		t.sparsePostingsOffsets[sName].LastValOffset = sOffsets.LastValOffset
	}
	return &t, err
}

func NewPostingOffsetTableReaderFromIndexHeader(
	decbufFactory streamencoding.DecbufFactory,
	tableOffset int,
	indexVersion int,
	indexLastPostingListEndBound uint64,
	sparseSampleFactor int,
	doChecksum bool,
) (PostingOffsetsTableReader, error) {
	switch indexVersion {
	case index.FormatV2:
		return newV2PostingOffsetTableReader(decbufFactory, tableOffset, indexLastPostingListEndBound, sparseSampleFactor, doChecksum)
	}

	return nil, fmt.Errorf("unknown or unsupported index version %v", indexVersion)
}

func newV2PostingOffsetTableReader(
	decbufFactory streamencoding.DecbufFactory,
	tableOffset int,
	indexLastPostingListEndBound uint64,
	sparseSampleFactor int,
	doChecksum bool,
) (table *PostingOffsetsTableV2, err error) {
	sparsePostingsOffsets, err := SparseValuesFromPostingsOffsetsTable(
		decbufFactory, tableOffset, doChecksum, sparseSampleFactor, indexLastPostingListEndBound,
	)

	return &PostingOffsetsTableV2{
		factory:               decbufFactory,
		tableOffset:           tableOffset,
		sparsePostingsOffsets: sparsePostingsOffsets,
		sparseSampleFactor:    sparseSampleFactor,
	}, nil
}

func SparseValuesFromPostingsOffsetsTable(
	decbufFactory streamencoding.DecbufFactory,
	tableOffset int,
	doChecksum bool,
	sparseSampleFactor int,
	indexLastPostingListEndBound uint64,
) (sparsePostingsOffsets map[string]*LabelSparsePostingsOffsets, err error) {
	var decbuf streamencoding.Decbuf
	if doChecksum {
		decbuf = decbufFactory.NewDecbufAtChecked(tableOffset, castagnoliTable)
	} else {
		decbuf = decbufFactory.NewDecbufAtUnchecked(tableOffset)
	}

	defer runutil.CloseWithErrCapture(&err, &decbuf, "decode postings offsets table")
	if err := decbuf.Err(); err != nil {
		return nil, fmt.Errorf("init postings offsets table decoding buffer: %w", decbuf.Err())
	}

	// Postings Offsets table format:
	// ┌─────────────────────┬──────────────────────┐
	// │ len <4b>            │ #entries <4b>        │
	// ├─────────────────────┴──────────────────────┤
	// │ ┌────────────────────────────────────────┐ │
	// │ │  n = 2 <1b>                            │ │
	// │ ├──────────────────────┬─────────────────┤ │
	// │ │ len(name) <uvarint>  │ name <bytes>    │ │
	// │ ├──────────────────────┼─────────────────┤ │
	// │ │ len(value) <uvarint> │ value <bytes>   │ │
	// │ ├──────────────────────┴─────────────────┤ │
	// │ │  offset <uvarint64>                    │ │
	// │ └────────────────────────────────────────┘ │
	// │                    . . .                   │
	// ├────────────────────────────────────────────┤
	// │  CRC32 <4b>                                │
	// └────────────────────────────────────────────┘

	sparsePostingsOffsets = map[string]*LabelSparsePostingsOffsets{}

	remainingCount := decbuf.Be32()
	currentName := ""
	valuesForCurrentKey := 0
	lastEntryOffsetInTable := -1

	for decbuf.Err() == nil && remainingCount > 0 {
		lastName := currentName
		offsetInTable := decbuf.Offset()
		keyCount := decbuf.Uvarint()

		// The Postings offset table takes only 2 keys per entry (name and value of label).
		if keyCount != 2 {
			return nil, errors.Errorf("unexpected key length for posting table %d", keyCount)
		}

		// Important: this value is only valid as long as we don't perform any further reads from decbuf.
		// If we need to retain its value, we must copy it before performing another read.
		if unsafeName := decbuf.UnsafeUvarintBytes(); len(sparsePostingsOffsets) == 0 || lastName != string(unsafeName) {
			newName := string(unsafeName)

			if lastEntryOffsetInTable != -1 {
				// We haven't recorded the last offset for the last value of the previous name.
				// Go back and read the last value for the previous name.
				newValueOffsetInTable := decbuf.Offset()
				decbuf.ResetAt(lastEntryOffsetInTable)
				decbuf.Uvarint()          // Skip the key count
				decbuf.SkipUvarintBytes() // Skip the name
				value := decbuf.UvarintStr()
				sparsePostingsOffsets[lastName].SparseOffsets = append(sparsePostingsOffsets[lastName].SparseOffsets, LabelValuePostingsOffset{value: value, tableOff: lastEntryOffsetInTable})

				// Skip ahead to where we were before we called ResetAt() above.
				decbuf.Skip(newValueOffsetInTable - decbuf.Offset())
			}

			currentName = newName
			sparsePostingsOffsets[currentName] = &LabelSparsePostingsOffsets{}
			valuesForCurrentKey = 0
		}

		// Retain every 1-in-sparseSampleFactor entries, starting with the first one.
		if valuesForCurrentKey%sparseSampleFactor == 0 {
			value := decbuf.UvarintStr()
			off := decbuf.Uvarint64()
			sparsePostingsOffsets[currentName].SparseOffsets = append(
				sparsePostingsOffsets[currentName].SparseOffsets,
				LabelValuePostingsOffset{value: value, tableOff: offsetInTable},
			)

			if lastName != currentName {
				sparsePostingsOffsets[lastName].LastValOffset = int64(off - crc32.Size)
			}

			// If the current value is the last one for this name, we don't need to record it again.
			lastEntryOffsetInTable = -1
		} else {
			// We only need to store this value if it's the last one for this name.
			// Record our current position in the table and come back to it if it turns out this is the last value.
			lastEntryOffsetInTable = offsetInTable

			// Skip over the value and offset.
			decbuf.SkipUvarintBytes()
			decbuf.Uvarint64()
		}

		valuesForCurrentKey++
		remainingCount--
	}

	if lastEntryOffsetInTable != -1 {
		// We haven't recorded the last offset for the last value of the last key
		// Go back and read the last value for the last key.
		decbuf.ResetAt(lastEntryOffsetInTable)
		decbuf.Uvarint()          // Skip the key count
		decbuf.SkipUvarintBytes() // Skip the key
		value := decbuf.UvarintStr()
		sparsePostingsOffsets[currentName].SparseOffsets = append(sparsePostingsOffsets[currentName].SparseOffsets, LabelValuePostingsOffset{value: value, tableOff: lastEntryOffsetInTable})
	}

	if decbuf.Err() != nil {
		return nil, errors.Wrap(decbuf.Err(), "read sparsePostingsOffsets table")
	}

	if len(sparsePostingsOffsets) > 0 {
		// In case LastValOffset is unknown as we don't have next posting anymore. Guess from the index table of contents.
		// The last posting list ends before the label offset table.
		// In worst case we will overfetch a few bytes.
		sparsePostingsOffsets[currentName].LastValOffset = int64(indexLastPostingListEndBound) - crc32.Size
	}

	// Trim any extra space in the slices.
	for k, v := range sparsePostingsOffsets {
		if len(v.SparseOffsets) == cap(v.SparseOffsets) {
			continue
		}

		l := make([]LabelValuePostingsOffset, len(v.SparseOffsets))
		copy(l, v.SparseOffsets)
		sparsePostingsOffsets[k].SparseOffsets = l
	}

	return sparsePostingsOffsets, nil
}

type PostingOffsetsTableV2 struct {
	// Map of LabelName to a list of some LabelValues's position in the offset table.
	// The first and last values for each name are always present,
	// we keep only 1/sparseSampleFactor of the rest.
	sparsePostingsOffsets map[string]*LabelSparsePostingsOffsets

	sparseSampleFactor int

	factory     streamencoding.DecbufFactory
	tableOffset int
}

func (t *PostingOffsetsTableV2) PostingsOffset(name string, value string) (r index.Range, found bool, err error) {
	e, ok := t.sparsePostingsOffsets[name]
	if !ok {
		return index.Range{}, false, nil
	}

	if value < e.SparseOffsets[0].value {
		// The desired value sorts before the first known value.
		return index.Range{}, false, nil
	}

	d := t.factory.NewDecbufAtUnchecked(t.tableOffset)
	defer runutil.CloseWithErrCapture(&err, &d, "get sparsePostingsOffsets SparseOffsets")
	if err := d.Err(); err != nil {
		return index.Range{}, false, err
	}

	i := sort.Search(len(e.SparseOffsets), func(i int) bool { return e.SparseOffsets[i].value >= value })

	if i == len(e.SparseOffsets) {
		// The desired value sorts after the last known value.
		return index.Range{}, false, nil
	}

	if i > 0 && e.SparseOffsets[i].value != value {
		// Need to look from previous entry.
		i--
	}

	d.ResetAt(e.SparseOffsets[i].tableOff)
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

			if i+1 == len(e.SparseOffsets) {
				// No more SparseOffsets for this name.
				rng.End = e.LastValOffset
			} else {
				// There's at least one more value for this name, use that as the end of the range.
				skipNAndName(&d, &nAndNameSize)
				d.SkipUvarintBytes() // Label value.
				postingOffset := int64(d.Uvarint64())
				rng.End = postingOffset - crc32.Size
			}

			if d.Err() != nil {
				return index.Range{}, false, errors.Wrap(d.Err(), "get sparsePostingsOffsets offset entry")
			}

			return rng, true, nil
		}
	}

	if d.Err() != nil {
		return index.Range{}, false, errors.Wrap(d.Err(), "get sparsePostingsOffsets offset entry")
	}

	// If we get to here, there is no entry for value.
	return index.Range{}, false, nil
}

func (t *PostingOffsetsTableV2) LabelValuesOffsets(ctx context.Context, name, prefix string, filter func(string) bool) (_ []PostingListOffset, err error) {
	e, ok := t.sparsePostingsOffsets[name]
	if !ok {
		return nil, nil
	}
	if len(e.SparseOffsets) == 0 {
		return nil, nil
	}

	offsetsStart, offsetsEnd := 0, len(e.SparseOffsets)
	if prefix != "" {
		offsetsStart, offsetsEnd, ok = e.prefixOffsets(prefix)
		if !ok {
			return nil, nil
		}
	}
	offsets := make([]PostingListOffset, 0, (offsetsEnd-offsetsStart)*t.sparseSampleFactor)

	// Don't Crc32 the entire postings offset table, this is very slow
	// so hope any issues were caught at startup.
	d := t.factory.NewDecbufAtUnchecked(t.tableOffset)
	defer runutil.CloseWithErrCapture(&err, &d, "get label values")

	d.ResetAt(e.SparseOffsets[offsetsStart].tableOff)

	// The last value of a label gets its own offset in e.SparseOffsets.
	// If that value matches, then later we should use e.LastValOffset
	// as the end offset of the value instead of reading the next value (because there will be no next value).
	lastValMatches := offsetsEnd == len(e.SparseOffsets)
	// noMoreMatchesMarkerVal is the value after which we know there are no more matching values.
	// noMoreMatchesMarkerVal itself may or may not match.
	noMoreMatchesMarkerVal := e.SparseOffsets[len(e.SparseOffsets)-1].value
	if !lastValMatches {
		noMoreMatchesMarkerVal = e.SparseOffsets[offsetsEnd].value
	}

	type pEntry struct {
		PostingListOffset
		isLast, matches bool
	}

	var skip int
	readNextList := func() (e pEntry) {
		if skip == 0 {
			// These are always the same number of bytes, since it's the same label name each time.
			// It's faster to skip than parse.
			skip = d.Len()
			d.Uvarint()          // Keycount.
			d.SkipUvarintBytes() // Label name.
			skip -= d.Len()
		} else {
			d.Skip(skip)
		}

		unsafeValue := yoloString(d.UnsafeUvarintBytes())

		prefixMatches := prefix == "" || strings.HasPrefix(unsafeValue, prefix)
		e.matches = prefixMatches && (filter == nil || filter(unsafeValue))
		e.isLast = unsafeValue == noMoreMatchesMarkerVal || (!prefixMatches && prefix < unsafeValue)
		// Clone the yolo string since its bytes will be invalidated as soon as
		// any other reads against the decoding buffer are performed.
		// We'll only need the string if it matches our filter.
		if e.matches {
			e.LabelValue = strings.Clone(unsafeValue)
		}
		// In the postings section of the index the information in each posting list for length and number
		// of entries is redundant, because every entry in the list is a fixed number of bytes (4).
		// So we can omit the first one - length - and return
		// the offset of the number_of_entries field.
		e.Off.Start = int64(d.Uvarint64()) + postingLengthFieldSize
		return
	}

	var (
		currEntry pEntry
		nextEntry pEntry
	)

	count := 1
	for d.Err() == nil && !currEntry.isLast {
		if count%CheckContextEveryNIterations == 0 && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		count++
		// Populate the current list either reading it from the pre-populated "next" or reading it from the index.
		if nextEntry != (pEntry{}) {
			currEntry = nextEntry
			nextEntry = pEntry{}
		} else {
			currEntry = readNextList()
		}

		// If the current value matches, we need to also populate its end offset and then call the visitor.
		if !currEntry.matches {
			continue
		}
		// We peek at the next list, so we can use its offset as the end offset of the current one.
		if currEntry.LabelValue == noMoreMatchesMarkerVal && lastValMatches {
			// There is no next value though. Since we only need the offset, we can use what we have in the sampled postings.
			currEntry.Off.End = e.LastValOffset
		} else {
			nextEntry = readNextList()

			// The end we want for the current posting list should be the byte offset of the CRC32 field.
			// The start of the next posting list is the byte offset of the number_of_entries field.
			// Between these two there is the posting list length field of the next list, and the CRC32 of the current list.
			currEntry.Off.End = nextEntry.Off.Start - crc32.Size - postingLengthFieldSize
		}
		offsets = append(offsets, currEntry.PostingListOffset)
	}
	return offsets, d.Err()
}

func (t *PostingOffsetsTableV2) LabelNames() ([]string, error) {
	labelNames := make([]string, 0, len(t.sparsePostingsOffsets))
	allPostingsKeyName, _ := index.AllPostingsKey()

	for name := range t.sparsePostingsOffsets {
		if name == allPostingsKeyName {
			continue
		}

		labelNames = append(labelNames, name)
	}

	slices.Sort(labelNames)

	return labelNames, nil
}

func (t *PostingOffsetsTableV2) PostingOffsetInMemSampling() int {
	if t != nil {
		return t.sparseSampleFactor
	}
	return 0
}

// ToSparsePostingOffsetTable loads all postings offset table data into a sparse index-header to be persisted to disk
func (t *PostingOffsetsTableV2) ToSparsePostingOffsetTable() (table *indexheaderpb.PostingOffsetTable) {
	sparseHeaders := &indexheaderpb.PostingOffsetTable{
		Postings:                      make(map[string]*indexheaderpb.PostingValueOffsets, len(t.sparsePostingsOffsets)),
		PostingOffsetInMemorySampling: int64(t.sparseSampleFactor),
	}

	for name, offsets := range t.sparsePostingsOffsets {
		sparseHeaders.Postings[name] = &indexheaderpb.PostingValueOffsets{}
		postingOffsets := make([]*indexheaderpb.PostingOffset, len(offsets.SparseOffsets))

		for i, postingOff := range offsets.SparseOffsets {
			postingOffsets[i] = &indexheaderpb.PostingOffset{Value: postingOff.value, TableOff: int64(postingOff.tableOff)}
		}
		sparseHeaders.Postings[name].Offsets = postingOffsets
		sparseHeaders.Postings[name].LastValOffset = offsets.LastValOffset
	}

	return sparseHeaders
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

func stepSize(cur, tgt int) (int, bool) {
	if cur > tgt || cur <= 0 || tgt <= 0 || tgt%cur != 0 {
		return 0, false
	}
	return tgt / cur, true
}
