// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/tsdb/index/postings.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package index

import (
	"context"
	"fmt"
	"hash/crc32"
	"math"
	"slices"
	"sort"
	"strings"

	"github.com/grafana/dskit/runutil"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb/index"

	streamencoding "github.com/grafana/mimir/pkg/storage/indexheader/encoding"
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

// LabelValueVisitor receives the label values that ForEachLabelValueOffset finds.
// The scan calls SizeHint and Visit while it holds the index-header reader, so neither function may call the reader.
type LabelValueVisitor struct {
	// SizeHint is optional. The scan calls it at most once, before the first Visit call,
	// with an estimate of the number of Visit calls. The scan skips it when no scan starts.
	// The estimate is never lower than the number of Visit calls for a whole label.
	// It can be lower for a prefix range of a sparse header that was downsampled, so callers must not treat it as a bound.
	SizeHint func(estimatedValues int)
	// Visit is required. It receives each label value that has the prefix and passes the filter, in lexicographic order.
	// The value stays valid only until Visit returns. Visit must copy it to keep it.
	// Visit returns false to stop the scan.
	Visit func(value []byte, off index.Range) (cont bool)
}

type PostingsOffsetsTable interface {
	// IsRemote flags if the Postings Offset table is accessed via a bucket reader
	// or other remote storage which may introduce extra read latency;
	// may be used to conditionally enable optimizations or set spans.
	IsRemote() bool

	// PostingsOffset returns the byte range of the postings section for the label with the given name and value.
	// The Start is inclusive and is the byte offset of the number_of_entries field of a posting list.
	// The End is exclusive and is typically the byte offset of the CRC32 field.
	// The End might be bigger than the actual posting ending, but not larger than the whole index file.
	PostingsOffset(ctx context.Context, name string, value string) (rng index.Range, found bool, err error)

	// LabelValuesOffsets returns all postings lists for the label named name that match filter and have the prefix provided.
	// The ranges of each posting list are the same as returned by PostingsOffset.
	// The returned label values are sorted lexicographically (which the same as sorted by posting offset).
	LabelValuesOffsets(ctx context.Context, name, prefix string, filter func(string) bool) ([]PostingListOffset, error)

	// ForEachLabelValueOffset calls visitor.Visit for each posting list of the label named name that matches filter and has the prefix provided.
	// It visits the same label values and ranges, in the same order, as LabelValuesOffsets returns, but it does not keep them in memory.
	// The scan does nothing, and does not call visitor.SizeHint, when the label does not exist or no label value can have the prefix.
	ForEachLabelValueOffset(ctx context.Context, name, prefix string, filter func(string) bool, visitor LabelValueVisitor) error

	// LabelNames returns a sorted list of all label names in this table.
	LabelNames() ([]string, error)

	// PostingsOffsetsInMemSampling returns the inverse of the fraction of postings held in memory.
	// A lower value indicates postings are sampled more frequently.
	PostingsOffsetsInMemSampling() int
}

func NewPostingsOffsetsTableReader(
	indexVersion int,
	decbufFactory streamencoding.DecbufFactory,
	tableOffset int,
	sparsePostingsOffsets map[string]*SparseTableOffsetsForLabel,
	sparseSampleFactor int,
) (PostingsOffsetsTable, error) {
	_, isRemote := decbufFactory.(*streamencoding.BucketDecbufFactory)

	switch indexVersion {
	case index.FormatV2:
		return &PostingsOffsetsTableV2{
			isRemote:              isRemote,
			decbufFactory:         decbufFactory,
			tableOffset:           tableOffset,
			SparsePostingsOffsets: sparsePostingsOffsets,
			SparseSampleFactor:    sparseSampleFactor,
		}, nil
	}
	return nil, fmt.Errorf("unknown or unsupported index version %v", indexVersion)
}

type PostingsOffsetsTableV2 struct {
	// Map of LabelName to a list of some LabelValues's position in the offset table.
	// The first and last values for each name are always present,
	// we keep only 1/SparseSampleFactor of the rest.
	SparsePostingsOffsets map[string]*SparseTableOffsetsForLabel

	SparseSampleFactor int

	isRemote      bool
	decbufFactory streamencoding.DecbufFactory
	tableOffset   int
}

func (t *PostingsOffsetsTableV2) IsRemote() bool {
	return t.isRemote
}

func (t *PostingsOffsetsTableV2) PostingsOffset(ctx context.Context, name string, value string) (rng index.Range, found bool, err error) {
	e, ok := t.SparsePostingsOffsets[name]
	if !ok {
		return index.Range{}, false, nil
	}

	if value < e.SparseTableOffsets[0].Value {
		// The desired value sorts before the first known value.
		return index.Range{}, false, nil
	}

	i := sort.Search(len(e.SparseTableOffsets), func(i int) bool { return e.SparseTableOffsets[i].Value >= value })

	if i == len(e.SparseTableOffsets) {
		// The desired value sorts after the last known value.
		return index.Range{}, false, nil
	}

	if i > 0 && e.SparseTableOffsets[i].Value != value {
		// Need to look from previous entry.
		i--
	}

	var d streamencoding.Decbuf
	if t.IsRemote() {
		var sectionEndOffset int
		// In order to correctly set Range.End, we need to read the next entry beyond the one we are looking for.
		// In the worst case where the value is found right before the next (i+1'th) sparse table offset,
		// we need to read the entry that starts at the i+1'th offset. To account for this, we create the reader up to the i+2'th sparse table offset.
		if i+2 < len(e.SparseTableOffsets) {
			sectionEndOffset = e.SparseTableOffsets[i+2].Offset
		} else {
			// Set this to something that will always be greater than the length of the table
			sectionEndOffset = math.MaxInt
		}
		// At this point we know that our value is somewhere between the i-th offset and the next one;
		// we shouldn't need to read/cache bucket ops beyond that.
		d = t.decbufFactory.NewDecbufInSection(ctx, t.tableOffset, e.SparseTableOffsets[i].Offset, sectionEndOffset)
	} else {
		d = t.decbufFactory.NewDecbufAtUnchecked(ctx, t.tableOffset)
		d.ResetAt(e.SparseTableOffsets[i].Offset)
	}
	defer runutil.CloseWithErrCapture(&err, &d, "get sparsePostingsOffsets SparseTableOffsets")
	if err := d.Err(); err != nil {
		return index.Range{}, false, err
	}

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

			if i+1 == len(e.SparseTableOffsets) {
				// No more SparseTableOffsets for this name.
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

// LabelValuesOffsets implements PostingsOffsetsTable.
// It collects the results of ForEachLabelValueOffset in a slice.
func (t *PostingsOffsetsTableV2) LabelValuesOffsets(ctx context.Context, name, prefix string, filter func(string) bool) ([]PostingListOffset, error) {
	var offsets []PostingListOffset
	err := t.ForEachLabelValueOffset(ctx, name, prefix, filter, LabelValueVisitor{
		SizeHint: func(estimatedValues int) { offsets = make([]PostingListOffset, 0, estimatedValues) },
		Visit: func(value []byte, off index.Range) bool {
			offsets = append(offsets, PostingListOffset{LabelValue: string(value), Off: off})
			return true
		},
	})
	if err != nil {
		return nil, err
	}
	return offsets, nil
}

// ForEachLabelValueOffset implements PostingsOffsetsTable.
func (t *PostingsOffsetsTableV2) ForEachLabelValueOffset(ctx context.Context, name, prefix string, filter func(string) bool, visitor LabelValueVisitor) (err error) {
	e, ok := t.SparsePostingsOffsets[name]
	if !ok {
		return nil
	}
	if len(e.SparseTableOffsets) == 0 {
		return nil
	}

	offsetsStart, offsetsEnd := 0, len(e.SparseTableOffsets)
	if prefix != "" {
		offsetsStart, offsetsEnd, ok = e.labelValuePrefixOffsets(prefix)
		if !ok {
			return nil
		}
	}

	var d streamencoding.Decbuf
	if t.IsRemote() {
		var sectionEndOffset int
		if offsetsEnd+1 < len(e.SparseTableOffsets) {
			// We buffer the end with the distance to one more sparse offset in case the last matching entry is right at the end of the section.
			sectionEndOffset = e.SparseTableOffsets[offsetsEnd+1].Offset
		} else {
			sectionEndOffset = math.MaxInt
		}
		// For the BucketDecbufFactory, we only read the section we care about to optimize
		// the size of cached GetRange bucket ops.
		d = t.decbufFactory.NewDecbufInSection(ctx, t.tableOffset, e.SparseTableOffsets[offsetsStart].Offset, sectionEndOffset)
	} else {
		// Don't Crc32 the entire postings offset table, this is very slow
		// so hope any issues were caught at startup.
		d = t.decbufFactory.NewDecbufAtUnchecked(ctx, t.tableOffset)
		d.ResetAt(e.SparseTableOffsets[offsetsStart].Offset)
	}
	defer runutil.CloseWithErrCapture(&err, &d, "get label values")

	if visitor.SizeHint != nil && d.Err() == nil {
		// Each sparse offset covers at most SparseSampleFactor label values, so this estimates the number of matches.
		visitor.SizeHint((offsetsEnd - offsetsStart) * t.SparseSampleFactor)
	}

	// The last value of a label gets its own offset in e.SparseTableOffsets.
	// If that value matches, then later we should use e.LastValOffset
	// as the end offset of the value instead of reading the next value (because there will be no next value).
	lastValMatches := offsetsEnd == len(e.SparseTableOffsets)
	// noMoreMatchesMarkerVal is the value after which we know there are no more matching values.
	// noMoreMatchesMarkerVal itself may or may not match.
	noMoreMatchesMarkerVal := e.SparseTableOffsets[len(e.SparseTableOffsets)-1].Value
	if !lastValMatches {
		noMoreMatchesMarkerVal = e.SparseTableOffsets[offsetsEnd].Value
	}

	type pEntry struct {
		// value is a view of one of the scratch buffers. The scan sets it only when matches is true.
		value           []byte
		off             index.Range
		isLast, matches bool
	}

	var (
		// The scan calls Visit for an entry only after it reads the next entry,
		// because the end offset of an entry is the start offset of the next entry.
		// So two matching entries are alive at the same time, and each one needs its own scratch buffer.
		// The scan uses the two buffers in turn, and each call owns them.
		scratch    [2][]byte
		scratchIdx int

		skip int
	)
	readNextList := func() (entry pEntry) {
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
		entry.matches = prefixMatches && (filter == nil || filter(unsafeValue))
		entry.isLast = unsafeValue == noMoreMatchesMarkerVal || (!prefixMatches && prefix < unsafeValue)
		// Copy the yolo string since its bytes will be invalidated as soon as
		// any other reads against the decoding buffer are performed.
		// We'll only need the value if it matches our filter.
		if entry.matches {
			scratchIdx ^= 1
			scratch[scratchIdx] = append(scratch[scratchIdx][:0], unsafeValue...)
			entry.value = scratch[scratchIdx]
		}
		// In the postings section of the index the information in each posting list for length and number
		// of entries is redundant, because every entry in the list is a fixed number of bytes (4).
		// So we can omit the first one - length - and return
		// the offset of the number_of_entries field.
		entry.off.Start = int64(d.Uvarint64()) + postingLengthFieldSize
		return
	}

	var (
		currEntry pEntry
		nextEntry pEntry
		hasNext   bool
	)

	count := 1
	for d.Err() == nil && !currEntry.isLast {
		if count%CheckContextEveryNIterations == 0 && ctx.Err() != nil {
			return ctx.Err()
		}
		count++
		// Populate the current list either reading it from the pre-populated "next" or reading it from the index.
		if hasNext {
			currEntry = nextEntry
			nextEntry, hasNext = pEntry{}, false
		} else {
			currEntry = readNextList()
		}

		// If the current value matches, we need to also populate its end offset and then call the visitor.
		if !currEntry.matches {
			continue
		}
		// We peek at the next list, so we can use its offset as the end offset of the current one.
		// For a matching entry, isLast is true only for noMoreMatchesMarkerVal.
		if currEntry.isLast && lastValMatches {
			// There is no next value though. Since we only need the offset, we can use what we have in the sampled postings.
			currEntry.off.End = e.LastValOffset
		} else {
			nextEntry, hasNext = readNextList(), true
			if d.Err() != nil {
				// The read of the next entry failed, so the end offset of the current entry is unknown.
				return d.Err()
			}

			// The end we want for the current posting list should be the byte offset of the CRC32 field.
			// The start of the next posting list is the byte offset of the number_of_entries field.
			// Between these two there is the posting list length field of the next list, and the CRC32 of the current list.
			currEntry.off.End = nextEntry.off.Start - crc32.Size - postingLengthFieldSize
		}
		if !visitor.Visit(currEntry.value, currEntry.off) {
			return d.Err()
		}
	}
	return d.Err()
}

func (t *PostingsOffsetsTableV2) LabelNames() ([]string, error) {
	labelNames := make([]string, 0, len(t.SparsePostingsOffsets))
	allPostingsKeyName, _ := index.AllPostingsKey()

	for name := range t.SparsePostingsOffsets {
		if name == allPostingsKeyName {
			continue
		}

		labelNames = append(labelNames, name)
	}

	slices.Sort(labelNames)

	return labelNames, nil
}

func (t *PostingsOffsetsTableV2) PostingsOffsetsInMemSampling() int {
	if t != nil {
		return t.SparseSampleFactor
	}
	return 0
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
