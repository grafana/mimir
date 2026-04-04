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

type PostingsOffsetsTable interface {
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
	switch indexVersion {
	case index.FormatV2:
		return &PostingsOffsetsTableV2{
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

	decbufFactory streamencoding.DecbufFactory
	tableOffset   int
}

func (t *PostingsOffsetsTableV2) PostingsOffset(name string, value string) (r index.Range, found bool, err error) {
	e, ok := t.SparsePostingsOffsets[name]
	if !ok {
		return index.Range{}, false, nil
	}

	if value < e.SparseTableOffsets[0].Value {
		// The desired value sorts before the first known value.
		return index.Range{}, false, nil
	}

	d := t.decbufFactory.NewDecbufAtUnchecked(t.tableOffset)
	defer runutil.CloseWithErrCapture(&err, &d, "get sparsePostingsOffsets SparseTableOffsets")
	if err := d.Err(); err != nil {
		return index.Range{}, false, err
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

	d.ResetAt(e.SparseTableOffsets[i].Offset)
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

func (t *PostingsOffsetsTableV2) LabelValuesOffsets(ctx context.Context, name, prefix string, filter func(string) bool) (_ []PostingListOffset, err error) {
	e, ok := t.SparsePostingsOffsets[name]
	if !ok {
		return nil, nil
	}
	if len(e.SparseTableOffsets) == 0 {
		return nil, nil
	}

	offsetsStart, offsetsEnd := 0, len(e.SparseTableOffsets)
	if prefix != "" {
		offsetsStart, offsetsEnd, ok = e.labelValuePrefixOffsets(prefix)
		if !ok {
			return nil, nil
		}
	}
	offsets := make([]PostingListOffset, 0, (offsetsEnd-offsetsStart)*t.SparseSampleFactor)

	// Don't Crc32 the entire postings offset table, this is very slow
	// so hope any issues were caught at startup.
	d := t.decbufFactory.NewDecbufAtUnchecked(t.tableOffset)
	defer runutil.CloseWithErrCapture(&err, &d, "get label values")

	d.ResetAt(e.SparseTableOffsets[offsetsStart].Offset)

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
