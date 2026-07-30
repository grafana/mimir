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

	streamencoding "github.com/grafana/mimir/pkg/storage/indexheader/encoding"
	"github.com/grafana/mimir/pkg/storage/indexheader/indexheaderpb"
)

// SparseTableOffsetsForLabel contains offsets within the Postings Offsets table
// for a sampled set of table entries with the same label name.
//
// This in-memory sampling enables fast in-memory binary search of table entries
// to bound the size of the scans of the full Postings Offset table.
//
// Each entry in the full Postings Offsets table contains a "key": a label (name, value) pair
// and a "value": the start offset in the Postings List for that label (name, value):
// │                    . . .                   │
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
// The sampled table offsets do _not_ capture the last "offset" value in the entry, which points to the Postings list.
// They only capture an offset pointing _into_ the Postings Offsets table itself to quickly reach the table entries.
//
// To keep the resident memory footprint and the number of live heap objects low,
// the sampled entries are stored in a pointer-free, columnar layout:
// all sampled values are concatenated into a single byte blob,
// and per-entry data is limited to two uint32s (value end and table offset).
// Both fit in uint32 because the postings offset table size is bounded by its 4-byte length field.
type SparseTableOffsetsForLabel struct {
	// valueBlob holds all sampled label values concatenated in sorted order.
	valueBlob []byte
	// valueEnds holds the end of each sampled value within valueBlob;
	// each value starts where the previous one ends.
	valueEnds []uint32
	// tableOffsets holds, for each sampled entry, its offset within the postings offset table.
	tableOffsets []uint32

	lastValOffset int64
}

func (e *SparseTableOffsetsForLabel) numOffsets() int {
	return len(e.tableOffsets)
}

// value returns the sampled label value at index i.
// The returned string references memory shared with valueBlob and is valid as long as e is alive:
// it's meant for comparisons, callers should copy it (e.g. with strings.Clone) before retaining it.
func (e *SparseTableOffsetsForLabel) value(i int) string {
	start := uint32(0)
	if i > 0 {
		start = e.valueEnds[i-1]
	}
	return yoloString(e.valueBlob[start:e.valueEnds[i]])
}

func (e *SparseTableOffsetsForLabel) tableOffset(i int) int {
	return int(e.tableOffsets[i])
}

// tableOffsetToUint32 converts a table offset to its in-memory uint32 representation.
// Offsets are bounded by the table's 4-byte length field, so this only fails on corrupt data.
func tableOffsetToUint32(offset int64, table string) (uint32, error) {
	if offset < 0 || offset > math.MaxUint32 {
		return 0, fmt.Errorf("sparse index-header %s table offset %d out of bounds", table, offset)
	}
	return uint32(offset), nil
}

func (e *SparseTableOffsetsForLabel) appendOffset(value string, tableOff int64) error {
	off, err := tableOffsetToUint32(tableOff, "postings offset")
	if err != nil {
		return err
	}
	end := len(e.valueBlob) + len(value)
	if end > math.MaxUint32 {
		return fmt.Errorf("sparse index-header postings offset table values for a label exceed %d bytes", math.MaxUint32)
	}
	e.valueBlob = append(e.valueBlob, value...)
	e.valueEnds = append(e.valueEnds, uint32(end))
	e.tableOffsets = append(e.tableOffsets, off)
	return nil
}

// grow pre-allocates space for numOffsets sampled entries totalling valueBytes of label values.
func (e *SparseTableOffsetsForLabel) grow(numOffsets, valueBytes int) {
	e.valueBlob = make([]byte, 0, valueBytes)
	e.valueEnds = make([]uint32, 0, numOffsets)
	e.tableOffsets = make([]uint32, 0, numOffsets)
}

// compact re-allocates the underlying storage to exactly fit the appended entries,
// releasing any extra capacity accumulated while growing through appendOffset.
func (e *SparseTableOffsetsForLabel) compact() {
	e.valueBlob = clipExact(e.valueBlob)
	e.valueEnds = clipExact(e.valueEnds)
	e.tableOffsets = clipExact(e.tableOffsets)
}

// clipExact returns s re-allocated to exactly fit its elements, or s itself if it has no extra capacity.
func clipExact[E any](s []E) []E {
	if cap(s) == len(s) {
		return s
	}
	return slices.Clone(s)
}

// labelValuePrefixOffsets returns the index of the first matching offset (start) and the index of the first non-matching (end).
// If all sampled offsets match the prefix, then end will equal the number of sampled offsets.
// labelValuePrefixOffsets returns false when no sampled offsets match this prefix.
func (e *SparseTableOffsetsForLabel) labelValuePrefixOffsets(prefix string) (start, end int, found bool) {
	// Find the first offset that is greater or equal to the value.
	start = sort.Search(e.numOffsets(), func(i int) bool {
		return prefix <= e.value(i)
	})

	// We always include the last value in the sampled offsets,
	// and given that prefix is always less or equal than the value,
	// we can conclude that there are no values with this prefix.
	if start == e.numOffsets() {
		return 0, 0, false
	}

	// Prefix is lower than the first value in the sampled offsets, and that first value doesn't have this prefix.
	// Next values won't have the prefix, so we can return early.
	if first := e.value(0); start == 0 && prefix < first && !strings.HasPrefix(first, prefix) {
		return 0, 0, false
	}

	// If the value is not equal to the prefix, this value might have the prefix.
	// But maybe the values in the previous offset also had the prefix,
	// so we need to step back one offset to find all values with this prefix.
	// Unless, of course, we are at the first offset.
	if start > 0 && e.value(start) != prefix {
		start--
	}

	// Find the first offset which is larger than the prefix and doesn't have the prefix.
	// All values at and after that offset will not match the prefix.
	end = sort.Search(e.numOffsets()-start, func(i int) bool {
		v := e.value(i + start)
		return prefix < v && !strings.HasPrefix(v, prefix)
	})
	end += start
	return start, end, true
}

func SparseValuesFromPostingsOffsetsTable(
	ctx context.Context,
	decbufFactory streamencoding.DecbufFactory,
	tableOffset int,
	postingsListEnd uint64,
	sparseSampleFactor int,
	doChecksum bool,
) (sparsePostingsOffsets map[string]*SparseTableOffsetsForLabel, err error) {
	var decbuf streamencoding.Decbuf
	if doChecksum {
		decbuf = decbufFactory.NewDecbufAtChecked(ctx, tableOffset, castagnoliTable)
	} else {
		decbuf = decbufFactory.NewDecbufAtUnchecked(ctx, tableOffset)
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

	sparsePostingsOffsets = map[string]*SparseTableOffsetsForLabel{}

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
				if err := sparsePostingsOffsets[lastName].appendOffset(value, int64(lastEntryOffsetInTable)); err != nil {
					return nil, err
				}

				// Skip ahead to where we were before we called ResetAt() above.
				decbuf.Skip(newValueOffsetInTable - decbuf.Offset())
			}

			currentName = newName
			sparsePostingsOffsets[currentName] = &SparseTableOffsetsForLabel{}
			valuesForCurrentKey = 0
		}

		// Retain every 1-in-sparseSampleFactor entries, starting with the first one.
		if valuesForCurrentKey%sparseSampleFactor == 0 {
			value := decbuf.UvarintStr()
			off := decbuf.Uvarint64()
			if err := sparsePostingsOffsets[currentName].appendOffset(value, int64(offsetInTable)); err != nil {
				return nil, err
			}

			if lastName != currentName {
				sparsePostingsOffsets[lastName].lastValOffset = int64(off - crc32.Size)
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
		if err := sparsePostingsOffsets[currentName].appendOffset(value, int64(lastEntryOffsetInTable)); err != nil {
			return nil, err
		}
	}

	if decbuf.Err() != nil {
		return nil, errors.Wrap(decbuf.Err(), "read sparsePostingsOffsets table")
	}

	if len(sparsePostingsOffsets) > 0 {
		// In case lastValOffset is unknown as we don't have next posting anymore. Guess from the index table of contents.
		// The last posting list ends before the label offset table.
		// In worst case we will overfetch a few bytes.
		sparsePostingsOffsets[currentName].lastValOffset = int64(postingsListEnd) - crc32.Size
	}

	// Trim any extra space in the slices.
	for _, v := range sparsePostingsOffsets {
		v.compact()
	}

	return sparsePostingsOffsets, nil
}

// SparsePostingsOffsetsTableToProto loads in-memory sparse postings offset table data into the protobuf format
func SparsePostingsOffsetsTableToProto(
	sparsePostingsOffsets map[string]*SparseTableOffsetsForLabel,
	sparseSampleFactor int,
) *indexheaderpb.PostingOffsetTable {
	proto := &indexheaderpb.PostingOffsetTable{
		Postings:                      make(map[string]*indexheaderpb.PostingValueOffsets, len(sparsePostingsOffsets)),
		PostingOffsetInMemorySampling: int64(sparseSampleFactor),
	}

	for labelName, offsets := range sparsePostingsOffsets {
		proto.Postings[labelName] = &indexheaderpb.PostingValueOffsets{}
		postingOffsets := make([]*indexheaderpb.PostingOffset, offsets.numOffsets())

		for i := range postingOffsets {
			postingOffsets[i] = &indexheaderpb.PostingOffset{Value: strings.Clone(offsets.value(i)), TableOff: int64(offsets.tableOffset(i))}
		}
		proto.Postings[labelName].Offsets = postingOffsets
		proto.Postings[labelName].LastValOffset = offsets.lastValOffset
	}

	return proto
}

// SparsePostingsOffsetsTableFromProto loads the protobuf format to in-memory sparse postings offsets data
func SparsePostingsOffsetsTableFromProto(proto *indexheaderpb.PostingOffsetTable, sparseSampleFactor int) (
	sparsePostingsOffsets map[string]*SparseTableOffsetsForLabel, err error,
) {
	protoSampleFactor := int(proto.GetPostingOffsetInMemorySampling())
	if protoSampleFactor == 0 {
		return nil, fmt.Errorf("sparse index-header sampling rate not set")
	}

	if protoSampleFactor > sparseSampleFactor {
		return nil, fmt.Errorf("sparse index-header sampling rate exceeds in-mem-sampling rate")
	}

	// if the sampling rate in the sparse index-header is set lower (more frequent) than
	// the configured sparseSampleFactor we downsample to the configured rate
	step, ok := stepSize(protoSampleFactor, sparseSampleFactor)
	if !ok {
		return nil, fmt.Errorf("sparse index-header sampling rate not compatible with in-mem-sampling rate")
	}

	sparsePostingsOffsets = make(map[string]*SparseTableOffsetsForLabel, len(proto.Postings))
	for sName, sOffsets := range proto.Postings {
		olen := len(sOffsets.Offsets)
		downsampledLen := (olen + step - 1) / step
		if (olen > 1) && (downsampledLen == 1) {
			downsampledLen++
		}

		offsets := &SparseTableOffsetsForLabel{lastValOffset: sOffsets.LastValOffset}
		sparsePostingsOffsets[sName] = offsets
		if olen == 0 {
			continue
		}

		// The downsampled entries are every step-th entry, except the last one,
		// which is always the last entry of the full-resolution table.
		srcIdx := func(k int) int {
			if k == downsampledLen-1 {
				return olen - 1
			}
			return k * step
		}

		valueBytes := 0
		for k := 0; k < downsampledLen; k++ {
			valueBytes += len(sOffsets.Offsets[srcIdx(k)].Value)
		}
		offsets.grow(downsampledLen, valueBytes)

		for k := 0; k < downsampledLen; k++ {
			sPostingOff := sOffsets.Offsets[srcIdx(k)]
			if err := offsets.appendOffset(sPostingOff.Value, sPostingOff.TableOff); err != nil {
				return nil, err
			}
		}
	}
	return sparsePostingsOffsets, err
}

func stepSize(cur, tgt int) (int, bool) {
	if cur > tgt || cur <= 0 || tgt <= 0 || tgt%cur != 0 {
		return 0, false
	}
	return tgt / cur, true
}
