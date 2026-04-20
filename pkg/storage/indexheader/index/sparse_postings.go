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
// SparseTableOffsets do _not_ capture the last "offset" value in the entry, which points to the Postings list.
// They only capture an offset pointing _into_ the Postings Offsets table itself to quickly reach the table entries.
type SparseTableOffsetsForLabel struct {
	SparseTableOffsets []tableOffsetForLabelValue
	LastValOffset      int64
}

type tableOffsetForLabelValue struct {
	// label value.
	Value string
	// offset of this entry in posting offset table in index-header file.
	Offset int
}

// labelValuePrefixOffsets returns the index of the first matching offset (start) and the index of the first non-matching (end).
// If all SparseTableOffsets match the prefix, then end will equal the length of SparseTableOffsets.
// labelValuePrefixOffsets returns false when no SparseTableOffsets match this prefix.
func (e *SparseTableOffsetsForLabel) labelValuePrefixOffsets(prefix string) (start, end int, found bool) {
	// Find the first offset that is greater or equal to the value.
	start = sort.Search(len(e.SparseTableOffsets), func(i int) bool {
		return prefix <= e.SparseTableOffsets[i].Value
	})

	// We always include the last value in the SparseTableOffsets,
	// and given that prefix is always less or equal than the value,
	// we can conclude that there are no values with this prefix.
	if start == len(e.SparseTableOffsets) {
		return 0, 0, false
	}

	// Prefix is lower than the first value in the SparseTableOffsets, and that first value doesn't have this prefix.
	// Next values won't have the prefix, so we can return early.
	if start == 0 && prefix < e.SparseTableOffsets[0].Value && !strings.HasPrefix(e.SparseTableOffsets[0].Value, prefix) {
		return 0, 0, false
	}

	// If the value is not equal to the prefix, this value might have the prefix.
	// But maybe the values in the previous offset also had the prefix,
	// so we need to step back one offset to find all values with this prefix.
	// Unless, of course, we are at the first offset.
	if start > 0 && e.SparseTableOffsets[start].Value != prefix {
		start--
	}

	// Find the first offset which is larger than the prefix and doesn't have the prefix.
	// All values at and after that offset will not match the prefix.
	end = sort.Search(len(e.SparseTableOffsets)-start, func(i int) bool {
		return prefix < e.SparseTableOffsets[i+start].Value && !strings.HasPrefix(e.SparseTableOffsets[i+start].Value, prefix)
	})
	end += start
	return start, end, true
}

func SparseValuesFromPostingsOffsetsTable(
	decbufFactory streamencoding.DecbufFactory,
	tableOffset int,
	postingsListEnd uint64,
	sparseSampleFactor int,
	doChecksum bool,
) (sparsePostingsOffsets map[string]*SparseTableOffsetsForLabel, err error) {
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
				sparsePostingsOffsets[lastName].SparseTableOffsets = append(sparsePostingsOffsets[lastName].SparseTableOffsets, tableOffsetForLabelValue{Value: value, Offset: lastEntryOffsetInTable})

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
			sparsePostingsOffsets[currentName].SparseTableOffsets = append(
				sparsePostingsOffsets[currentName].SparseTableOffsets,
				tableOffsetForLabelValue{Value: value, Offset: offsetInTable},
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
		sparsePostingsOffsets[currentName].SparseTableOffsets = append(sparsePostingsOffsets[currentName].SparseTableOffsets, tableOffsetForLabelValue{Value: value, Offset: lastEntryOffsetInTable})
	}

	if decbuf.Err() != nil {
		return nil, errors.Wrap(decbuf.Err(), "read sparsePostingsOffsets table")
	}

	if len(sparsePostingsOffsets) > 0 {
		// In case LastValOffset is unknown as we don't have next posting anymore. Guess from the index table of contents.
		// The last posting list ends before the label offset table.
		// In worst case we will overfetch a few bytes.
		sparsePostingsOffsets[currentName].LastValOffset = int64(postingsListEnd) - crc32.Size
	}

	// Trim any extra space in the slices.
	for k, v := range sparsePostingsOffsets {
		if len(v.SparseTableOffsets) == cap(v.SparseTableOffsets) {
			continue
		}

		l := make([]tableOffsetForLabelValue, len(v.SparseTableOffsets))
		copy(l, v.SparseTableOffsets)
		sparsePostingsOffsets[k].SparseTableOffsets = l
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
		postingOffsets := make([]*indexheaderpb.PostingOffset, len(offsets.SparseTableOffsets))

		for i, tableOffset := range offsets.SparseTableOffsets {
			postingOffsets[i] = &indexheaderpb.PostingOffset{Value: tableOffset.Value, TableOff: int64(tableOffset.Offset)}
		}
		proto.Postings[labelName].Offsets = postingOffsets
		proto.Postings[labelName].LastValOffset = offsets.LastValOffset
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

		sparsePostingsOffsets[sName] = &SparseTableOffsetsForLabel{
			SparseTableOffsets: make([]tableOffsetForLabelValue, downsampledLen),
		}
		for i, sPostingOff := range sOffsets.Offsets {
			if i%step == 0 {
				sparsePostingsOffsets[sName].SparseTableOffsets[i/step] = tableOffsetForLabelValue{
					Value: sPostingOff.Value, Offset: int(sPostingOff.TableOff),
				}
			}

			if i == olen-1 {
				sparsePostingsOffsets[sName].SparseTableOffsets[downsampledLen-1] = tableOffsetForLabelValue{
					Value: sPostingOff.Value, Offset: int(sPostingOff.TableOff),
				}
			}
		}
		sparsePostingsOffsets[sName].LastValOffset = sOffsets.LastValOffset
	}
	return sparsePostingsOffsets, err
}

func stepSize(cur, tgt int) (int, bool) {
	if cur > tgt || cur <= 0 || tgt <= 0 || tgt%cur != 0 {
		return 0, false
	}
	return tgt / cur, true
}
