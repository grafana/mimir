// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/tsdb/index/index.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package index

import (
	"context"
	"fmt"

	"github.com/grafana/dskit/runutil"

	streamencoding "github.com/grafana/mimir/pkg/storage/indexheader/encoding"
	"github.com/grafana/mimir/pkg/storage/indexheader/indexheaderpb"
)

// SparseSymbols is the sampled in-memory representation of the symbols table:
// the total number of symbols plus the table offset of every symbolFactor-th symbol.
//
// Offsets are stored relative to the start of the table's content (after the leading length field):
// the index writer caps the content at 2^32-1 bytes, so content-relative offsets always fit in uint32,
// while offsets relative to the table start would not, because of the leading length field.
// This is internal to SparseSymbols: offsets passed to appendOffset and returned by tableOffset
// are relative to the table start.
type SparseSymbols struct {
	count   int
	offsets []uint32
}

// Count returns the total number of symbols in the table.
func (s SparseSymbols) Count() int {
	return s.count
}

// NumOffsets returns the number of sampled symbols.
func (s SparseSymbols) NumOffsets() int {
	return len(s.offsets)
}

// tableOffset returns the offset of the i-th sampled symbol, relative to the start of the symbols table.
func (s SparseSymbols) tableOffset(i int) int {
	return int(s.offsets[i]) + tableLengthFieldSize
}

// appendOffset records a sampled symbol at the given offset relative to the start of the symbols table.
func (s *SparseSymbols) appendOffset(tableOff int64) error {
	off, err := tableOffsetToUint32(tableOff-tableLengthFieldSize, "symbols")
	if err != nil {
		return err
	}
	s.offsets = append(s.offsets, off)
	return nil
}

func SparseValuesFromSymbolsTable(
	ctx context.Context,
	decbufFactory streamencoding.DecbufFactory,
	tableOffset int,
	doChecksum bool,
) (sparseSymbols SparseSymbols, err error) {
	var decbuf streamencoding.Decbuf
	if doChecksum {
		decbuf = decbufFactory.NewDecbufAtChecked(ctx, tableOffset, castagnoliTable)
	} else {
		decbuf = decbufFactory.NewDecbufAtUnchecked(ctx, tableOffset)
	}

	defer runutil.CloseWithErrCapture(&err, &decbuf, "decode symbols table")
	if err := decbuf.Err(); err != nil {
		return SparseSymbols{}, fmt.Errorf("init symbol table decoding buffer: %w", decbuf.Err())
	}

	// Symbols table format:
	// ┌────────────────────┬─────────────────────┐
	// │ len <4b>           │ #symbols <4b>       │
	// ├────────────────────┴─────────────────────┤
	// │ ┌──────────────────────┬───────────────┐ │
	// │ │ len(str_1) <uvarint> │ str_1 <bytes> │ │
	// │ ├──────────────────────┴───────────────┤ │
	// │ │                . . .                 │ │
	// │ ├──────────────────────┬───────────────┤ │
	// │ │ len(str_n) <uvarint> │ str_n <bytes> │ │
	// │ └──────────────────────┴───────────────┘ │
	// ├──────────────────────────────────────────┤
	// │ CRC32 <4b>                               │
	// └──────────────────────────────────────────┘

	// Get symbols count; decbuf has already consumed the len field.
	sparseSymbols.count = decbuf.Be32int()

	seen := 0
	sparseSymbols.offsets = make([]uint32, 0, 1+sparseSymbols.count/symbolFactor)
	for decbuf.Err() == nil && seen < sparseSymbols.count {
		if seen%symbolFactor == 0 {
			if err := sparseSymbols.appendOffset(int64(decbuf.Offset())); err != nil {
				return SparseSymbols{}, err
			}
		}
		decbuf.SkipUvarintBytes() // The symbol.
		seen++
	}

	if decbuf.Err() != nil {
		return SparseSymbols{}, decbuf.Err()
	}

	return sparseSymbols, nil
}

// SparseSymbolsToProto loads the in-memory sparse symbols data into the protobuf format.
// The protobuf offsets are relative to the start of the symbols table (including its length field),
// as written by all Mimir versions.
func SparseSymbolsToProto(sparseSymbols SparseSymbols) *indexheaderpb.Symbols {
	proto := &indexheaderpb.Symbols{}

	offsets := make([]int64, sparseSymbols.NumOffsets())
	for i := range offsets {
		offsets[i] = int64(sparseSymbols.tableOffset(i))
	}

	proto.Offsets = offsets
	proto.SymbolsCount = int64(sparseSymbols.count)

	return proto
}

// SparseSymbolsFromProto loads the protobuf format to in-memory sparse symbols data
func SparseSymbolsFromProto(proto *indexheaderpb.Symbols) (SparseSymbols, error) {
	sparseSymbols := SparseSymbols{
		count:   int(proto.SymbolsCount),
		offsets: make([]uint32, 0, len(proto.Offsets)),
	}

	for _, offset := range proto.Offsets {
		if err := sparseSymbols.appendOffset(offset); err != nil {
			return SparseSymbols{}, err
		}
	}

	return sparseSymbols, nil
}
