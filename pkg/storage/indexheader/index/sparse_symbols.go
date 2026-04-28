// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/tsdb/index/index.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package index

import (
	"fmt"

	"github.com/grafana/dskit/runutil"

	streamencoding "github.com/grafana/mimir/pkg/storage/indexheader/encoding"
	"github.com/grafana/mimir/pkg/storage/indexheader/indexheaderpb"
)

func SparseValuesFromSymbolsTable(
	decbufFactory streamencoding.DecbufFactory,
	tableOffset int,
	doChecksum bool,
) (allSymbolsCount int, sparseSymbolsOffsets []int, err error) {
	var decbuf streamencoding.Decbuf
	if doChecksum {
		decbuf = decbufFactory.NewDecbufAtChecked(tableOffset, castagnoliTable)
	} else {
		decbuf = decbufFactory.NewDecbufAtUnchecked(tableOffset)
	}

	defer runutil.CloseWithErrCapture(&err, &decbuf, "decode symbols table")
	if err := decbuf.Err(); err != nil {
		return -1, nil, fmt.Errorf("init symbol table decoding buffer: %w", decbuf.Err())
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
	allSymbolsCount = decbuf.Be32int()

	seen := 0
	sparseSymbolsOffsets = make([]int, 0, 1+allSymbolsCount/symbolFactor)
	for decbuf.Err() == nil && seen < allSymbolsCount {
		if seen%symbolFactor == 0 {
			sparseSymbolsOffsets = append(sparseSymbolsOffsets, decbuf.Offset())
		}
		decbuf.SkipUvarintBytes() // The symbol.
		seen++
	}

	if decbuf.Err() != nil {
		return -1, nil, decbuf.Err()
	}

	return allSymbolsCount, sparseSymbolsOffsets, nil
}

// SparseSymbolsToProto loads the in-memory sparse symbols data into the protobuf format
func SparseSymbolsToProto(allSymbolsCount int, sparseOffsets []int) *indexheaderpb.Symbols {
	proto := &indexheaderpb.Symbols{}

	offsets := make([]int64, len(sparseOffsets))
	for i, offset := range sparseOffsets {
		offsets[i] = int64(offset)
	}

	proto.Offsets = offsets
	proto.SymbolsCount = int64(allSymbolsCount)

	return proto
}

// SparseSymbolsFromProto loads the protobuf format to in-memory sparse symbols data
func SparseSymbolsFromProto(proto *indexheaderpb.Symbols) (allSymbolsCount int, sparseOffsets []int) {
	allSymbolsCount = int(proto.SymbolsCount)
	sparseOffsets = make([]int, len(proto.Offsets))

	for i, offset := range proto.Offsets {
		sparseOffsets[i] = int(offset)
	}

	return allSymbolsCount, sparseOffsets
}
