// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"flag"
	"fmt"
	"hash/crc32"
	"log"
	"unsafe"

	"github.com/dustin/go-humanize"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/grafana/mimir/pkg/storegateway/indexheader"
)

func main() {
	flag.Parse()

	filepath := flag.Arg(0)

	f, err := fileutil.OpenMmapFile(filepath)
	if err != nil {
		log.Fatalf(err.Error())
	}

	toc, err := index.NewTOCFromByteSlice(realByteSlice(f.Bytes()))
	if err != nil {
		log.Fatalf(err.Error())
	}

	// See https://github.com/prometheus/prometheus/blob/main/tsdb/docs/format/index.md on the index format.
	symbolTableSize := toc.Series - toc.Symbols
	seriesSize := toc.LabelIndices - toc.Series
	labelIndicesSize := toc.Postings - toc.LabelIndices
	postingsSize := toc.LabelIndicesTable - toc.Postings
	labelOffsetTableSize := toc.PostingsTable - toc.LabelIndicesTable
	fmt.Printf("Symbols table size:         %12v  %10v\n", symbolTableSize, humanize.SIWithDigits(float64(symbolTableSize), 1, "B"))
	fmt.Printf("Series size:                %12v  %10v\n", seriesSize, humanize.SIWithDigits(float64(seriesSize), 1, "B"))
	fmt.Printf("Label indices size:         %12v  %10v\n", labelIndicesSize, humanize.SIWithDigits(float64(labelIndicesSize), 1, "B"))
	fmt.Printf("Postings size:              %12v  %10v\n", postingsSize, humanize.SIWithDigits(float64(postingsSize), 1, "B"))
	fmt.Printf("Label offset table size:    %12v  %10v\n", labelOffsetTableSize, humanize.SIWithDigits(float64(labelOffsetTableSize), 1, "B"))

	// Requires the full index to be correct.
	if uint64(len(f.Bytes())) > toc.PostingsTable {
		// TOC is a simple struct so unsafe.Sizeof() works correctly.
		tocLength := uint64(unsafe.Sizeof(index.TOC{})) + crc32.Size
		postingsOffsetTableSize := uint64(len(f.Bytes())) - toc.PostingsTable - tocLength

		fmt.Printf("Postings offset table size: %12v  %10v\n", postingsOffsetTableSize, humanize.SIWithDigits(float64(postingsOffsetTableSize), 1, "B"))

		indexHeaderPreambleSize := uint64(4 + 1 + 1 + 8)
		indexHeaderTOCSize := uint64(unsafe.Sizeof(indexheader.BinaryTOC{}))
		indexHeaderSize := symbolTableSize + postingsOffsetTableSize + indexHeaderPreambleSize + indexHeaderTOCSize
		fmt.Println()
		fmt.Printf("Index header size:          %12v  %10v\n", indexHeaderSize, humanize.SIWithDigits(float64(indexHeaderSize), 1, "B"))
	} else {
		fmt.Printf("Postings offset table size:          N/A\n")
	}

}

type realByteSlice []byte

func (b realByteSlice) Len() int {
	return len(b)
}

func (b realByteSlice) Range(start, end int) []byte {
	return b[start:end]
}
