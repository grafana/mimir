// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/prometheus/prometheus/tsdb/fileutil"
	"github.com/prometheus/prometheus/tsdb/index"
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
	fmt.Println("Symbols table size:   ", toc.Series-toc.Symbols)
	fmt.Println("Series size:          ", toc.LabelIndices-toc.Series)
	fmt.Println("Label indices:        ", toc.Postings-toc.LabelIndices)
	fmt.Println("Postings:             ", toc.LabelIndicesTable-toc.Postings)
	fmt.Println("Label offset table:   ", toc.PostingsTable-toc.LabelIndicesTable)

	// Requires the full index to be correct.
	if uint64(len(f.Bytes())) > toc.PostingsTable {
		fmt.Println("Postings offset table:", uint64(len(f.Bytes()))-toc.PostingsTable)
	} else {
		fmt.Println("Postings offset table: N/A")
	}
}

type realByteSlice []byte

func (b realByteSlice) Len() int {
	return len(b)
}

func (b realByteSlice) Range(start, end int) []byte {
	return b[start:end]
}
