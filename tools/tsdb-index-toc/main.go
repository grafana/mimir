// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"flag"
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"unsafe"

	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	"github.com/prometheus/prometheus/tsdb/index"
)

func main() {
	// Clean up all flags registered via init() methods of 3rd-party libraries.
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	// Parse CLI arguments.
	args, err := flagext.ParseFlagsAndArguments(flag.CommandLine)
	if err != nil {
		log.Fatalln(err.Error())
	}

	if len(args) != 1 {
		log.Fatalf("the command expect 1 argument but %d was provided\n", len(args))
	}

	filepath := args[0]

	finfo, err := os.Stat(filepath)
	if err != nil {
		log.Fatal(err.Error())
	}
	indexSize := finfo.Size()

	f, err := fileutil.OpenMmapFile(filepath)
	if err != nil {
		log.Fatal(err.Error())
	}

	toc, err := index.NewTOCFromByteSlice(realByteSlice(f.Bytes()))
	if err != nil {
		log.Fatal(err.Error())
	}

	// See https://github.com/prometheus/prometheus/blob/main/tsdb/docs/format/index.md on the index format.
	fmt.Println("Index size:             ", indexSize)
	fmt.Println("Symbols table size:     ", toc.Series-toc.Symbols)
	fmt.Println("Series size:            ", toc.LabelIndices-toc.Series)
	fmt.Println("Label indices:          ", toc.Postings-toc.LabelIndices)
	fmt.Println("Postings:               ", toc.LabelIndicesTable-toc.Postings)
	fmt.Println("Label offset table:     ", toc.PostingsTable-toc.LabelIndicesTable)

	// Requires the full index to be correct.
	if uint64(len(f.Bytes())) > toc.PostingsTable {
		// TOC is a simple struct so unsafe.Sizeof() works correctly.
		tocLength := uint64(unsafe.Sizeof(index.TOC{})) + crc32.Size
		postingsTbl := uint64(len(f.Bytes())) - toc.PostingsTable - tocLength
		indexHdr := toc.Series - toc.Symbols + postingsTbl + 34
		fmt.Println("Postings offset table:  ", postingsTbl)
		fmt.Println("Index-header size (est):", indexHdr)
		fmt.Printf("Index-header %% (est):    %.3f\n", 100.0*float64(indexHdr)/float64(indexSize))
	} else {
		fmt.Println("Postings offset table:   N/A")
		fmt.Println("Index-header size (est): N/A")
		fmt.Println("Index-header % (est):    N/A")
	}

}

type realByteSlice []byte

func (b realByteSlice) Len() int {
	return len(b)
}

func (b realByteSlice) Range(start, end int) []byte {
	return b[start:end]
}
