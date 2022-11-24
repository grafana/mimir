// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"encoding/csv"
	"flag"
	"hash/crc32"
	"log"
	"os"
	"path"
	"strconv"
	"unsafe"

	"github.com/prometheus/prometheus/tsdb/fileutil"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/grafana/mimir/pkg/storage/tsdb/metadata"
	"github.com/grafana/mimir/pkg/storegateway/indexheader"
)

func main() {
	flag.Parse()
	blockPath := flag.Arg(0)

	meta, err := metadata.ReadFromDir(blockPath)

	if err != nil {
		log.Fatalf("could not load metadata: %v", err)
	}

	indexHeaderSizeDetails := calculateIndexHeaderSize(blockPath)

	w := csv.NewWriter(os.Stdout)

	record := []string{
		meta.ULID.String(),
		strconv.FormatInt(meta.MinTime, 10),
		strconv.FormatInt(meta.MaxTime, 10),
		strconv.FormatUint(meta.Stats.NumSeries, 10),
		strconv.Itoa(meta.Compaction.Level),
		strconv.FormatUint(indexHeaderSizeDetails.symbolTableSize, 10),
		strconv.FormatUint(indexHeaderSizeDetails.postingsOffsetTableSize, 10),
		strconv.FormatUint(indexHeaderSizeDetails.indexHeaderSize, 10),
	}

	if err := w.Write(record); err != nil {
		log.Fatalf("could not write CSV: %v", err)
	}

	w.Flush()
	if w.Error() != nil {
		log.Fatalf("could not flush CSV: %v", err)
	}
}

type indexHeaderSizeDetails struct {
	symbolTableSize         uint64
	postingsOffsetTableSize uint64
	indexHeaderSize         uint64
}

func calculateIndexHeaderSize(blockPath string) indexHeaderSizeDetails {
	indexPath := path.Join(blockPath, "index")
	f, err := fileutil.OpenMmapFile(indexPath)
	if err != nil {
		log.Fatalf(err.Error())
	}

	toc, err := index.NewTOCFromByteSlice(realByteSlice(f.Bytes()))
	if err != nil {
		log.Fatalf(err.Error())
	}

	// See https://github.com/prometheus/prometheus/blob/main/tsdb/docs/format/index.md on the index format.
	symbolTableSize := toc.Series - toc.Symbols

	// Requires the full index to be correct.
	if uint64(len(f.Bytes())) <= toc.PostingsTable {
		log.Fatalf("Index is incomplete, cannot calculate postings offset table size")
	}

	// TOC is a simple struct so unsafe.Sizeof() works correctly.
	tocLength := uint64(unsafe.Sizeof(index.TOC{})) + crc32.Size
	postingsOffsetTableSize := uint64(len(f.Bytes())) - toc.PostingsTable - tocLength
	indexHeaderPreambleSize := uint64(4 + 1 + 1 + 8)
	indexHeaderTOCSize := uint64(unsafe.Sizeof(indexheader.BinaryTOC{}))
	indexHeaderSize := symbolTableSize + postingsOffsetTableSize + indexHeaderPreambleSize + indexHeaderTOCSize

	return indexHeaderSizeDetails{
		symbolTableSize:         symbolTableSize,
		postingsOffsetTableSize: postingsOffsetTableSize,
		indexHeaderSize:         indexHeaderSize,
	}
}

type realByteSlice []byte

func (b realByteSlice) Len() int {
	return len(b)
}

func (b realByteSlice) Range(start, end int) []byte {
	return b[start:end]
}
