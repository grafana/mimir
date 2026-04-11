// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"flag"
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"path/filepath"
	"strings"

	gokitlog "github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/grafana/mimir/pkg/storage/indexheader"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

func main() {
	// Clean up all flags registered via init() methods of 3rd-party libraries.
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	// Define CLI flags.
	analyzeLabels := flag.String("analyze-labels", "", "Comma-separated list of label names to analyze value distribution")
	analyzeChunksFlag := flag.Bool("analyze-chunks", false, "Analyze chunk distribution across series (requires full index)")

	// Parse CLI arguments.
	args, err := flagext.ParseFlagsAndArguments(flag.CommandLine)
	if err != nil {
		log.Fatalln(err.Error())
	}

	if len(args) != 1 {
		log.Fatalf("Usage: %s [-analyze-labels=label1,label2,...] <block-directory>\n", os.Args[0])
	}

	blockDir := args[0]

	// Auto-detect whether we have an index or index-header file.
	indexPath := filepath.Join(blockDir, block.IndexFilename)
	indexHeaderPath := filepath.Join(blockDir, block.IndexHeaderFilename)

	var analyzer IndexAnalyzer
	var info *IndexInfo

	// Prefer full index if available (more complete data).
	if finfo, err := os.Stat(indexPath); err == nil {
		analyzer, info, err = openFullIndex(indexPath, finfo.Size())
		if err != nil {
			log.Fatalf("Failed to open full index: %v\n", err)
		}
	} else if finfo, err := os.Stat(indexHeaderPath); err == nil {
		analyzer, info, err = openIndexHeader(blockDir, indexHeaderPath, finfo.Size())
		if err != nil {
			log.Fatalf("Failed to open index-header: %v\n", err)
		}
	} else {
		log.Fatalf("No index or index-header found in block directory %q\n", blockDir)
	}
	defer analyzer.Close()

	ctx := context.Background()

	// Check TSDB index version - only V2+ is supported for symbol iteration.
	indexVersion, err := analyzer.IndexVersion(ctx)
	if err != nil {
		log.Fatalf("Failed to get index version: %v\n", err)
	}
	if indexVersion == 1 {
		log.Fatalln("TSDB index V1 format is not supported")
	}

	info.IndexVersion = indexVersion

	printIndexInfo(ctx, os.Stdout, info, analyzer)

	fmt.Println()
	fmt.Println("=== Symbols Analysis ===")
	symbolStats := analyzeSymbols(ctx, analyzer)
	printSymbolStats(ctx, os.Stdout, symbolStats)

	fmt.Println()
	fmt.Println("=== Label Cardinality Analysis ===")
	labelStats := analyzeLabelCardinality(ctx, analyzer)
	printLabelStats(ctx, os.Stdout, labelStats)

	// Analyze chunks if requested.
	if *analyzeChunksFlag {
		if info.IsIndexHeader {
			log.Fatalln("Chunk analysis requires a full index, but an index-header was loaded")
		}
		chunkStats := analyzeChunks(ctx, analyzer)
		if chunkStats == nil {
			log.Fatalln("Chunk analysis returned no results; this is unexpected for a full index")
		}
		fmt.Println()
		fmt.Println("=== Chunk Distribution Analysis ===")
		printChunkStats(ctx, os.Stdout, chunkStats)
	}

	// Analyze specific labels if requested.
	if *analyzeLabels != "" {
		labelNames := strings.Split(*analyzeLabels, ",")
		for _, labelName := range labelNames {
			labelName = strings.TrimSpace(labelName)
			if labelName == "" {
				continue
			}
			fmt.Println()
			fmt.Printf("=== Label Value Distribution: %s ===\n", labelName)
			labelValueStats := analyzeLabelValues(ctx, analyzer, labelName)
			if labelValueStats != nil {
				printLabelValueStats(ctx, os.Stdout, labelValueStats)
			}

			// Analyze metric names that have this label (only works with full index).
			metricStats := analyzeMetricNamesForLabel(ctx, analyzer, labelName)
			if metricStats != nil {
				printMetricNameStats(ctx, os.Stdout, metricStats)
			}
		}
	}
}

// tsdbIndexTOCLen is the size of the TSDB index TOC (6 uint64 fields + CRC32).
const tsdbIndexTOCLen = 6*8 + crc32.Size

// openFullIndex opens a full TSDB index file and returns an analyzer.
func openFullIndex(indexPath string, size int64) (IndexAnalyzer, *IndexInfo, error) {
	reader, err := index.NewFileReader(indexPath, index.DecodePostingsRaw)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read index: %w", err)
	}

	info := &IndexInfo{
		Path:          indexPath,
		Size:          size,
		IsIndexHeader: false,
	}

	// Read the TOC to compute section sizes.
	toc, err := readFullIndexTOC(indexPath, size)
	if err != nil {
		reader.Close()
		return nil, nil, fmt.Errorf("failed to read index TOC: %w", err)
	}

	info.FullIndexSymbolsSize = toc.Series - toc.Symbols
	info.FullIndexSeriesSize = toc.LabelIndices - toc.Series
	info.FullIndexLabelIndicesSize = toc.Postings - toc.LabelIndices
	info.FullIndexPostingsSize = toc.LabelIndicesTable - toc.Postings
	info.FullIndexLabelIndicesTableSize = toc.PostingsTable - toc.LabelIndicesTable
	info.FullIndexPostingsTableSize = uint64(size) - uint64(tsdbIndexTOCLen) - toc.PostingsTable
	info.FullIndexTOCSize = uint64(tsdbIndexTOCLen)

	return newFullIndexAnalyzer(reader), info, nil
}

// readFullIndexTOC reads the TOC from a full TSDB index file on disk.
func readFullIndexTOC(indexPath string, size int64) (*index.TOC, error) {
	if size < int64(tsdbIndexTOCLen) {
		return nil, fmt.Errorf("file too small to contain a valid TSDB index TOC (size: %d, minimum: %d)", size, tsdbIndexTOCLen)
	}

	f, err := os.Open(indexPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Read the TOC bytes from the end of the file.
	tocBytes := make([]byte, tsdbIndexTOCLen)
	if _, err := f.ReadAt(tocBytes, size-int64(tsdbIndexTOCLen)); err != nil {
		return nil, fmt.Errorf("read TOC bytes: %w", err)
	}

	return index.NewTOCFromByteSlice(realByteSlice(tocBytes))
}

// realByteSlice implements index.ByteSlice over a plain byte slice.
// Mirrors the realByteSlice type in github.com/prometheus/prometheus/tsdb/index.
type realByteSlice []byte

func (b realByteSlice) Len() int                     { return len(b) }
func (b realByteSlice) Range(s, e int) []byte        { return b[s:e] }
func (b realByteSlice) Sub(s, e int) index.ByteSlice { return b[s:e] }

// openIndexHeader opens an index-header file and returns an analyzer.
func openIndexHeader(blockDir, indexHeaderPath string, size int64) (IndexAnalyzer, *IndexInfo, error) {
	// The index-header file is expected to be at <dir>/<block-id>/index-header
	// We need to extract the block ID from the path.

	blockIDStr := filepath.Base(blockDir)
	blockID, err := ulid.Parse(blockIDStr)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse block ID from path %q: %w", blockDir, err)
	}

	// Create a filesystem bucket pointing to the parent of the block directory.
	bucketDir := filepath.Dir(blockDir)
	ubkt, err := filesystem.NewBucket(bucketDir)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create filesystem bucket: %w", err)
	}
	bkt := objstore.WithNoopInstr(ubkt)

	// Create metrics (nil registry since we don't need metrics for the CLI tool).
	metrics := indexheader.NewStreamBinaryReaderMetrics(nil)

	// Create a no-op logger.
	logger := gokitlog.NewNopLogger()

	// Parse the index-header.
	ctx := context.Background()
	indexHeaderCfg := indexheader.Config{} // Default config creates a file-based reader.
	reader, err := indexheader.NewStreamBinaryReader(
		ctx,
		blockID,
		bkt,
		filepath.Dir(blockDir),
		indexHeaderCfg,
		32, // postingOffsetsInMemSampling - default value
		logger,
		metrics,
	)
	if err != nil {
		ubkt.Close()
		return nil, nil, fmt.Errorf("failed to read index-header: %w", err)
	}

	// Get TOC info for index-header specific details.
	adapter := newIndexHeaderAnalyzer(reader, ubkt).(*indexHeaderAnalyzer)
	toc := adapter.TOC()

	info := &IndexInfo{
		Path:               indexHeaderPath,
		Size:               size,
		IsIndexHeader:      true,
		IndexHeaderVersion: adapter.IndexHeaderVersion(),
		SymbolsSize:        toc.PostingsOffsetTable - toc.Symbols,
		PostingsTableSize:  uint64(size) - indexheader.BinaryTOCLen - toc.PostingsOffsetTable,
	}

	return adapter, info, nil
}
