// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"flag"
	"fmt"
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

	return newFullIndexAnalyzer(reader), info, nil
}

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
	sparseHeadersPath := filepath.Join(blockDir, block.SparseIndexHeaderFilename)
	reader, err := indexheader.NewFileStreamBinaryReader(
		ctx,
		indexHeaderPath,
		blockID,
		sparseHeadersPath,
		32, // postingOffsetsInMemSampling - default value
		logger,
		bkt,
		metrics,
		indexheader.Config{},
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
