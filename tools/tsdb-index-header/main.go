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
		log.Fatalf("Usage: %s [-analyze-labels=label1,label2,...] <path-to-index-header>\n", os.Args[0])
	}

	indexHeaderPath := args[0]

	finfo, err := os.Stat(indexHeaderPath)
	if err != nil {
		log.Fatal(err.Error())
	}
	indexHeaderSize := finfo.Size()

	// The index-header file is expected to be at <dir>/<block-id>/index-header
	// We need to extract the block ID from the path.
	blockDir := filepath.Dir(indexHeaderPath)
	blockIDStr := filepath.Base(blockDir)
	blockID, err := ulid.Parse(blockIDStr)
	if err != nil {
		log.Fatalf("Failed to parse block ID from path %q: %v\n", blockDir, err)
	}

	// Create a filesystem bucket pointing to the parent of the block directory.
	bucketDir := filepath.Dir(blockDir)
	ubkt, err := filesystem.NewBucket(bucketDir)
	if err != nil {
		log.Fatalf("Failed to create filesystem bucket: %v\n", err)
	}
	defer ubkt.Close()
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
		log.Fatalf("Failed to read index-header: %v\n", err)
	}
	defer reader.Close()

	// Check TSDB index version - only V2 is supported for symbol iteration.
	indexVersion, err := reader.IndexVersion(ctx)
	if err != nil {
		log.Fatalf("Failed to get index version: %v\n", err)
	}
	if indexVersion == 1 {
		log.Fatalln("TSDB index V1 format is not supported")
	}

	tocInfo := analyzeTOC(ctx, reader, indexHeaderSize)
	printTOCInfo(ctx, os.Stdout, tocInfo)

	fmt.Println()
	fmt.Println("=== Symbols Analysis ===")
	symbolStats := analyzeSymbols(ctx, reader)
	printSymbolStats(ctx, os.Stdout, symbolStats)

	fmt.Println()
	fmt.Println("=== Label Cardinality Analysis ===")
	labelStats := analyzeLabelCardinality(ctx, reader)
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
			labelValueStats := analyzeLabelValues(ctx, reader, labelName)
			if labelValueStats != nil {
				printLabelValueStats(ctx, os.Stdout, labelValueStats)
			}
		}
	}
}
