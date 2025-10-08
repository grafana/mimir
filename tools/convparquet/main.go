package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"runtime"
	"runtime/pprof"
	"runtime/trace"

	"github.com/grafana/dskit/runutil"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/prometheus-community/parquet-common/convert"
	"github.com/prometheus/prometheus/tsdb"
)

func main() {
	// Parse command line flags
	var traceFlag = flag.Bool("trace", false, "Enable runtime tracing")
	var heapFlag = flag.Bool("heap", false, "Enable heap profiling")
	flag.Parse()

	if len(flag.Args()) < 1 {
		log.Fatal("Usage: conv [-trace] [-heap] <block-path>")
	}

	blockPath := flag.Args()[0]

	// Start tracing if requested
	if *traceFlag {
		traceFile, err := os.Create("trace.out")
		if err != nil {
			log.Fatalf("Failed to create trace file: %v", err)
		}
		defer traceFile.Close()

		if err := trace.Start(traceFile); err != nil {
			log.Fatalf("Failed to start trace: %v", err)
		}
		defer trace.Stop()
	}

	// Start heap profiling if requested
	if *heapFlag {
		heapFile, err := os.Create("heap.prof")
		if err != nil {
			log.Fatalf("Failed to create heap profile file: %v", err)
		}
		defer heapFile.Close()

		// Force garbage collection before profiling
		runtime.GC()

		if err := pprof.WriteHeapProfile(heapFile); err != nil {
			log.Fatalf("Failed to write heap profile: %v", err)
		}
	}
	metafile, err := os.Open(path.Join(blockPath, "meta.json"))
	if err != nil {
		log.Fatalf("Failed to open meta file: %v", err)
	}
	defer metafile.Close()

	// Create a context
	ctx := context.Background()

	// Read the block meta
	meta, err := block.ReadMeta(metafile)
	if err != nil {
		log.Fatalf("Failed to read block meta: %v", err)
	}

	// Convert the block
	err = ConvertBlock(ctx, meta, blockPath, []convert.ConvertOption{convert.WithConcurrency(3)})
	if err != nil {
		log.Fatalf("Failed to convert block: %v", err)
	}

	fmt.Println("Block conversion completed successfully")
}

func ConvertBlock(ctx context.Context, meta *block.Meta, localBlockDir string, opts []convert.ConvertOption) error {
	tsdbBlock, err := tsdb.OpenBlock(nil, localBlockDir, nil, tsdb.DefaultPostingsDecoderFactory)
	if err != nil {
		return err
	}
	defer runutil.CloseWithErrCapture(&err, tsdbBlock, "close tsdb block")

	_, err = convert.ConvertTSDBBlock(
		ctx,
		nil,
		meta.MinTime,
		meta.MaxTime,
		[]convert.Convertible{tsdbBlock},
		opts...,
	)
	return err
}
