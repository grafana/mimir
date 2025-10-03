package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path"

	"github.com/grafana/dskit/runutil"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/prometheus-community/parquet-common/convert"
	"github.com/prometheus/prometheus/tsdb"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: conv <block-path>")
	}

	blockPath := os.Args[1]
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
	err = ConvertBlock(ctx, meta, blockPath, nil)
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
