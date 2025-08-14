package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	gokitlog "github.com/go-kit/log"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/mimir/pkg/storage/bucket"
)

const (
	defaultBatchSize               = 1000
	defaultHistScale               = 5
	defaultHistNumBins             = 20
	defaultResourceAttributeLabels = "service,service_name,service_instance_id,instance_id,server_address,server_port,url_scheme,server,address,port,scheme,cluster,namespace"
	defaultOutputFormat            = "protobuf"
)

var (
	writtenBytesCount = 0
	verbose           = false
	pprofPort         = "6060"
)

type config struct {
	bucket                  bucket.Config
	action                  string
	userID                  string
	block                   string
	dest                    string
	count                   bool
	batchSize               int
	chunkSize               time.Duration
	histScale               int
	histMax                 int
	histNumBins             int
	resourceAttributeLabels string
	dedupe                  bool
	outputFormat            string
	compressionType         string
	numBatches              int
}

func main() {
	// Clean up all flags registered via init() methods of 3rd-party libraries.
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	cfg := config{}
	cfg.bucket.RegisterFlags(flag.CommandLine)
	flag.StringVar(&cfg.action, "action", "", "The action to take (options: 'download', 'convert', 'analyze')")
	flag.StringVar(&cfg.userID, "user", "", "The user (tenant) that owns the blocks to be listed")
	flag.StringVar(&cfg.block, "block", "", "The block ID of the block to download or the directory of the block to convert is at")
	flag.StringVar(&cfg.dest, "dest", "", "The path to write the resulting files to")
	flag.BoolVar(&cfg.count, "count", false, "Only count the number of bytes that would've been written to disk")
	flag.IntVar(&cfg.batchSize, "batch-size", defaultBatchSize, "The number of postings to consume before outputting chunks")
	flag.DurationVar(&cfg.chunkSize, "chunk-size", 5*time.Minute, "The size as a time duration of a chunk")
	flag.IntVar(&cfg.histScale, "hist-scale", defaultHistScale, "Scaling factor used to output analysis histograms")
	flag.IntVar(&cfg.histMax, "hist-max", -1, "Filter cardinalities higher than this value out of the histogram (-1 to include all cardinalities)")
	flag.IntVar(&cfg.histNumBins, "hist-num-bins", defaultHistNumBins, "The number of bins to display in the analysis histogram")
	flag.StringVar(&cfg.resourceAttributeLabels, "resource-attribute-labels", defaultResourceAttributeLabels, "Comma-delimited list of labels to promote to resource attributes")
	flag.BoolVar(&cfg.dedupe, "dedupe", false, "Dedupe by batching metrics with the same resource and scope attributes")
	flag.StringVar(&cfg.outputFormat, "output-format", defaultOutputFormat, "Format to output chunks in (options: 'json', 'protobuf', 'arrow')")
	flag.StringVar(&cfg.compressionType, "compression-type", "", "Compression type to use")
	flag.IntVar(&cfg.numBatches, "num-batches", 0, "The number of batches to convert (<=0 for all)")

	flag.BoolVar(&verbose, "verbose", false, "Enable verbose logging (this will run much slower than non-verbose)")
	flag.StringVar(&pprofPort, "pprof-port", "6060", "The pprof server port")

	// Parse CLI flags.
	if err := flagext.ParseFlagsWithoutArguments(flag.CommandLine); err != nil {
		log.Fatalln(err.Error())
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT)
	defer cancel()

	go func() {
		log.Println(http.ListenAndServe(fmt.Sprintf("localhost:%s", pprofPort), nil))
	}()

	logger := gokitlog.NewNopLogger()

	switch cfg.action {
	case "download":
		if err := validateDownloadConfig(cfg); err != nil {
			log.Fatalln("config failed validation:", err)
		}

		log.Printf("downloading block %s to %s...\n", cfg.block, cfg.dest)
		if err := downloadBlock(ctx, cfg.bucket, cfg.userID, cfg.block, cfg.dest, logger); err != nil {
			log.Fatalln("failed to download block:", err)
		}
	case "convert":
		if err := validateConvertConfig(cfg); err != nil {
			log.Fatalln("config failed validation:", err)
		}

		if cfg.count {
			log.Printf("dry run converting block at %s in %s format...\n", cfg.block, cfg.outputFormat)
		} else {
			log.Printf("converting block at %s to %s in %s format...\n", cfg.block, cfg.dest, cfg.outputFormat)
		}

		if err := convertBlock(ctx, cfg, logger); err != nil {
			log.Fatalln("failed to convert block:", err)
		}
	case "analyze":
		if err := validateAnalyzeConfig(cfg); err != nil {
			log.Fatalln("config failed validation:", err)
		}

		if err := analyzeBlock(ctx, cfg.block, cfg.dest, cfg.histScale, cfg.histMax, cfg.histNumBins, logger); err != nil {
			log.Fatalln("failed to analyze block:", err)
		}
	case "download-convert":
		if err := validateConvertConfig(cfg); err != nil {
			log.Fatalln("config failed validation:", err)
		}

		blockDest := filepath.Join(os.TempDir(), cfg.block)
		if err := os.MkdirAll(blockDest, 0755); err != nil {
			log.Fatalln("failed to create temp block dir:", err)
		}

		log.Printf("downloading block %s to %s...\n", cfg.block, blockDest)
		if err := downloadBlock(ctx, cfg.bucket, cfg.userID, cfg.block, blockDest, logger); err != nil {
			log.Fatalln("failed to download block:", err)
		}

		// Get the config ready for the conversion.
		cfg.block = blockDest

		if cfg.count {
			log.Printf("dry run converting block at %s in %s format...\n", cfg.block, cfg.outputFormat)
		} else {
			log.Printf("converting block at %s to %s in %s format...\n", cfg.block, cfg.dest, cfg.outputFormat)
		}

		if err := convertBlock(ctx, cfg, logger); err != nil {
			log.Fatalln("failed to convert block:", err)
		}
	default:
		log.Fatalln("--action must be 'download' or 'convert' or 'analyze' or 'download-convert'")
	}

	log.Println("done.")
}
