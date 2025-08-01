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
	"syscall"

	gokitlog "github.com/go-kit/log"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/mimir/pkg/storage/bucket"
)

const (
	defaultMDChunkSizeBytes = 500_000_000
)

var (
	writtenBytesCount = 0
	verbose           = false
	pprofPort         = "6060"
)

type config struct {
	bucket    bucket.Config
	action    string
	userID    string
	block     string
	dest      string
	count     bool
	chunkSize int
}

func main() {
	// Clean up all flags registered via init() methods of 3rd-party libraries.
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	cfg := config{}
	cfg.bucket.RegisterFlags(flag.CommandLine)
	flag.StringVar(&cfg.action, "action", "", "The action to take (options: 'download', 'convert')")
	flag.StringVar(&cfg.userID, "user", "", "The user (tenant) that owns the blocks to be listed")
	flag.StringVar(&cfg.block, "block", "", "The block ID of the block to download or the directory of the block to convert is at")
	flag.StringVar(&cfg.dest, "dest", "", "The path to write the resulting files to")
	flag.BoolVar(&cfg.count, "count", false, "Only count the number of bytes that would've been written to disk")
	flag.IntVar(&cfg.chunkSize, "chunk-size", defaultMDChunkSizeBytes, "Output chunk size")

	flag.BoolVar(&verbose, "verbose", false, "Enable verbose logging")
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
			log.Printf("dry run converting block at %s...\n", cfg.block)
		} else {
			log.Printf("converting block at %s to %s...\n", cfg.block, cfg.dest)
		}

		if err := convertBlock(ctx, cfg.block, cfg.dest, cfg.count, cfg.chunkSize, logger); err != nil {
			log.Fatalln("failed to convert block:", err)
		}
	default:
		log.Fatalln("--action must be 'download' or 'convert'")
	}

	log.Println("done.")
}
