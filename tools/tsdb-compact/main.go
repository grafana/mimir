// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"

	golog "github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/prometheus/tsdb"

	util_log "github.com/grafana/mimir/pkg/util/log"
)

func main() {
	// Clean up all flags registered via init() methods of 3rd-party libraries.
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	var (
		outputDir     string
		shardCount    int
		cpuProf       string
		segmentSizeMB int64
	)

	flag.StringVar(&outputDir, "output-dir", ".", "Output directory for new block(s)")
	flag.StringVar(&cpuProf, "cpuprofile", "", "Where to store CPU profile (it not empty)")
	flag.IntVar(&shardCount, "shard-count", 1, "Number of shards for splitting")
	flag.Int64Var(&segmentSizeMB, "segment-file-size", 512, "Size of segment file")

	// Parse CLI arguments.
	args, err := flagext.ParseFlagsAndArguments(flag.CommandLine)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	logger := golog.NewLogfmtLogger(os.Stderr)

	var blockDirs []string
	var blocks []*tsdb.Block
	for _, d := range args {
		s, err := os.Stat(d)
		if err != nil {
			panic(err)
		}
		if !s.IsDir() {
			log.Fatalln("not a directory: ", d)
		}

		blockDirs = append(blockDirs, d)

		b, err := tsdb.OpenBlock(util_log.SlogFromGoKit(logger), d, nil)
		if err != nil {
			log.Fatalln("failed to open block:", d, err)
		}

		blocks = append(blocks, b)
		defer b.Close()
	}

	if len(blockDirs) == 0 {
		log.Fatalln("no blocks to compact")
	}

	if cpuProf != "" {
		f, err := os.Create(cpuProf)
		if err != nil {
			log.Fatalln(err)
		}

		log.Println("writing to", cpuProf)
		err = pprof.StartCPUProfile(f)
		if err != nil {
			log.Fatalln(err)
		}

		defer pprof.StopCPUProfile()
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	c, err := tsdb.NewLeveledCompactorWithChunkSize(ctx, nil, util_log.SlogFromGoKit(logger), []int64{0}, nil, segmentSizeMB*1024*1024, nil)
	if err != nil {
		log.Fatalln("creating compator", err)
	}

	_, err = c.CompactWithSplitting(outputDir, blockDirs, blocks, uint64(shardCount))
	if err != nil {
		log.Fatalln("compacting", err)
	}
}
