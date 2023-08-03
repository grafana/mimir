// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

var logger = log.NewLogfmtLogger(os.Stderr)

func main() {
	// Cleanup all flags registered via init() methods of 3rd-party libraries.
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	verifyChunks := flag.Bool("check-chunks", false, "Verify chunks in segment files.")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [options...] <block-dir> [<block-dir> ...]:\n", os.Args[0])
		fmt.Fprintln(flag.CommandLine.Output())
		flag.PrintDefaults()
	}

	// Parse CLI arguments.
	args, err := flagext.ParseFlagsAndArguments(flag.CommandLine)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	if len(args) == 0 {
		flag.Usage()
		return
	}

	for _, b := range args {
		meta, err := block.ReadMetaFromDir(b)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Failed to read meta from block dir", b, "error:", err)
			continue
		}

		stats, err := block.GatherBlockHealthStats(logger, b, meta.MinTime, meta.MaxTime, *verifyChunks)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Failed to gather health stats from block dir", b, "error:", err)
			continue
		}

		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("  ", "  ")
		_ = enc.Encode(stats)
	}
}
