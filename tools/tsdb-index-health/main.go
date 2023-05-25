// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/go-kit/log"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

var logger = log.NewLogfmtLogger(os.Stderr)

func main() {
	verifyChunks := flag.Bool("check-chunks", false, "Verify chunks in segment files.")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [options...] <block-dir> [<block-dir> ...]:\n", os.Args[0])
		fmt.Fprintln(flag.CommandLine.Output())
		flag.PrintDefaults()
	}

	flag.Parse()

	if flag.NArg() == 0 {
		flag.Usage()
		return
	}

	for _, b := range flag.Args() {
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
