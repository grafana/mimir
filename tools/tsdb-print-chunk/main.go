// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

var logger = log.NewLogfmtLogger(os.Stderr)

func main() {
	args := os.Args

	if len(args) < 3 {
		fmt.Println("Usage:", args[0], "<block-dir> [chunkRef chunkRef ...]")
		return
	}

	b, err := tsdb.OpenBlock(logger, args[1], nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to open TSDB block", args[1], "due to error:", err)
		os.Exit(1)
	}

	cr, err := b.Chunks()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to get chunks reader for block", args[1], "due to error:", err)
		os.Exit(1)
	}

	for _, ref := range args[2:] {
		val, err := strconv.ParseUint(ref, 10, 64)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Failed to parse chunk ref:", ref)
			continue
		}

		ch, err := cr.Chunk(chunks.ChunkRef(val))
		if err != nil {
			fmt.Fprintln(os.Stderr, "Failed to open chunk", val, "due to error:", err)
			continue
		}

		fmt.Println("Chunk ref:", ref, "samples:", ch.NumSamples(), "bytes:", len(ch.Bytes()))

		it := ch.Iterator(nil)
		for it.Err() == nil && it.Next() {
			ts, val := it.At()
			fmt.Printf("%g\t%d (%s)\n", val, ts, timestamp.Time(ts).UTC().Format(time.RFC3339Nano))
		}
		if e := it.Err(); e != nil {
			fmt.Fprintln(os.Stderr, "Failed to iterate chunk", val, "due to error:", err)
		}
	}
}
