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
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

var logger = log.NewLogfmtLogger(os.Stderr)

func main() {
	args := os.Args

	if len(args) < 3 {
		fmt.Println("Usage:", args[0], "<block-dir> chunkRef...")
		return
	}

	printChunks(args[1], args[2:])
}

func printChunks(blockDir string, chunkRefs []string) {
	b, err := tsdb.OpenBlock(logger, blockDir, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to open TSDB block", blockDir, "due to error:", err)
		os.Exit(1)
	}

	cr, err := b.Chunks()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to get chunks reader for block", blockDir, "due to error:", err)
		os.Exit(1)
	}

	for _, ref := range chunkRefs {
		val, err := strconv.ParseUint(ref, 10, 64)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Failed to parse chunk ref:", ref)
			continue
		}

		ch, iter, err := cr.ChunkOrIterable(chunks.Meta{Ref: chunks.ChunkRef(val)})
		if err != nil {
			fmt.Fprintln(os.Stderr, "Failed to open chunk", val, "due to error:", err)
			continue
		}
		if iter != nil {
			fmt.Fprintln(os.Stderr, "Failed to open chunk", val, "got iterable")
			continue
		}

		fmt.Println("Chunk ref:", ref, "samples:", ch.NumSamples(), "bytes:", len(ch.Bytes()))

		it := ch.Iterator(nil)
		for valType := it.Next(); valType != chunkenc.ValNone; valType = it.Next() {
			switch valType {
			case chunkenc.ValFloat:
				ts, v := it.At()
				fmt.Printf("%g\t%d (%s)\n", v, ts, timestamp.Time(ts).UTC().Format(time.RFC3339Nano))
			case chunkenc.ValHistogram:
				ts, hist := it.AtHistogram()
				fmt.Printf("%s\t%d (%s)\n", hist.String(), ts, timestamp.Time(ts).UTC().Format(time.RFC3339Nano))
			case chunkenc.ValFloatHistogram:
				ts, hist := it.AtFloatHistogram()
				fmt.Printf("%s\t%d (%s)\n", hist.String(), ts, timestamp.Time(ts).UTC().Format(time.RFC3339Nano))
			default:
				fmt.Printf("skipping unsupported value type %v\n", valType)
			}
		}
		if e := it.Err(); e != nil {
			fmt.Fprintln(os.Stderr, "Failed to iterate chunk", val, "due to error:", err)
		}
	}
}
