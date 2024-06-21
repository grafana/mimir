// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
)

var logger = log.NewLogfmtLogger(os.Stderr)

func main() {
	// Clean up all flags registered via init() methods of 3rd-party libraries.
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	metricSelector := flag.String("select", "", "PromQL metric selector")
	printChunks := flag.Bool("show-chunks", false, "Print chunk details")
	seriesStats := flag.Bool("stats", false, "Show series stats: minT, maxT, samples, DPM")

	// Parse CLI arguments.
	args, err := flagext.ParseFlagsAndArguments(flag.CommandLine)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	if len(args) == 0 {
		fmt.Println("No block directory specified.")
		return
	}
	ctx := context.Background()

	var matchers []*labels.Matcher
	if *metricSelector != "" {
		var err error
		matchers, err = parser.ParseMetricSelector(*metricSelector)
		if err != nil {
			level.Error(logger).Log("msg", "failed to parse matcher selector", "err", err)
			os.Exit(1)
		}

		var matchersStr []interface{}
		matchersStr = append(matchersStr, "msg", "using matchers")
		for _, m := range matchers {
			matchersStr = append(matchersStr, "matcher", m.String())
		}

		level.Error(logger).Log(matchersStr...)
	}

	for _, blockDir := range args {
		printBlockIndex(ctx, blockDir, *printChunks, *seriesStats, matchers)
	}
}

func printBlockIndex(ctx context.Context, blockDir string, printChunks bool, seriesStats bool, matchers []*labels.Matcher) {
	block, err := tsdb.OpenBlock(logger, blockDir, nil)
	if err != nil {
		level.Error(logger).Log("msg", "failed to open block", "dir", blockDir, "err", err)
		return
	}
	defer block.Close()

	idx, err := block.Index()
	if err != nil {
		level.Error(logger).Log("msg", "failed to open block index", "err", err)
		return
	}
	defer idx.Close()

	chr, err := block.Chunks()
	if err != nil {
		level.Error(logger).Log("msg", "failed to open block chunks", "err", err)
		return
	}
	defer chr.Close()

	k, v := index.AllPostingsKey()

	// If there is any "equal" matcher, we can use it for getting postings instead,
	// it can massively speed up iteration over the index, especially for large blocks.
	for _, m := range matchers {
		if m.Type == labels.MatchEqual {
			k = m.Name
			v = m.Value
			break
		}
	}

	p, err := idx.Postings(ctx, k, v)
	if err != nil {
		level.Error(logger).Log("msg", "failed to get postings", "err", err)
		return
	}

	var builder labels.ScratchBuilder
	for p.Next() {
		chks := []chunks.Meta(nil)
		err := idx.Series(p.At(), &builder, &chks)
		if err != nil {
			level.Error(logger).Log("msg", "error getting series", "seriesID", p.At(), "err", err)
			continue
		}

		lbls := builder.Labels()
		matches := true
		for _, m := range matchers {
			val := lbls.Get(m.Name)
			if !m.Matches(val) {
				matches = false
				break
			}
		}

		if !matches {
			continue
		}

		if seriesStats {
			minT, maxT, samples := computeChunkStats(chr, chks)
			dpm := float64(samples) / (maxT.Sub(minT).Minutes())
			fmt.Println("series:", lbls.String(), "minT:", formatTime(minT), "maxT:", formatTime(maxT), "samples:", samples, "dpm:", dpm)
		} else {
			fmt.Println("series:", lbls.String())
		}

		if printChunks {
			for _, c := range chks {
				fmt.Println("chunk:", c.Ref,
					"min time:", c.MinTime, formatTime(timestamp.Time(c.MinTime)),
					"max time:", c.MaxTime, formatTime(timestamp.Time(c.MaxTime)))
			}
		}
	}

	if p.Err() != nil {
		level.Error(logger).Log("msg", "error iterating postings", "err", p.Err())
		return
	}
}

func formatTime(ts time.Time) string {
	return ts.UTC().Format(time.RFC3339Nano)
}

func computeChunkStats(chr tsdb.ChunkReader, chks []chunks.Meta) (time.Time, time.Time, int) {
	if len(chks) == 0 {
		return time.Time{}, time.Time{}, 0
	}

	minTS := chks[0].MinTime
	maxTS := chks[len(chks)-1].MaxTime
	totalSamples := 0

	for _, cm := range chks {
		c, it, err := chr.ChunkOrIterable(cm)
		if err != nil {
			level.Error(logger).Log("msg", "failed to open chunk", "err", err, "chunk", cm.Ref)
			return time.Time{}, time.Time{}, 0
		}

		if c != nil {
			totalSamples += c.NumSamples()
		} else if it != nil {
			i := it.Iterator(nil)
			for i.Next() != chunkenc.ValNone {
				totalSamples++
			}
			if err := i.Err(); err != nil {
				level.Error(logger).Log("msg", "got error while iterating chunk", "chunk", cm.Ref, "err", err)
				return time.Time{}, time.Time{}, 0
			}
		} else {
			level.Error(logger).Log("msg", "can't determine samples in chunk", "chunk", cm.Ref)
			return time.Time{}, time.Time{}, 0
		}
	}

	return time.UnixMilli(minTS), time.UnixMilli(maxTS), totalSamples
}
