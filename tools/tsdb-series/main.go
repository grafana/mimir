// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
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

	util_log "github.com/grafana/mimir/pkg/util/log"
)

var logger = log.NewLogfmtLogger(os.Stderr)

func main() {
	// Clean up all flags registered via init() methods of 3rd-party libraries.
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	metricSelector := flag.String("select", "", "PromQL metric selector")
	printChunks := flag.Bool("show-chunks", false, "Print chunk details")
	seriesStats := flag.Bool("stats", false, "Show series stats: minT, maxT, samples, DPM")
	jsonOutput := flag.Bool("json", false, "Output as JSON, one object per series.")

	var minTime, maxTime flagext.Time
	flag.Var(&minTime, "min-time", "If set, only samples with timestamp >= this value are considered when computing chunk stats")
	flag.Var(&maxTime, "max-time", "If set, only samples with timestamp <= this value are considered when computing chunk stats")

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

	if !time.Time(minTime).IsZero() {
		level.Info(logger).Log("msg", "using minTime for samples", "minTime", formatTime(time.Time(minTime)))
	}
	if !time.Time(maxTime).IsZero() {
		level.Info(logger).Log("msg", "using maxTime for samples", "maxTime", formatTime(time.Time(maxTime)))
	}

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
		printBlockIndex(ctx, blockDir, *printChunks, *seriesStats, matchers, time.Time(minTime), time.Time(maxTime), *jsonOutput)
	}
}

type chunk struct {
	Ref     chunks.ChunkRef
	MinTime int64 `json:"minTime"`
	MaxTime int64 `json:"maxTime"`
}

type series struct {
	Series map[string]string `json:"series"`

	Chunks []chunk `json:"chunks,omitempty"`
}

type seriesWithStats struct {
	series

	// Stats
	MinTime int64   `json:"minTime"`
	MaxTime int64   `json:"maxTime"`
	Samples int     `json:"samples"`
	DPM     float64 `json:"dpm"`
}

func printBlockIndex(ctx context.Context, blockDir string, printChunks bool, seriesStats bool, matchers []*labels.Matcher, minTime time.Time, maxTime time.Time, jsonOutput bool) {
	block, err := tsdb.OpenBlock(util_log.SlogFromGoKit(logger), blockDir, nil)
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

	jsonEnc := json.NewEncoder(os.Stdout)

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

		output := seriesWithStats{}
		output.Series = lbls.Map()

		if seriesStats {
			output.MinTime, output.MaxTime, output.Samples = computeChunkStats(chr, chks, minTime.UnixMilli(), maxTime.UnixMilli())
			output.DPM = float64(output.Samples) / (time.Millisecond * time.Duration(output.MaxTime-output.MinTime)).Minutes()
			if math.IsNaN(output.DPM) || math.IsInf(output.DPM, 0) {
				output.DPM = 0
			}
		}

		if printChunks {
			for _, c := range chks {
				output.Chunks = append(output.Chunks, chunk{
					Ref:     c.Ref,
					MinTime: c.MinTime,
					MaxTime: c.MaxTime,
				})
			}
		}

		if jsonOutput {
			var err error
			if seriesStats {
				err = jsonEnc.Encode(output)
			} else {
				err = jsonEnc.Encode(output.Series)
			}
			if err != nil {
				level.Error(logger).Log("msg", "failed to JSON encode series", "err", err)
			}
		} else {
			if seriesStats {
				fmt.Println("series:", lbls.String(), "minT:", formatTime(timestamp.Time(output.MinTime)), "maxT:", formatTime(timestamp.Time(output.MaxTime)), "samples:", output.Samples, "dpm:", output.DPM)
			} else {
				fmt.Println("series:", lbls.String())
			}

			if printChunks {
				for _, c := range output.Chunks {
					fmt.Println("chunk:", c.Ref,
						"min time:", c.MinTime, formatTime(timestamp.Time(c.MinTime)),
						"max time:", c.MaxTime, formatTime(timestamp.Time(c.MaxTime)))
				}
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

func computeChunkStats(chr tsdb.ChunkReader, chks []chunks.Meta, minTimeFilter, maxTimeFilter int64) (minTime int64, maxTime int64, totalSamples int) {
	if len(chks) == 0 {
		return 0, 0, 0
	}

	for _, cm := range chks {
		c, itb, err := chr.ChunkOrIterable(cm)
		if err != nil {
			level.Error(logger).Log("msg", "failed to open chunk", "err", err, "chunk", cm.Ref)
			return 0, 0, 0
		}

		var it chunkenc.Iterator
		if c != nil {
			it = c.Iterator(nil)
		} else if itb != nil {
			it = itb.Iterator(nil)
		} else {
			level.Error(logger).Log("msg", "can't determine samples in chunk", "chunk", cm.Ref)
			return 0, 0, 0
		}

		for it.Next() != chunkenc.ValNone {
			include := true
			ts := it.AtT()

			if minTimeFilter > 0 && ts < minTimeFilter {
				include = false
			}
			if maxTimeFilter > 0 && ts > maxTimeFilter {
				include = false
			}
			if include {
				totalSamples++
				if minTime == 0 {
					minTime = ts
				}
				if ts > maxTime {
					maxTime = ts
				}
			}
		}
		if err := it.Err(); err != nil {
			level.Error(logger).Log("msg", "got error while iterating chunk", "chunk", cm.Ref, "err", err)
			return 0, 0, 0
		}
	}

	return minTime, maxTime, totalSamples
}
