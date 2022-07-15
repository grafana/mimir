// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
)

var logger = log.NewLogfmtLogger(os.Stderr)

func main() {
	metricSelector := flag.String("select", "", "PromQL metric selector")
	flag.Parse()

	if flag.NArg() == 0 {
		fmt.Println("No block directory specified.")
		return
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

	for _, blockDir := range flag.Args() {
		printBlockIndex(blockDir, matchers)
	}

	ratio := float64(totalBytes) / float64(totalSamples)
	fmt.Println("total_chunk_bytes:", totalBytes, "total_chunk_samples:", totalSamples, "ratio:", fmt.Sprintf("%0.2f", ratio))
}

var totalBytes, totalSamples int
var sameBytes, chunksWithSameSamples int

func printBlockIndex(blockDir string, matchers []*labels.Matcher) {
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

	k, v := index.AllPostingsKey()
	p, err := idx.Postings(k, v)

	if err != nil {
		level.Error(logger).Log("msg", "failed to get postings", "err", err)
		return
	}

	cr, err := block.Chunks()
	if err != nil {
		panic(err)
	}

	for p.Next() {
		lbls := labels.Labels(nil)
		chks := []chunks.Meta(nil)
		err := idx.Series(p.At(), &lbls, &chks)
		if err != nil {
			level.Error(logger).Log("msg", "error getting series", "seriesID", p.At(), "err", err)
			continue
		}

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

		//fmt.Println("series", lbls.String())
		for _, c := range chks {
			chk := c.Chunk
			if chk == nil {
				chk, err = cr.Chunk(c)
				if err != nil {
					panic(err)
				}
			}

			bytes := len(chk.Bytes())
			totalBytes += bytes
			samples := chk.NumSamples()
			totalSamples += samples
			ratio := float64(bytes) / float64(samples)

			fmt.Println("chunk", c.Ref,
				"min time:", c.MinTime, timestamp.Time(c.MinTime).UTC().Format(time.RFC3339Nano),
				"max time:", c.MaxTime, timestamp.Time(c.MaxTime).UTC().Format(time.RFC3339Nano),
				"bytes:", bytes, "samples:", samples, "ratio:", fmt.Sprintf("%0.2f", ratio),
			)

			if samples > 1 {
				it := chk.Iterator(nil)
				var val float64
				same := true
				first := true
				for it.Next() && same {
					_, next := it.At()
					if first {
						first = false
						val = next
						continue
					}
					same = next == val
				}
				if same {
					fmt.Println("chunk with same values", val, "chunk_bytes:", bytes, "samples", samples)

					sameBytes += bytes
					chunksWithSameSamples += 1
				}
			}

			totalRatio := float64(totalBytes) / float64(totalSamples)
			fmt.Println("total_chunk_bytes:", totalBytes, "total_chunk_samples:", totalSamples, "ratio:", fmt.Sprintf("%0.2f", totalRatio))
			fmt.Println("same_bytes:", sameBytes, "chunks_with_same_samples:", chunksWithSameSamples, "ratio of same bytes", float64(sameBytes)/float64(totalBytes), "ratio of bytes per chunk with same samples", float64(sameBytes)/float64(chunksWithSameSamples))
		}
	}

	if p.Err() != nil {
		level.Error(logger).Log("msg", "error iterating postings", "err", p.Err())
		return
	}
}
