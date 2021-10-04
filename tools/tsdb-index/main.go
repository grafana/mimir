package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
)

var logger = log.NewLogfmtLogger(os.Stderr)

func main() {
	metricSelector := flag.String("select", "", "PromQL metric selector")
	printChunks := flag.Bool("chunks", false, "Print chunk details")
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
		printBlockIndex(blockDir, *printChunks, matchers)
	}
}

func printBlockIndex(blockDir string, printChunks bool, matchers []*labels.Matcher) {
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

		fmt.Println("series", lbls.String())
		if printChunks {
			for _, c := range chks {
				fmt.Println("chunk", c.Ref, "min time:", c.MinTime, "max time:", c.MaxTime)
			}
		}
	}

	if p.Err() != nil {
		level.Error(logger).Log("msg", "error iterating postings", "err", p.Err())
		return
	}
}
