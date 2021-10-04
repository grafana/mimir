package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
)

var logger = log.NewLogfmtLogger(os.Stderr)

func main() {
	shards := 0

	flag.IntVar(&shards, "shard-count", 0, "number of shards")
	flag.Parse()

	if flag.NArg() == 0 {
		fmt.Println("no block directory specified")
		return
	}

	for _, blockDir := range flag.Args() {
		analyseSymbols(blockDir, shards)
	}
}

// nolint:errcheck
func analyseSymbols(blockDir string, shards int) {
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

	totalSymbolsCount := 0
	totalSymbolsLength := 0
	si := idx.Symbols()
	for si.Next() {
		totalSymbolsCount++
		totalSymbolsLength += len(si.At())
	}
	if si.Err() != nil {
		level.Error(logger).Log("msg", "error iterating symbols", "err", si.Err())
		return
	}

	fmt.Println("Symbols table has", totalSymbolsCount, "symbols, total length", totalSymbolsLength, "bytes")

	k, v := index.AllPostingsKey()
	p, err := idx.Postings(k, v)

	if err != nil {
		level.Error(logger).Log("msg", "failed to get postings", "err", err)
		return
	}

	uniqueSymbolsPerBlock := make(map[string]struct{}, totalSymbolsCount)
	var uniqueSymbolsPerShard []map[string]struct{}
	if shards > 1 {
		uniqueSymbolsPerShard = make([]map[string]struct{}, shards)

		for ix := 0; ix < len(uniqueSymbolsPerShard); ix++ {
			uniqueSymbolsPerShard[ix] = make(map[string]struct{}, totalSymbolsCount/2)
		}
	}

	for p.Next() {
		lbls := labels.Labels(nil)
		err := idx.Series(p.At(), &lbls, nil)
		if err != nil {
			level.Error(logger).Log("msg", "error getting series", "seriesID", p.At(), "err", err)
			continue
		}

		six := uint64(0)
		if len(uniqueSymbolsPerShard) > 0 {
			six = lbls.Hash() % uint64(shards)
		}

		for _, l := range lbls {
			uniqueSymbolsPerBlock[l.Name] = struct{}{}
			uniqueSymbolsPerBlock[l.Value] = struct{}{}

			if len(uniqueSymbolsPerShard) > 0 {
				uniqueSymbolsPerShard[six][l.Name] = struct{}{}
				uniqueSymbolsPerShard[six][l.Value] = struct{}{}
			}
		}
	}

	if p.Err() != nil {
		level.Error(logger).Log("msg", "error iterating postings", "err", p.Err())
		return
	}

	fmt.Println("Found", len(uniqueSymbolsPerBlock), "unique symbols from series in the block")

	for ix := range uniqueSymbolsPerShard {
		fmt.Printf("Found %d unique symbols from series in the shard %d (%0.4g %%)\n",
			len(uniqueSymbolsPerShard[ix]), ix, (float64(len(uniqueSymbolsPerShard[ix]))/float64(totalSymbolsCount))*100.0)
	}
}
