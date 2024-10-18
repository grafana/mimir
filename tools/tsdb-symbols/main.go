// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	gokitlog "github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util"
)

func main() {
	// Clean up all flags registered via init() methods of 3rd-party libraries.
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	shards := 0
	flag.IntVar(&shards, "shard-count", 0, "number of shards")

	// Parse CLI arguments.
	args, err := flagext.ParseFlagsAndArguments(flag.CommandLine)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	if len(args) == 0 {
		fmt.Println("no block directory specified")
		return
	}

	startTime := time.Now()

	uniqueSymbols := map[string]struct{}{}
	var uniqueSymbolsPerShard []map[string]struct{}
	if shards > 1 {
		uniqueSymbolsPerShard = make([]map[string]struct{}, shards)

		for ix := 0; ix < len(uniqueSymbolsPerShard); ix++ {
			uniqueSymbolsPerShard[ix] = make(map[string]struct{})
		}
	}

	ctx := context.Background()
	for _, blockDir := range args {
		err := analyseSymbols(ctx, blockDir, uniqueSymbols, uniqueSymbolsPerShard)
		if err != nil {
			log.Println("failed to analyse symbols for", blockDir, "due to error:", err)
		}
		fmt.Println()
	}

	uniqueSymbolsCount := len(uniqueSymbols)
	uniqueSymbolsLength := int64(0)

	for k := range uniqueSymbols {
		uniqueSymbolsLength += int64(len(k))
	}
	fmt.Println("Found", len(uniqueSymbols), "unique symbols from series across ALL blocks, with total length", uniqueSymbolsLength, "bytes")

	for ix := range uniqueSymbolsPerShard {
		shardSymbolsLength := int64(0)
		for k := range uniqueSymbolsPerShard[ix] {
			shardSymbolsLength += int64(len(k))
		}

		fmt.Printf("Shard %d: Found %d unique symbols from series in the shard (%0.4g %%), length of symbols in the shard: %d bytes (%0.4g %%)\n",
			ix,
			len(uniqueSymbolsPerShard[ix]),
			(float64(len(uniqueSymbolsPerShard[ix]))/float64(uniqueSymbolsCount))*100.0,
			shardSymbolsLength,
			(float64(shardSymbolsLength)/float64(uniqueSymbolsLength))*100.0)
	}

	fmt.Println()
	fmt.Println("Analysis complete in", time.Since(startTime))
}

func analyseSymbols(ctx context.Context, blockDir string, uniqueSymbols map[string]struct{}, uniqueSymbolsPerShard []map[string]struct{}) error {
	block, err := tsdb.OpenBlock(gokitlog.NewLogfmtLogger(os.Stderr), blockDir, nil)
	if err != nil {
		return fmt.Errorf("failed to open block: %v", err)
	}
	defer block.Close()

	idx, err := block.Index()
	if err != nil {
		return fmt.Errorf("failed to open block index: %v", err)
	}
	defer idx.Close()

	fmt.Printf("%s: mint=%d (%v), maxt=%d (%v), duration: %v\n", block.Meta().ULID.String(),
		block.MinTime(), util.TimeFromMillis(block.MinTime()).UTC().Format(time.RFC3339),
		block.MaxTime(), util.TimeFromMillis(block.MaxTime()).UTC().Format(time.RFC3339),
		util.TimeFromMillis(block.MaxTime()).Sub(util.TimeFromMillis(block.MinTime())))

	if thanosMeta, err := readMetadata(blockDir); err == nil {
		fmt.Printf("%s: %v\n", block.Meta().ULID.String(), labels.FromMap(thanosMeta.Thanos.Labels))
	}

	symbolsTableSizeFromFile, symbolsCountFromFile, err := readSymbolsTableSizeAndSymbolsCount(filepath.Join(blockDir, "index"))
	if err != nil {
		fmt.Printf("%s: failed to read symbols table size and symbols count from index: %v\n", block.Meta().ULID.String(), err)
	} else {
		fmt.Printf("%s: index: symbol table size: %d bytes, symbols: %d\n", block.Meta().ULID.String(), symbolsTableSizeFromFile, symbolsCountFromFile)
	}

	{
		count := 0
		length := 0
		si := idx.Symbols()
		for si.Next() {
			count++
			length += len(si.At())
		}
		if si.Err() != nil {
			return fmt.Errorf("error iterating symbols: %v", err)
		}

		fmt.Printf("%s: symbols iteration: total length of symbols: %d bytes, symbols: %d\n", block.Meta().ULID.String(), length, count)
		if symbolsTableSizeFromFile > 0 {
			fmt.Printf("%s: index structure overhead: %d bytes\n", block.Meta().ULID.String(), int64(symbolsTableSizeFromFile)-int64(length))
		}
	}

	k, v := index.AllPostingsKey()
	p, err := idx.Postings(ctx, k, v)

	if err != nil {
		return fmt.Errorf("failed to get postings: %v", err)
	}

	shards := len(uniqueSymbolsPerShard)

	var builder labels.ScratchBuilder
	uniqueSymbolsPerBlock := map[string]struct{}{}
	for p.Next() {
		err := idx.Series(p.At(), &builder, nil)
		if err != nil {
			return fmt.Errorf("error getting series seriesID=%d: %v", p.At(), err)
		}

		lbls := builder.Labels()
		shardID := uint64(0)
		if shards > 0 {
			shardID = labels.StableHash(lbls) % uint64(shards)
		}

		lbls.Range(func(l labels.Label) {
			uniqueSymbols[l.Name] = struct{}{}
			uniqueSymbols[l.Value] = struct{}{}

			uniqueSymbolsPerBlock[l.Name] = struct{}{}
			uniqueSymbolsPerBlock[l.Value] = struct{}{}

			if shards > 0 {
				uniqueSymbolsPerShard[shardID][l.Name] = struct{}{}
				uniqueSymbolsPerShard[shardID][l.Value] = struct{}{}
			}
		})
	}

	if p.Err() != nil {
		return fmt.Errorf("error iterating postings: %v", err)
	}

	fmt.Printf("%s: found %d unique symbols from series in the block\n", block.Meta().ULID.String(), len(uniqueSymbolsPerBlock))
	return nil
}

func readMetadata(dir string) (*block.Meta, error) {
	f, err := os.Open(filepath.Join(dir, "meta.json")) //#nosec G109 -- this is intentionally taking operator input, not an injection.

	if err != nil {
		return nil, err
	}

	// this also closes reader
	return block.ReadMeta(f)
}

// https://github.com/prometheus/prometheus/blob/release-2.30/tsdb/docs/format/index.md
// Symbols table is at the beginning of index, right after magic header and version.
// It starts with length (4bytes) and number of symbols (4bytes), which is what we return.
func readSymbolsTableSizeAndSymbolsCount(indexFile string) (symbolTableSize, symbolsCount uint32, _ error) {
	file, err := os.Open(indexFile) // #nosec G109 -- this is intentionally taking operator input, not an injection.

	if err != nil {
		return 0, 0, err
	}

	defer file.Close()

	header := make([]byte, 4+1+4+4) // 4 for magic header, 1 for version, 4 for length, 4 for number of symbols.
	_, err = io.ReadFull(file, header)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to read header: %w", err)
	}

	if binary.BigEndian.Uint32(header) != index.MagicIndex {
		return 0, 0, fmt.Errorf("file doesn't start with magic prefix")
	}

	// Check version. We support V1 and V2, both have symbol table at the beginning, and it uses the same format.
	if header[4] != 0x01 && header[4] != 0x02 {
		return 0, 0, fmt.Errorf("invalid index version: 0x%02x", header[4])
	}

	header = header[5:]
	symbolTableSize = binary.BigEndian.Uint32(header)
	symbolsCount = binary.BigEndian.Uint32(header[4:])
	return symbolTableSize, symbolsCount, nil
}
