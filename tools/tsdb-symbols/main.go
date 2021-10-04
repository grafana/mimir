package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	gokitlog "github.com/go-kit/log"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
)

func main() {
	shards := 0

	flag.IntVar(&shards, "shard-count", 0, "number of shards")
	flag.Parse()

	if flag.NArg() == 0 {
		fmt.Println("no block directory specified")
		return
	}

	uniqueSymbols := map[string]struct{}{}
	var uniqueSymbolsPerShard []map[string]struct{}
	if shards > 1 {
		uniqueSymbolsPerShard = make([]map[string]struct{}, shards)

		for ix := 0; ix < len(uniqueSymbolsPerShard); ix++ {
			uniqueSymbolsPerShard[ix] = make(map[string]struct{})
		}
	}

	for _, blockDir := range flag.Args() {
		err := analyseSymbols(blockDir, uniqueSymbols, uniqueSymbolsPerShard)
		if err != nil {
			log.Fatalln("failed to analyse symbols for", blockDir, "due to error:", err)
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
}

func analyseSymbols(blockDir string, uniqueSymbols map[string]struct{}, uniqueSymbolsPerShard []map[string]struct{}) error {
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
	p, err := idx.Postings(k, v)

	if err != nil {
		return fmt.Errorf("failed to get postings: %v", err)
	}

	shards := len(uniqueSymbolsPerShard)

	uniqueSymbolsPerBlock := map[string]struct{}{}
	for p.Next() {
		lbls := labels.Labels(nil)
		err := idx.Series(p.At(), &lbls, nil)
		if err != nil {
			return fmt.Errorf("error getting series seriesID=%d: %v", p.At(), err)
		}

		shardID := uint64(0)
		if shards > 0 {
			shardID = lbls.Hash() % uint64(shards)
		}

		for _, l := range lbls {
			uniqueSymbols[l.Name] = struct{}{}
			uniqueSymbols[l.Value] = struct{}{}

			uniqueSymbolsPerBlock[l.Name] = struct{}{}
			uniqueSymbolsPerBlock[l.Value] = struct{}{}

			if shards > 0 {
				uniqueSymbolsPerShard[shardID][l.Name] = struct{}{}
				uniqueSymbolsPerShard[shardID][l.Value] = struct{}{}
			}
		}
	}

	if p.Err() != nil {
		return fmt.Errorf("error iterating postings: %v", err)
	}

	fmt.Printf("%s: found %d unique symbols from series in the block\n", block.Meta().ULID.String(), len(uniqueSymbolsPerBlock))
	return nil
}

// https://github.com/prometheus/prometheus/blob/release-2.30/tsdb/docs/format/index.md
// Symbols table is at the beginning of index, right after magic header and version.
// It starts with length (4bytes) and number of symbols (4bytes), which is what we return.
func readSymbolsTableSizeAndSymbolsCount(indexFile string) (symbolTableSize, symbolsCount uint32, _ error) {
	file, err := os.Open(indexFile)
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

	// Check version. We support V1 and V2, both have symbol table at the beggining, and it uses the same format.
	if header[4] != 0x01 && header[4] != 0x02 {
		return 0, 0, fmt.Errorf("invalid index version: 0x%02x", header[4])
	}

	header = header[5:]
	symbolTableSize = binary.BigEndian.Uint32(header)
	symbolsCount = binary.BigEndian.Uint32(header[4:])
	return symbolTableSize, symbolsCount, nil
}
