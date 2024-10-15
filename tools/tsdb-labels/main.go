// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	"github.com/prometheus/prometheus/tsdb/index"
)

func main() {
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
	for _, blockDir := range args {
		err = printLabelValues(blockDir)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
	}
}

func printLabelValues(blockDir string) error {
	indexPath := filepath.Join(blockDir, "index")
	f, err := fileutil.OpenMmapFile(indexPath)
	if err != nil {
		return fmt.Errorf("opening block index %s: %w", indexPath, err)
	}

	indexBytes := realByteSlice(f.Bytes())
	toc, err := index.NewTOCFromByteSlice(indexBytes)
	if err != nil {
		return fmt.Errorf("calculating TOC %s: %w", indexPath, err)
	}

	symbols, err := index.NewSymbols(indexBytes, index.FormatV2, int(toc.Symbols))
	if err != nil {
		return fmt.Errorf("opening symbols table: %w", err)
	}

	labelOffsets, err := readLabelsOffsets(indexBytes, int(toc.LabelIndicesTable))
	if err != nil {
		return fmt.Errorf("reading labels offsets: %w", err)
	}

	var values []string
	for _, label := range labelOffsets {
		values = values[:0]
		valuesRefs, err := readLabelValuesRefs(indexBytes, int(label.valuesOffset))
		if err != nil {
			return fmt.Errorf("reading label values symbols refs for label %q: %w", label.labelName, err)
		}
		for _, ref := range valuesRefs {
			val, err := symbols.Lookup(ref)
			if err != nil {
				return fmt.Errorf("looking up symbol: %w", err)
			}
			values = append(values, val)
		}

		fmt.Println(label.labelName, len(values), values)
	}
	return nil
}

type realByteSlice []byte

func (r realByteSlice) Len() int { return len(r) }

func (r realByteSlice) Range(start, end int) []byte { return r[start:end] }

type labelIndexOffset struct {
	labelName    string
	valuesOffset uint64 // valuesOffset is an offset relative to the start of the index file
}

func readLabelsOffsets(b index.ByteSlice, offset int) ([]labelIndexOffset, error) {
	d := encoding.NewDecbufAt(b, offset, nil)
	numEntries := d.Be32int()

	offsets := make([]labelIndexOffset, numEntries)
	for i := range offsets {
		if b := d.Byte(); b != 1 {
			return nil, fmt.Errorf("parsing labels offsets for label #%d, expecting n = 1, got n = %d", i+1, b)
		}
		offsets[i].labelName = d.UvarintStr()
		offsets[i].valuesOffset = d.Uvarint64()
	}
	return offsets, nil
}

// readLabelValuesRefs reads a slice of
func readLabelValuesRefs(b realByteSlice, labelIndexOffset int) ([]uint32, error) {
	d := encoding.NewDecbufAt(b, labelIndexOffset, nil)
	if numLNames := d.Be32int(); numLNames != 1 {
		return nil, fmt.Errorf("got unexpected number of label values; expected 1, got %d", numLNames)
	}
	numEntries := d.Be32int()
	refs := make([]uint32, numEntries)
	for i := range refs {
		refs[i] = d.Be32()
	}
	return refs, nil
}
