// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/efficientgo/core/errors"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	"github.com/prometheus/prometheus/tsdb/index"
)

var logger = log.NewLogfmtLogger(os.Stderr)

func main() {
	// Clean up all flags registered via init() methods of 3rd-party libraries.
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	metricSelector := flag.String("select", "", "PromQL metric selector")
	printChunks := flag.Bool("show-chunks", false, "Print chunk details")
	showLabelValues := flag.Bool("show-label-values", false, "Print label names, their cardinality, and label values. When used, -show-chunks and -select are ignored.")

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
	if *showLabelValues {
		for _, blockDir := range args {
			err = printLabelValues(ctx, blockDir)
			if err != nil {
				fmt.Fprintln(os.Stderr, err.Error())
				os.Exit(1)
			}
		}
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

	for _, blockDir := range args {
		printBlockIndex(ctx, blockDir, *printChunks, matchers)
	}
}

func printLabelValues(ctx context.Context, blockDir string) error {
	f, err := fileutil.OpenMmapFile(filepath.Join(blockDir, "index"))
	if err != nil {
		return errors.Wrapf(err, "opening block %s", blockDir)
	}

	indexBytes := realByteSlice(f.Bytes())
	toc, err := index.NewTOCFromByteSlice(indexBytes)
	if err != nil {
		return errors.Wrapf(err, "calculating TOC %s", blockDir)
	}

	symbols, err := index.NewSymbols(indexBytes, index.FormatV2, int(toc.Symbols))
	if err != nil {
		return errors.Wrap(err, "opening symbols table")
	}

	labelOffsets, err := readLabelsOffsets(indexBytes, int(toc.LabelIndicesTable))
	if err != nil {
		return errors.Wrap(err, "reading labels offsets")
	}

	var values []string
	for _, label := range labelOffsets {
		values = values[:0]
		valuesRefs, err := readLabelValuesRefs(indexBytes, int(label.valuesOffset))
		if err != nil {
			return errors.Wrapf(err, "reading label values symbols refs for label %q", label.labelName)
		}
		for _, ref := range valuesRefs {
			val, err := symbols.Lookup(ctx, ref)
			if err != nil {
				return errors.Wrap(err, "looking up symbol")
			}
			values = append(values, val)
		}

		fmt.Println(label.labelName, len(values), values)
	}
	return nil
}

// readLabelValuesRefs reads a slice of
func readLabelValuesRefs(b realByteSlice, labelIndexOffset int) ([]uint32, error) {
	d := encoding.NewDecbufAt(b, labelIndexOffset, nil)
	if numLNames := d.Be32int(); numLNames != 1 {
		return nil, errors.Newf("got unexpected number of label values; expected 1, got %d", numLNames)
	}
	numEntries := d.Be32int()
	refs := make([]uint32, numEntries)
	for i := range refs {
		refs[i] = d.Be32()
	}
	return refs, nil
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
			return nil, errors.Newf("parsing labels offsets for label #%d, expecting n = 1, got n = %d", i+1, b)
		}
		offsets[i].labelName = d.UvarintStr()
		offsets[i].valuesOffset = d.Uvarint64()
	}
	return offsets, nil
}

func printBlockIndex(ctx context.Context, blockDir string, printChunks bool, matchers []*labels.Matcher) {
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

		fmt.Println("series", lbls.String())
		if printChunks {
			for _, c := range chks {
				fmt.Println("chunk", c.Ref,
					"min time:", c.MinTime, timestamp.Time(c.MinTime).UTC().Format(time.RFC3339Nano),
					"max time:", c.MaxTime, timestamp.Time(c.MaxTime).UTC().Format(time.RFC3339Nano))
			}
		}
	}

	if p.Err() != nil {
		level.Error(logger).Log("msg", "error iterating postings", "err", p.Err())
		return
	}
}
