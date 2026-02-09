// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"

	"github.com/grafana/mimir/pkg/storage/indexheader"
	streamindex "github.com/grafana/mimir/pkg/storage/indexheader/index"
)

// indexHeaderAnalyzer adapts *indexheader.StreamBinaryReader to IndexAnalyzer.
type indexHeaderAnalyzer struct {
	reader *indexheader.StreamBinaryReader
}

// newIndexHeaderAnalyzer creates an IndexAnalyzer from a StreamBinaryReader.
func newIndexHeaderAnalyzer(reader *indexheader.StreamBinaryReader) IndexAnalyzer {
	return &indexHeaderAnalyzer{reader: reader}
}

func (a *indexHeaderAnalyzer) Close() error {
	return a.reader.Close()
}

func (a *indexHeaderAnalyzer) IndexVersion(ctx context.Context) (int, error) {
	return a.reader.IndexVersion(ctx)
}

func (a *indexHeaderAnalyzer) SymbolsIterator(ctx context.Context) (SymbolIterator, error) {
	sr, err := a.reader.SymbolsReader(ctx)
	if err != nil {
		return nil, err
	}
	return &indexHeaderSymbolIterator{reader: sr, idx: 0}, nil
}

func (a *indexHeaderAnalyzer) LabelNames(ctx context.Context) ([]string, error) {
	return a.reader.LabelNames(ctx)
}

func (a *indexHeaderAnalyzer) LabelValues(ctx context.Context, name string) ([]string, error) {
	offsets, err := a.reader.LabelValuesOffsets(ctx, name, "", nil)
	if err != nil {
		return nil, err
	}
	values := make([]string, len(offsets))
	for i, o := range offsets {
		values[i] = o.LabelValue
	}
	return values, nil
}

func (a *indexHeaderAnalyzer) SeriesWithLabel(_ context.Context, _ string) SeriesIterator {
	// Not supported for index-headers because they don't contain the series data
	// needed to iterate series (only postings offsets, not actual postings).
	return nil
}

// TOC returns the table of contents for index-header specific analysis.
func (a *indexHeaderAnalyzer) TOC() *indexheader.BinaryTOC {
	return a.reader.TOC()
}

// IndexHeaderVersion returns the index-header version.
func (a *indexHeaderAnalyzer) IndexHeaderVersion() int {
	return a.reader.IndexHeaderVersion()
}

// indexHeaderSymbolIterator adapts SymbolsReader to SymbolIterator.
type indexHeaderSymbolIterator struct {
	reader  streamindex.SymbolsReader
	idx     uint32
	current string
	err     error
	done    bool
}

func (it *indexHeaderSymbolIterator) Close() error {
	return it.reader.Close()
}

func (it *indexHeaderSymbolIterator) Next() bool {
	if it.done || it.err != nil {
		return false
	}

	sym, err := it.reader.Read(it.idx)
	if err != nil {
		// End of symbols or error.
		it.done = true
		return false
	}
	it.current = sym
	it.idx++
	return true
}

func (it *indexHeaderSymbolIterator) At() string {
	return it.current
}

func (it *indexHeaderSymbolIterator) Err() error {
	return it.err
}
