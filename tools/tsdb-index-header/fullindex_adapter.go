// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"errors"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/index"
)

// fullIndexAnalyzer adapts *index.Reader to IndexAnalyzer.
type fullIndexAnalyzer struct {
	reader *index.Reader
}

// newFullIndexAnalyzer creates an IndexAnalyzer from a full index.Reader.
func newFullIndexAnalyzer(reader *index.Reader) IndexAnalyzer {
	return &fullIndexAnalyzer{reader: reader}
}

func (a *fullIndexAnalyzer) Close() error {
	return a.reader.Close()
}

func (a *fullIndexAnalyzer) IndexVersion(_ context.Context) (int, error) {
	return a.reader.Version(), nil
}

func (a *fullIndexAnalyzer) SymbolsIterator(_ context.Context) (SymbolIterator, error) {
	return &stringIterAdapter{iter: a.reader.Symbols()}, nil
}

func (a *fullIndexAnalyzer) LabelNames(ctx context.Context) ([]string, error) {
	return a.reader.LabelNames(ctx)
}

func (a *fullIndexAnalyzer) LabelValues(ctx context.Context, name string) ([]string, error) {
	return a.reader.LabelValues(ctx, name, nil)
}

func (a *fullIndexAnalyzer) SeriesWithLabel(ctx context.Context, labelName string) SeriesIterator {
	postings := a.reader.PostingsForAllLabelValues(ctx, labelName)
	return &fullIndexSeriesIterator{
		reader:   a.reader,
		postings: postings,
		builder:  labels.NewScratchBuilder(10),
	}
}

// fullIndexSeriesIterator iterates over series using postings from a full index.
type fullIndexSeriesIterator struct {
	reader   *index.Reader
	postings index.Postings
	builder  labels.ScratchBuilder
	current  labels.Labels
	err      error
}

func (it *fullIndexSeriesIterator) Next() bool {
	for it.postings.Next() {
		ref := it.postings.At()
		if err := it.reader.Series(ref, &it.builder, nil); err != nil {
			// Skip stale postings (series that no longer exist).
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			it.err = err
			return false
		}
		it.current = it.builder.Labels()
		return true
	}
	it.err = it.postings.Err()
	return false
}

func (it *fullIndexSeriesIterator) Labels() labels.Labels {
	return it.current
}

func (it *fullIndexSeriesIterator) Err() error {
	return it.err
}

// stringIterAdapter adapts index.StringIter to SymbolIterator.
type stringIterAdapter struct {
	iter index.StringIter
}

func (a *stringIterAdapter) Close() error {
	// StringIter doesn't have a Close method, no-op.
	return nil
}

func (a *stringIterAdapter) Next() bool {
	return a.iter.Next()
}

func (a *stringIterAdapter) At() string {
	return a.iter.At()
}

func (a *stringIterAdapter) Err() error {
	return a.iter.Err()
}
