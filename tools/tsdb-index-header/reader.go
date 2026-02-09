// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
)

// IndexAnalyzer is the common interface for analyzing TSDB indexes.
// It abstracts over both full index files and index-header files.
type IndexAnalyzer interface {
	Close() error
	IndexVersion(ctx context.Context) (int, error)
	SymbolsIterator(ctx context.Context) (SymbolIterator, error)
	LabelNames(ctx context.Context) ([]string, error)
	LabelValues(ctx context.Context, name string) ([]string, error)
	// SeriesWithLabel returns an iterator over series that have the given label name.
	// Returns nil if not supported (e.g., index-header format).
	SeriesWithLabel(ctx context.Context, labelName string) SeriesIterator
}

// SymbolIterator iterates through all symbols in the index.
type SymbolIterator interface {
	Close() error
	Next() bool
	At() string
	Err() error
}

// SeriesIterator iterates over series and their labels.
type SeriesIterator interface {
	Next() bool
	// Labels returns the labels for the current series.
	Labels() labels.Labels
	Err() error
}

// IndexInfo holds information about the index being analyzed.
type IndexInfo struct {
	Path               string
	Size               int64
	IsIndexHeader      bool
	IndexVersion       int
	IndexHeaderVersion int    // Only for index-header
	SymbolsSize        uint64 // Only for index-header
	PostingsTableSize  uint64 // Only for index-header
}
