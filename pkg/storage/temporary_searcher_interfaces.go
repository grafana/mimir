package storage

import (
	"context"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/util/annotations"
)

// FilteredResult is an intermediate result which is created when applying a Filter when searching for label/values.
// A fuzzy match score (if any) is returned so that it can be used by any subsequent comparators.
type FilteredResult struct {
	Value string  // The metric name, label name or label value.
	Score float64 // Relevance score in [0.0, 1.0]. 1.0 is returned if no fuzzy filter was applied in the filtering.
}

// Filter is used to reduce the set of labels/values.
type Filter interface {
	// Accept returns whether the value should be included (accepted).
	// Score is the input from the user indicating the fuzzy score threshold required for a match. 0 requests no fuzzy matching, 100 requests an exact match.
	Accept(value string) (accepted bool, score float64)
}

// Comparator is used for ordering the returned SearchResults.
// The implementation may use the SearchResult.Score as is, or may use this as an input to its own algorithm for sorting.
// For instance, a Filter may have applied a Jaro fuzzy filter and this is returned in the SearchResult.Score. The Comparator implementation may add a prefix boost score
// to the Jaro score, there by applying a Jaro-Winkler sort ordering.
type Comparator interface {
	Compare(a, b FilteredResult) int
}

// SearchHints is the input to the labels/values Searcher. It allows for a filter and comparator function to be passed into the Searcher.
// Note that both the filter and comparator are optional.
type SearchHints struct {
	// Filter determines which values to include and their relevance scores.
	// A nil Filter accepts all values and will return a SearchResult.Score of 1.0
	Filter Filter

	// Limit is the maximum number of results to return.
	// Use 0 to disable limiting.
	Limit int

	// Compare is used for ordering results.
	// A nil value means NO sort ordering is applied.
	Compare Comparator
}

// SearcherValueSet is an iterator returned from the Searcher label/value search functions.
type SearcherValueSet interface {
	Next() bool
	At() FilteredResult
	Warnings() annotations.Annotations
	Err() error
	// Close this iterator and releases its resources. This does not close the Searcher. The iterator should be closed before the Searcher is closed.
	Close()
}

// Searcher allows for the searching, filtering and ordering of label names and values. The result set is accessible via an SearcherValueSet iterator.
type Searcher interface {
	// SearchLabelNames returns label names matching the search criteria.
	// The SearcherValueSet iterator is ordered by any given Comparator.
	SearchLabelNames(ctx context.Context, hints *SearchHints, matchers ...*labels.Matcher) (SearcherValueSet, error)

	// SearchLabelValues returns label values for the given label name.
	// The SearcherValueSet iterator is ordered by any given Comparator.
	SearchLabelValues(ctx context.Context, name string, hints *SearchHints, matchers ...*labels.Matcher) (SearcherValueSet, error)
}
