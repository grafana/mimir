package storage

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/util/annotations"
)

// TODO This file will be removed once https://github.com/prometheus/prometheus/pull/18293/changes#diff-168e8306e8ccb157b33ea81d4c73f550c64539e3045eca3b06c59f3d9cbadd9d is merged and vendored

// SearchResult represents a single search result with its relevance score.
type SearchResult struct {
	// Value is the label name or label value.
	Value string

	// Score represents relevance, with 1.0 being a perfect match.
	// Score range is [0.0, 1.0].
	Score float64
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
	Compare(a, b SearchResult) int
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

	// CompareFunc is used for ordering results.
	// A nil value means NO sort ordering is applied.
	CompareFunc Comparator
}

// SearchResultSet is an iterator over search results.
// Callers must call Close when done, regardless of whether all results were consumed.
type SearchResultSet interface {
	// Next advances the iterator. Returns false when exhausted or on error.
	Next() bool
	// At returns the current search result. Must only be called after a successful Next.
	At() SearchResult
	// Warnings returns any warnings accumulated during iteration.
	Warnings() annotations.Annotations
	// Err returns any error that caused iteration to stop.
	Err() error
	// Close releases resources associated with this result set.
	Close() error
}

// Searcher provides search capabilities with relevance scoring.
// This interface is designed for autocomplete and search UIs that need
// to rank results by relevance rather than just filter them.
type Searcher interface {
	// SearchLabelNames returns an iterator over label names matching the search criteria.
	// Results include relevance scores based on the Filter.
	// The caller must call Close on the returned SearchResultSet when done.
	SearchLabelNames(ctx context.Context, hints *SearchHints, matchers ...*labels.Matcher) SearchResultSet

	// SearchLabelValues returns an iterator over label values for the given label name.
	// Results include relevance scores based on the Filter.
	// The caller must call Close on the returned SearchResultSet when done.
	SearchLabelValues(ctx context.Context, name string, hints *SearchHints, matchers ...*labels.Matcher) SearchResultSet
}

type MimirSearchHints struct {
	Search          []string
	CaseInsensitive bool
	FuzzAlg         string
	FuzzThreshold   float64
	SortBy          SortBy
	SortOrder       SortDirection
	Limit           int
}

func (h *MimirSearchHints) Comparator() Comparator {
	if h == nil {
		return nil
	}
	return NewComparator(h.SortBy, h.SortOrder)
}

type MimirSearcher interface {
	SearchLabelNames(ctx context.Context, hints *MimirSearchHints, matchers ...*labels.Matcher) (SearchResultSet, annotations.Annotations)
	SearchLabelValues(ctx context.Context, name string, hints *MimirSearchHints, matchers ...*labels.Matcher) (SearchResultSet, annotations.Annotations)
}

// ErrorSearchResultSet returns a SearchResultSet that immediately reports err via Err().
func ErrorSearchResultSet(err error) SearchResultSet {
	return &errorSearchResultSet{err: err}
}

type errorSearchResultSet struct {
	err error
}

func (e *errorSearchResultSet) Next() bool                        { return false }
func (e *errorSearchResultSet) At() SearchResult                  { return SearchResult{} }
func (e *errorSearchResultSet) Warnings() annotations.Annotations { return nil }
func (e *errorSearchResultSet) Err() error                        { return e.err }
func (e *errorSearchResultSet) Close() error                      { return nil }
