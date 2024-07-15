// SPDX-License-Identifier: AGPL-3.0-only

package activeseries

import (
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/index"
)

type BucketCountPostings interface {
	index.Postings
	// AtBucketCount returns the series reference currently pointed to and its bucket count (if it's a native histogram series).
	AtBucketCount() (storage.SeriesRef, int)
}

// NativeHistogramPostings is a wrapper around ActiveSeries and index.Postings.
// Similar to Postings, but filters its output to native histogram series.
// Implements index.Postings interface and returns only series references that
// are active in ActiveSeries and are native histograms.
// It is only valid to use NativeHistogramPostings if the postings are from the
// open TSDB head. It is not valid to use NativeHistogramPostings if the
// postings are from a block.
type NativeHistogramPostings struct {
	activeSeries       *ActiveSeries
	postings           index.Postings
	currentBucketCount int
}

func NewNativeHistogramPostings(activeSeries *ActiveSeries, postings index.Postings) *NativeHistogramPostings {
	return &NativeHistogramPostings{
		activeSeries: activeSeries,
		postings:     postings,
	}
}

// Type check.
var _ index.Postings = &NativeHistogramPostings{}
var _ BucketCountPostings = &NativeHistogramPostings{}

// At implements index.Postings.
func (a *NativeHistogramPostings) At() storage.SeriesRef {
	return a.postings.At()
}

// AtBucketCount returns the current bucket count for the series reference at the current position.
func (a *NativeHistogramPostings) AtBucketCount() (storage.SeriesRef, int) {
	return a.postings.At(), a.currentBucketCount
}

// Err implements index.Postings.
func (a *NativeHistogramPostings) Err() error {
	return a.postings.Err()
}

// Next implements index.Postings.
func (a *NativeHistogramPostings) Next() bool {
	for a.postings.Next() {
		if count, ok := a.activeSeries.NativeHistogramBuckets(a.postings.At()); ok {
			a.currentBucketCount = count
			return true
		}
	}
	return false
}

// Seek implements index.Postings.
func (a *NativeHistogramPostings) Seek(v storage.SeriesRef) bool {
	// Seek in the underlying postings.
	// If the underlying postings don't contain a value, return false.
	if !a.postings.Seek(v) {
		return false
	}

	// If the underlying postings contain a value, check if it's active.
	if count, ok := a.activeSeries.NativeHistogramBuckets(a.postings.At()); ok {
		a.currentBucketCount = count
		return true
	}

	// If the underlying postings contain a value, but it's not active,
	// seek to the next active value.
	return a.Next()
}
