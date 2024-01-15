// SPDX-License-Identifier: AGPL-3.0-only

package activeseries

import (
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/index"
)

// Postings is a wrapper around ActiveSeries and index.Postings. It
// implements index.Postings interface and returns only series references that
// are active in ActiveSeries. It is only valid to use Postings if
// the postings are from the open TSDB head. It is not valid to use
// Postings if the postings are from a block.
type Postings struct {
	activeSeries *ActiveSeries
	postings     index.Postings
}

func NewPostings(activeSeries *ActiveSeries, postings index.Postings) *Postings {
	return &Postings{
		activeSeries: activeSeries,
		postings:     postings,
	}
}

var _ index.Postings = &Postings{}

// At implements index.Postings.
func (a *Postings) At() storage.SeriesRef {
	return a.postings.At()
}

// Err implements index.Postings.
func (a *Postings) Err() error {
	return a.postings.Err()
}

// Next implements index.Postings.
func (a *Postings) Next() bool {
	for a.postings.Next() {
		if a.activeSeries.ContainsRef(a.postings.At()) {
			return true
		}
	}
	return false
}

// Seek implements index.Postings.
func (a *Postings) Seek(v storage.SeriesRef) bool {
	// Seek in the underlying postings.
	// If the underlying postings don't contain a value, return false.
	if !a.postings.Seek(v) {
		return false
	}

	// If the underlying postings contain a value, check if it's active.
	if a.activeSeries.ContainsRef(a.postings.At()) {
		return true
	}

	// If the underlying postings contain a value, but it's not active,
	// seek to the next active value.
	return a.Next()
}

func (a *Postings) Reset() {
	a.postings.Reset()
}
