// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/ingester/client"
	mimirstorage "github.com/grafana/mimir/pkg/storage"
)

// ingesterSearcherValueSet is a thin adapter over the shared storage.SearchValueSet.
// It translates the ingester-specific SearchLabelValuesFilter into the plain-int sort
// parameters accepted by the shared type.
type ingesterSearcherValueSet = mimirstorage.SearchValueSet

// newingesterSearcherValueSet starts a producer goroutine and returns an ingesterSearcherValueSet.
// produce writes SearchResult values to ch; ch is closed after produce returns (warnings and err set first).
// limitExceededWarning, if non-nil, is emitted as a warning when the count limit is reached.
func newingesterSearcherValueSet(
	produce func(ch chan<- mimirstorage.SearchResult) (annotations.Annotations, error),
	sf *client.SearchLabelValuesFilter,
	limit, maxBytesLimit int,
	limitExceededWarning error,
) *ingesterSearcherValueSet {
	sortBy, sortOrder := 0, 0
	if sf != nil {
		sortBy = int(sf.SortBy)
		sortOrder = int(sf.SortOrder)
	}
	return mimirstorage.NewSearchValueSet(produce, sortBy, sortOrder, limit, maxBytesLimit, limitExceededWarning)
}
