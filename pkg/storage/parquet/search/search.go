// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus-community/parquet-common/blob/306c8486441da41d2a655fa29d0e83820437cf23/search/search.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package search

import (
	"github.com/parquet-go/parquet-go"
)

type Constraint interface {
	// filter returns a set of non-overlapping increasing row indexes that may satisfy the constraint.
	filter(rg parquet.RowGroup, primary bool, rr []rowRange) ([]rowRange, error)
	// init initializes the constraint with respect to the file schema and projections.
	init(s *parquet.Schema) error
	// path is the path for the column that is constrained
	path() string
}
