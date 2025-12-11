// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import "context"

// Statistics provides cardinality information about a TSDB component for query planning.
type Statistics interface {
	// TotalSeries returns the number of series in the TSDB component.
	TotalSeries() uint64

	// LabelValuesCount returns the number of values for a label name. If the given label name does not exist,
	// it is valid to return 0.
	LabelValuesCount(ctx context.Context, name string) uint64

	// LabelValuesCardinality returns the cardinality of a given label name (i.e., the number of series which
	// contain that label name). If values are provided, it returns the combined cardinality of all given values;
	// otherwise, it returns the total cardinality across all values for the label name. If the label name does not exist,
	// it returns 0.
	LabelValuesCardinality(ctx context.Context, name string, values ...string) uint64
}
