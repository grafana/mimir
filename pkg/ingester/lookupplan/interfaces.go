// SPDX-License-Identifier: AGPL-3.0-only
// interfaces.go contains interfaces that are meant to be deleted once they're upstreamed to prometheus and moved to mimir-prometheus

package lookupplan

import (
	"context"
)

type Statistics interface {
	TotalSeries() uint64
	// LabelValuesCount should return 0 if the label doesn't exist
	LabelValuesCount(ctx context.Context, name string) (uint64, error)

	// LabelValuesCardinality should return 0 if the label doesn't exist
	LabelValuesCardinality(ctx context.Context, name string, values ...string) (uint64, error)
}
