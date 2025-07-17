// SPDX-License-Identifier: AGPL-3.0-only
// Package lookupplan contains interfaces that are meant to be deleted once they're upstreamed to prometheus and moved to mimir-prometheus

package lookupplan

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
)

// LookupPlan represents the decision of which matchers to apply during
// index lookup versus during series scanning.
type LookupPlan interface {
	// ScanMatchers returns matchers that should be applied during series scanning
	ScanMatchers() []*labels.Matcher
	// IndexMatchers returns matchers that should be applied during index lookup
	IndexMatchers() []*labels.Matcher
}

// LookupPlanner plans how to execute index lookups by deciding which matchers
// to apply during index lookup versus after series retrieval.
type LookupPlanner interface {
	PlanIndexLookup(ctx context.Context, plan LookupPlan, minT, maxT int64) (LookupPlan, error)
}

type Statistics interface {
	TotalSeries() int64
	LabelValuesCount(ctx context.Context, name string) (int64, error)
	LabelValuesCardinality(ctx context.Context, name string, values ...string) (int64, error)
}
