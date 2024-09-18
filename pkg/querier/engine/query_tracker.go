// SPDX-License-Identifier: AGPL-3.0-only

package engine

import (
	"bytes"
	"context"

	"github.com/grafana/dskit/tenant" //lint:ignore faillint queryTracker needs tenant package
	"github.com/grafana/dskit/tracing"

	"github.com/grafana/mimir/pkg/util/activitytracker" //lint:ignore faillint queryTracker needs activitytracker
)

// queryTracker implements promql.QueryTracker interface and can be used by PromQL engine for tracking queries.
type queryTracker struct {
	tracker *activitytracker.ActivityTracker
}

// newQueryTracker creates new queryTracker for use by PromQL engine. Activity tracker may be nil.
func newQueryTracker(tracker *activitytracker.ActivityTracker) *queryTracker {

	return &queryTracker{
		tracker: tracker,
	}
}

func (q *queryTracker) GetMaxConcurrent() int {
	// No limit.
	return -1
}

func (q *queryTracker) Insert(ctx context.Context, query string) (int, error) {
	id := q.tracker.Insert(func() string {
		return generateActivityDescription(ctx, query)
	})

	return id, nil
}

func generateActivityDescription(ctx context.Context, query string) string {
	buf := bytes.Buffer{}
	traceID, _ := tracing.ExtractSampledTraceID(ctx)

	sep := ""
	if traceID != "" {
		buf.WriteString("traceID=")
		buf.WriteString(traceID)
		sep = " "
	}

	tenantID, _ := tenant.TenantID(ctx)
	if tenantID != "" {
		buf.WriteString(sep)
		buf.WriteString("tenant=")
		buf.WriteString(tenantID)
		sep = " "
	}

	buf.WriteString(sep)
	buf.WriteString("query=")
	buf.WriteString(query)

	return buf.String()
}

func (q queryTracker) Delete(insertIndex int) {
	q.tracker.Delete(insertIndex)
}

func (q *queryTracker) Close() error {
	return q.tracker.Close()
}
