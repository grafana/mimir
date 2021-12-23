package engine

import (
	"bytes"
	"context"
	"math"

	"github.com/weaveworks/common/tracing"
	"golang.org/x/sync/semaphore"

	"github.com/grafana/mimir/pkg/tenant"
	"github.com/grafana/mimir/pkg/util/activitytracker"
)

// queryTracker implements promql.QueryTracker interface and can be used by PromQL engine for tracking queries.
type queryTracker struct {
	maxConcurrent int
	sema          *semaphore.Weighted
	tracker       *activitytracker.ActivityTracker
}

// newQueryTracker creates new queryTracker for use by PromQL engine. Activity tracker may be nil.
func newQueryTracker(maxConcurrent int, tracker *activitytracker.ActivityTracker) *queryTracker {
	semaIniValue := maxConcurrent
	if semaIniValue <= 0 {
		semaIniValue = math.MaxInt
	}

	return &queryTracker{
		maxConcurrent: maxConcurrent,
		sema:          semaphore.NewWeighted(int64(semaIniValue)),
		tracker:       tracker,
	}
}

func (q *queryTracker) GetMaxConcurrent() int {
	return q.maxConcurrent
}

func (q *queryTracker) Insert(ctx context.Context, query string) (int, error) {
	err := q.sema.Acquire(ctx, 1)
	if err != nil {
		return -1, err
	}

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
	q.sema.Release(1)
	q.tracker.Delete(insertIndex)
}
