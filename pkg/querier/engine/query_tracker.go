// SPDX-License-Identifier: AGPL-3.0-only

package engine

import (
	"bytes"
	"context"
	"strconv"

	"github.com/grafana/dskit/tenant" //lint:ignore faillint queryTracker needs tenant package
	"github.com/grafana/dskit/tracing"
	"go.opentelemetry.io/otel"

	"github.com/grafana/mimir/pkg/streamingpromql/types" //lint:ignore faillint streamingpromql is fine
	"github.com/grafana/mimir/pkg/util/activitytracker"  //lint:ignore faillint queryTracker needs activitytracker
)

var tracer = otel.Tracer("pkg/querier/engine")

// queryTracker implements promql.QueryTracker and streamingpromql.QueryTracker interfaces and can be used by PromQL engine for tracking queries.
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
		return generateActivityDescription(ctx, query, "", types.QueryTimeRange{}, false)
	})

	return id, nil
}

func (q *queryTracker) InsertWithDetails(ctx context.Context, query string, stage string, timeRange types.QueryTimeRange) (int, error) {
	id := q.tracker.Insert(func() string {
		return generateActivityDescription(ctx, query, stage, timeRange, true)
	})

	return id, nil
}

func generateActivityDescription(ctx context.Context, query string, stage string, timeRange types.QueryTimeRange, includeTimeRange bool) string {
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

	if stage != "" {
		buf.WriteString(sep)
		buf.WriteString("stage=")
		buf.WriteString(stage)
		sep = " "
	}

	if includeTimeRange {
		buf.WriteString(sep)

		if timeRange.IsInstant {
			buf.WriteString("instant_ts_ms=")
			buf.WriteString(strconv.FormatInt(timeRange.StartT, 10))
		} else {
			buf.WriteString("start_ts_ms=")
			buf.WriteString(strconv.FormatInt(timeRange.StartT, 10))
			buf.WriteString(" ")
			buf.WriteString("end_ts_ms=")
			buf.WriteString(strconv.FormatInt(timeRange.EndT, 10))
			buf.WriteString(" ")
			buf.WriteString("interval_ms=")
			buf.WriteString(strconv.FormatInt(timeRange.IntervalMilliseconds, 10))
		}

		sep = " "
	}

	// Put the query last, so that all the other information is still present if the query is long and is truncated during logging.
	buf.WriteString(sep)
	buf.WriteString("query=")
	buf.WriteString(strconv.Quote(query))

	return buf.String()
}

func (q *queryTracker) Delete(insertIndex int) {
	q.tracker.Delete(insertIndex)
}

func (q *queryTracker) Close() error {
	return q.tracker.Close()
}
