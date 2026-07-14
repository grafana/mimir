// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/propagation"
)

// TestDispatcher_HandleProtobuf_SumOverMixedHistogramSchemas is a regression test for a double-free
// of HPoint slices when a sum() group mixes a float sample with histograms that cannot be added
// together. It evaluates the aggregation subtree through the dispatcher, like the query-frontend
// does, so results are freed while the query is still running and the double-free surfaces as a
// panic in FinishedReading rather than at query close.
//
// See pkg/streamingpromql/testdata/ours/sum_mixed_histogram_schemas.test for the minimal version.
func TestDispatcher_HandleProtobuf_SumOverMixedHistogramSchemas(t *testing.T) {
	storage := promqltest.LoadedStorage(t, `
		load 1m
			some_metric{job="q", pod="p1", route_name="a", status_code="200"} {{schema:0 sum:5 count:4 buckets:[1 2 1]}}x10
			some_metric{job="q", pod="p2", route_name="a", status_code="200"} {{schema:-53 sum:1 count:1 custom_values:[5 10] buckets:[1 0]}} {{schema:-53 sum:2 count:2 custom_values:[5 10] buckets:[2 0]}}
			some_metric{job="q", pod="p3", route_name="a", status_code="200"} 0 1
	`)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })

	opts := streamingpromql.NewTestEngineOpts()
	ctx := context.Background()
	planner, err := streamingpromql.NewQueryPlanner(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	engine, err := streamingpromql.NewEngine(opts, stats.NewQueryMetrics(nil), planner)
	require.NoError(t, err)

	startT := timestamp.Time(0)
	timeRange := types.NewRangeQueryTimeRange(startT, startT.Add(10*time.Minute), time.Minute)
	req := createQueryRequestForSpecificNodes(t, ctx, planner, `sum by (route_name, status_code) (rate({job="q"}[5m]))`, timeRange, true, true, 1, []string{
		"DeduplicateAndMerge",
		"DropName",
		"AggregateExpression: sum by (route_name, status_code)",
	})

	_, ctx = stats.ContextWithEmptyStats(ctx)
	ctx = user.InjectOrgID(ctx, "tenant-1")

	reg, requestMetrics, serverMetrics := newMetrics()
	stream := &mockQueryResultStream{t: t, route: "querierpb.EvaluateQueryRequest", reg: reg}
	dispatcher := NewDispatcher(engine, storage, requestMetrics, serverMetrics, &propagation.NoopExtractor{}, opts.Logger)

	dispatcher.HandleProtobuf(ctx, req, &propagation.MapCarrier{}, stream)

	// HandleProtobuf never returns an error: on failure it writes an error message to the stream
	// and returns normally. Assert the query actually ran to completion so this stays a real
	// regression guard even if the double-free is ever turned into a written error, or the query
	// starts failing before it reaches the aggregation (e.g. the node paths above drift).
	require.NotEmpty(t, stream.messages, "expected the query to stream at least one result message")
	for _, msg := range stream.messages {
		require.Nil(t, msg.GetError(), "query returned an error instead of exercising the aggregation: %v", msg.GetError())
	}
}
