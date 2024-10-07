// SPDX-License-Identifier: AGPL-3.0-only

package compat

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/stats"
	"github.com/stretchr/testify/require"

	util_test "github.com/grafana/mimir/pkg/util/test"
)

func TestEngineWithFallback(t *testing.T) {
	logger := log.NewNopLogger()

	generators := map[string]func(ctx context.Context, engine promql.QueryEngine, expr string) (promql.Query, error){
		"instant query": func(ctx context.Context, engine promql.QueryEngine, expr string) (promql.Query, error) {
			return engine.NewInstantQuery(ctx, nil, nil, expr, time.Now())
		},
		"range query": func(ctx context.Context, engine promql.QueryEngine, expr string) (promql.Query, error) {
			return engine.NewRangeQuery(ctx, nil, nil, expr, time.Now(), time.Now().Add(-time.Minute), time.Second)
		},
	}

	for name, createQuery := range generators {
		t.Run(name, func(t *testing.T) {
			t.Run("should not fall back for supported expressions", func(t *testing.T) {
				ctx := context.Background()
				reg := prometheus.NewPedanticRegistry()
				preferredEngine := newFakeEngineThatSupportsLimitedQueries()
				fallbackEngine := newFakeEngineThatSupportsAllQueries()
				engineWithFallback := NewEngineWithFallback(preferredEngine, fallbackEngine, reg, logger)

				query, err := createQuery(ctx, engineWithFallback, "a_supported_expression")
				require.NoError(t, err)
				require.Equal(t, preferredEngine.query, query, "should return query from preferred engine")
				require.False(t, fallbackEngine.wasCalled, "should not call fallback engine if expression is supported by preferred engine")

				util_test.AssertGatherAndCompare(t, reg, `
					# HELP cortex_mimir_query_engine_supported_queries_total Total number of queries that were supported by the Mimir query engine.
					# TYPE cortex_mimir_query_engine_supported_queries_total counter
					cortex_mimir_query_engine_supported_queries_total 1
				`, "cortex_mimir_query_engine_supported_queries_total", "cortex_mimir_query_engine_unsupported_queries_total")
			})

			t.Run("should fall back for unsupported expressions", func(t *testing.T) {
				ctx := context.Background()
				reg := prometheus.NewPedanticRegistry()
				preferredEngine := newFakeEngineThatSupportsLimitedQueries()
				fallbackEngine := newFakeEngineThatSupportsAllQueries()
				engineWithFallback := NewEngineWithFallback(preferredEngine, fallbackEngine, reg, logger)

				query, err := createQuery(ctx, engineWithFallback, "a_non_supported_expression")
				require.NoError(t, err)
				require.Equal(t, fallbackEngine.query, query, "should return query from fallback engine if expression is not supported by preferred engine")

				util_test.AssertGatherAndCompare(t, reg, `
					# HELP cortex_mimir_query_engine_supported_queries_total Total number of queries that were supported by the Mimir query engine.
					# TYPE cortex_mimir_query_engine_supported_queries_total counter
					cortex_mimir_query_engine_supported_queries_total 0
					# HELP cortex_mimir_query_engine_unsupported_queries_total Total number of queries that were not supported by the Mimir query engine and so fell back to Prometheus' engine.
					# TYPE cortex_mimir_query_engine_unsupported_queries_total counter
					cortex_mimir_query_engine_unsupported_queries_total{reason="this expression is not supported"} 1
				`, "cortex_mimir_query_engine_supported_queries_total", "cortex_mimir_query_engine_unsupported_queries_total")
			})

			t.Run("should not fall back if creating query fails for another reason", func(t *testing.T) {
				ctx := context.Background()
				reg := prometheus.NewPedanticRegistry()
				preferredEngine := newFakeEngineThatSupportsLimitedQueries()
				fallbackEngine := newFakeEngineThatSupportsAllQueries()
				engineWithFallback := NewEngineWithFallback(preferredEngine, fallbackEngine, reg, logger)

				_, err := createQuery(ctx, engineWithFallback, "an_invalid_expression")
				require.EqualError(t, err, "the query is invalid")
				require.False(t, fallbackEngine.wasCalled, "should not call fallback engine if creating query fails for another reason")
			})

			t.Run("should fall back if falling back has been explicitly requested, even if the expression is supported", func(t *testing.T) {
				ctx := withForceFallbackEnabled(context.Background())
				reg := prometheus.NewPedanticRegistry()
				preferredEngine := newFakeEngineThatSupportsLimitedQueries()
				fallbackEngine := newFakeEngineThatSupportsAllQueries()
				engineWithFallback := NewEngineWithFallback(preferredEngine, fallbackEngine, reg, logger)

				query, err := createQuery(ctx, engineWithFallback, "a_supported_expression")
				require.NoError(t, err)
				require.Equal(t, fallbackEngine.query, query, "should return query from fallback engine if expression is not supported by preferred engine")

				util_test.AssertGatherAndCompare(t, reg, `
					# HELP cortex_mimir_query_engine_supported_queries_total Total number of queries that were supported by the Mimir query engine.
					# TYPE cortex_mimir_query_engine_supported_queries_total counter
					cortex_mimir_query_engine_supported_queries_total 0
					# HELP cortex_mimir_query_engine_unsupported_queries_total Total number of queries that were not supported by the Mimir query engine and so fell back to Prometheus' engine.
					# TYPE cortex_mimir_query_engine_unsupported_queries_total counter
					cortex_mimir_query_engine_unsupported_queries_total{reason="fallback forced by HTTP header"} 1
				`, "cortex_mimir_query_engine_supported_queries_total", "cortex_mimir_query_engine_unsupported_queries_total")
			})
		})
	}
}

type fakeEngineThatSupportsAllQueries struct {
	query     *fakeQuery
	wasCalled bool
}

func newFakeEngineThatSupportsAllQueries() *fakeEngineThatSupportsAllQueries {
	return &fakeEngineThatSupportsAllQueries{
		query: &fakeQuery{"query from fallback engine"},
	}
}

func (f *fakeEngineThatSupportsAllQueries) NewInstantQuery(context.Context, storage.Queryable, promql.QueryOpts, string, time.Time) (promql.Query, error) {
	f.wasCalled = true
	return f.query, nil
}

func (f *fakeEngineThatSupportsAllQueries) NewRangeQuery(context.Context, storage.Queryable, promql.QueryOpts, string, time.Time, time.Time, time.Duration) (promql.Query, error) {
	f.wasCalled = true
	return f.query, nil
}

type fakeEngineThatSupportsLimitedQueries struct {
	query *fakeQuery
}

func newFakeEngineThatSupportsLimitedQueries() *fakeEngineThatSupportsLimitedQueries {
	return &fakeEngineThatSupportsLimitedQueries{
		query: &fakeQuery{"query from preferred engine"},
	}
}

func (f *fakeEngineThatSupportsLimitedQueries) NewInstantQuery(_ context.Context, _ storage.Queryable, _ promql.QueryOpts, qs string, _ time.Time) (promql.Query, error) {
	if qs == "a_supported_expression" {
		return f.query, nil
	} else if qs == "an_invalid_expression" {
		return nil, errors.New("the query is invalid")
	}

	return nil, NewNotSupportedError("this expression is not supported")
}

func (f *fakeEngineThatSupportsLimitedQueries) NewRangeQuery(_ context.Context, _ storage.Queryable, _ promql.QueryOpts, qs string, _, _ time.Time, _ time.Duration) (promql.Query, error) {
	if qs == "a_supported_expression" {
		return f.query, nil
	} else if qs == "an_invalid_expression" {
		return nil, errors.New("the query is invalid")
	}

	return nil, NewNotSupportedError("this expression is not supported")
}

type fakeQuery struct {
	name string
}

func (f fakeQuery) Exec(context.Context) *promql.Result {
	panic("fakeQuery: Exec() not supported")
}

func (f fakeQuery) Close() {
	panic("fakeQuery: Close() not supported")
}

func (f fakeQuery) Statement() parser.Statement {
	panic("fakeQuery: Statement() not supported")
}

func (f fakeQuery) Stats() *stats.Statistics {
	panic("fakeQuery: Stats() not supported")
}

func (f fakeQuery) Cancel() {
	panic("fakeQuery: Cancel() not supported")
}

func (f fakeQuery) String() string {
	panic("fakeQuery: String() not supported")
}
