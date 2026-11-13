// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

func TestStartTimestampsEngine(t *testing.T) {
	enabled, disabled := true, false

	testCases := map[string]struct {
		tenantID      string
		opts          promql.QueryOpts
		expectedErr   error
		expectedUseST *bool
	}{
		"disabled for tenant, no per-query setting": {
			tenantID:      "disabled",
			opts:          promql.NewPrometheusQueryOpts(false, 0, nil),
			expectedUseST: nil,
		},
		"enabled for tenant, no query options": {
			tenantID:      "enabled",
			expectedUseST: &enabled,
		},
		"enabled for tenant, no per-query setting": {
			tenantID:      "enabled",
			opts:          promql.NewPrometheusQueryOpts(true, 3*time.Minute, nil),
			expectedUseST: &enabled,
		},
		"enabled for tenant, no per-query setting, per-query cost limits": {
			tenantID:      "enabled",
			opts:          promql.NewPrometheusQueryOptsWithLimits(true, 3*time.Minute, 10, 20, time.Second, nil),
			expectedUseST: &enabled,
		},
		"enabled for tenant, disabled per query": {
			tenantID:      "enabled",
			opts:          promql.NewPrometheusQueryOpts(false, 0, &disabled),
			expectedUseST: &disabled,
		},
		"disabled for tenant, enabled per query": {
			tenantID:      "disabled",
			opts:          promql.NewPrometheusQueryOpts(false, 0, &enabled),
			expectedUseST: &enabled,
		},
		"enabled for all tenants of a federated query": {
			tenantID:      "enabled|enabled-too",
			expectedUseST: &enabled,
		},
		"enabled for some tenants of a federated query": {
			tenantID:      "enabled|disabled",
			expectedUseST: nil,
		},
		"no tenant": {
			expectedErr: user.ErrNoOrgID,
		},
	}

	limits := startTimestampsLimitsMock{"enabled": true, "enabled-too": true}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			if tc.tenantID != "" {
				ctx = user.InjectOrgID(ctx, tc.tenantID)
			}
			next := &captureQueryEngine{}
			eng := newStartTimestampsEngine(next, limits)

			newQueries := map[string]func() (promql.Query, error){
				"instant": func() (promql.Query, error) {
					return eng.NewInstantQuery(ctx, captureQueryable{}, tc.opts, "up", time.Now())
				},
				"range": func() (promql.Query, error) {
					return eng.NewRangeQuery(ctx, captureQueryable{}, tc.opts, "up", time.Now().Add(-time.Hour), time.Now(), time.Minute)
				},
			}
			for queryType, newQuery := range newQueries {
				t.Run(queryType, func(t *testing.T) {
					_, err := newQuery()
					if tc.expectedErr != nil {
						require.ErrorIs(t, err, tc.expectedErr)
						return
					}
					require.NoError(t, err)

					if tc.expectedUseST == nil {
						require.Equal(t, tc.opts, next.opts)
					} else {
						require.Equal(t, tc.expectedUseST, next.opts.UseStartTimestamps())
						requireSameQueryOpts(t, tc.opts, next.opts)
					}

					q, err := next.queryable.Querier(0, 1)
					require.NoError(t, err)
					captured := &captureQuerier{}
					switch q := q.(type) {
					case startTimestampsQuerier:
						q.Querier = captured
						q.Select(t.Context(), false, nil)
					case *captureQuerier:
						captured = q
						q.Select(t.Context(), false, nil)
					default:
						require.Failf(t, "unexpected querier type", "%T", q)
					}
					require.Equal(t, tc.expectedUseST != nil && *tc.expectedUseST, collectStartTimestampsFromContext(captured.ctx))
				})
			}
		})
	}
}

// requireSameQueryOpts checks that the options other than the start timestamp setting are unchanged.
func requireSameQueryOpts(t *testing.T, expected, actual promql.QueryOpts) {
	t.Helper()

	if expected == nil {
		expected = promql.NewPrometheusQueryOpts(false, 0, nil)
	}
	require.Equal(t, expected.EnablePerStepStats(), actual.EnablePerStepStats())
	require.Equal(t, expected.LookbackDelta(), actual.LookbackDelta())

	expectedCostOpts, expectedOK := expected.(promql.QueryCostOpts)
	actualCostOpts, actualOK := actual.(promql.QueryCostOpts)
	require.Equal(t, expectedOK, actualOK)
	if expectedOK {
		require.Equal(t, expectedCostOpts.MaxSeries(), actualCostOpts.MaxSeries())
		require.Equal(t, expectedCostOpts.MaxSamplesRead(), actualCostOpts.MaxSamplesRead())
		require.Equal(t, expectedCostOpts.MaxQueryDuration(), actualCostOpts.MaxQueryDuration())
	}
}

type startTimestampsLimitsMock map[string]bool

func (m startTimestampsLimitsMock) UseStartTimestamps(userID string) bool {
	return m[userID]
}

type captureQueryEngine struct {
	queryable storage.Queryable
	opts      promql.QueryOpts
}

func (e *captureQueryEngine) NewInstantQuery(_ context.Context, q storage.Queryable, opts promql.QueryOpts, _ string, _ time.Time) (promql.Query, error) {
	e.queryable, e.opts = q, opts
	return nil, nil
}

func (e *captureQueryEngine) NewRangeQuery(_ context.Context, q storage.Queryable, opts promql.QueryOpts, _ string, _, _ time.Time, _ time.Duration) (promql.Query, error) {
	e.queryable, e.opts = q, opts
	return nil, nil
}

type captureQueryable struct{}

func (captureQueryable) Querier(_, _ int64) (storage.Querier, error) {
	return &captureQuerier{}, nil
}

// captureQuerier records the context of the last Select call.
type captureQuerier struct {
	storage.Querier
	ctx context.Context
}

func (q *captureQuerier) Select(ctx context.Context, _ bool, _ *storage.SelectHints, _ ...*labels.Matcher) storage.SeriesSet {
	q.ctx = ctx
	return storage.EmptySeriesSet()
}
