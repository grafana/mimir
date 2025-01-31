package querier

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestFilteringQueryablesViaHttpHeader(t *testing.T) {
	type testCase struct {
		transformContext      func(context.Context) context.Context
		expectedQuerier1Calls int
		expectedQuerier2Calls int
	}

	runTestCase := func(t *testing.T, tc testCase) {
		ctx := context.Background()
		logger := log.NewNopLogger()
		reg := prometheus.NewPedanticRegistry()
		metrics := stats.NewQueryMetrics(reg)
		overrides, err := validation.NewOverrides(defaultLimitsConfig(), nil)
		require.NoError(t, err)

		cfg := Config{}
		flagext.DefaultValues(&cfg)

		matcher := labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "metric")
		expectedMatchers := []*labels.Matcher{matcher}
		querier1 := &mockBlocksStorageQuerier{}
		querier1.On("Select", mock.Anything, true, mock.Anything, expectedMatchers).Return(storage.EmptySeriesSet())
		querier2 := &mockBlocksStorageQuerier{}
		querier2.On("Select", mock.Anything, true, mock.Anything, expectedMatchers).Return(storage.EmptySeriesSet())

		alwaysApplicable := func(_ context.Context, _ string, _ time.Time, _, _ int64, _ log.Logger, _ ...*labels.Matcher) bool {
			return true
		}
		querierQueryables := []TimeRangeQueryable{{
			Queryable:    newMockBlocksStorageQueryable(querier1),
			IsApplicable: alwaysApplicable,
			StorageName:  "querier1",
		}, {
			Queryable:    newMockBlocksStorageQueryable(querier2),
			IsApplicable: alwaysApplicable,
			StorageName:  "querier2",
		}}

		queryable := newQueryable(querierQueryables, cfg, overrides, metrics, logger)
		querier, err := queryable.Querier(0, 10)
		require.NoError(t, err)

		ctx = user.InjectOrgID(ctx, "0")
		ctx = tc.transformContext(ctx)
		series := querier.Select(ctx, false, nil, matcher)
		require.NoError(t, series.Err())

		assert.Equal(t, tc.expectedQuerier1Calls, len(querier1.Calls))
		assert.Equal(t, tc.expectedQuerier2Calls, len(querier2.Calls))
	}

	t.Run("do not set header", func(t *testing.T) {
		runTestCase(t, testCase{
			transformContext: func(ctx context.Context) context.Context {
				return ctx
			},
			expectedQuerier1Calls: 1,
			expectedQuerier2Calls: 1,
		})
	})

	t.Run("set header to unknown queryable", func(t *testing.T) {
		runTestCase(t, testCase{
			transformContext: func(ctx context.Context) context.Context {
				return addFilterQueryablesToContext(ctx, "querier3")
			},
			expectedQuerier1Calls: 0,
			expectedQuerier2Calls: 0,
		})
	})

	t.Run("set header to only querier1", func(t *testing.T) {
		runTestCase(t, testCase{
			transformContext: func(ctx context.Context) context.Context {
				return addFilterQueryablesToContext(ctx, "querier1")
			},
			expectedQuerier1Calls: 1,
			expectedQuerier2Calls: 0,
		})
	})

	t.Run("set header to only querier2", func(t *testing.T) {
		runTestCase(t, testCase{
			transformContext: func(ctx context.Context) context.Context {
				return addFilterQueryablesToContext(ctx, "querier2")
			},
			expectedQuerier1Calls: 0,
			expectedQuerier2Calls: 1,
		})
	})

	t.Run("set header to both querier1 and querier2", func(t *testing.T) {
		runTestCase(t, testCase{
			transformContext: func(ctx context.Context) context.Context {
				return addFilterQueryablesToContext(ctx, "querier2,querier1")
			},
			expectedQuerier1Calls: 1,
			expectedQuerier2Calls: 1,
		})
	})

	t.Run("set header to querier1 and querier2 and unknown querier", func(t *testing.T) {
		runTestCase(t, testCase{
			transformContext: func(ctx context.Context) context.Context {
				return addFilterQueryablesToContext(ctx, "querier2,querier1,querier3")
			},
			expectedQuerier1Calls: 1,
			expectedQuerier2Calls: 1,
		})
	})

	t.Run("set header to querier1 and unknown querier, with spaces", func(t *testing.T) {
		runTestCase(t, testCase{
			transformContext: func(ctx context.Context) context.Context {
				return addFilterQueryablesToContext(ctx, "querier1 , querier3")
			},
			expectedQuerier1Calls: 1,
			expectedQuerier2Calls: 0,
		})
	})
}
