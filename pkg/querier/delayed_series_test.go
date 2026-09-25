// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/storage/series"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestMultiQuerier_DelayedSeriesQueryBlockStoreForRecentData(t *testing.T) {
	const tenantID = "delayed"

	tests := map[string]struct {
		metric           string
		expectedStoreHit bool
		expectedAnnotate bool
	}{
		"selector can match delayed series":          {metric: "delayed_metric", expectedStoreHit: true, expectedAnnotate: true},
		"selector can match a recently retired rule": {metric: "retired_metric", expectedStoreHit: true, expectedAnnotate: false},
		"selector can't match delayed series":        {metric: "standard_metric", expectedStoreHit: false},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := Config{}
			flagext.DefaultValues(&cfg)
			cfg.QueryStoreAfter = 12 * time.Hour

			tenantLimits := defaultLimitsConfig()
			tenantLimits.DelayedSeries = validation.DelayedSeriesConfig{
				{Match: `{__name__="delayed_metric"}`},
				{Match: `{__name__="retired_metric"}`, RetiredAt: time.Now().Add(-time.Hour)},
			}
			require.NoError(t, tenantLimits.DelayedSeries.Validate())
			overrides := validation.NewOverrides(defaultLimitsConfig(), validation.NewMockTenantLimits(map[string]*validation.Limits{tenantID: &tenantLimits}))

			distributor := &mockDistributor{}
			distributor.On("QueryStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(client.CombinedQueryStreamResponse{}, nil)

			matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, tc.metric)}
			storeQuerier := &mockBlocksStorageQuerier{}
			storeQuerier.On("Select", mock.Anything, true, mock.Anything, matchers).Return(storage.EmptySeriesSet())

			planner, err := streamingpromql.NewQueryPlanner(cfg.EngineConfig.MimirQueryEngine, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
			require.NoError(t, err)
			queryable, _, _, _, err := New(cfg, overrides, distributor, newMockBlocksStorageQueryable(storeQuerier), prometheus.NewRegistry(), log.NewNopLogger(), nil, planner, unlimitedQueryLimitsProvider())
			require.NoError(t, err)

			ctx := user.InjectOrgID(context.Background(), tenantID)
			ctx = limiter.AddMemoryTrackerToContext(ctx, limiter.NewUnlimitedMemoryConsumptionTracker(ctx))
			now := time.Now()
			q, err := queryable.Querier(now.Add(-5*time.Minute).UnixMilli(), now.UnixMilli())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, q.Close()) })

			set := q.Select(ctx, true, nil, matchers...)
			for set.Next() {
			}
			require.NoError(t, set.Err())

			if !tc.expectedStoreHit {
				storeQuerier.AssertNotCalled(t, "Select", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
				return
			}
			storeQuerier.AssertCalled(t, "Select", mock.MatchedBy(func(ctx context.Context) bool {
				return isDelayedSeriesRead(ctx) && annotateDelayedSeriesRead(ctx) == tc.expectedAnnotate
			}), true, mock.Anything, matchers)
		})
	}
}

func TestMultiQuerier_MergeSeriesSetsKeepsWarnings(t *testing.T) {
	var ingesterWarnings, storeWarnings annotations.Annotations
	ingesterWarnings.Add(errors.New("from ingesters"))
	storeWarnings.Add(newDelayedSeriesCompleteThroughInfo(1000))

	mq := &multiQuerier{}
	set := mq.mergeSeriesSets([]storage.SeriesSet{
		series.NewSeriesSetWithWarnings(storage.EmptySeriesSet(), ingesterWarnings),
		series.NewSeriesSetWithWarnings(storage.EmptySeriesSet(), storeWarnings),
	})
	for set.Next() {
	}
	require.NoError(t, set.Err())

	warnings, infos := set.Warnings().AsStrings("", 0, 0)
	require.Equal(t, []string{"from ingesters"}, warnings)
	require.Equal(t, []string{"PromQL info: the query selects delayed series, which are published through about 1970-01-01T00:00:01Z; newer samples are not queryable yet"}, infos)
}
