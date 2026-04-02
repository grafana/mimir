// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	mimirstorage "github.com/grafana/mimir/pkg/storage"
	"github.com/grafana/mimir/pkg/util"
)

// simpleSearchResultSet returns a channel-backed SearchResultSet that yields the given string values.
// It is used in tests to avoid standing up a full NewSearchValueSet producer goroutine.
func simpleSearchResultSet(values ...string) mimirstorage.SearchResultSet {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan mimirstorage.SearchResult, len(values))
	for _, v := range values {
		ch <- mimirstorage.SearchResult{Value: v}
	}
	close(ch)
	return &labelSearchStream{ch: ch, ctx: ctx, cancel: cancel}
}

func TestDistributorQuerier_SearchLabelNames(t *testing.T) {
	now := time.Now()

	tests := map[string]struct {
		mint                 int64
		maxt                 int64
		queryIngestersWithin time.Duration
		hints                *mimirstorage.MimirSearchHints
		setupMock            func(*mockDistributor)
		expectedValues       []string
		expectErr            bool
	}{
		"returns empty when query range is entirely outside QueryIngestersWithin": {
			// maxt is 2 hours ago, queryIngestersWithin is 1 hour → skip
			mint:                 util.TimeToMillis(now.Add(-3 * time.Hour)),
			maxt:                 util.TimeToMillis(now.Add(-2 * time.Hour)),
			queryIngestersWithin: time.Hour,
			setupMock:            func(*mockDistributor) {}, // must not be called
			expectedValues:       nil,
		},
		"delegates to distributor when in range": {
			mint:                 util.TimeToMillis(now.Add(-30 * time.Minute)),
			maxt:                 util.TimeToMillis(now),
			queryIngestersWithin: time.Hour,
			setupMock: func(d *mockDistributor) {
				d.On("SearchLabelNames", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					Return(simpleSearchResultSet("__name__", "job"), nil)
			},
			expectedValues: []string{"__name__", "job"},
		},
		"returns error result set when distributor errors": {
			mint:                 util.TimeToMillis(now.Add(-30 * time.Minute)),
			maxt:                 util.TimeToMillis(now),
			queryIngestersWithin: time.Hour,
			setupMock: func(d *mockDistributor) {
				// mockDistributor.SearchLabelNames does args.Get(0).(SearchResultSet) so we must
				// pass a non-nil value even for the error path.
				d.On("SearchLabelNames", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					Return(mimirstorage.ErrorSearchResultSet(errors.New("placeholder")), errors.New("ingester unavailable"))
			},
			expectErr: true,
		},
		"passes hints through to distributor": {
			mint:                 util.TimeToMillis(now.Add(-30 * time.Minute)),
			maxt:                 util.TimeToMillis(now),
			queryIngestersWithin: time.Hour,
			hints:                &mimirstorage.MimirSearchHints{Search: []string{"job"}, Limit: 10},
			setupMock: func(d *mockDistributor) {
				d.On("SearchLabelNames", mock.Anything, mock.Anything, mock.Anything,
					&mimirstorage.MimirSearchHints{Search: []string{"job"}, Limit: 10},
					mock.Anything,
				).Return(simpleSearchResultSet("job"), nil)
			},
			expectedValues: []string{"job"},
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			d := &mockDistributor{}
			tc.setupMock(d)

			ctx := user.InjectOrgID(context.Background(), "test-org")
			configProvider := newMockConfigProvider(tc.queryIngestersWithin)
			queryable := NewDistributorQueryable(d, configProvider, nil, log.NewNopLogger())
			querier, err := queryable.Querier(tc.mint, tc.maxt)
			require.NoError(t, err)

			mq, ok := querier.(mimirstorage.MimirSearcher)
			require.True(t, ok, "distributorQuerier must implement MimirSearcher")

			vs, _ := mq.SearchLabelNames(ctx, tc.hints)
			defer vs.Close()

			got, iterErr := drainValueSet(vs)
			if tc.expectErr {
				require.Error(t, iterErr)
				return
			}
			require.NoError(t, iterErr)
			assert.Equal(t, tc.expectedValues, got)
		})
	}
}

func TestDistributorQuerier_SearchLabelValues(t *testing.T) {
	now := time.Now()
	const labelName = model.MetricNameLabel

	tests := map[string]struct {
		mint                 int64
		maxt                 int64
		queryIngestersWithin time.Duration
		setupMock            func(*mockDistributor)
		expectedValues       []string
		expectErr            bool
	}{
		"returns empty when query range is entirely outside QueryIngestersWithin": {
			mint:                 util.TimeToMillis(now.Add(-3 * time.Hour)),
			maxt:                 util.TimeToMillis(now.Add(-2 * time.Hour)),
			queryIngestersWithin: time.Hour,
			setupMock:            func(*mockDistributor) {},
			expectedValues:       nil,
		},
		"delegates to distributor when in range": {
			mint:                 util.TimeToMillis(now.Add(-30 * time.Minute)),
			maxt:                 util.TimeToMillis(now),
			queryIngestersWithin: time.Hour,
			setupMock: func(d *mockDistributor) {
				d.On("SearchLabelValues", mock.Anything, mock.Anything, mock.Anything, model.LabelName(labelName), mock.Anything, mock.Anything).
					Return(simpleSearchResultSet("metric_a", "metric_b"), nil)
			},
			expectedValues: []string{"metric_a", "metric_b"},
		},
		"returns error result set when distributor errors": {
			mint:                 util.TimeToMillis(now.Add(-30 * time.Minute)),
			maxt:                 util.TimeToMillis(now),
			queryIngestersWithin: time.Hour,
			setupMock: func(d *mockDistributor) {
				d.On("SearchLabelValues", mock.Anything, mock.Anything, mock.Anything, model.LabelName(labelName), mock.Anything, mock.Anything).
					Return(mimirstorage.ErrorSearchResultSet(errors.New("placeholder")), errors.New("ingester unavailable"))
			},
			expectErr: true,
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			d := &mockDistributor{}
			tc.setupMock(d)

			ctx := user.InjectOrgID(context.Background(), "test-org")
			configProvider := newMockConfigProvider(tc.queryIngestersWithin)
			queryable := NewDistributorQueryable(d, configProvider, nil, log.NewNopLogger())
			querier, err := queryable.Querier(tc.mint, tc.maxt)
			require.NoError(t, err)

			mq, ok := querier.(mimirstorage.MimirSearcher)
			require.True(t, ok, "distributorQuerier must implement MimirSearcher")

			vs, _ := mq.SearchLabelValues(ctx, string(labelName), nil)
			defer vs.Close()

			got, iterErr := drainValueSet(vs)
			if tc.expectErr {
				require.Error(t, iterErr)
				return
			}
			require.NoError(t, iterErr)
			assert.Equal(t, tc.expectedValues, got)
		})
	}
}
