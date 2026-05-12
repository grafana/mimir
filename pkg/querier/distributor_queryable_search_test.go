// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streaminglabelvalues"
)

func TestDistributorQuerier_SearchLabelNames_OutOfRetentionWindow(t *testing.T) {
	// Window entirely outside retention — distributor must not be called.
	nowMs := time.Now().UnixMilli()
	queryIngestersWithin := 1 * time.Hour

	dist := &mockDistributor{}
	q := newTestDistributorQuerier(t, dist, newMockConfigProvider(queryIngestersWithin),
		nowMs-3*time.Hour.Milliseconds(), // mint
		nowMs-2*time.Hour.Milliseconds(), // maxt (< now - 1h)
	)

	rs := q.SearchLabelNames(user.InjectOrgID(context.Background(), "user-1"), nil, nil)
	defer rs.Close()
	assert.False(t, rs.Next())
	assert.NoError(t, rs.Err())
	assert.Zero(t, dist.searchLabelNamesCalls.Load(), "distributor must not be called when outside retention")
}

func TestDistributorQuerier_SearchLabelNames_ClampsMinTime(t *testing.T) {
	// q.mint older than retention horizon — distributor sees the clamped from.
	nowMs := time.Now().UnixMilli()
	queryIngestersWithin := 1 * time.Hour
	expectedClampedMin := nowMs - queryIngestersWithin.Milliseconds()

	var observedFrom model.Time
	dist := &mockDistributor{
		searchLabelNamesFn: func(_ context.Context, from, _ model.Time, _ *streaminglabelvalues.Params, _ *storage.SearchHints, _ []*labels.Matcher) storage.SearchResultSet {
			observedFrom = from
			return storage.EmptySearchResultSet()
		},
	}
	q := newTestDistributorQuerier(t, dist, newMockConfigProvider(queryIngestersWithin),
		nowMs-3*time.Hour.Milliseconds(), // mint (before clamp window)
		nowMs+1*time.Hour.Milliseconds(), // maxt (in retention)
	)

	rs := q.SearchLabelNames(user.InjectOrgID(context.Background(), "user-1"), nil, nil)
	defer rs.Close()
	_ = rs.Next()
	// Allow ±1 second drift for time.Now() between test invocation and clamp.
	assert.InDelta(t, expectedClampedMin, int64(observedFrom), 1000.0, "minT must be clamped upward to now-QueryIngestersWithin")
}

func TestDistributorQuerier_SearchLabelValues_Passthrough(t *testing.T) {
	nowMs := time.Now().UnixMilli()
	dist := &mockDistributor{
		searchLabelValuesFn: func(_ context.Context, _, _ model.Time, name string, _ *streaminglabelvalues.Params, _ *storage.SearchHints, _ []*labels.Matcher) storage.SearchResultSet {
			assert.Equal(t, "env", name)
			return storage.NewSearchResultSetFromSlice([]storage.SearchResult{{Value: "prod", Score: 1.0}}, nil)
		},
	}
	q := newTestDistributorQuerier(t, dist, newMockConfigProvider(1*time.Hour), 0, nowMs)
	rs := q.SearchLabelValues(user.InjectOrgID(context.Background(), "user-1"), "env", nil, &storage.SearchHints{Limit: 10})
	defer rs.Close()
	var got []storage.SearchResult
	for rs.Next() {
		got = append(got, rs.At())
	}
	require.NoError(t, rs.Err())
	assert.Equal(t, []storage.SearchResult{{Value: "prod", Score: 1.0}}, got)
}

// newTestDistributorQuerier sets mint/maxt explicitly for retention-window
// tests, bypassing the NewDistributorQueryable factory.
func newTestDistributorQuerier(_ *testing.T, dist *mockDistributor, cfg distributorQueryableConfigProvider, mint, maxt int64) *distributorQuerier {
	return &distributorQuerier{
		logger:      log.NewNopLogger(),
		distributor: dist,
		mint:        mint,
		maxt:        maxt,
		cfgProvider: cfg,
	}
}
