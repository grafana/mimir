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
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/scrape"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/streaminglabelvalues"
)

// searchMethodInvocation captures how to drive a given streaming-search
// method on distributorQuerier through a single table-driven test. Both
// SearchLabelNames and SearchLabelValues share the retention-clamp logic;
// the only differences are the call signature, the spy hook to install on
// the mock distributor, and the call-counter to read.
type searchMethodInvocation struct {
	name        string
	invoke      func(q *distributorQuerier, ctx context.Context) storage.SearchResultSet
	captureFrom func(d *mockDistributor, dst *model.Time)
	callsCount  func(d *mockDistributor) int32
}

func searchMethodInvocations() []searchMethodInvocation {
	return []searchMethodInvocation{
		{
			name: "SearchLabelNames",
			invoke: func(q *distributorQuerier, ctx context.Context) storage.SearchResultSet {
				return q.SearchLabelNames(ctx, nil, nil)
			},
			captureFrom: func(d *mockDistributor, dst *model.Time) {
				d.searchLabelNamesFn = func(_ context.Context, from, _ model.Time, _ *streaminglabelvalues.Params, _ *storage.SearchHints, _ []*labels.Matcher) storage.SearchResultSet {
					*dst = from
					return storage.EmptySearchResultSet()
				}
			},
			callsCount: func(d *mockDistributor) int32 { return d.searchLabelNamesCalls.Load() },
		},
		{
			name: "SearchLabelValues",
			invoke: func(q *distributorQuerier, ctx context.Context) storage.SearchResultSet {
				return q.SearchLabelValues(ctx, "env", nil, nil)
			},
			captureFrom: func(d *mockDistributor, dst *model.Time) {
				d.searchLabelValuesFn = func(_ context.Context, from, _ model.Time, _ string, _ *streaminglabelvalues.Params, _ *storage.SearchHints, _ []*labels.Matcher) storage.SearchResultSet {
					*dst = from
					return storage.EmptySearchResultSet()
				}
			},
			callsCount: func(d *mockDistributor) int32 { return d.searchLabelValuesCalls.Load() },
		},
	}
}

func TestDistributorQuerier_Search_OutOfRetentionWindow(t *testing.T) {
	// Window entirely outside retention — distributor must not be called.
	nowMs := time.Now().UnixMilli()
	queryIngestersWithin := 1 * time.Hour

	for _, inv := range searchMethodInvocations() {
		t.Run(inv.name, func(t *testing.T) {
			dist := &mockDistributor{}
			q := newTestDistributorQuerier(t, dist, newMockConfigProvider(queryIngestersWithin),
				nowMs-3*time.Hour.Milliseconds(), // mint
				nowMs-2*time.Hour.Milliseconds(), // maxt (< now - 1h)
			)
			rs := inv.invoke(q, user.InjectOrgID(context.Background(), "user-1"))
			defer rs.Close()
			assert.False(t, rs.Next())
			assert.NoError(t, rs.Err())
			assert.Zero(t, inv.callsCount(dist), "distributor must not be called when outside retention")
		})
	}
}

func TestDistributorQuerier_Search_ClampsMinTime(t *testing.T) {
	// q.mint older than retention horizon — distributor sees the clamped from.
	nowMs := time.Now().UnixMilli()
	queryIngestersWithin := 1 * time.Hour
	expectedClampedMin := nowMs - queryIngestersWithin.Milliseconds()

	for _, inv := range searchMethodInvocations() {
		t.Run(inv.name, func(t *testing.T) {
			var observedFrom model.Time
			dist := &mockDistributor{}
			inv.captureFrom(dist, &observedFrom)
			q := newTestDistributorQuerier(t, dist, newMockConfigProvider(queryIngestersWithin),
				nowMs-3*time.Hour.Milliseconds(), // mint (before clamp window)
				nowMs+1*time.Hour.Milliseconds(), // maxt (in retention)
			)
			rs := inv.invoke(q, user.InjectOrgID(context.Background(), "user-1"))
			defer rs.Close()
			_ = rs.Next()
			// Allow ±1 second drift for time.Now() between test invocation and clamp.
			assert.InDelta(t, expectedClampedMin, int64(observedFrom), 1000.0, "minT must be clamped upward to now-QueryIngestersWithin")
		})
	}
}

func TestDistributorQuerier_SearchLabelValues_PassesLabelName(t *testing.T) {
	// Value-specific: verify the label name reaches the distributor.
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

func TestDistributorQuerier_FetchMetricMetadata(t *testing.T) {
	nowMs := time.Now().UnixMilli()

	t.Run("joins by metric family, first record wins, and sends the expected request", func(t *testing.T) {
		var gotReq *client.MetricsMetadataRequest
		dist := &mockDistributor{}
		dist.On("MetricsMetadata", mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) { gotReq = args.Get(1).(*client.MetricsMetadataRequest) }).
			Return([]scrape.MetricMetadata{
				{MetricFamily: "a", Type: model.MetricTypeCounter, Help: "help a", Unit: "s"},
				{MetricFamily: "a", Type: model.MetricTypeGauge, Help: "second a"}, // duplicate family: must be ignored
				{MetricFamily: "b", Type: model.MetricTypeGauge, Help: "help b"},
			}, nil)
		q := newTestDistributorQuerier(t, dist, newMockConfigProvider(time.Hour), 0, nowMs)

		got, err := q.FetchMetricMetadata(user.InjectOrgID(t.Context(), "user-1"), []string{"a", "b"})
		require.NoError(t, err)
		assert.Equal(t, map[string]metadata.Metadata{
			"a": {Type: model.MetricTypeCounter, Help: "help a", Unit: "s"},
			"b": {Type: model.MetricTypeGauge, Help: "help b"},
		}, got)

		require.NotNil(t, gotReq)
		assert.Equal(t, []string{"a", "b"}, gotReq.MetricNames)
		assert.Equal(t, int32(2), gotReq.Limit, "Limit bounds the response to the number of requested names")
		assert.Equal(t, int32(1), gotReq.LimitPerMetric, "LimitPerMetric=1 keeps the response small")
		assert.Empty(t, gotReq.Metric, "the single-name Metric field must not be used")
	})

	t.Run("propagates the fetch error", func(t *testing.T) {
		dist := &mockDistributor{}
		dist.On("MetricsMetadata", mock.Anything, mock.Anything).Return([]scrape.MetricMetadata(nil), errors.New("boom"))
		q := newTestDistributorQuerier(t, dist, newMockConfigProvider(time.Hour), 0, nowMs)

		_, err := q.FetchMetricMetadata(user.InjectOrgID(t.Context(), "user-1"), []string{"a"})
		require.EqualError(t, err, "boom")
	})
}
