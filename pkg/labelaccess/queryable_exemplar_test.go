// SPDX-License-Identifier: AGPL-3.0-only

package labelaccess

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockExemplarQuerier struct {
	matchers []promSelector
	results  []exemplar.QueryResult
	err      error
}

func (e *mockExemplarQuerier) Select(_, _ int64, matchers ...[]*labels.Matcher) ([]exemplar.QueryResult, error) {
	for _, m := range matchers {
		e.matchers = append(e.matchers, m)
	}

	if e.err != nil {
		return nil, e.err
	}

	return e.results, nil
}

type mockExemplarQueryable struct {
	querier storage.ExemplarQuerier
	err     error
}

func (e *mockExemplarQueryable) ExemplarQuerier(_ context.Context) (storage.ExemplarQuerier, error) {
	if e.err != nil {
		return nil, e.err
	}

	return e.querier, nil
}

func newTestExemplarQueryable(next storage.ExemplarQueryable) *labelAccessExemplarQueryable {
	var logger log.Logger
	if testing.Verbose() {
		logger = log.NewLogfmtLogger(os.Stdout)
	} else {
		logger = log.NewNopLogger()
	}

	return &labelAccessExemplarQueryable{
		next:   next,
		logger: logger,
	}
}

func TestLabelAccessExemplarQueryable_ExemplarQuerier(t *testing.T) {
	t.Run("no instance policy map in context passes through to upstream", func(t *testing.T) {
		ctx := context.Background()
		ctx = user.InjectOrgID(ctx, "team-a")

		next := &mockExemplarQueryable{}
		next.querier = &mockExemplarQuerier{}
		queryable := newTestExemplarQueryable(next)
		q, err := queryable.ExemplarQuerier(ctx)

		require.NoError(t, err)
		assert.IsType(t, &mockExemplarQuerier{}, q)
	})

	t.Run("no instance ID in context results in error", func(t *testing.T) {
		ctx := context.Background()
		ctx = InjectLabelMatchersContext(ctx, LabelPolicySet{
			"team-a": []*LabelPolicy{newSingleLabelPolicy(labels.MatchEqual, "secret", "false")},
		})

		next := &mockExemplarQueryable{}
		next.querier = &mockExemplarQuerier{}
		queryable := newTestExemplarQueryable(next)
		_, err := queryable.ExemplarQuerier(ctx)

		assert.Error(t, err)
	})

	t.Run("no matching policy results in using fallback queryable", func(t *testing.T) {
		ctx := context.Background()
		ctx = user.InjectOrgID(ctx, "team-b")
		ctx = InjectLabelMatchersContext(ctx, LabelPolicySet{
			"team-a": []*LabelPolicy{newSingleLabelPolicy(labels.MatchEqual, "secret", "false")},
		})

		next := &mockExemplarQueryable{}
		next.querier = &mockExemplarQuerier{}
		queryable := newTestExemplarQueryable(next)
		q, err := queryable.ExemplarQuerier(ctx)

		// The tenant ID in the context doesn't match any of the injected LBAC matchers so the
		// label access queryable should delegate to the upstream queryable. We test that the
		// returned querier is the same type as our mock one that we've set as upstream.
		require.NoError(t, err)
		assert.IsType(t, &mockExemplarQuerier{}, q)
	})

	t.Run("success", func(t *testing.T) {
		ctx := context.Background()
		ctx = user.InjectOrgID(ctx, "team-a")
		ctx = InjectLabelMatchersContext(ctx, LabelPolicySet{
			"team-a": []*LabelPolicy{newSingleLabelPolicy(labels.MatchEqual, "secret", "false")},
		})

		next := &mockExemplarQueryable{}
		next.querier = &mockExemplarQuerier{}
		queryable := newTestExemplarQueryable(next)
		q, err := queryable.ExemplarQuerier(ctx)

		require.NoError(t, err)
		assert.IsType(t, &labelAccessExemplarQuerier{}, q)
	})
}

func TestLabelAccessExemplarQuerier_Select(t *testing.T) {
	now := time.Now()
	startMs := now.Add(-1 * time.Hour).UnixMilli()
	endMs := now.Add(1 * time.Minute).UnixMilli()

	seriesMatcher1 := labels.MustNewMatcher(labels.MatchEqual, "__name__", "series_1")
	seriesMatcher2 := labels.MustNewMatcher(labels.MatchEqual, "__name__", "series_2")
	clusterMatcher := labels.MustNewMatcher(labels.MatchEqual, "cluster", "prod")

	result1 := exemplar.QueryResult{
		SeriesLabels: labels.FromStrings(
			"__name__", "series_1",
			"method", "GET",
			"status_code", "500",
			"path", "/details/",
		),
		Exemplars: []exemplar.Exemplar{
			{
				Labels: labels.FromStrings("trace_id", "1234"),
				Value:  1234.5,
				Ts:     now.UnixMilli(),
			},
		},
	}

	result2 := exemplar.QueryResult{
		SeriesLabels: labels.FromStrings(
			"__name__", "series_1",
			"method", "POST",
			"status_code", "201",
			"path", "/login/",
		),
		Exemplars: []exemplar.Exemplar{
			{
				Labels: labels.FromStrings("trace_id", "1234"),
				Value:  1234.5,
				Ts:     now.UnixMilli(),
			},
		},
	}
	result3 := exemplar.QueryResult{
		SeriesLabels: labels.FromStrings(
			"__name__", "series_1",
			"method", "POST",
			"status_code", "200",
			"path", "/login/",
		),
		Exemplars: []exemplar.Exemplar{
			{
				Labels: labels.FromStrings("trace_id", "1234"),
				Value:  1234.5,
				Ts:     now.UnixMilli(),
			},
		},
	}

	t.Run("no selectors", func(t *testing.T) {
		upstream := &mockExemplarQuerier{results: []exemplar.QueryResult{result1, result2, result3}}
		querier := newLabelAccessExemplarQuerier(context.Background(), []promSelector{}, upstream, log.NewNopLogger())

		res, err := querier.Select(startMs, endMs, []*labels.Matcher{seriesMatcher1})
		require.NoError(t, err)
		require.Len(t, res, 3)

		assert.Equal(t, "series_1", res[0].SeriesLabels.Get("__name__"))
		assert.Equal(t, "GET", res[0].SeriesLabels.Get("method"))
		assert.Equal(t, "500", res[0].SeriesLabels.Get("status_code"))

		assert.Equal(t, "series_1", res[1].SeriesLabels.Get("__name__"))
		assert.Equal(t, "POST", res[1].SeriesLabels.Get("method"))
		assert.Equal(t, "201", res[1].SeriesLabels.Get("status_code"))

		assert.Equal(t, "series_1", res[2].SeriesLabels.Get("__name__"))
		assert.Equal(t, "POST", res[2].SeriesLabels.Get("method"))
		assert.Equal(t, "200", res[2].SeriesLabels.Get("status_code"))
	})

	t.Run("single selector multiple matcher passed directly to upstream", func(t *testing.T) {
		selectors := labelPoliciesToPromSelectors([]*LabelPolicy{
			newDoubleLabelPolicy(labels.MatchEqual, "method", "GET", "status_code", "200"),
		})

		upstream := &mockExemplarQuerier{}
		querier := newLabelAccessExemplarQuerier(context.Background(), selectors, upstream, log.NewNopLogger())

		_, err := querier.Select(startMs, endMs, []*labels.Matcher{seriesMatcher1, clusterMatcher}, []*labels.Matcher{seriesMatcher2, clusterMatcher})
		require.NoError(t, err)
		// The Select method gets multiple slices of matchers. Each slice is unioned together while all
		// matchers within a slice are treated as an intersection. If we only have a single LBAC selector
		// we can add our matchers to each slice of matchers passed to the upstream querier to enforce
		// that anything returned also satisfies our matchers (intersection). Test that our matchers
		// were appended to each slice of matchers
		assert.Contains(t, upstream.matchers[0], seriesMatcher1)
		assert.Contains(t, upstream.matchers[0], clusterMatcher)
		assert.Contains(t, upstream.matchers[0], selectors[0][0])
		assert.Contains(t, upstream.matchers[0], selectors[0][1])
		assert.Contains(t, upstream.matchers[1], seriesMatcher2)
		assert.Contains(t, upstream.matchers[1], clusterMatcher)
		assert.Contains(t, upstream.matchers[1], selectors[0][0])
		assert.Contains(t, upstream.matchers[1], selectors[0][1])
	})

	t.Run("upstream error results in error", func(t *testing.T) {
		// Multiple selectors so we don't short-circuit and just pass the LBAC selector
		// to the Select method of the upstream querier.
		selectors := labelPoliciesToPromSelectors([]*LabelPolicy{
			newSingleLabelPolicy(labels.MatchEqual, "method", "GET"),
			newSingleLabelPolicy(labels.MatchEqual, "status_code", "200"),
		})

		expectedError := errors.New("things are crashing")
		upstream := &mockExemplarQuerier{err: expectedError}
		querier := newLabelAccessExemplarQuerier(context.Background(), selectors, upstream, log.NewNopLogger())

		_, err := querier.Select(startMs, endMs, []*labels.Matcher{seriesMatcher1})
		assert.ErrorIs(t, err, expectedError)
	})

	t.Run("matches", func(t *testing.T) {
		// Multiple selectors so we don't short-circuit and just pass the LBAC selector
		// to the Select method of the upstream querier.
		selectors := labelPoliciesToPromSelectors([]*LabelPolicy{
			newSingleLabelPolicy(labels.MatchEqual, "method", "GET"),
			newSingleLabelPolicy(labels.MatchEqual, "status_code", "200"),
		})

		upstream := &mockExemplarQuerier{
			// Multiple results to verify that LBAC rules are applied and they are "OR'd" together.
			// The first one should match the "method=GET" LBAC rule. The second one will not match
			// either LBAC rule. The third one should match the "status_code=200" LBAC rule.
			results: []exemplar.QueryResult{result1, result2, result3},
		}
		querier := newLabelAccessExemplarQuerier(context.Background(), selectors, upstream, log.NewNopLogger())

		res, err := querier.Select(startMs, endMs, []*labels.Matcher{seriesMatcher1})
		require.NoError(t, err)
		require.Len(t, res, 2)

		assert.Equal(t, "series_1", res[0].SeriesLabels.Get("__name__"))
		assert.Equal(t, "GET", res[0].SeriesLabels.Get("method"))
		assert.Equal(t, "500", res[0].SeriesLabels.Get("status_code"))

		assert.Equal(t, "series_1", res[1].SeriesLabels.Get("__name__"))
		assert.Equal(t, "POST", res[1].SeriesLabels.Get("method"))
		assert.Equal(t, "200", res[1].SeriesLabels.Get("status_code"))
	})

	t.Run("no matches", func(t *testing.T) {
		// Multiple selectors so we don't short-circuit and just pass the LBAC selector
		// to the Select method of the upstream querier.
		selectors := labelPoliciesToPromSelectors([]*LabelPolicy{
			newSingleLabelPolicy(labels.MatchEqual, "method", "GET"),
			newSingleLabelPolicy(labels.MatchEqual, "status_code", "200"),
		})

		upstream := &mockExemplarQuerier{
			// Single result that should match neither LBAC selector (POST, HTTP 201)
			results: []exemplar.QueryResult{result2},
		}
		querier := newLabelAccessExemplarQuerier(context.Background(), selectors, upstream, log.NewNopLogger())

		res, err := querier.Select(startMs, endMs, []*labels.Matcher{seriesMatcher1})
		require.NoError(t, err)
		assert.Empty(t, res)
	})
}
