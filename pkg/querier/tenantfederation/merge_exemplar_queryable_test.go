// SPDX-License-Identifier: AGPL-3.0-only

package tenantfederation

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/grafana/dskit/tenant"

	"github.com/grafana/mimir/pkg/util/test"
)

type mockExemplarQueryable struct {
	queriers map[string]storage.ExemplarQuerier
	err      error
}

func (m *mockExemplarQueryable) ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error) {
	if m.err != nil {
		return nil, m.err
	}

	ids, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, err
	}

	if len(ids) != 1 {
		return nil, fmt.Errorf("single tenant ID expected in mock queryable, got %+v", ids)
	}

	if q, ok := m.queriers[ids[0]]; ok {
		return q, nil
	}

	return nil, fmt.Errorf("no error or mock queriers configured for mock exemplar queryable. context: %+v", ctx)
}

type mockExemplarQuerier struct {
	res []exemplar.QueryResult
	err error
}

func (m *mockExemplarQuerier) Select(_, _ int64, allMatchers ...[]*labels.Matcher) ([]exemplar.QueryResult, error) {
	if m.err != nil {
		return nil, m.err
	}

	unique := make(map[string]exemplar.QueryResult)

	for _, r := range m.res {
		for _, matchers := range allMatchers {
			// If the result matches this bundle of matchers, put it in the output and
			// break the matcher loop (the contract is that the matchers within each slice
			// are AND'd together which each slice of matches is OR'd together) and move
			// on the testing the next result.
			if m.matches(r, matchers) {
				unique[r.SeriesLabels.String()] = r
				break
			}
		}
	}

	out := make([]exemplar.QueryResult, 0, len(unique))
	for _, r := range unique {
		out = append(out, r)
	}

	return out, nil
}

func (m *mockExemplarQuerier) matches(res exemplar.QueryResult, matchers []*labels.Matcher) bool {
	ret := true
	res.SeriesLabels.Range(func(l labels.Label) {
		for _, m := range matchers {
			if m.Name == l.Name && !m.Matches(l.Value) {
				ret = false
			}
		}
	})

	return ret
}

func TestMergeExemplarQueryable_ExemplarQuerier(t *testing.T) {
	t.Run("error getting tenant IDs", func(t *testing.T) {
		upstream := &mockExemplarQueryable{}
		federated := NewExemplarQueryable(upstream, false, test.NewTestingLogger(t))

		q, err := federated.ExemplarQuerier(context.Background())
		assert.ErrorIs(t, err, user.ErrNoOrgID)
		assert.Nil(t, q)
	})

	t.Run("error getting upstream querier", func(t *testing.T) {
		ctx := user.InjectOrgID(context.Background(), "123")
		upstream := &mockExemplarQueryable{err: errors.New("unable to get querier")}
		federated := NewExemplarQueryable(upstream, false, test.NewTestingLogger(t))

		q, err := federated.ExemplarQuerier(ctx)
		assert.Error(t, err)
		assert.Nil(t, q)
	})

	t.Run("single tenant bypass single querier happy path", func(t *testing.T) {
		ctx := user.InjectOrgID(context.Background(), "123")
		querier := &mockExemplarQuerier{}
		upstream := &mockExemplarQueryable{queriers: map[string]storage.ExemplarQuerier{"123": querier}}
		federated := NewExemplarQueryable(upstream, true, test.NewTestingLogger(t))

		q, err := federated.ExemplarQuerier(ctx)
		assert.NoError(t, err)
		assert.Same(t, q, querier)
	})

	t.Run("single tenant federated happy path", func(t *testing.T) {
		ctx := user.InjectOrgID(context.Background(), "123")
		querier := &mockExemplarQuerier{}
		upstream := &mockExemplarQueryable{queriers: map[string]storage.ExemplarQuerier{"123": querier}}
		federated := NewExemplarQueryable(upstream, false, test.NewTestingLogger(t))

		q, err := federated.ExemplarQuerier(ctx)
		require.NoError(t, err)
		require.IsType(t, q, &mergeExemplarQuerier{})

		mergeQ := q.(*mergeExemplarQuerier)
		assert.Len(t, mergeQ.tenants, 1)
		assert.Len(t, mergeQ.queriers, 1)
	})

	t.Run("multi tenant federated happy path", func(t *testing.T) {
		ctx := user.InjectOrgID(context.Background(), "123|456")
		querier1 := &mockExemplarQuerier{}
		querier2 := &mockExemplarQuerier{}
		upstream := &mockExemplarQueryable{queriers: map[string]storage.ExemplarQuerier{
			"123": querier1,
			"456": querier2,
		}}
		federated := NewExemplarQueryable(upstream, false, test.NewTestingLogger(t))

		q, err := federated.ExemplarQuerier(ctx)
		require.NoError(t, err)
		require.IsType(t, q, &mergeExemplarQuerier{})

		mergeQ := q.(*mergeExemplarQuerier)
		assert.Len(t, mergeQ.tenants, 2)
		assert.Len(t, mergeQ.queriers, 2)
	})
}

func TestMergeExemplarQuerier_Select(t *testing.T) {
	now := time.Now()

	// fixtureResults returns two slices of exemplar results, one for each of two
	// mock queriers that will be used as the upstream for various merge queryable
	// tests below.
	fixtureResults := func() ([]exemplar.QueryResult, []exemplar.QueryResult) {
		res1 := []exemplar.QueryResult{
			{
				SeriesLabels: labels.FromStrings("__name__", "request_duration_seconds"),
				Exemplars: []exemplar.Exemplar{
					{
						Labels: labels.FromStrings("traceID", "abc123"),
						Value:  123.4,
						Ts:     now.UnixMilli(),
					},
				},
			},
		}

		res2 := []exemplar.QueryResult{
			{
				SeriesLabels: labels.FromStrings("__name__", "request_duration_seconds"),
				Exemplars: []exemplar.Exemplar{
					{
						Labels: labels.FromStrings("traceID", "abc456"),
						Value:  456.7,
						Ts:     now.UnixMilli(),
					},
				},
			},
		}

		return res1, res2
	}

	// Matchers included a matcher looking for a specific tenant ID. Ensure that we only
	// get results from a single tenant in this case even though both queriers return results.
	t.Run("two tenants one filtered", func(t *testing.T) {
		matchers := [][]*labels.Matcher{{
			labels.MustNewMatcher(labels.MatchEqual, "__name__", "request_duration_seconds"),
			labels.MustNewMatcher(labels.MatchEqual, "__tenant_id__", "123"),
		}}

		res1, res2 := fixtureResults()
		upstream := &mockExemplarQueryable{queriers: map[string]storage.ExemplarQuerier{
			"123": &mockExemplarQuerier{res: res1},
			"456": &mockExemplarQuerier{res: res2},
		}}

		federated := NewExemplarQueryable(upstream, false, test.NewTestingLogger(t))
		q, err := federated.ExemplarQuerier(user.InjectOrgID(context.Background(), "123|456"))
		require.NoError(t, err)

		exemplars, err := q.Select(0, now.UnixMilli(), matchers...)
		require.NoError(t, err)
		require.Len(t, exemplars, 1)
		assert.Equal(t, exemplars[0].SeriesLabels.Get("__tenant_id__"), "123")
		assert.Equal(t, exemplars[0].Exemplars[0].Value, 123.4)
		assert.Equal(t, exemplars[0].Exemplars[0].Labels.Get("traceID"), "abc123")
	})

	// There are no matchers that reference __tenant_id__, make sure we get the expected
	// number of exemplar results (i.e. one from each of the two upstream queriers).
	t.Run("two tenants no filtering", func(t *testing.T) {
		matchers := [][]*labels.Matcher{{
			labels.MustNewMatcher(labels.MatchEqual, "__name__", "request_duration_seconds"),
		}}

		res1, res2 := fixtureResults()
		upstream := &mockExemplarQueryable{queriers: map[string]storage.ExemplarQuerier{
			"123": &mockExemplarQuerier{res: res1},
			"456": &mockExemplarQuerier{res: res2},
		}}

		federated := NewExemplarQueryable(upstream, false, test.NewTestingLogger(t))
		q, err := federated.ExemplarQuerier(user.InjectOrgID(context.Background(), "123|456"))
		require.NoError(t, err)

		exemplars, err := q.Select(0, now.UnixMilli(), matchers...)
		assert.NoError(t, err)
		assert.Len(t, exemplars, 2)
	})

	// Each of the groups of matchers require a specific __tenant_id__ BUT, since they
	// each reference one of the two IDs we're querying for we end up returning results
	// from each of the upstream queriers.
	t.Run("two tenants two groups of matchers filtering", func(t *testing.T) {
		matchers := [][]*labels.Matcher{
			{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "request_duration_seconds"),
				labels.MustNewMatcher(labels.MatchEqual, "__tenant_id__", "123"),
			},
			{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "request_duration_seconds"),
				labels.MustNewMatcher(labels.MatchEqual, "__tenant_id__", "456"),
			},
		}

		res1, res2 := fixtureResults()
		upstream := &mockExemplarQueryable{queriers: map[string]storage.ExemplarQuerier{
			"123": &mockExemplarQuerier{res: res1},
			"456": &mockExemplarQuerier{res: res2},
		}}

		federated := NewExemplarQueryable(upstream, false, test.NewTestingLogger(t))
		q, err := federated.ExemplarQuerier(user.InjectOrgID(context.Background(), "123|456"))
		require.NoError(t, err)

		exemplars, err := q.Select(0, now.UnixMilli(), matchers...)
		assert.NoError(t, err)
		assert.Len(t, exemplars, 2)
	})

	// With no matchers, none of the results returned from each of the upstream queriers
	// will actually be selected. Ensure that we get no results from the merge querier in
	// this case.
	t.Run("no matchers to filter", func(t *testing.T) {
		var matchers [][]*labels.Matcher

		res1, res2 := fixtureResults()
		upstream := &mockExemplarQueryable{queriers: map[string]storage.ExemplarQuerier{
			"123": &mockExemplarQuerier{res: res1},
			"456": &mockExemplarQuerier{res: res2},
		}}

		federated := NewExemplarQueryable(upstream, false, test.NewTestingLogger(t))
		q, err := federated.ExemplarQuerier(user.InjectOrgID(context.Background(), "123|456"))
		require.NoError(t, err)

		exemplars, err := q.Select(0, now.UnixMilli(), matchers...)
		assert.NoError(t, err)
		assert.Empty(t, exemplars)
	})

	// Ensure that an error from an upstream querier means we get an error response from the
	// merge querier, regardless if other queriers return results successfully.
	t.Run("upstream error", func(t *testing.T) {
		matchers := [][]*labels.Matcher{{
			labels.MustNewMatcher(labels.MatchEqual, "__name__", "request_duration_seconds"),
		}}

		res1, _ := fixtureResults()
		upstream := &mockExemplarQueryable{queriers: map[string]storage.ExemplarQuerier{
			"123": &mockExemplarQuerier{res: res1},
			"456": &mockExemplarQuerier{err: errors.New("timeout running exemplar query")},
		}}

		federated := NewExemplarQueryable(upstream, false, test.NewTestingLogger(t))
		q, err := federated.ExemplarQuerier(user.InjectOrgID(context.Background(), "123|456"))
		require.NoError(t, err)

		exemplars, err := q.Select(0, now.UnixMilli(), matchers...)
		assert.Error(t, err)
		assert.Empty(t, exemplars)
	})
}
