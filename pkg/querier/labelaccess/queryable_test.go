// SPDX-License-Identifier: AGPL-3.0-only

package labelaccess

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	shared "github.com/grafana/mimir/pkg/labelaccess"
)

func newSingleLabelPolicy(t labels.MatchType, n string, v string) *shared.LabelPolicy {
	return &shared.LabelPolicy{
		Selector: []*labels.Matcher{labels.MustNewMatcher(t, n, v)},
	}
}

func newDoubleLabelPolicy(t labels.MatchType, n1 string, v1 string, n2 string, v2 string) *shared.LabelPolicy {
	return &shared.LabelPolicy{
		Selector: []*labels.Matcher{
			labels.MustNewMatcher(t, n1, v1),
			labels.MustNewMatcher(t, n2, v2),
		},
	}
}

func TestLabelAccessQuerier_Select(t *testing.T) {
	t.Run("label matchers not in context", func(t *testing.T) {
		ctx := user.InjectOrgID(context.Background(), "test")
		next := &mockQueryable{
			querier: func(mint, maxt int64) (storage.Querier, error) {
				require.Equal(t, int64(0), mint)
				require.Equal(t, int64(1), maxt)
				return &mockSelectQuerier{
					selectFn: func(_ context.Context, _ bool, _ *storage.SelectHints, _ ...*labels.Matcher) storage.SeriesSet {
						return nil
					},
				}, nil
			},
		}
		queryable := &labelAccessQueryable{
			logger: log.NewNopLogger(),
			next:   next,
		}
		q, err := queryable.Querier(0, 1)
		require.NoError(t, err)
		ss := q.Select(ctx, false, nil)
		require.Nil(t, ss, "the Select call should be delegated to the upstream querier")
	})

	t.Run("empty label matchers set in context", func(t *testing.T) {
		const tenantID = "test"
		policySet := shared.LabelPolicySet{
			tenantID: {},
		}
		ctx := user.InjectOrgID(shared.InjectLabelMatchersContext(context.Background(), policySet), tenantID)
		next := &mockQueryable{
			querier: func(mint, maxt int64) (storage.Querier, error) {
				require.Equal(t, int64(0), mint)
				require.Equal(t, int64(1), maxt)
				return &mockSelectQuerier{
					selectFn: func(_ context.Context, _ bool, _ *storage.SelectHints, _ ...*labels.Matcher) storage.SeriesSet {
						return nil
					},
				}, nil
			},
		}
		queryable := &labelAccessQueryable{
			logger: log.NewNopLogger(),
			next:   next,
		}
		q, err := queryable.Querier(0, 1)
		require.NoError(t, err)
		ss := q.Select(ctx, false, nil)
		require.Nil(t, ss, "the Select call should be delegated to the upstream querier")
	})

	t.Run("single label selectors use upstream matchers and dedupe", func(t *testing.T) {
		const tenantID = "test"

		// One LBAC matcher is a duplicate of the matchers passed to the Select() call. Make
		// sure that only a single instance of the matcher is passed to the upstream querier.
		selectMatcher1 := labels.MustNewMatcher(labels.MatchEqual, "env", "prd")
		selectMatcher2 := labels.MustNewMatcher(labels.MatchEqual, "user", tenantID)
		lbacMatcher1 := labels.MustNewMatcher(labels.MatchNotEqual, "class", "secret")
		lbacMatcher2 := labels.MustNewMatcher(labels.MatchEqual, "user", tenantID)

		policySet := shared.LabelPolicySet{
			tenantID: {
				{
					Selector: []*labels.Matcher{lbacMatcher1, lbacMatcher2},
				},
			},
		}
		ctx := user.InjectOrgID(shared.InjectLabelMatchersContext(context.Background(), policySet), tenantID)
		next := &mockQueryable{
			querier: func(mint, maxt int64) (storage.Querier, error) {
				require.Equal(t, int64(0), mint)
				require.Equal(t, int64(1), maxt)
				return &mockSelectQuerier{
					selectFn: func(_ context.Context, _ bool, _ *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
						require.Equal(t, []*labels.Matcher{selectMatcher1, selectMatcher2, lbacMatcher1}, matchers)
						return nil
					},
				}, nil
			},
		}
		queryable := &labelAccessQueryable{
			logger: log.NewNopLogger(),
			next:   next,
		}
		q, err := queryable.Querier(0, 1)
		require.NoError(t, err)
		ss := q.Select(ctx, false, nil, selectMatcher1, selectMatcher2)
		require.Nil(t, ss, "the Select call should be delegated to the upstream querier")
	})
}

func TestLabelAccessChunkQuerier_Select(t *testing.T) {
	t.Run("label matchers not in context", func(t *testing.T) {
		ctx := user.InjectOrgID(context.Background(), "test")
		next := &mockQueryable{
			chunkQuerier: func(mint, maxt int64) (storage.ChunkQuerier, error) {
				require.Equal(t, int64(0), mint)
				require.Equal(t, int64(1), maxt)
				return &mockChunkSelectQuerier{
					selectFn: func(_ context.Context, _ bool, _ *storage.SelectHints, _ ...*labels.Matcher) storage.ChunkSeriesSet {
						return nil
					},
				}, nil
			},
		}
		queryable := &labelAccessQueryable{
			logger: log.NewNopLogger(),
			next:   next,
		}
		q, err := queryable.ChunkQuerier(0, 1)
		require.NoError(t, err)
		ss := q.Select(ctx, false, nil)
		require.Nil(t, ss, "the Select call should be delegated to the upstream querier")
	})

	t.Run("empty label matchers set in context", func(t *testing.T) {
		const tenantID = "test"
		policySet := shared.LabelPolicySet{
			tenantID: {},
		}
		ctx := user.InjectOrgID(shared.InjectLabelMatchersContext(context.Background(), policySet), tenantID)
		next := &mockQueryable{
			chunkQuerier: func(mint, maxt int64) (storage.ChunkQuerier, error) {
				require.Equal(t, int64(0), mint)
				require.Equal(t, int64(1), maxt)
				return &mockChunkSelectQuerier{
					selectFn: func(_ context.Context, _ bool, _ *storage.SelectHints, _ ...*labels.Matcher) storage.ChunkSeriesSet {
						return nil
					},
				}, nil
			},
		}
		queryable := &labelAccessQueryable{
			logger: log.NewNopLogger(),
			next:   next,
		}
		q, err := queryable.ChunkQuerier(0, 1)
		require.NoError(t, err)
		ss := q.Select(ctx, false, nil)
		require.Nil(t, ss, "the Select call should be delegated to the upstream querier")
	})

	t.Run("single label selector merges LBAC matchers into upstream call", func(t *testing.T) {
		const tenantID = "test"

		selectMatcher1 := labels.MustNewMatcher(labels.MatchEqual, "env", "prd")
		selectMatcher2 := labels.MustNewMatcher(labels.MatchEqual, "user", tenantID)
		lbacMatcher1 := labels.MustNewMatcher(labels.MatchNotEqual, "class", "secret")
		lbacMatcher2 := labels.MustNewMatcher(labels.MatchEqual, "user", tenantID) // duplicate of selectMatcher2

		policySet := shared.LabelPolicySet{
			tenantID: {
				{Selector: []*labels.Matcher{lbacMatcher1, lbacMatcher2}},
			},
		}
		ctx := user.InjectOrgID(shared.InjectLabelMatchersContext(context.Background(), policySet), tenantID)
		next := &mockQueryable{
			chunkQuerier: func(mint, maxt int64) (storage.ChunkQuerier, error) {
				require.Equal(t, int64(0), mint)
				require.Equal(t, int64(1), maxt)
				return &mockChunkSelectQuerier{
					selectFn: func(_ context.Context, _ bool, _ *storage.SelectHints, matchers ...*labels.Matcher) storage.ChunkSeriesSet {
						require.Equal(t, []*labels.Matcher{selectMatcher1, selectMatcher2, lbacMatcher1}, matchers)
						return nil
					},
				}, nil
			},
		}
		queryable := &labelAccessQueryable{
			logger: log.NewNopLogger(),
			next:   next,
		}
		q, err := queryable.ChunkQuerier(0, 1)
		require.NoError(t, err)
		ss := q.Select(ctx, false, nil, selectMatcher1, selectMatcher2)
		require.Nil(t, ss, "the Select call should be delegated to the upstream querier with merged matchers")
	})
}

func TestLabelAccessChunkSeriesSet_Next_ConsumesSkippedSeriesChunksOnceAcrossChunkAccessMethods(t *testing.T) {
	var skippedIteratorCalls int
	var allowedIteratorCalls int

	set := &labelAccessChunkSeriesSet{
		selectors: []promSelector{
			{labels.MustNewMatcher(labels.MatchEqual, "allow", "true")},
		},
		upstream: &mockChunkSeriesSet{
			series: []storage.ChunkSeries{
				&trackingChunkSeries{
					labels:        labels.FromStrings("allow", "false"),
					iteratorCalls: &skippedIteratorCalls,
				},
				&trackingChunkSeries{
					labels:        labels.FromStrings("allow", "true"),
					iteratorCalls: &allowedIteratorCalls,
				},
			},
		},
		logger: log.NewNopLogger(),
	}

	require.True(t, set.Next())
	require.NotNil(t, set.At())

	// Call all three access methods: skipped series must be consumed exactly once
	// across all of them, even though each path previously drained seriesToSkip independently.
	_, err := set.At().ChunkCount()
	require.NoError(t, err)
	_ = set.At().Iterator(nil)
	factory := set.At().IteratorFactory()
	require.NotNil(t, factory)
	_ = factory.Iterator(nil)

	// Skipped series drained exactly once despite three access method calls.
	assert.Equal(t, 1, skippedIteratorCalls)
	// Iterator and IteratorFactory().Iterator each call through; ChunkCount does not.
	assert.Equal(t, 2, allowedIteratorCalls)
}

func TestPromSelector_Matches(t *testing.T) {
	t.Run("does not match", func(t *testing.T) {
		matcher1, err := labels.NewMatcher(labels.MatchEqual, "method", "GET")
		require.NoError(t, err)
		matcher2, err := labels.NewMatcher(labels.MatchEqual, "path", "/")
		require.NoError(t, err)

		selector := promSelector{matcher1, matcher2}
		assert.False(t, selector.matches(labels.FromStrings(
			"method", "GET",
			"path", "/login",
		)))
	})

	t.Run("matches", func(t *testing.T) {
		matcher1 := labels.MustNewMatcher(labels.MatchEqual, "method", "GET")
		matcher2 := labels.MustNewMatcher(labels.MatchEqual, "path", "/")

		selector := promSelector{matcher1, matcher2}
		assert.True(t, selector.matches(labels.FromStrings(
			"method", "GET",
			"path", "/",
		)))
	})
}

type trackingChunkSeries struct {
	labels        labels.Labels
	iteratorCalls *int
}

func (s *trackingChunkSeries) Labels() labels.Labels {
	return s.labels
}

func (s *trackingChunkSeries) Iterator(chunks.Iterator) chunks.Iterator {
	*s.iteratorCalls = *s.iteratorCalls + 1
	return noopChunkIterator{}
}

func (s *trackingChunkSeries) ChunkCount() (int, error) {
	return 0, nil
}

func (s *trackingChunkSeries) IteratorFactory() storage.ChunkIterable {
	return s
}

type mockChunkSeriesSet struct {
	series []storage.ChunkSeries
	idx    int
}

func (s *mockChunkSeriesSet) Next() bool {
	if s.idx >= len(s.series) {
		return false
	}
	s.idx++
	return true
}

func (s *mockChunkSeriesSet) At() storage.ChunkSeries {
	if s.idx == 0 || s.idx > len(s.series) {
		return nil
	}
	return s.series[s.idx-1]
}

func (s *mockChunkSeriesSet) Err() error {
	return nil
}

func (s *mockChunkSeriesSet) Warnings() annotations.Annotations {
	return nil
}

type noopChunkIterator struct{}

func (noopChunkIterator) At() chunks.Meta {
	return chunks.Meta{}
}

func (noopChunkIterator) Next() bool {
	return false
}

func (noopChunkIterator) Err() error {
	return nil
}

func TestLabelPoliciesToPromSelectors(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		policies := []*shared.LabelPolicy{
			newSingleLabelPolicy(labels.MatchEqual, "status_code", "200"),
			newSingleLabelPolicy(labels.MatchEqual, "method", "GET"),
		}

		matcher1 := labels.MustNewMatcher(labels.MatchEqual, "status_code", "200")
		matcher2 := labels.MustNewMatcher(labels.MatchEqual, "method", "GET")
		selectors := labelPoliciesToPromSelectors(policies)

		assert.Equal(t, []promSelector{{matcher1}, {matcher2}}, selectors)
	})
}

type mockQuerier struct {
	series []labels.Labels
	err    error
}

func (q *mockQuerier) LabelValues(_ context.Context, name string, _ *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	if q.err != nil {
		return nil, nil, q.err
	}

	// Verify that matchers passed to this querier do not have any duplicates.
	unique := make(map[string]struct{})
	for _, m := range matchers {
		s := m.String()
		if _, ok := unique[s]; ok {
			panic(fmt.Sprintf("unexpected duplicate matcher %s. existing %+v", s, unique))
		}
		unique[s] = struct{}{}
	}

	var values []string
	for _, s := range q.series {
		target := s.Get(name)
		if target == "" {
			continue
		}

		matched := true
		for _, m := range matchers {
			if v := s.Get(m.Name); !m.Matches(v) {
				matched = false
				break
			}
		}

		if matched {
			values = append(values, target)
		}
	}

	return values, nil, nil
}

func (q *mockQuerier) LabelNames(_ context.Context, _ *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	if q.err != nil {
		return nil, nil, q.err
	}

	// Verify that matchers passed to this querier do not have any duplicates.
	unique := make(map[string]struct{})
	for _, m := range matchers {
		s := m.String()
		if _, ok := unique[s]; ok {
			panic(fmt.Sprintf("unexpected duplicate matcher %s. existing %+v", s, unique))
		}
		unique[s] = struct{}{}
	}

	var values []string
	for _, s := range q.series {
		matched := true
		for _, m := range matchers {
			if v := s.Get(m.Name); !m.Matches(v) {
				matched = false
				break
			}
		}

		if !matched {
			continue
		}

		s.Range(func(l labels.Label) {
			if !slices.Contains(values, l.Name) {
				values = append(values, l.Name)
			}
		})
	}

	sort.Strings(values)
	return values, nil, nil
}

func (q *mockQuerier) Close() error {
	panic("unimplemented")
}

func (q *mockQuerier) Select(_ context.Context, _ bool, _ *storage.SelectHints, _ ...*labels.Matcher) storage.SeriesSet {
	return storage.EmptySeriesSet()
}

func TestMockQuerier_LabelValues(t *testing.T) {
	q := mockQuerier{
		series: []labels.Labels{
			labels.FromStrings("env", "dev", "class", "open", "foo", "bar"),
			labels.FromStrings("env", "prd", "class", "secret", "foo", "bar"),
		},
	}

	ctx := context.Background()
	matcher1 := labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")
	matcher2 := labels.MustNewMatcher(labels.MatchEqual, "class", "open")

	t.Run("single matcher", func(t *testing.T) {
		lbls, _, err := q.LabelValues(ctx, "env", &storage.LabelHints{}, matcher1)
		require.NoError(t, err)
		require.Len(t, lbls, 2)
		sort.Strings(lbls)
		assert.Equal(t, []string{"dev", "prd"}, lbls)
	})

	t.Run("multiple matchers", func(t *testing.T) {
		lbls, _, err := q.LabelValues(ctx, "env", &storage.LabelHints{}, matcher1, matcher2)
		require.NoError(t, err)
		require.Len(t, lbls, 1)
		assert.Equal(t, "dev", lbls[0])
	})
}

func TestMockQuerier_LabelNames(t *testing.T) {
	q := mockQuerier{
		series: []labels.Labels{
			labels.FromStrings("env", "dev", "class", "open", "foo", "bar", "series1", "value1"),
			labels.FromStrings("env", "prd", "class", "secret", "foo", "bar", "series2", "value2"),
		},
	}

	ctx := context.Background()
	matcher1 := labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")
	matcher2 := labels.MustNewMatcher(labels.MatchEqual, "class", "open")

	t.Run("single matcher", func(t *testing.T) {
		names, _, err := q.LabelNames(ctx, &storage.LabelHints{}, matcher1)
		require.NoError(t, err)
		require.Len(t, names, 5)
		sort.Strings(names)
		assert.Equal(t, []string{"class", "env", "foo", "series1", "series2"}, names)
	})

	t.Run("multiple matchers", func(t *testing.T) {
		names, _, err := q.LabelNames(ctx, &storage.LabelHints{}, matcher1, matcher2)
		require.NoError(t, err)
		require.Len(t, names, 4)
		sort.Strings(names)
		assert.Equal(t, []string{"class", "env", "foo", "series1"}, names)
	})
}

func TestLabelAccessQuerier_LabelValues(t *testing.T) {
	upstream := mockQuerier{
		series: []labels.Labels{
			labels.FromStrings("env", "dev", "class", "open", "foo", "bar"),
			labels.FromStrings("env", "prd", "class", "secret", "foo", "bar"),
		},
	}
	const tenantID = "test-instance"
	ctx := user.InjectOrgID(context.Background(), tenantID)

	t.Run("no selectors", func(t *testing.T) {
		q := labelAccessQuerier{querier: &upstream, labelQuerier: &upstream, logger: log.NewNopLogger()}
		ctx = shared.InjectLabelMatchersContext(ctx, shared.LabelPolicySet{})
		lbls, _, err := q.LabelValues(ctx, "class", &storage.LabelHints{})
		require.NoError(t, err)
		sort.Strings(lbls)
		assert.Equal(t, []string{"open", "secret"}, lbls)
	})

	t.Run("single selector", func(t *testing.T) {
		q := labelAccessQuerier{querier: &upstream, labelQuerier: &upstream, logger: log.NewNopLogger()}
		ctx = shared.InjectLabelMatchersContext(ctx, shared.LabelPolicySet{
			tenantID: {{Selector: []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, "class", "secret")}}},
		})
		lbls, _, err := q.LabelValues(ctx, "env", &storage.LabelHints{})
		require.NoError(t, err)
		assert.Equal(t, []string{"dev"}, lbls)
	})

	t.Run("multiple selectors", func(t *testing.T) {
		q := labelAccessQuerier{querier: &upstream, labelQuerier: &upstream, logger: log.NewNopLogger()}
		ctx = shared.InjectLabelMatchersContext(ctx, shared.LabelPolicySet{
			tenantID: {
				{Selector: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "env", "prd")}},
				{Selector: []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, "class", "secret")}},
			},
		})
		for range 20 {
			lbls, _, err := q.LabelValues(ctx, "env", &storage.LabelHints{})
			require.NoError(t, err)
			assert.Equal(t, []string{"dev", "prd"}, lbls)
		}
	})

	t.Run("single selector and matcher", func(t *testing.T) {
		q := labelAccessQuerier{querier: &upstream, labelQuerier: &upstream, logger: log.NewNopLogger()}
		ctx = shared.InjectLabelMatchersContext(ctx, shared.LabelPolicySet{
			tenantID: {{Selector: []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, "class", "secret")}}},
		})
		matcher := labels.MustNewMatcher(labels.MatchEqual, "env", "dev")
		lbls, _, err := q.LabelValues(ctx, "class", &storage.LabelHints{}, matcher)
		require.NoError(t, err)
		assert.Equal(t, []string{"open"}, lbls)
	})

	t.Run("single selector and matcher no values", func(t *testing.T) {
		q := labelAccessQuerier{querier: &upstream, labelQuerier: &upstream, logger: log.NewNopLogger()}
		ctx = shared.InjectLabelMatchersContext(ctx, shared.LabelPolicySet{
			tenantID: {{Selector: []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, "class", "secret")}}},
		})
		matcher := labels.MustNewMatcher(labels.MatchEqual, "env", "prd")
		lbls, _, err := q.LabelValues(ctx, "foo", &storage.LabelHints{}, matcher)
		require.NoError(t, err)
		assert.Empty(t, lbls)
	})

	t.Run("single selector and duplicate matcher", func(t *testing.T) {
		q := labelAccessQuerier{querier: &upstream, labelQuerier: &upstream, logger: log.NewNopLogger()}
		ctx = shared.InjectLabelMatchersContext(ctx, shared.LabelPolicySet{
			tenantID: {{Selector: []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, "class", "secret")}}},
		})
		matcher1 := labels.MustNewMatcher(labels.MatchEqual, "env", "dev")
		matcher2 := labels.MustNewMatcher(labels.MatchNotEqual, "class", "secret")
		lbls, _, err := q.LabelValues(ctx, "class", &storage.LabelHints{}, matcher1, matcher2)
		require.NoError(t, err)
		assert.Equal(t, []string{"open"}, lbls)
	})

	t.Run("multiple selectors and duplicate matcher", func(t *testing.T) {
		q := labelAccessQuerier{querier: &upstream, labelQuerier: &upstream, logger: log.NewNopLogger()}
		ctx = shared.InjectLabelMatchersContext(ctx, shared.LabelPolicySet{
			tenantID: {
				{Selector: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "env", "dev")}},
				{Selector: []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, "class", "secret")}},
			},
		})
		matcher1 := labels.MustNewMatcher(labels.MatchEqual, "env", "dev")
		matcher2 := labels.MustNewMatcher(labels.MatchNotEqual, "class", "secret")
		lbls, _, err := q.LabelValues(ctx, "class", &storage.LabelHints{}, matcher1, matcher2)
		require.NoError(t, err)
		assert.Equal(t, []string{"open"}, lbls)
	})

	t.Run("upstream error", func(t *testing.T) {
		upstreamErr := mockQuerier{err: errors.New("something bad")}
		q := labelAccessQuerier{querier: &upstreamErr, labelQuerier: &upstreamErr, logger: log.NewNopLogger()}
		ctx = shared.InjectLabelMatchersContext(ctx, shared.LabelPolicySet{
			tenantID: {
				{Selector: []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, "env", "")}},
				{Selector: []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, "class", "secret")}},
			},
		})
		lbls, _, err := q.LabelValues(ctx, "env", &storage.LabelHints{})
		assert.Empty(t, lbls)
		assert.Error(t, err)
	})
}

func TestLabelAccessQuerier_LabelNames(t *testing.T) {
	upstream := mockQuerier{
		series: []labels.Labels{
			labels.FromStrings("env", "dev", "class", "open", "foo", "bar", "series1", "value1"),
			labels.FromStrings("env", "prd", "class", "secret", "foo", "bar", "series2", "value2"),
			labels.FromStrings("env", "stg", "class", "secret", "foo", "bar", "series3", "value2"),
		},
	}
	const tenantID = "test-instance"
	ctx := user.InjectOrgID(context.Background(), tenantID)

	t.Run("no selectors", func(t *testing.T) {
		q := labelAccessQuerier{querier: &upstream, labelQuerier: &upstream, logger: log.NewNopLogger()}
		ctx = shared.InjectLabelMatchersContext(ctx, shared.LabelPolicySet{})
		names, _, err := q.LabelNames(ctx, &storage.LabelHints{})
		require.NoError(t, err)
		assert.Equal(t, []string{"class", "env", "foo", "series1", "series2", "series3"}, names)
	})

	t.Run("single selector", func(t *testing.T) {
		q := labelAccessQuerier{querier: &upstream, labelQuerier: &upstream, logger: log.NewNopLogger()}
		ctx = shared.InjectLabelMatchersContext(ctx, shared.LabelPolicySet{
			tenantID: {{Selector: []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, "class", "secret")}}},
		})
		names, _, err := q.LabelNames(ctx, &storage.LabelHints{})
		require.NoError(t, err)
		assert.Equal(t, []string{"class", "env", "foo", "series1"}, names)
	})

	t.Run("multiple selectors", func(t *testing.T) {
		q := labelAccessQuerier{querier: &upstream, labelQuerier: &upstream, logger: log.NewNopLogger()}
		ctx = shared.InjectLabelMatchersContext(ctx, shared.LabelPolicySet{
			tenantID: {
				{Selector: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "env", "prd")}},
				{Selector: []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, "class", "secret")}},
			},
		})
		names, _, err := q.LabelNames(ctx, &storage.LabelHints{})
		require.NoError(t, err)
		assert.Equal(t, []string{"class", "env", "foo", "series1", "series2"}, names)
	})

	t.Run("single selector and matcher", func(t *testing.T) {
		q := labelAccessQuerier{querier: &upstream, labelQuerier: &upstream, logger: log.NewNopLogger()}
		ctx = shared.InjectLabelMatchersContext(ctx, shared.LabelPolicySet{
			tenantID: {{Selector: []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, "class", "secret")}}},
		})
		matcher := labels.MustNewMatcher(labels.MatchEqual, "env", "dev")
		names, _, err := q.LabelNames(ctx, &storage.LabelHints{}, matcher)
		require.NoError(t, err)
		assert.Equal(t, []string{"class", "env", "foo", "series1"}, names)
	})

	t.Run("single selector and matcher no values", func(t *testing.T) {
		q := labelAccessQuerier{querier: &upstream, labelQuerier: &upstream, logger: log.NewNopLogger()}
		ctx = shared.InjectLabelMatchersContext(ctx, shared.LabelPolicySet{
			tenantID: {{Selector: []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, "class", "secret")}}},
		})
		matcher := labels.MustNewMatcher(labels.MatchEqual, "env", "prd")
		names, _, err := q.LabelNames(ctx, &storage.LabelHints{}, matcher)
		require.NoError(t, err)
		assert.Empty(t, names)
	})

	t.Run("single selector and duplicate matcher", func(t *testing.T) {
		q := labelAccessQuerier{querier: &upstream, labelQuerier: &upstream, logger: log.NewNopLogger()}
		ctx = shared.InjectLabelMatchersContext(ctx, shared.LabelPolicySet{
			tenantID: {{Selector: []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, "class", "secret")}}},
		})
		matcher1 := labels.MustNewMatcher(labels.MatchEqual, "env", "dev")
		matcher2 := labels.MustNewMatcher(labels.MatchNotEqual, "class", "secret")
		names, _, err := q.LabelNames(ctx, &storage.LabelHints{}, matcher1, matcher2)
		require.NoError(t, err)
		assert.Equal(t, []string{"class", "env", "foo", "series1"}, names)
	})

	t.Run("multiple selectors and duplicate matcher", func(t *testing.T) {
		q := labelAccessQuerier{querier: &upstream, labelQuerier: &upstream, logger: log.NewNopLogger()}
		ctx = shared.InjectLabelMatchersContext(ctx, shared.LabelPolicySet{
			tenantID: {
				{Selector: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "env", "dev")}},
				{Selector: []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, "class", "secret")}},
			},
		})
		matcher1 := labels.MustNewMatcher(labels.MatchEqual, "env", "dev")
		matcher2 := labels.MustNewMatcher(labels.MatchNotEqual, "class", "secret")
		names, _, err := q.LabelNames(ctx, &storage.LabelHints{}, matcher1, matcher2)
		require.NoError(t, err)
		assert.Equal(t, []string{"class", "env", "foo", "series1"}, names)
	})

	t.Run("upstream error", func(t *testing.T) {
		upstreamErr := mockQuerier{err: errors.New("something bad")}
		q := labelAccessQuerier{querier: &upstreamErr, labelQuerier: &upstreamErr, logger: log.NewNopLogger()}
		ctx = shared.InjectLabelMatchersContext(ctx, shared.LabelPolicySet{
			tenantID: {
				{Selector: []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, "env", "")}},
				{Selector: []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, "class", "secret")}},
			},
		})
		names, _, err := q.LabelNames(ctx, &storage.LabelHints{})
		assert.Empty(t, names)
		assert.Error(t, err)
	})
}

func TestLBACMetrics_FilteredSeries(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	metrics := newLBACMetrics(reg)

	// Test via labelAccessChunkSeriesSet, which has existing mock infrastructure.
	set := &labelAccessChunkSeriesSet{
		selectors: []promSelector{
			{labels.MustNewMatcher(labels.MatchEqual, "allow", "true")},
		},
		upstream: &mockChunkSeriesSet{
			series: []storage.ChunkSeries{
				&trackingChunkSeries{labels: labels.FromStrings("allow", "false")},
				&trackingChunkSeries{labels: labels.FromStrings("allow", "false")},
				&trackingChunkSeries{labels: labels.FromStrings("allow", "true")},
			},
		},
		logger:  log.NewNopLogger(),
		metrics: metrics,
	}

	for set.Next() {
	}

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_querier_lbac_filtered_series_total Total number of series filtered out by LBAC enforcement.
		# TYPE cortex_querier_lbac_filtered_series_total counter
		cortex_querier_lbac_filtered_series_total 2
		# HELP cortex_querier_lbac_policies_applied_total Total number of queries where LBAC enforcement was applied.
		# TYPE cortex_querier_lbac_policies_applied_total counter
		cortex_querier_lbac_policies_applied_total 0
	`), "cortex_querier_lbac_filtered_series_total", "cortex_querier_lbac_policies_applied_total"))
}

func TestLBACMetrics_PoliciesApplied(t *testing.T) {
	const tenantID = "test"
	reg := prometheus.NewPedanticRegistry()
	metrics := newLBACMetrics(reg)

	upstreamSeries := []labels.Labels{
		labels.FromStrings("env", "dev", "class", "open"),
		labels.FromStrings("env", "prd", "class", "secret"),
	}
	upstream := &mockQuerier{series: upstreamSeries}

	q := &labelAccessQuerier{
		querier:      upstream,
		labelQuerier: upstream,
		logger:       log.NewNopLogger(),
		metrics:      metrics,
	}

	// Single-selector LBAC: policiesAppliedTotal should be incremented.
	ctx := user.InjectOrgID(context.Background(), tenantID)
	ctx = shared.InjectLabelMatchersContext(ctx, shared.LabelPolicySet{
		tenantID: {{Selector: []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, "class", "secret")}}},
	})
	q.Select(ctx, false, nil)

	// Multi-selector LBAC: policiesAppliedTotal should be incremented again.
	ctx = shared.InjectLabelMatchersContext(ctx, shared.LabelPolicySet{
		tenantID: {
			{Selector: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "env", "dev")}},
			{Selector: []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, "class", "secret")}},
		},
	})
	q.Select(ctx, false, nil)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_querier_lbac_filtered_series_total Total number of series filtered out by LBAC enforcement.
		# TYPE cortex_querier_lbac_filtered_series_total counter
		cortex_querier_lbac_filtered_series_total 0
		# HELP cortex_querier_lbac_policies_applied_total Total number of queries where LBAC enforcement was applied.
		# TYPE cortex_querier_lbac_policies_applied_total counter
		cortex_querier_lbac_policies_applied_total 2
	`), "cortex_querier_lbac_filtered_series_total", "cortex_querier_lbac_policies_applied_total"))
}
