// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streaminglabelvalues"
	"github.com/grafana/mimir/pkg/util/validation"
)

// mockSearchQuerier satisfies storage.Querier and mimirSearcher. The search
// methods return either an err or the configured slice; non-search methods
// return zero values (not used in these tests).
//
// When metadata is non-nil the mock also satisfies metricMetadataFetcher,
// standing in for the distributor querier's ingester metadata fan-out.
type mockSearchQuerier struct {
	namesResults  []storage.SearchResult
	namesErr      error
	valuesResults []storage.SearchResult
	valuesErr     error

	metadata    map[string]metadata.Metadata
	metadataErr error
}

func (m *mockSearchQuerier) Select(_ context.Context, _ bool, _ *storage.SelectHints, _ ...*labels.Matcher) storage.SeriesSet {
	return storage.EmptySeriesSet()
}
func (m *mockSearchQuerier) LabelValues(_ context.Context, _ string, _ *storage.LabelHints, _ ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}
func (m *mockSearchQuerier) LabelNames(_ context.Context, _ *storage.LabelHints, _ ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}
func (m *mockSearchQuerier) Close() error { return nil }

func (m *mockSearchQuerier) SearchLabelNames(_ context.Context, _ *streaminglabelvalues.Params, _ *storage.SearchHints, _ ...*labels.Matcher) storage.SearchResultSet {
	if m.namesErr != nil {
		return storage.ErrSearchResultSet(m.namesErr)
	}
	return storage.NewSearchResultSetFromSlice(m.namesResults, nil)
}

func (m *mockSearchQuerier) SearchLabelValues(_ context.Context, _ string, _ *streaminglabelvalues.Params, _ *storage.SearchHints, _ ...*labels.Matcher) storage.SearchResultSet {
	if m.valuesErr != nil {
		return storage.ErrSearchResultSet(m.valuesErr)
	}
	return storage.NewSearchResultSetFromSlice(m.valuesResults, nil)
}

func (m *mockSearchQuerier) fetchMetricMetadata(_ context.Context, names []string) (map[string]metadata.Metadata, error) {
	if m.metadataErr != nil {
		return nil, m.metadataErr
	}
	out := make(map[string]metadata.Metadata, len(names))
	for _, n := range names {
		if md, ok := m.metadata[n]; ok {
			out[n] = md
		}
	}
	return out, nil
}

// mockSearchQueryable returns its configured mockSearchQuerier for any (mint, maxt).
type mockSearchQueryable struct {
	q *mockSearchQuerier
}

func (qq *mockSearchQueryable) Querier(_, _ int64) (storage.Querier, error) {
	return qq.q, nil
}

// nonMetadataSearchQuerier implements mimirSearcher and storage.Querier but NOT
// metricMetadataFetcher, standing in for a source that can't supply metadata
// (e.g. the blocks-store querier).
type nonMetadataSearchQuerier struct{ values []storage.SearchResult }

func (nonMetadataSearchQuerier) Select(_ context.Context, _ bool, _ *storage.SelectHints, _ ...*labels.Matcher) storage.SeriesSet {
	return storage.EmptySeriesSet()
}
func (nonMetadataSearchQuerier) LabelValues(_ context.Context, _ string, _ *storage.LabelHints, _ ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}
func (nonMetadataSearchQuerier) LabelNames(_ context.Context, _ *storage.LabelHints, _ ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}
func (nonMetadataSearchQuerier) Close() error { return nil }
func (nonMetadataSearchQuerier) SearchLabelNames(_ context.Context, _ *streaminglabelvalues.Params, _ *storage.SearchHints, _ ...*labels.Matcher) storage.SearchResultSet {
	return storage.EmptySearchResultSet()
}
func (q nonMetadataSearchQuerier) SearchLabelValues(_ context.Context, _ string, _ *streaminglabelvalues.Params, _ *storage.SearchHints, _ ...*labels.Matcher) storage.SearchResultSet {
	return storage.NewSearchResultSetFromSlice(q.values, nil)
}

type nonMetadataSearchQueryable struct{ q storage.Querier }

func (qq nonMetadataSearchQueryable) Querier(_, _ int64) (storage.Querier, error) { return qq.q, nil }

// buildSearchTestMultiQuerier wires a multiQuerier with two child queryables.
// Setting either to nil simulates that source not being configured.
func buildSearchTestMultiQuerier(t *testing.T, distQ, blockQ *mockSearchQuerier, opts ...func(*validation.Limits)) *multiQuerier {
	t.Helper()
	limits := defaultLimitsConfig()
	limits.QueryIngestersWithin = 0
	for _, opt := range opts {
		opt(&limits)
	}
	overrides := validation.NewOverrides(limits, nil)

	var cfg Config
	flagext.DefaultValues(&cfg)
	mq := &multiQuerier{
		queryMetrics: stats.NewQueryMetrics(prometheus.NewRegistry()),
		cfg:          cfg,
		minT:         0,
		maxT:         1000,
		limits:       overrides,
		logger:       log.NewNopLogger(),
	}
	if distQ != nil {
		mq.distributor = &mockSearchQueryable{q: distQ}
	}
	if blockQ != nil {
		mq.blockStore = &mockSearchQueryable{q: blockQ}
	}
	return mq
}

func searchResult(v string, score float64) storage.SearchResult {
	return storage.SearchResult{Value: v, Score: score}
}

func drainSearchResultSet(t *testing.T, rs storage.SearchResultSet) (vals []string, scores []float64, warns []string, err error) {
	t.Helper()
	defer rs.Close()
	for rs.Next() {
		r := rs.At()
		vals = append(vals, r.Value)
		scores = append(scores, r.Score)
	}
	err = rs.Err()
	for _, w := range rs.Warnings() {
		warns = append(warns, w.Error())
	}
	return
}

func TestMultiQuerier_SearchLabelNames_BothChildrenOK(t *testing.T) {
	distQ := &mockSearchQuerier{namesResults: []storage.SearchResult{searchResult("a", 1.0), searchResult("c", 1.0)}}
	blockQ := &mockSearchQuerier{namesResults: []storage.SearchResult{searchResult("b", 1.0), searchResult("d", 1.0)}}
	mq := buildSearchTestMultiQuerier(t, distQ, blockQ)

	ctx := user.InjectOrgID(context.Background(), "user-1")
	rs := mq.SearchLabelNames(ctx, nil, &storage.SearchHints{OrderBy: storage.OrderByValueAsc})
	vals, _, _, err := drainSearchResultSet(t, rs)
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c", "d"}, vals)
}

func TestMultiQuerier_SearchLabelNames_DistributorErrorSurfacesViaMerge(t *testing.T) {
	wantErr := errors.New("boom from distributor")
	distQ := &mockSearchQuerier{namesErr: wantErr}
	blockQ := &mockSearchQuerier{namesResults: []storage.SearchResult{searchResult("b", 1.0)}}
	mq := buildSearchTestMultiQuerier(t, distQ, blockQ)

	ctx := user.InjectOrgID(context.Background(), "user-1")
	rs := mq.SearchLabelNames(ctx, nil, &storage.SearchHints{OrderBy: storage.OrderByValueAsc})
	_, _, _, err := drainSearchResultSet(t, rs)
	require.Error(t, err)
	assert.ErrorIs(t, err, wantErr)
}

func TestMultiQuerier_SearchLabelNames_BlockStoreErrorSurfacesViaMerge(t *testing.T) {
	wantErr := errors.New("boom from blockstore")
	distQ := &mockSearchQuerier{namesResults: []storage.SearchResult{searchResult("a", 1.0)}}
	blockQ := &mockSearchQuerier{namesErr: wantErr}
	mq := buildSearchTestMultiQuerier(t, distQ, blockQ)

	ctx := user.InjectOrgID(context.Background(), "user-1")
	rs := mq.SearchLabelNames(ctx, nil, &storage.SearchHints{OrderBy: storage.OrderByValueAsc})
	_, _, _, err := drainSearchResultSet(t, rs)
	require.Error(t, err)
	assert.ErrorIs(t, err, wantErr)
}

func TestMultiQuerier_SearchLabelNames_BothChildrenEmptyReturnsEmpty(t *testing.T) {
	mq := buildSearchTestMultiQuerier(t, &mockSearchQuerier{}, &mockSearchQuerier{})
	ctx := user.InjectOrgID(context.Background(), "user-1")
	rs := mq.SearchLabelNames(ctx, nil, &storage.SearchHints{OrderBy: storage.OrderByValueAsc})
	vals, _, _, err := drainSearchResultSet(t, rs)
	require.NoError(t, err)
	assert.Empty(t, vals)
}

func TestMultiQuerier_SearchLabelNames_TimeRangeShortCircuit(t *testing.T) {
	// MaxLabelsQueryLength clamps minT to maxT - duration. With minT > maxT after
	// the implicit nothing-changes path, we can force errEmptyTimeRange via
	// minT > maxT directly.
	distQ := &mockSearchQuerier{namesResults: []storage.SearchResult{searchResult("a", 1.0)}}
	mq := buildSearchTestMultiQuerier(t, distQ, nil)
	mq.minT = 1000
	mq.maxT = 500 // minT > maxT triggers validateQueryTimeRange's empty-range path.

	ctx := user.InjectOrgID(context.Background(), "user-1")
	rs := mq.SearchLabelNames(ctx, nil, &storage.SearchHints{OrderBy: storage.OrderByValueAsc})
	vals, _, _, err := drainSearchResultSet(t, rs)
	require.NoError(t, err)
	assert.Empty(t, vals)
}

func TestMultiQuerier_SearchLabelNames_LimitClampTriggersWarning(t *testing.T) {
	distQ := &mockSearchQuerier{namesResults: []storage.SearchResult{searchResult("a", 1.0), searchResult("b", 1.0), searchResult("c", 1.0)}}
	mq := buildSearchTestMultiQuerier(t, distQ, nil, func(l *validation.Limits) {
		l.MaxLabelNamesLimit = 100
	})

	ctx := user.InjectOrgID(context.Background(), "user-1")
	rs := mq.SearchLabelNames(ctx, nil, &storage.SearchHints{OrderBy: storage.OrderByValueAsc, Limit: 10_000})
	_, _, warns, err := drainSearchResultSet(t, rs)
	require.NoError(t, err)
	require.Len(t, warns, 1, "expected one clamp warning")
	assert.Contains(t, warns[0], validation.MaxLabelNamesLimitFlag)
}

func TestMultiQuerier_SearchLabelNames_LimitClampNotTriggered(t *testing.T) {
	distQ := &mockSearchQuerier{namesResults: []storage.SearchResult{searchResult("a", 1.0)}}
	mq := buildSearchTestMultiQuerier(t, distQ, nil, func(l *validation.Limits) {
		l.MaxLabelNamesLimit = 100
	})

	ctx := user.InjectOrgID(context.Background(), "user-1")
	rs := mq.SearchLabelNames(ctx, nil, &storage.SearchHints{OrderBy: storage.OrderByValueAsc, Limit: 50})
	_, _, warns, err := drainSearchResultSet(t, rs)
	require.NoError(t, err)
	assert.Empty(t, warns)
}

func TestMultiQuerier_SearchLabelNames_DedupAcrossSourcesKeepsHighestScoreUnderValueOrder(t *testing.T) {
	// Same value "foo" appears in both sources. Under OrderByValueAsc the merge
	// collapses to one entry, keeping the higher score.
	distQ := &mockSearchQuerier{namesResults: []storage.SearchResult{searchResult("bar", 1.0), searchResult("foo", 0.7)}}
	blockQ := &mockSearchQuerier{namesResults: []storage.SearchResult{searchResult("baz", 1.0), searchResult("foo", 0.9)}}
	mq := buildSearchTestMultiQuerier(t, distQ, blockQ)

	ctx := user.InjectOrgID(context.Background(), "user-1")
	rs := mq.SearchLabelNames(ctx, nil, &storage.SearchHints{OrderBy: storage.OrderByValueAsc})
	vals, scores, _, err := drainSearchResultSet(t, rs)
	require.NoError(t, err)
	assert.Equal(t, []string{"bar", "baz", "foo"}, vals)
	require.Len(t, scores, 3)
	assert.InDelta(t, 0.9, scores[2], 1e-9, "duplicate value collapses to higher score under value ordering")
}

func TestMultiQuerier_SearchLabelNames_OrderByValueDesc(t *testing.T) {
	distQ := &mockSearchQuerier{namesResults: []storage.SearchResult{searchResult("c", 1.0), searchResult("a", 1.0)}}
	blockQ := &mockSearchQuerier{namesResults: []storage.SearchResult{searchResult("d", 1.0), searchResult("b", 1.0)}}
	mq := buildSearchTestMultiQuerier(t, distQ, blockQ)

	ctx := user.InjectOrgID(context.Background(), "user-1")
	rs := mq.SearchLabelNames(ctx, nil, &storage.SearchHints{OrderBy: storage.OrderByValueDesc})
	vals, _, _, err := drainSearchResultSet(t, rs)
	require.NoError(t, err)
	assert.Equal(t, []string{"d", "c", "b", "a"}, vals)
}

func TestMultiQuerier_SearchLabelNames_OrderByScoreDesc(t *testing.T) {
	// Sources pre-sorted by (Score desc, Value asc).
	distQ := &mockSearchQuerier{namesResults: []storage.SearchResult{searchResult("alpha", 0.95), searchResult("delta", 0.5)}}
	blockQ := &mockSearchQuerier{namesResults: []storage.SearchResult{searchResult("beta", 0.9), searchResult("gamma", 0.6)}}
	mq := buildSearchTestMultiQuerier(t, distQ, blockQ)

	ctx := user.InjectOrgID(context.Background(), "user-1")
	rs := mq.SearchLabelNames(ctx, nil, &storage.SearchHints{OrderBy: storage.OrderByScoreDesc})
	vals, _, _, err := drainSearchResultSet(t, rs)
	require.NoError(t, err)
	assert.Equal(t, []string{"alpha", "beta", "gamma", "delta"}, vals)
}

func TestMultiQuerier_SearchLabelValues_ForwardsLabelName(t *testing.T) {
	distQ := &mockSearchQuerier{valuesResults: []storage.SearchResult{searchResult("prod", 1.0), searchResult("staging", 1.0)}}
	blockQ := &mockSearchQuerier{valuesResults: []storage.SearchResult{searchResult("dev", 1.0), searchResult("prod", 1.0)}}
	mq := buildSearchTestMultiQuerier(t, distQ, blockQ)

	ctx := user.InjectOrgID(context.Background(), "user-1")
	rs := mq.SearchLabelValues(ctx, "env", nil, &storage.SearchHints{OrderBy: storage.OrderByValueAsc})
	vals, _, _, err := drainSearchResultSet(t, rs)
	require.NoError(t, err)
	assert.Equal(t, []string{"dev", "prod", "staging"}, vals)
}

func TestMultiQuerier_SearchLabelValues_LimitClampUsesValuesLimit(t *testing.T) {
	distQ := &mockSearchQuerier{valuesResults: []storage.SearchResult{searchResult("a", 1.0), searchResult("b", 1.0), searchResult("c", 1.0)}}
	mq := buildSearchTestMultiQuerier(t, distQ, nil, func(l *validation.Limits) {
		l.MaxLabelValuesLimit = 100
		l.MaxLabelNamesLimit = 50 // distinct value so we can assert which one fired.
	})

	ctx := user.InjectOrgID(context.Background(), "user-1")
	rs := mq.SearchLabelValues(ctx, "env", nil, &storage.SearchHints{OrderBy: storage.OrderByValueAsc, Limit: 10_000})
	_, _, warns, err := drainSearchResultSet(t, rs)
	require.NoError(t, err)
	require.Len(t, warns, 1)
	assert.Contains(t, warns[0], validation.MaxLabelValuesLimitFlag)
	assert.NotContains(t, warns[0], validation.MaxLabelNamesLimitFlag)
}

func metadataResult(v string, score float64, md metadata.Metadata) storage.SearchResult {
	return storage.SearchResult{Value: v, Score: score, Metadata: &md}
}

func drainSearchResultSetWithMetadata(t *testing.T, rs storage.SearchResultSet) (vals []string, metas []*metadata.Metadata) {
	t.Helper()
	defer rs.Close()
	for rs.Next() {
		r := rs.At()
		vals = append(vals, r.Value)
		metas = append(metas, r.Metadata)
	}
	require.NoError(t, rs.Err())
	return
}

func TestMultiQuerier_SearchLabelValues_ShouldSupportMetadata(t *testing.T) {
	ctx := user.InjectOrgID(t.Context(), "user-1")

	t.Run("metric-name search enriches results with metadata fetched by name, surviving dedup across sources", func(t *testing.T) {
		distQ := &mockSearchQuerier{
			valuesResults: []storage.SearchResult{searchResult("a", 1.0), searchResult("b", 1.0)},
			metadata: map[string]metadata.Metadata{
				"a": {Type: model.MetricTypeCounter, Help: "help a", Unit: "s"},
				"b": {Type: model.MetricTypeGauge, Help: "help b"},
			},
		}
		// Blocks return "a" too but without metadata: the dedup happens below
		// the enrichment, so the metadata must still be attached.
		blockQ := &mockSearchQuerier{valuesResults: []storage.SearchResult{searchResult("a", 1.0)}}
		mq := buildSearchTestMultiQuerier(t, distQ, blockQ)

		params := &streaminglabelvalues.Params{IncludeMetadata: true}
		rs := mq.SearchLabelValues(ctx, model.MetricNameLabel, params, &storage.SearchHints{OrderBy: storage.OrderByValueAsc})
		vals, metas := drainSearchResultSetWithMetadata(t, rs)

		assert.Equal(t, []string{"a", "b"}, vals)
		require.NotNil(t, metas[0])
		assert.Equal(t, model.MetricTypeCounter, metas[0].Type)
		assert.Equal(t, "help a", metas[0].Help)
		assert.Equal(t, "s", metas[0].Unit)
		require.NotNil(t, metas[1])
		assert.Equal(t, model.MetricTypeGauge, metas[1].Type)
		assert.Equal(t, "help b", metas[1].Help)
	})

	t.Run("no metadata is fetched when include_metadata is unset", func(t *testing.T) {
		distQ := &mockSearchQuerier{
			valuesResults: []storage.SearchResult{searchResult("a", 1.0)},
			metadata:      map[string]metadata.Metadata{"a": {Type: model.MetricTypeCounter, Help: "help a"}},
		}
		mq := buildSearchTestMultiQuerier(t, distQ, nil)

		rs := mq.SearchLabelValues(ctx, model.MetricNameLabel, nil, &storage.SearchHints{OrderBy: storage.OrderByValueAsc})
		_, metas := drainSearchResultSetWithMetadata(t, rs)
		require.Len(t, metas, 1)
		assert.Nil(t, metas[0])
	})

	t.Run("a metadata fetch error leaves results un-enriched and surfaces a warning", func(t *testing.T) {
		distQ := &mockSearchQuerier{
			valuesResults: []storage.SearchResult{searchResult("a", 1.0)},
			metadataErr:   errors.New("ingesters unavailable"),
		}
		mq := buildSearchTestMultiQuerier(t, distQ, nil)

		params := &streaminglabelvalues.Params{IncludeMetadata: true}
		rs := mq.SearchLabelValues(ctx, model.MetricNameLabel, params, &storage.SearchHints{OrderBy: storage.OrderByValueAsc})
		vals, metas := drainSearchResultSetWithMetadata(t, rs)
		assert.Equal(t, []string{"a"}, vals)
		assert.Nil(t, metas[0])
		var warnStrs []string
		for _, w := range rs.Warnings() {
			warnStrs = append(warnStrs, w.Error())
		}
		require.Len(t, warnStrs, 1)
		assert.Contains(t, warnStrs[0], "failed to fetch metric metadata")
	})

	t.Run("the k-way merge carries metadata forward when duplicate values collapse", func(t *testing.T) {
		withMD := storage.NewSearchResultSetFromSlice([]storage.SearchResult{
			metadataResult("foo", 1.0, metadata.Metadata{Type: model.MetricTypeGauge, Help: "h"}),
		}, nil)
		withoutMD := storage.NewSearchResultSetFromSlice([]storage.SearchResult{
			searchResult("foo", 1.0),
		}, nil)

		// Order the metadata-less set first so, absent the fix, it would win the
		// tie and drop the metadata.
		merged := storage.MergeSearchResultSets([]storage.SearchResultSet{withoutMD, withMD}, &storage.SearchHints{OrderBy: storage.OrderByValueAsc})
		vals, metas := drainSearchResultSetWithMetadata(t, merged)
		require.Equal(t, []string{"foo"}, vals)
		require.NotNil(t, metas[0])
		assert.Equal(t, model.MetricTypeGauge, metas[0].Type)
	})

	t.Run("a non-__name__ label search is not enriched even with include_metadata set", func(t *testing.T) {
		// The fetcher holds metadata for "prod", but the label being searched is
		// "env", not __name__, so enrichment must be skipped entirely.
		distQ := &mockSearchQuerier{
			valuesResults: []storage.SearchResult{searchResult("prod", 1.0)},
			metadata:      map[string]metadata.Metadata{"prod": {Type: model.MetricTypeCounter, Help: "help prod"}},
		}
		mq := buildSearchTestMultiQuerier(t, distQ, nil)

		params := &streaminglabelvalues.Params{IncludeMetadata: true}
		rs := mq.SearchLabelValues(ctx, "env", params, &storage.SearchHints{OrderBy: storage.OrderByValueAsc})
		vals, metas := drainSearchResultSetWithMetadata(t, rs)
		assert.Equal(t, []string{"prod"}, vals)
		assert.Nil(t, metas[0], "metadata must not be attached for a non-__name__ label")
	})

	t.Run("include_metadata surfaces a warning when no source can supply metadata", func(t *testing.T) {
		// Only a blocks-store-like source (no metadata fetcher) is configured —
		// as happens when the time range is outside the ingesters' retention.
		mq := buildSearchTestMultiQuerier(t, nil, nil)
		mq.blockStore = nonMetadataSearchQueryable{q: nonMetadataSearchQuerier{values: []storage.SearchResult{searchResult("foo", 1.0)}}}

		params := &streaminglabelvalues.Params{IncludeMetadata: true}
		rs := mq.SearchLabelValues(ctx, model.MetricNameLabel, params, &storage.SearchHints{OrderBy: storage.OrderByValueAsc})
		vals, metas := drainSearchResultSetWithMetadata(t, rs)
		assert.Equal(t, []string{"foo"}, vals)
		assert.Nil(t, metas[0], "no source can enrich, so metadata stays nil")

		var warns []string
		for _, w := range rs.Warnings() {
			warns = append(warns, w.Error())
		}
		require.Len(t, warns, 1)
		assert.Contains(t, warns[0], "metric metadata is only available")
	})
}

func TestMetadataEnrichingSearchResultSet_Next(t *testing.T) {
	md := func(help string) metadata.Metadata {
		return metadata.Metadata{Type: model.MetricTypeCounter, Help: help, Unit: "s"}
	}

	// drain iterates the set, asserting At() is idempotent between Next() calls,
	// and returns the emitted results plus the batches of names passed to fetch.
	drain := func(t *testing.T, rs *metadataEnrichingSearchResultSet) []storage.SearchResult {
		t.Helper()
		var got []storage.SearchResult
		for rs.Next() {
			a := rs.At()
			require.Equal(t, a, rs.At(), "At() must be idempotent between Next() calls")
			got = append(got, a)
		}
		return got
	}

	newInner := func(warn string, vals ...string) storage.SearchResultSet {
		results := make([]storage.SearchResult, len(vals))
		for i, v := range vals {
			results[i] = searchResult(v, 1.0)
		}
		var warns annotations.Annotations
		if warn != "" {
			warns.Add(errors.New(warn))
		}
		return storage.NewSearchResultSetFromSlice(results, warns)
	}

	t.Run("empty inner emits nothing, never fetches, and At() is the zero value", func(t *testing.T) {
		fetched := false
		fetch := func(context.Context, []string) (map[string]metadata.Metadata, error) {
			fetched = true
			return nil, nil
		}
		rs := newMetadataEnrichingSearchResultSet(t.Context(), newInner(""), fetch, 10)

		assert.Equal(t, storage.SearchResult{}, rs.At(), "At() before Next() must be the zero value")
		require.False(t, rs.Next())
		assert.False(t, fetched, "an empty inner must not trigger a metadata fetch")
	})

	t.Run("attaches metadata by value and leaves unmatched results nil", func(t *testing.T) {
		fetch := func(_ context.Context, _ []string) (map[string]metadata.Metadata, error) {
			return map[string]metadata.Metadata{"a": md("help a")}, nil
		}
		rs := newMetadataEnrichingSearchResultSet(t.Context(), newInner("", "a", "b"), fetch, 10)

		got := drain(t, rs)
		require.Len(t, got, 2)
		assert.Equal(t, "a", got[0].Value)
		require.NotNil(t, got[0].Metadata)
		assert.Equal(t, "help a", got[0].Metadata.Help)
		assert.Equal(t, "b", got[1].Value)
		assert.Nil(t, got[1].Metadata, "values missing from the fetch result stay un-enriched")
	})

	t.Run("buffers one batch at a time and fetches once per batch", func(t *testing.T) {
		var calls [][]string
		fetch := func(_ context.Context, names []string) (map[string]metadata.Metadata, error) {
			calls = append(calls, append([]string(nil), names...))
			out := map[string]metadata.Metadata{}
			for _, n := range names {
				out[n] = md("help " + n)
			}
			return out, nil
		}
		rs := newMetadataEnrichingSearchResultSet(t.Context(), newInner("", "a", "b", "c", "d", "e"), fetch, 2)

		got := drain(t, rs)
		var vals []string
		for _, r := range got {
			vals = append(vals, r.Value)
			require.NotNil(t, r.Metadata, "%q must be enriched", r.Value)
			assert.Equal(t, "help "+r.Value, r.Metadata.Help)
		}
		assert.Equal(t, []string{"a", "b", "c", "d", "e"}, vals)
		assert.Equal(t, [][]string{{"a", "b"}, {"c", "d"}, {"e"}}, calls, "each response batch must trigger exactly one fetch of that batch's names")
	})

	t.Run("a fetch error emits the batch un-enriched and records a warning", func(t *testing.T) {
		fetch := func(context.Context, []string) (map[string]metadata.Metadata, error) {
			return nil, errors.New("ingesters unavailable")
		}
		rs := newMetadataEnrichingSearchResultSet(t.Context(), newInner("", "a", "b"), fetch, 10)

		got := drain(t, rs)
		require.Len(t, got, 2)
		assert.Nil(t, got[0].Metadata)
		assert.Nil(t, got[1].Metadata)
		var warns []string
		for _, w := range rs.Warnings() {
			warns = append(warns, w.Error())
		}
		require.Len(t, warns, 1)
		assert.Contains(t, warns[0], "failed to fetch metric metadata")
	})

	t.Run("a per-batch fetch error only de-enriches that batch", func(t *testing.T) {
		fetch := func(_ context.Context, names []string) (map[string]metadata.Metadata, error) {
			if names[0] == "a" {
				return nil, errors.New("boom")
			}
			out := map[string]metadata.Metadata{}
			for _, n := range names {
				out[n] = md("help " + n)
			}
			return out, nil
		}
		rs := newMetadataEnrichingSearchResultSet(t.Context(), newInner("", "a", "b", "c", "d"), fetch, 2)

		got := drain(t, rs)
		require.Len(t, got, 4)
		assert.Nil(t, got[0].Metadata, "first batch failed the fetch")
		assert.Nil(t, got[1].Metadata)
		require.NotNil(t, got[2].Metadata, "second batch fetched successfully")
		require.NotNil(t, got[3].Metadata)
	})

	t.Run("inner warnings are propagated", func(t *testing.T) {
		fetch := func(context.Context, []string) (map[string]metadata.Metadata, error) {
			return nil, nil
		}
		rs := newMetadataEnrichingSearchResultSet(t.Context(), newInner("inner warn", "a"), fetch, 10)

		_ = drain(t, rs)
		var warns []string
		for _, w := range rs.Warnings() {
			warns = append(warns, w.Error())
		}
		assert.Equal(t, []string{"inner warn"}, warns)
	})
}
