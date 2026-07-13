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
type mockSearchQuerier struct {
	namesResults  []storage.SearchResult
	namesErr      error
	valuesResults []storage.SearchResult
	valuesErr     error
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

// mockSearchQueryable returns its configured mockSearchQuerier for any (mint, maxt).
type mockSearchQueryable struct {
	q *mockSearchQuerier
}

func (qq *mockSearchQueryable) Querier(_, _ int64) (storage.Querier, error) {
	return qq.q, nil
}

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

func searchResultMetadata(v string, score float64, mt model.MetricType, help, unit string) storage.SearchResult {
	return storage.SearchResult{
		Value: v,
		Score: score,
		Metadata: &metadata.Metadata{
			Type: mt,
			Help: help,
			Unit: unit,
		},
	}
}

func drainSearchResultSet(t *testing.T, rs storage.SearchResultSet) (vals []string, scores []float64, metadatas []*metadata.Metadata, warns []string, err error) {
	t.Helper()
	defer rs.Close()
	for rs.Next() {
		r := rs.At()
		vals = append(vals, r.Value)
		scores = append(scores, r.Score)
		metadatas = append(metadatas, r.Metadata)
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
	vals, _, _, _, err := drainSearchResultSet(t, rs)
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
	_, _, _, _, err := drainSearchResultSet(t, rs)
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
	_, _, _, _, err := drainSearchResultSet(t, rs)
	require.Error(t, err)
	assert.ErrorIs(t, err, wantErr)
}

func TestMultiQuerier_SearchLabelNames_BothChildrenEmptyReturnsEmpty(t *testing.T) {
	mq := buildSearchTestMultiQuerier(t, &mockSearchQuerier{}, &mockSearchQuerier{})
	ctx := user.InjectOrgID(context.Background(), "user-1")
	rs := mq.SearchLabelNames(ctx, nil, &storage.SearchHints{OrderBy: storage.OrderByValueAsc})
	vals, _, _, _, err := drainSearchResultSet(t, rs)
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
	vals, _, _, _, err := drainSearchResultSet(t, rs)
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
	_, _, _, warns, err := drainSearchResultSet(t, rs)
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
	_, _, _, warns, err := drainSearchResultSet(t, rs)
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
	vals, scores, _, _, err := drainSearchResultSet(t, rs)
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
	vals, _, _, _, err := drainSearchResultSet(t, rs)
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
	vals, _, _, _, err := drainSearchResultSet(t, rs)
	require.NoError(t, err)
	assert.Equal(t, []string{"alpha", "beta", "gamma", "delta"}, vals)
}

func TestMultiQuerier_SearchLabelValues_ForwardsLabelName(t *testing.T) {
	distQ := &mockSearchQuerier{valuesResults: []storage.SearchResult{searchResult("prod", 1.0), searchResult("staging", 1.0)}}
	blockQ := &mockSearchQuerier{valuesResults: []storage.SearchResult{searchResult("dev", 1.0), searchResult("prod", 1.0)}}
	mq := buildSearchTestMultiQuerier(t, distQ, blockQ)

	ctx := user.InjectOrgID(context.Background(), "user-1")
	rs := mq.SearchLabelValues(ctx, "env", nil, &storage.SearchHints{OrderBy: storage.OrderByValueAsc})
	vals, _, _, _, err := drainSearchResultSet(t, rs)
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
	_, _, _, warns, err := drainSearchResultSet(t, rs)
	require.NoError(t, err)
	require.Len(t, warns, 1)
	assert.Contains(t, warns[0], validation.MaxLabelValuesLimitFlag)
	assert.NotContains(t, warns[0], validation.MaxLabelNamesLimitFlag)
}

func TestMultiQuerier_SearchLabelValues_MetadataKept(t *testing.T) {
	distQ := &mockSearchQuerier{namesResults: []storage.SearchResult{searchResultMetadata("a", 1.0, model.MetricTypeGauge, "It's a.", "seconds"), searchResult("b", 0.5)}}
	blockQ := &mockSearchQuerier{namesResults: []storage.SearchResult{searchResult("a", 1.0), searchResultMetadata("b", 0.5, model.MetricTypeGauge, "It's b.", "seconds")}}
	mq := buildSearchTestMultiQuerier(t, distQ, blockQ)

	ctx := user.InjectOrgID(context.Background(), "user-1")
	rs := mq.SearchLabelNames(ctx, nil, &storage.SearchHints{OrderBy: storage.OrderByValueAsc})
	vals, _, metadatas, _, err := drainSearchResultSet(t, rs)
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, vals)
	require.NotNil(t, metadatas[0])
	assert.Equal(t, model.MetricTypeGauge, metadatas[0].Type)
	assert.Equal(t, "It's a.", metadatas[0].Help)
	assert.Equal(t, "seconds", metadatas[0].Unit)
	require.NotNil(t, metadatas[1])
	assert.Equal(t, model.MetricTypeGauge, metadatas[1].Type)
	assert.Equal(t, "It's b.", metadatas[1].Help)
	assert.Equal(t, "seconds", metadatas[1].Unit)
}
