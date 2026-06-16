// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streaminglabelvalues"
)

// passThroughQuerier satisfies storage.Querier and mimirSearcher. The search
// methods record which one was called and return a single configured result so
// the test can verify pass-through happened without re-testing the merge logic.
type passThroughQuerier struct {
	searchNamesResults  []storage.SearchResult
	searchValuesResults []storage.SearchResult
	searchValuesName    string
	searchNamesCalls    int
	searchValuesCalls   int
}

func (p *passThroughQuerier) Select(_ context.Context, _ bool, _ *storage.SelectHints, _ ...*labels.Matcher) storage.SeriesSet {
	return storage.EmptySeriesSet()
}
func (p *passThroughQuerier) LabelValues(_ context.Context, _ string, _ *storage.LabelHints, _ ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}
func (p *passThroughQuerier) LabelNames(_ context.Context, _ *storage.LabelHints, _ ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}
func (p *passThroughQuerier) Close() error { return nil }

func (p *passThroughQuerier) SearchLabelNames(_ context.Context, _ *streaminglabelvalues.Params, _ *storage.SearchHints, _ ...*labels.Matcher) storage.SearchResultSet {
	p.searchNamesCalls++
	return storage.NewSearchResultSetFromSlice(p.searchNamesResults, nil)
}

func (p *passThroughQuerier) SearchLabelValues(_ context.Context, name string, _ *streaminglabelvalues.Params, _ *storage.SearchHints, _ ...*labels.Matcher) storage.SearchResultSet {
	p.searchValuesCalls++
	p.searchValuesName = name
	return storage.NewSearchResultSetFromSlice(p.searchValuesResults, nil)
}

// nonSearcherQuerier deliberately omits the mimirSearcher methods so we can
// verify the graceful-degradation path through memoryTrackingQuerier.
type nonSearcherQuerier struct{}

func (n *nonSearcherQuerier) Select(_ context.Context, _ bool, _ *storage.SelectHints, _ ...*labels.Matcher) storage.SeriesSet {
	return storage.EmptySeriesSet()
}
func (n *nonSearcherQuerier) LabelValues(_ context.Context, _ string, _ *storage.LabelHints, _ ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}
func (n *nonSearcherQuerier) LabelNames(_ context.Context, _ *storage.LabelHints, _ ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}
func (n *nonSearcherQuerier) Close() error { return nil }

func TestMemoryTrackingQuerier_SearchLabelNames_PassesThrough(t *testing.T) {
	inner := &passThroughQuerier{searchNamesResults: []storage.SearchResult{{Value: "foo", Score: 1.0}}}
	q := &memoryTrackingQuerier{inner: inner}

	rs := q.SearchLabelNames(context.Background(), nil, nil)
	defer rs.Close()
	var got []string
	for rs.Next() {
		got = append(got, rs.At().Value)
	}
	require.NoError(t, rs.Err())
	assert.Equal(t, []string{"foo"}, got)
	assert.Equal(t, 1, inner.searchNamesCalls, "inner.SearchLabelNames must be called exactly once")
}

func TestMemoryTrackingQuerier_SearchLabelValues_PassesThroughLabelName(t *testing.T) {
	inner := &passThroughQuerier{searchValuesResults: []storage.SearchResult{{Value: "prod", Score: 1.0}}}
	q := &memoryTrackingQuerier{inner: inner}

	rs := q.SearchLabelValues(context.Background(), "env", nil, nil)
	defer rs.Close()
	var got []string
	for rs.Next() {
		got = append(got, rs.At().Value)
	}
	require.NoError(t, rs.Err())
	assert.Equal(t, []string{"prod"}, got)
	assert.Equal(t, "env", inner.searchValuesName, "label name must reach inner querier")
	assert.Equal(t, 1, inner.searchValuesCalls)
}

func TestMemoryTrackingQuerier_SearchLabelNames_InnerWithoutSearchReturnsErr(t *testing.T) {
	q := &memoryTrackingQuerier{inner: &nonSearcherQuerier{}}
	rs := q.SearchLabelNames(context.Background(), nil, nil)
	defer rs.Close()
	assert.False(t, rs.Next())
	require.Error(t, rs.Err())
}

func TestMemoryTrackingQuerier_SearchLabelValues_InnerWithoutSearchReturnsErr(t *testing.T) {
	q := &memoryTrackingQuerier{inner: &nonSearcherQuerier{}}
	rs := q.SearchLabelValues(context.Background(), "env", nil, nil)
	defer rs.Close()
	assert.False(t, rs.Next())
	require.Error(t, rs.Err())
}
