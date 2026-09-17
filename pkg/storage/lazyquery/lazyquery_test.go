// SPDX-License-Identifier: AGPL-3.0-only

package lazyquery

import (
	"context"
	"reflect"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streaminglabelvalues"
)

func TestCopyParamsDeepCopy(t *testing.T) {
	original := &storage.SelectHints{
		Start:    1000,
		End:      2000,
		Step:     10,
		Range:    3600,
		Func:     "rate",
		Grouping: []string{"label1", "label2"},
	}

	copied := copyParams(original)

	// First verify the structs themselves are different
	assert.NotSame(t, original, copied)

	// Then check each field is a different pointer
	originalVal := reflect.ValueOf(original).Elem()
	copiedVal := reflect.ValueOf(copied).Elem()
	typ := originalVal.Type()
	for i := 0; i < typ.NumField(); i++ {
		originalField := originalVal.Field(i)
		copiedField := copiedVal.Field(i)

		// Check if values are equal
		assert.Equal(t, originalField.Interface(), copiedField.Interface(), "Field %s has different values", typ.Field(i).Name)

		switch originalField.Kind() {
		// For reference types, ensure they point to different memory
		case reflect.Slice, reflect.Map, reflect.Pointer:
			if !originalField.IsNil() {
				assert.NotEqual(t, originalField.UnsafePointer(), copiedField.UnsafePointer(), "Field %s shares memory between original and copy", typ.Field(i).Name)
			}
		default:
			// Any other types are copied by value, so the assert.Equal above is enough.
		}
	}
}

// passThroughSearchQuerier satisfies storage.Querier and the local searcher
// interface declared in lazyquery.go. It records call counts so the test can
// assert pass-through behaviour.
type passThroughSearchQuerier struct {
	searchNamesResults  []storage.SearchResult
	searchValuesResults []storage.SearchResult
	searchValuesName    string
	searchNamesCalls    int
	searchValuesCalls   int
}

func (p *passThroughSearchQuerier) Select(_ context.Context, _ bool, _ *storage.SelectHints, _ ...*labels.Matcher) storage.SeriesSet {
	return storage.EmptySeriesSet()
}
func (p *passThroughSearchQuerier) LabelValues(_ context.Context, _ string, _ *storage.LabelHints, _ ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}
func (p *passThroughSearchQuerier) LabelNames(_ context.Context, _ *storage.LabelHints, _ ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}
func (p *passThroughSearchQuerier) Close() error { return nil }

func (p *passThroughSearchQuerier) SearchLabelNames(_ context.Context, _ *streaminglabelvalues.Params, _ *storage.SearchHints, _ ...*labels.Matcher) storage.SearchResultSet {
	p.searchNamesCalls++
	return storage.NewSearchResultSetFromSlice(p.searchNamesResults, nil)
}

func (p *passThroughSearchQuerier) SearchLabelValues(_ context.Context, name string, _ *streaminglabelvalues.Params, _ *storage.SearchHints, _ ...*labels.Matcher) storage.SearchResultSet {
	p.searchValuesCalls++
	p.searchValuesName = name
	return storage.NewSearchResultSetFromSlice(p.searchValuesResults, nil)
}

// nonSearcherQuerier omits the search methods so we exercise the
// graceful-degradation path in LazyQuerier.SearchLabel*.
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

func TestLazyQuerier_SearchLabelNames_PassesThrough(t *testing.T) {
	inner := &passThroughSearchQuerier{searchNamesResults: []storage.SearchResult{{Value: "foo", Score: 1.0}}}
	lq := NewLazyQuerier(inner)

	rs := lq.(LazyQuerier).SearchLabelNames(context.Background(), nil, nil)
	defer rs.Close()
	var got []string
	for rs.Next() {
		got = append(got, rs.At().Value)
	}
	require.NoError(t, rs.Err())
	assert.Equal(t, []string{"foo"}, got)
	assert.Equal(t, 1, inner.searchNamesCalls)
}

func TestLazyQuerier_SearchLabelValues_PassesThroughLabelName(t *testing.T) {
	inner := &passThroughSearchQuerier{searchValuesResults: []storage.SearchResult{{Value: "prod", Score: 1.0}}}
	lq := NewLazyQuerier(inner)

	rs := lq.(LazyQuerier).SearchLabelValues(context.Background(), "env", nil, nil)
	defer rs.Close()
	var got []string
	for rs.Next() {
		got = append(got, rs.At().Value)
	}
	require.NoError(t, rs.Err())
	assert.Equal(t, []string{"prod"}, got)
	assert.Equal(t, "env", inner.searchValuesName)
	assert.Equal(t, 1, inner.searchValuesCalls)
}

func TestLazyQuerier_SearchLabelNames_WrappedWithoutSearchReturnsErr(t *testing.T) {
	lq := NewLazyQuerier(&nonSearcherQuerier{})
	rs := lq.(LazyQuerier).SearchLabelNames(context.Background(), nil, nil)
	defer rs.Close()
	assert.False(t, rs.Next())
	require.Error(t, rs.Err())
}

func TestLazyQuerier_SearchLabelValues_WrappedWithoutSearchReturnsErr(t *testing.T) {
	lq := NewLazyQuerier(&nonSearcherQuerier{})
	rs := lq.(LazyQuerier).SearchLabelValues(context.Background(), "env", nil, nil)
	defer rs.Close()
	assert.False(t, rs.Next())
	require.Error(t, rs.Err())
}

func TestLazyQuerier_SearchLabelNames_CloseBeforeNextIsSafe(t *testing.T) {
	inner := &passThroughSearchQuerier{searchNamesResults: []storage.SearchResult{{Value: "foo", Score: 1.0}}}
	lq := NewLazyQuerier(inner)
	rs := lq.(LazyQuerier).SearchLabelNames(context.Background(), nil, nil)
	require.NoError(t, rs.Close())
}
