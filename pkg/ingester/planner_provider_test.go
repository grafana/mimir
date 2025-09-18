// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/lookupplan"
)

// MockPlannerFactory is a mock implementation of IPlannerFactory
type MockPlannerFactory struct {
	mock.Mock
}

func (m *MockPlannerFactory) CreatePlanner(meta tsdb.BlockMeta, reader tsdb.IndexReader) index.LookupPlanner {
	args := m.Called(meta, reader)
	return args.Get(0).(index.LookupPlanner)
}

// mockIndexReader is a mock implementation of tsdb.IndexReader
type mockIndexReader struct {
	mock.Mock
}

func (m *mockIndexReader) Symbols() index.StringIter {
	args := m.Called()
	return args.Get(0).(index.StringIter)
}

func (m *mockIndexReader) SortedLabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, error) {
	args := m.Called(ctx, name, hints, matchers)
	return args.Get(0).([]string), args.Error(1)
}

func (m *mockIndexReader) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, error) {
	args := m.Called(ctx, name, hints, matchers)
	return args.Get(0).([]string), args.Error(1)
}

func (m *mockIndexReader) Postings(ctx context.Context, name string, values ...string) (index.Postings, error) {
	args := m.Called(ctx, name, values)
	return args.Get(0).(index.Postings), args.Error(1)
}

func (m *mockIndexReader) PostingsForLabelMatching(ctx context.Context, name string, match func(value string) bool) index.Postings {
	args := m.Called(ctx, name, match)
	return args.Get(0).(index.Postings)
}

func (m *mockIndexReader) PostingsForAllLabelValues(ctx context.Context, name string) index.Postings {
	args := m.Called(ctx, name)
	return args.Get(0).(index.Postings)
}

func (m *mockIndexReader) PostingsForMatchers(ctx context.Context, concurrent bool, ms ...*labels.Matcher) (index.Postings, error) {
	args := m.Called(ctx, concurrent, ms)
	return args.Get(0).(index.Postings), args.Error(1)
}

func (m *mockIndexReader) SortedPostings(p index.Postings) index.Postings {
	args := m.Called(p)
	return args.Get(0).(index.Postings)
}

func (m *mockIndexReader) ShardedPostings(p index.Postings, shardIndex, shardCount uint64) index.Postings {
	args := m.Called(p, shardIndex, shardCount)
	return args.Get(0).(index.Postings)
}

func (m *mockIndexReader) Series(ref storage.SeriesRef, builder *labels.ScratchBuilder, chks *[]chunks.Meta) error {
	args := m.Called(ref, builder, chks)
	return args.Error(0)
}

func (m *mockIndexReader) LabelNames(ctx context.Context, matchers ...*labels.Matcher) ([]string, error) {
	args := m.Called(ctx, matchers)
	return args.Get(0).([]string), args.Error(1)
}

func (m *mockIndexReader) LabelValueFor(ctx context.Context, id storage.SeriesRef, label string) (string, error) {
	args := m.Called(ctx, id, label)
	return args.String(0), args.Error(1)
}

func (m *mockIndexReader) LabelValuesFor(p index.Postings, name string) storage.LabelValues {
	args := m.Called(p, name)
	return args.Get(0).(storage.LabelValues)
}

func (m *mockIndexReader) LabelValuesExcluding(p index.Postings, name string) storage.LabelValues {
	args := m.Called(p, name)
	return args.Get(0).(storage.LabelValues)
}

func (m *mockIndexReader) LabelNamesFor(ctx context.Context, postings index.Postings) ([]string, error) {
	args := m.Called(ctx, postings)
	return args.Get(0).([]string), args.Error(1)
}

func (m *mockIndexReader) IndexLookupPlanner() index.LookupPlanner {
	args := m.Called()
	return args.Get(0).(index.LookupPlanner)
}

func (m *mockIndexReader) Close() error {
	args := m.Called()
	return args.Error(0)
}

func TestPlannerProvider_getPlanner_DoesNotCachePlanners(t *testing.T) {
	blockID := ulid.MustNew(1, nil)
	expectedPlanner := &lookupplan.CostBasedPlanner{}
	mockFactory := &MockPlannerFactory{}
	mockFactory.On("CreatePlanner", mock.AnythingOfType("tsdb.BlockMeta"), mock.AnythingOfType("*ingester.mockIndex")).Return(expectedPlanner).Twice()

	blockMeta := tsdb.BlockMeta{
		ULID: blockID,
		Stats: tsdb.BlockStats{
			NumSeries: 15000,
		},
	}

	provider := newPlannerProvider(mockFactory)
	resultPlanner := provider.getPlanner(blockMeta, &mockIndex{})
	require.NotNil(t, resultPlanner, "should return a planner")
	assert.Equal(t, expectedPlanner, resultPlanner, "should return planner from factory")

	resultPlanner = provider.getPlanner(blockMeta, &mockIndex{})
	require.NotNil(t, resultPlanner, "should return a planner")
	assert.Equal(t, expectedPlanner, resultPlanner, "should return planner from factory")
	mockFactory.AssertExpectations(t)
}

func TestPlannerProvider_generateAndStorePlanner_CachesPlanners(t *testing.T) {
	blockID := ulid.MustNew(1, nil)
	expectedPlanner := &lookupplan.CostBasedPlanner{}
	mockFactory := &MockPlannerFactory{}
	mockFactory.On("CreatePlanner", mock.AnythingOfType("tsdb.BlockMeta"), mock.AnythingOfType("*ingester.mockIndex")).Return(expectedPlanner).Once()

	blockMeta := tsdb.BlockMeta{
		ULID: blockID,
		Stats: tsdb.BlockStats{
			NumSeries: 15000,
		},
	}

	provider := newPlannerProvider(mockFactory)
	provider.generateAndStorePlanner(blockMeta, &mockIndex{})
	resultPlanner := provider.getPlanner(blockMeta, &mockIndex{})

	require.NotNil(t, resultPlanner, "should return a planner")
	assert.Equal(t, expectedPlanner, resultPlanner, "should return cached planner")
	mockFactory.AssertNotCalled(t, "CreatePlanner")
}
