// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"testing"

	"github.com/go-kit/log"
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

// MockPlannerFactory is a mock implementation of the planner factory for testing
type MockPlannerFactory struct {
	mock.Mock
}

func (m *MockPlannerFactory) CreatePlanner(meta tsdb.BlockMeta, reader tsdb.IndexReader) index.LookupPlanner {
	args := m.Called(meta, reader)
	return args.Get(0).(index.LookupPlanner)
}

type mockIndexReader struct{}

func (m *mockIndexReader) Close() error              { return nil }
func (m *mockIndexReader) Symbols() index.StringIter { return nil }
func (m *mockIndexReader) SortedLabelValues(context.Context, string, *storage.LabelHints, ...*labels.Matcher) ([]string, error) {
	return []string{}, nil
}
func (m *mockIndexReader) LabelValues(context.Context, string, *storage.LabelHints, ...*labels.Matcher) ([]string, error) {
	return []string{}, nil
}
func (m *mockIndexReader) Postings(context.Context, string, ...string) (index.Postings, error) {
	return index.EmptyPostings(), nil
}
func (m *mockIndexReader) PostingsForLabelMatching(context.Context, string, func(value string) bool) index.Postings {
	return index.EmptyPostings()
}
func (m *mockIndexReader) PostingsForAllLabelValues(context.Context, string) index.Postings {
	return index.EmptyPostings()
}
func (m *mockIndexReader) PostingsForMatchers(context.Context, bool, ...*labels.Matcher) (index.Postings, error) {
	return index.EmptyPostings(), nil
}
func (m *mockIndexReader) SortedPostings(index.Postings) index.Postings { return nil }
func (m *mockIndexReader) ShardedPostings(index.Postings, uint64, uint64) index.Postings {
	return nil
}
func (m *mockIndexReader) Series(storage.SeriesRef, *labels.ScratchBuilder, *[]chunks.Meta) error {
	return nil
}
func (m *mockIndexReader) LabelNames(context.Context, ...*labels.Matcher) ([]string, error) {
	return []string{"__name__"}, nil
}
func (m *mockIndexReader) LabelValueFor(context.Context, storage.SeriesRef, string) (string, error) {
	return "", nil
}
func (m *mockIndexReader) LabelValuesExcluding(index.Postings, string) storage.LabelValues {
	return nil
}
func (m *mockIndexReader) LabelValuesFor(index.Postings, string) storage.LabelValues {
	return nil
}
func (m *mockIndexReader) LabelNamesFor(context.Context, index.Postings) ([]string, error) {
	return nil, nil
}
func (m *mockIndexReader) IndexLookupPlanner() index.LookupPlanner {
	return lookupplan.NoopPlanner{}
}

func TestStatisticsService_generateStats(t *testing.T) {
	logger := log.NewNopLogger()

	// Create mock components
	mockFactory := &MockPlannerFactory{}

	// Create test data
	blockID := ulid.MustNew(1, nil)
	expectedPlanner := lookupplan.NoopPlanner{}

	// Set up expectations
	mockFactory.On("CreatePlanner", mock.AnythingOfType("tsdb.BlockMeta"), mock.AnythingOfType("*ingester.mockIndexReader")).
		Return(expectedPlanner)

	blockMeta := tsdb.BlockMeta{
		ULID: blockID,
		Stats: tsdb.BlockStats{
			NumSeries: 15000,
		},
	}

	// Create service
	service := newPlannerProvider(mockFactory)

	// Test generateAndStorePlanner with block metadata
	mockReader := &mockIndexReader{}
	service.generateAndStorePlanner(blockMeta, mockReader)

	// Verify planner was stored in service
	storedPlanner := service.getPlanner(blockID)
	require.NotNil(t, storedPlanner)
	assert.Equal(t, expectedPlanner, storedPlanner)

	// Verify expectations
	mockFactory.AssertExpectations(t)
}

func TestStatisticsService_Interface(t *testing.T) {
	logger := log.NewNopLogger()
	mockFactory := &MockPlannerFactory{}

	// Test that plannerProvider can be created
	service := newPlannerProvider(mockFactory)

	// Should be able to call service methods
	require.NotNil(t, service)
}

func TestStatisticsService_GetPlanner(t *testing.T) {
	logger := log.NewNopLogger()
	mockFactory := &MockPlannerFactory{}

	service := newPlannerProvider(mockFactory)

	// Test getting a planner that doesn't exist
	blockID := ulid.MustNew(1, nil)
	planner := service.getPlanner(blockID)
	assert.Nil(t, planner, "should return nil for non-existent planner")

	// Test storing and getting a planner
	expectedPlanner := lookupplan.NoopPlanner{}
	service.storePlanner(blockID, expectedPlanner)

	storedPlanner := service.getPlanner(blockID)
	require.NotNil(t, storedPlanner)
	assert.Equal(t, expectedPlanner, storedPlanner)
}
