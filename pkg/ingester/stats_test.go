// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"testing"
	"time"

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

// MockPlannerRepository is a mock implementation of the planner repository for testing
type MockPlannerRepository struct {
	mock.Mock
}

func (m *MockPlannerRepository) GetPlanner(blockULID ulid.ULID) index.LookupPlanner {
	args := m.Called(blockULID)
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(index.LookupPlanner)
}

func (m *MockPlannerRepository) StorePlanner(blockULID ulid.ULID, planner index.LookupPlanner) {
	m.Called(blockULID, planner)
}

// MockTSDB is a mock implementation for testing
type MockTSDB struct {
	headULID ulid.ULID
	head     *MockHead
}

func (m *MockTSDB) Head() HeadInterface {
	return m.head
}

type MockHead struct {
	ulid ulid.ULID
}

func (m *MockHead) ULID() ulid.ULID {
	return m.ulid
}

func (m *MockHead) IndexReader() (tsdb.IndexReader, error) {
	return &mockIndexReader{}, nil
}

func (m *MockHead) BlockMeta() tsdb.BlockMeta {
	return tsdb.BlockMeta{
		ULID: m.ulid,
		Stats: tsdb.BlockStats{
			NumSeries: 15000, // Above the threshold for planning
		},
	}
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

func TestStatisticsService_generateStatsForUser(t *testing.T) {
	logger := log.NewNopLogger()

	// Create mock components
	mockFactory := &MockPlannerFactory{}
	mockRepo := &MockPlannerRepository{}

	// Create test data
	blockID := ulid.MustNew(1, nil)
	expectedPlanner := lookupplan.NoopPlanner{}
	mockTSDB := &MockTSDB{
		headULID: blockID,
		head: &MockHead{
			ulid: blockID,
		},
	}

	// Set up expectations
	mockRepo.On("GetPlanner", blockID).Return(nil) // No existing planner
	mockFactory.On("CreatePlanner", mock.AnythingOfType("tsdb.BlockMeta"), mock.AnythingOfType("*ingester.mockIndexReader")).
		Return(expectedPlanner)
	mockRepo.On("StorePlanner", blockID, expectedPlanner).Return()

	// Create service
	service := NewStatisticsService(logger, mockFactory, time.Minute)

	// Test generateStatsForUser
	service.generateStatsForUser("test-user", mockTSDB, mockRepo)

	// Verify expectations
	mockFactory.AssertExpectations(t)
	mockRepo.AssertExpectations(t)
}

func TestStatisticsService_Interface(t *testing.T) {
	logger := log.NewNopLogger()
	mockFactory := &MockPlannerFactory{}

	// Test that StatisticsService implements the expected interface
	service := NewStatisticsService(logger, mockFactory, time.Minute)

	// Should be able to call service methods
	require.NotNil(t, service)
}

func TestStatisticsService_statsLoop_Context(t *testing.T) {
	logger := log.NewNopLogger()
	mockFactory := &MockPlannerFactory{}

	service := NewStatisticsService(logger, mockFactory, 10*time.Millisecond)

	// Test context cancellation
	ctx, cancel := context.WithCancel(context.Background())

	// Start the service
	done := make(chan error, 1)
	go func() {
		done <- service.statsLoop(ctx, func() map[string]*userTSDB { return make(map[string]*userTSDB) })
	}()

	// Cancel context quickly
	time.Sleep(5 * time.Millisecond)
	cancel()

	// Should exit quickly
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("statsLoop should have exited after context cancellation")
	}
}
