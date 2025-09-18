// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"testing"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/lookupplan"
)

func TestStatisticsService_Integration(t *testing.T) {
	// Create test data
	blockID := ulid.MustNew(1, nil)

	// Create a mock factory that returns a specific planner
	expectedPlanner := &lookupplan.CostBasedPlanner{}
	mockFactory := &MockPlannerFactory{}
	mockFactory.On("CreatePlanner", mockAnyBlockMeta(), mockAnyIndexReader()).Return(expectedPlanner)

	blockMeta := tsdb.BlockMeta{
		ULID: blockID,
		Stats: tsdb.BlockStats{
			NumSeries: 15000,
		},
	}

	// Create the statistics service
	logger := log.NewNopLogger()
	service := NewStatisticsService(logger, mockFactory)

	// Initially, service should not have the planner
	cachedPlanner := service.getPlanner(blockID)
	assert.Nil(t, cachedPlanner, "service should not have planner initially")

	// Generate stats with block metadata
	mockReader := &mockIndexReader{}
	service.generateStats(blockMeta, mockReader)

	// Now service should have the planner
	cachedPlanner = service.getPlanner(blockID)
	require.NotNil(t, cachedPlanner, "service should contain the generated planner")
	assert.Equal(t, expectedPlanner, cachedPlanner, "cached planner should match the generated one")

	// Verify mock expectations
	mockFactory.AssertExpectations(t)
}

func TestUserTSDB_getIndexLookupPlanner_WithCache(t *testing.T) {
	// Setup: Create a userTSDB with a pre-populated statistics service
	blockID := ulid.MustNew(1, nil)

	// Create cached planner
	cachedPlanner := &lookupplan.CostBasedPlanner{}
	logger := log.NewNopLogger()
	mockFactory := &MockPlannerFactory{}

	statisticsService := NewStatisticsService(logger, mockFactory)
	statisticsService.storePlanner(blockID, cachedPlanner)

	// Create user TSDB with the statistics service
	userTSDB := &userTSDB{
		userID:            "test-user",
		plannerFactory:    mockFactory,
		statisticsService: statisticsService,
	}

	// Test that it returns the cached planner
	meta := tsdb.BlockMeta{ULID: blockID}
	mockReader := &mockIndexReader{}

	resultPlanner := userTSDB.getIndexLookupPlanner(meta, mockReader)

	// Should return the cached planner, not generate a new one
	assert.Equal(t, cachedPlanner, resultPlanner, "should return cached planner from statistics service")
}

func TestUserTSDB_getIndexLookupPlanner_FallbackToGeneration(t *testing.T) {
	// Setup: Create a userTSDB with empty statistics service
	blockID := ulid.MustNew(1, nil)

	logger := log.NewNopLogger()
	mockFactory := &MockPlannerFactory{}

	// Set up fallback factory to return a CostBasedPlanner
	expectedPlanner := &lookupplan.CostBasedPlanner{}
	mockFactory.On("CreatePlanner", mock.AnythingOfType("tsdb.BlockMeta"), mock.AnythingOfType("*ingester.mockIndexReader")).Return(expectedPlanner)

	// Empty statistics service
	statisticsService := NewStatisticsService(logger, mockFactory)

	// Create user TSDB
	userTSDB := &userTSDB{
		userID:            "test-user",
		plannerFactory:    mockFactory,
		statisticsService: statisticsService,
	}

	// Test that it falls back to generation when no cached planner exists
	meta := tsdb.BlockMeta{
		ULID: blockID,
		Stats: tsdb.BlockStats{
			NumSeries: 15000, // Above threshold for cost-based planning
		},
	}
	mockReader := &mockIndexReader{}

	resultPlanner := userTSDB.getIndexLookupPlanner(meta, mockReader)

	// Should generate a new planner from factory
	require.NotNil(t, resultPlanner, "should generate a planner")
	assert.Equal(t, expectedPlanner, resultPlanner, "should return planner from factory")

	// Verify factory was called
	mockFactory.AssertExpectations(t)
}

// Mock helpers for tests
func mockAnyBlockMeta() interface{} {
	return mock.AnythingOfType("tsdb.BlockMeta")
}

func mockAnyIndexReader() interface{} {
	return mock.AnythingOfType("*ingester.mockIndexReader")
}
