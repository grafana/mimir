// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/lookupplan"
)

func TestStatisticsService_Integration(t *testing.T) {
	// Create a real repository
	repo := lookupplan.NewInMemoryPlannerRepository()

	// Create test data
	blockID := ulid.MustNew(1, nil)

	// Create a mock factory that returns a specific planner
	expectedPlanner := &lookupplan.CostBasedPlanner{}
	mockFactory := &MockPlannerFactory{}
	mockFactory.On("CreatePlanner", mockAnyBlockMeta(), mockAnyIndexReader()).Return(expectedPlanner)

	// Create mock provider
	mockProvider := &MockTSDBProvider{}
	blockMeta := tsdb.BlockMeta{
		ULID: blockID,
		Stats: tsdb.BlockStats{
			NumSeries: 15000,
		},
	}
	mockProvider.On("getTSDBUsers").Return([]string{"test-user"})
	mockProvider.On("openHeadBlock", "test-user").Return(blockMeta, &mockIndexReader{}, repo, nil)

	// Create the statistics service
	logger := log.NewNopLogger()
	service := NewStatisticsService(logger, mockFactory, time.Minute, mockProvider)

	// Initially, repository should be empty
	cachedPlanner := repo.GetPlanner(blockID)
	assert.Nil(t, cachedPlanner, "repository should be empty initially")

	// Generate stats for the user by calling iteration
	ctx := context.Background()
	err := service.iteration(ctx)
	require.NoError(t, err)

	// Now repository should have the planner
	cachedPlanner = repo.GetPlanner(blockID)
	require.NotNil(t, cachedPlanner, "repository should contain the generated planner")
	assert.Equal(t, expectedPlanner, cachedPlanner, "cached planner should match the generated one")

	// Verify mock expectations
	mockFactory.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestIndexLookupPlannerFunc_Integration(t *testing.T) {
	// Setup: Create an ingester-like structure
	blockID := ulid.MustNew(1, nil)
	repo := lookupplan.NewInMemoryPlannerRepository()

	// Pre-populate repository with a cached planner
	cachedPlanner := &lookupplan.CostBasedPlanner{}
	repo.StorePlanner(blockID, cachedPlanner)

	// Create user TSDB with the repository
	userTSDB := &testUserTSDB{
		plannerRepo: repo,
	}

	// Create a mock ingester-like struct
	ingester := &testIngesterForPlannerFunc{
		tsdbs: map[string]*testUserTSDB{
			"test-user": userTSDB,
		},
		planningEnabled: true,
		logger:          log.NewNopLogger(),
	}

	// Get the planner function
	plannerFunc := ingester.getIndexLookupPlannerFunc(nil, "test-user")

	// Test that it returns the cached planner
	meta := tsdb.BlockMeta{ULID: blockID}
	mockReader := &mockIndexReader{}

	resultPlanner := plannerFunc(meta, mockReader)

	// Should return the cached planner, not generate a new one
	assert.Equal(t, cachedPlanner, resultPlanner, "should return cached planner from repository")
}

func TestIndexLookupPlannerFunc_FallbackToGeneration(t *testing.T) {
	// Setup: Create an ingester-like structure with empty repository
	blockID := ulid.MustNew(1, nil)
	repo := lookupplan.NewInMemoryPlannerRepository() // Empty repository

	// Create user TSDB with the empty repository
	userTSDB := &testUserTSDB{
		plannerRepo: repo,
	}

	// Create a mock ingester-like struct
	ingester := &testIngesterForPlannerFunc{
		tsdbs: map[string]*testUserTSDB{
			"test-user": userTSDB,
		},
		planningEnabled: true,
		logger:          log.NewNopLogger(),
	}

	// Get the planner function
	plannerFunc := ingester.getIndexLookupPlannerFunc(nil, "test-user")

	// Test that it falls back to generation when no cached planner exists
	meta := tsdb.BlockMeta{
		ULID: blockID,
		Stats: tsdb.BlockStats{
			NumSeries: 15000, // Above threshold for cost-based planning
		},
	}
	mockReader := &mockIndexReader{}

	resultPlanner := plannerFunc(meta, mockReader)

	// Should generate a new planner (CostBasedPlanner for large blocks)
	require.NotNil(t, resultPlanner, "should generate a planner")
	assert.IsType(t, &lookupplan.CostBasedPlanner{}, resultPlanner, "should generate CostBasedPlanner for large blocks")
}

// Helper structs for testing
type testUserTSDB struct {
	plannerRepo lookupplan.PlannerRepository
}

type testIngesterForPlannerFunc struct {
	tsdbs           map[string]*testUserTSDB
	planningEnabled bool
	logger          log.Logger
}

func (i *testIngesterForPlannerFunc) getTSDB(userID string) *testUserTSDB {
	return i.tsdbs[userID]
}

func (i *testIngesterForPlannerFunc) getIndexLookupPlannerFunc(r interface{}, userID string) func(tsdb.BlockMeta, tsdb.IndexReader) index.LookupPlanner {
	if !i.planningEnabled {
		return func(tsdb.BlockMeta, tsdb.IndexReader) index.LookupPlanner { return lookupplan.NoopPlanner{} }
	}

	// Create fallback planner factory for when repository doesn't have the planner
	metrics := lookupplan.NewMetrics(nil)
	statsGenerator := lookupplan.NewStatisticsGenerator(i.logger)
	fallbackFactory := lookupplan.NewPlannerFactory(metrics, i.logger, statsGenerator)

	return func(meta tsdb.BlockMeta, reader tsdb.IndexReader) index.LookupPlanner {
		// First, try to get the planner from the repository
		userTSDB := i.getTSDB(userID)
		if userTSDB != nil && userTSDB.plannerRepo != nil {
			if cachedPlanner := userTSDB.plannerRepo.GetPlanner(meta.ULID); cachedPlanner != nil {
				return cachedPlanner
			}
		}

		// Fall back to generating planner on-demand
		return fallbackFactory.CreatePlanner(meta, reader)
	}
}

// Mock helpers for tests
func mockAnyBlockMeta() interface{} {
	return mock.AnythingOfType("tsdb.BlockMeta")
}

func mockAnyIndexReader() interface{} {
	return mock.AnythingOfType("*ingester.mockIndexReader")
}
