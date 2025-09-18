// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb"
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
