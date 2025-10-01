// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/lookupplan"
)

// mockPlannerFactory is a hand-written mock implementation of iPlannerFactory
type mockPlannerFactory struct {
	createPlannerFunc func(meta tsdb.BlockMeta, reader tsdb.IndexReader) index.LookupPlanner
}

func (m *mockPlannerFactory) CreatePlanner(meta tsdb.BlockMeta, reader tsdb.IndexReader) index.LookupPlanner {
	return m.createPlannerFunc(meta, reader)
}

func TestPlannerProvider_getPlanner_DoesNotCachePlanners(t *testing.T) {
	blockID := ulid.MustNew(1, nil)
	expectedPlanner := &lookupplan.CostBasedPlanner{}
	callCount := 0

	mockFactory := &mockPlannerFactory{
		createPlannerFunc: func(meta tsdb.BlockMeta, reader tsdb.IndexReader) index.LookupPlanner {
			callCount++
			return expectedPlanner
		},
	}

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

	assert.Equal(t, 2, callCount, "CreatePlanner should be called twice (no caching)")
}

func TestPlannerProvider_generateAndStorePlanner_CachesPlanners(t *testing.T) {
	blockID := ulid.MustNew(1, nil)
	expectedPlanner := &lookupplan.CostBasedPlanner{}
	callCount := 0

	mockFactory := &mockPlannerFactory{
		createPlannerFunc: func(meta tsdb.BlockMeta, reader tsdb.IndexReader) index.LookupPlanner {
			callCount++
			return expectedPlanner
		},
	}

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
	assert.Equal(t, 1, callCount, "CreatePlanner should be called only once (planner is cached)")
}
