// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryPlannerRepository(t *testing.T) {
	repo := NewInMemoryPlannerRepository()

	// Test empty repository
	planner := repo.GetPlanner(ulid.MustNew(1, nil))
	assert.Nil(t, planner, "should return nil for non-existent planner")

	// Test storing and retrieving planner
	blockID := ulid.MustNew(2, nil)
	expectedPlanner := NoopPlanner{}
	repo.StorePlanner(blockID, expectedPlanner)

	retrievedPlanner := repo.GetPlanner(blockID)
	require.NotNil(t, retrievedPlanner, "should return stored planner")
	assert.Equal(t, expectedPlanner, retrievedPlanner, "should return the same planner that was stored")

	// Test overwriting planner
	newPlanner := &CostBasedPlanner{}
	repo.StorePlanner(blockID, newPlanner)

	retrievedPlanner = repo.GetPlanner(blockID)
	require.NotNil(t, retrievedPlanner, "should return updated planner")
	assert.Equal(t, newPlanner, retrievedPlanner, "should return the updated planner")

	// Test multiple planners
	blockID2 := ulid.MustNew(3, nil)
	secondPlanner := NoopPlanner{}
	repo.StorePlanner(blockID2, secondPlanner)

	// Both planners should be retrievable
	assert.Equal(t, newPlanner, repo.GetPlanner(blockID), "first planner should still be accessible")
	assert.Equal(t, secondPlanner, repo.GetPlanner(blockID2), "second planner should be accessible")
}

func TestPlannerRepositoryReader_Interface(t *testing.T) {
	// Test that InMemoryPlannerRepository implements PlannerRepositoryReader
	var reader PlannerRepositoryReader = NewInMemoryPlannerRepository()

	// Test interface method
	planner := reader.GetPlanner(ulid.MustNew(1, nil))
	assert.Nil(t, planner, "interface method should work")
}

func TestPlannerRepository_Interface(t *testing.T) {
	// Test that InMemoryPlannerRepository implements PlannerRepository
	var repo PlannerRepository = NewInMemoryPlannerRepository()

	// Test interface methods
	blockID := ulid.MustNew(2, nil)
	expectedPlanner := NoopPlanner{}

	repo.StorePlanner(blockID, expectedPlanner)
	retrievedPlanner := repo.GetPlanner(blockID)
	assert.Equal(t, expectedPlanner, retrievedPlanner, "interface methods should work")
}
