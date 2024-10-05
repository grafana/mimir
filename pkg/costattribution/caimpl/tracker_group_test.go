package caimpl

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/grafana/mimir/pkg/util/validation"
)

func TestUpdateAttributionTimestampForUser(t *testing.T) {
	cooldownTimeout := 10 * time.Second
	t.Run("Should not update the timestamp for the user if attribution lable is not set", func(t *testing.T) {
		// Create mock limits
		limiter, err := validation.NewOverrides(validation.Limits{CostAttributionLabel: "", MaxCostAttributionPerUser: 5}, nil)
		assert.NoError(t, err)
		trackerGroup := newAttributionTrackerGroup(limiter, cooldownTimeout)
		assert.NotNil(t, trackerGroup)

		ts := time.Unix(1, 0)
		trackerGroup.updateAttributionCacheForUser("tenantA", "platform", "platformA", ts)
		trackerGroup.updateAttributionCacheForUser("tenantB", "platform", "teamB", ts)

		assert.Equal(t, 0, len(trackerGroup.trackersByUserID))
	})

	t.Run("Should not update the timestamp for the user if max cost attribution per user is 0", func(t *testing.T) {
		// Create mock limits
		limiter, err := validation.NewOverrides(validation.Limits{CostAttributionLabel: "platform", MaxCostAttributionPerUser: 0}, nil)
		assert.NoError(t, err)

		trackerGroup := newAttributionTrackerGroup(limiter, cooldownTimeout)
		assert.NotNil(t, trackerGroup)

		ts := time.Unix(1, 0)
		trackerGroup.updateAttributionCacheForUser("tenantA", "platform", "platformA", ts)
		trackerGroup.updateAttributionCacheForUser("tenantB", "platform", "teamB", ts)

		assert.Equal(t, 0, len(trackerGroup.trackersByUserID))
	})

	t.Run("Should update the timestamp for the user attribution", func(t *testing.T) {
		// Create mock limits
		limiter, err := validation.NewOverrides(validation.Limits{CostAttributionLabel: "platform", MaxCostAttributionPerUser: 5}, nil)
		assert.NoError(t, err)

		trackerGroup := newAttributionTrackerGroup(limiter, cooldownTimeout)
		assert.NotNil(t, trackerGroup)

		ts := time.Unix(1, 0)
		trackerGroup.updateAttributionCacheForUser("tenantA", "platform", "fooA", ts)
		trackerGroup.updateAttributionCacheForUser("tenantB", "platform", "barA", ts)

		assert.Equal(t, 2, len(trackerGroup.trackersByUserID))
		fmt.Println(trackerGroup.trackersByUserID)
		assert.NotNil(t, trackerGroup.trackersByUserID["tenantA"])
		assert.NotNil(t, trackerGroup.trackersByUserID["tenantA"].attributionTimestamps["fooA"])
		assert.Equal(t, int64(1), trackerGroup.trackersByUserID["tenantA"].attributionTimestamps["fooA"].Load())

		trackerGroup.updateAttributionCacheForUser("tenantB", "platform", "barA", ts.Add(time.Second))
		assert.Equal(t, int64(2), trackerGroup.trackersByUserID["tenantB"].attributionTimestamps["barA"].Load())
	})
}

func TestGetUserAttributionLabel(t *testing.T) {
	cooldownTimeout := 10 * time.Second
	t.Run("Should return the cost attribution label for the user", func(t *testing.T) {
		// Create mock limits
		limiter, err := validation.NewOverrides(validation.Limits{CostAttributionLabel: "platform", MaxCostAttributionPerUser: 5}, nil)
		assert.NoError(t, err)

		trackerGroup := newAttributionTrackerGroup(limiter, cooldownTimeout)
		assert.NotNil(t, trackerGroup)
		trackerGroup.updateAttributionCacheForUser("tenantA", "platform", "fooA", time.Unix(0, 0))

		assert.Equal(t, "platform", trackerGroup.getUserAttributionLabelFromCache("tenantA"))
	})

	t.Run("Should return the default cost attribution label for the user if it is in cache", func(t *testing.T) {
		// Create mock limits
		limiter, err := validation.NewOverrides(validation.Limits{CostAttributionLabel: "platform", MaxCostAttributionPerUser: 5}, nil)
		assert.NoError(t, err)

		trackerGroup := newAttributionTrackerGroup(limiter, cooldownTimeout)
		assert.NotNil(t, trackerGroup)

		assert.Equal(t, "platform", trackerGroup.getUserAttributionLabelFromCache("tenantA"))

		// update the timestamp for the user, so cache is updated
		trackerGroup.updateAttributionCacheForUser("tenantA", "platform", "fooA", time.Unix(0, 0))

		// still read the cost attribution label from cache until cache is updated by timed service
		assert.Equal(t, "platform", trackerGroup.getUserAttributionLabelFromCache("tenantA"))
	})
}
