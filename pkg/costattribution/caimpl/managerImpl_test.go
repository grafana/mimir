package caimpl

import (
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/validation"
)

func newTestManager() *ManagerImpl {
	logger := log.NewNopLogger()
	limits, _ := validation.NewOverrides(validation.Limits{}, validation.NewMockTenantLimits(map[string]*validation.Limits{
		"user1": {
			MaxCostAttributionPerUser: 5,
			CostAttributionLabel:      "team",
		},
		"user2": {
			MaxCostAttributionPerUser: 2,
			CostAttributionLabel:      "",
		},
		"user3": {
			MaxCostAttributionPerUser: 2,
			CostAttributionLabel:      "department",
		},
	}))
	inactiveTimeout := 2 * time.Minute
	cooldownTimeout := 1 * time.Minute
	cleanupInterval := 1 * time.Minute
	return NewManager(cleanupInterval, inactiveTimeout, cooldownTimeout, logger, limits)
}

func Test_NewManager(t *testing.T) {
	manager := newTestManager()
	assert.NotNil(t, manager, "Expected manager to be initialized")
	assert.NotNil(t, manager.attributionTracker, "Expected attribution tracker to be initialized")
	assert.Equal(t, "__unaccounted__", manager.invalidValue, "Expected invalidValue to be initialized")
}

func Test_EnabledForUser(t *testing.T) {
	manager := newTestManager()
	assert.True(t, manager.EnabledForUser("user1"), "Expected cost attribution to be enabled for user1")
	assert.False(t, manager.EnabledForUser("user2"), "Expected cost attribution to be disabled for user2")
	assert.False(t, manager.EnabledForUser("user4"), "Expected cost attribution to be disabled for user4")
}

func Test_GetUserAttributionLabel(t *testing.T) {
	manager := newTestManager()
	assert.Equal(t, "team", manager.GetUserAttributionLabel("user1"))
	assert.Equal(t, "", manager.GetUserAttributionLabel("user2"))
	assert.Equal(t, "department", manager.GetUserAttributionLabel("user3"))
	assert.Equal(t, 2, len(manager.attributionTracker.trackersByUserID))
	assert.Equal(t, "team", manager.attributionTracker.trackersByUserID["user1"].trackedLabel)
	assert.Equal(t, "department", manager.attributionTracker.trackersByUserID["user3"].trackedLabel)
}

func Test_GetUserAttributionLimit(t *testing.T) {
	manager := newTestManager()
	assert.Equal(t, 5, manager.GetUserAttributionLimit("user1"))
	assert.Equal(t, 0, manager.GetUserAttributionLimit("user2"))
	assert.Equal(t, 0, manager.GetUserAttributionLimit("user4"))
}

func Test_UpdateAttributionTimestamp(t *testing.T) {
	manager := newTestManager()

	lbls := labels.NewBuilder(labels.EmptyLabels())
	tm1, tm2, tm3 := "bar", "foo", "baz"
	t.Run("Should update the timestamp when limit not reached for the user attribution", func(t *testing.T) {
		lbls.Set("department", tm1)
		isOutdated, result := manager.UpdateAttributionTimestamp("user3", "department", lbls.Labels(), time.Unix(0, 0))
		assert.False(t, isOutdated, "Expected label to be the same as the one in the cache")
		assert.Equal(t, tm1, result, "Expected attribution to be returned since user is enabled for cost attribution, and limit is not reached")
		assert.NotNil(t, manager.attributionTracker.trackersByUserID["user3"].attributionTimestamps[tm1])
		assert.Equal(t, int64(0), manager.attributionTracker.trackersByUserID["user3"].attributionTimestamps[tm1].Load())

		lbls.Set("department", tm2)
		isOutdated, result = manager.UpdateAttributionTimestamp("user3", "department", lbls.Labels(), time.Unix(1, 0))
		assert.False(t, isOutdated)
		assert.Equal(t, tm2, result, "Expected attribution to be returned since user is enabled for cost attribution, and limit is not reached")
		assert.NotNil(t, manager.attributionTracker.trackersByUserID["user3"].attributionTimestamps[tm2])
		assert.Equal(t, int64(1), manager.attributionTracker.trackersByUserID["user3"].attributionTimestamps[tm2].Load())
	})

	t.Run("Should only update the timestamp of invalide when limit reached for the user attribution", func(t *testing.T) {
		lbls.Set("department", tm3)
		isOutdated, result := manager.UpdateAttributionTimestamp("user3", "department", lbls.Labels(), time.Unix(2, 0))
		assert.False(t, isOutdated)
		assert.Equal(t, manager.invalidValue, result, "Expected invalidValue to be returned since user has reached the limit of cost attribution labels")
		assert.NotNil(t, manager.attributionTracker.trackersByUserID["user3"].attributionTimestamps[manager.invalidValue])
		assert.Equal(t, int64(2), manager.attributionTracker.trackersByUserID["user3"].attributionTimestamps[manager.invalidValue].Load())

		lbls.Set("department", tm1)
		isOutdated, result = manager.UpdateAttributionTimestamp("user3", "department", lbls.Labels(), time.Unix(3, 0))
		assert.False(t, isOutdated)
		assert.Equal(t, manager.invalidValue, result, "Expected invalidValue to be returned since user has reached the limit of cost attribution labels")
		assert.Equal(t, int64(3), manager.attributionTracker.trackersByUserID["user3"].attributionTimestamps[manager.invalidValue].Load())
	})
}

func Test_SetActiveSeries(t *testing.T) {
	manager := newTestManager()
	reg := prometheus.NewRegistry()
	err := reg.Register(manager)
	require.NoError(t, err)
	userID := "user1"

	lbls := labels.NewBuilder(labels.EmptyLabels())

	t.Run("Should set the active series gauge for the given user and attribution", func(t *testing.T) {
		lbls.Set("team", "foo")
		isOutdated, val := manager.UpdateAttributionTimestamp(userID, "team", lbls.Labels(), time.Unix(0, 0))
		assert.False(t, isOutdated)
		manager.SetActiveSeries(userID, "team", val, 1.0)
		expectedMetrics := `
		# HELP cortex_ingester_active_series_attribution The total number of active series per user and attribution.
		# TYPE cortex_ingester_active_series_attribution gauge
		cortex_ingester_active_series_attribution{team="foo",user="user1"} 1
		`
		metricNames := []string{
			"cortex_ingester_active_series_attribution",
		}
		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics), metricNames...))
	})

	t.Run("Should set the active series gauge for all users and attributions enabled and ignore disabled user", func(t *testing.T) {
		userID = "user3"
		lbls.Set("department", "bar")
		isOutdated, val := manager.UpdateAttributionTimestamp(userID, "department", lbls.Labels(), time.Unix(0, 0))
		assert.False(t, isOutdated)
		manager.SetActiveSeries(userID, "department", val, 2.0)

		lbls.Set("department", "baz")
		isOutdated, val = manager.UpdateAttributionTimestamp(userID, "department", lbls.Labels(), time.Unix(1, 0))
		assert.False(t, isOutdated)
		manager.SetActiveSeries(userID, "department", val, 3.0)

		expectedMetrics := `
		# HELP cortex_ingester_active_series_attribution The total number of active series per user and attribution.
		# TYPE cortex_ingester_active_series_attribution gauge
		cortex_ingester_active_series_attribution{department="bar",user="user3"} 2
		cortex_ingester_active_series_attribution{department="baz",user="user3"} 3
		cortex_ingester_active_series_attribution{team="foo",user="user1"} 1
		`
		metricNames := []string{
			"cortex_ingester_active_series_attribution",
		}
		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics), metricNames...))
	})

	t.Run("Cleanup the active series gauge for the given user and attribution when cost attribution disabled", func(t *testing.T) {
		limits := manager.attributionTracker.limits
		defer func() { manager.attributionTracker.limits = limits }()
		userID = "user3"
		lbls.Set("department", "baz")

		overrides, _ := validation.NewOverrides(validation.Limits{}, validation.NewMockTenantLimits(map[string]*validation.Limits{
			userID: {
				MaxCostAttributionPerUser: 2,
				CostAttributionLabel:      "",
			},
		}))
		manager.attributionTracker.limits = overrides
		isOutdated, val := manager.UpdateAttributionTimestamp(userID, "department", lbls.Labels(), time.Unix(5, 0))
		assert.False(t, isOutdated)
		manager.SetActiveSeries(userID, val, "department", 3.0)

		expectedMetrics := `
		# HELP cortex_ingester_active_series_attribution The total number of active series per user and attribution.
		# TYPE cortex_ingester_active_series_attribution gauge
		cortex_ingester_active_series_attribution{team="foo",user="user1"} 1
		`
		metricNames := []string{
			"cortex_ingester_active_series_attribution",
		}
		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics), metricNames...))
	})

	t.Run("Should ignore setting the active series gauge for disabled user", func(t *testing.T) {
		userID = "user2"
		lbls.Set("department", "bar")
		isOutdated, val := manager.UpdateAttributionTimestamp(userID, "department", lbls.Labels(), time.Unix(0, 0))
		assert.False(t, isOutdated)
		manager.SetActiveSeries(userID, val, "department", 4.0)

		expectedMetrics := `
		# HELP cortex_ingester_active_series_attribution The total number of active series per user and attribution.
		# TYPE cortex_ingester_active_series_attribution gauge
		cortex_ingester_active_series_attribution{team="foo",user="user1"} 1
		`
		metricNames := []string{
			"cortex_ingester_active_series_attribution",
		}
		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics), metricNames...))
	})
}
