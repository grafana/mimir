// SPDX-License-Identifier: AGPL-3.0-only

package costattribution

// func newTestManager() *Manager {
// 	logger := log.NewNopLogger()
// 	limits, _ := validation.NewOverrides(validation.Limits{}, validation.NewMockTenantLimits(map[string]*validation.Limits{
// 		"user1": {
// 			MaxCostAttributionPerUser: 5,
// 			CostAttributionLabel:      "team",
// 		},
// 		"user2": {
// 			MaxCostAttributionPerUser: 2,
// 			CostAttributionLabel:      "",
// 		},
// 		"user3": {
// 			MaxCostAttributionPerUser: 2,
// 			CostAttributionLabel:      "department",
// 		},
// 	}))
// 	inactiveTimeout := 2 * time.Minute
// 	cooldownTimeout := 1 * time.Minute
// 	cleanupInterval := 1 * time.Minute
// 	return NewManager(cleanupInterval, inactiveTimeout, cooldownTimeout, logger, limits)
// }

// func Test_NewManager(t *testing.T) {
// 	manager := newTestManager()
// 	assert.NotNil(t, manager, "Expected manager to be initialized")
// 	assert.NotNil(t, manager.attributionTracker, "Expected attribution tracker to be initialized")
// 	assert.Equal(t, "__overflow__", manager.invalidValue, "Expected invalidValue to be initialized")
// }

// func Test_EnabledForUser(t *testing.T) {
// 	manager := newTestManager()
// 	assert.True(t, manager.EnabledForUser("user1"), "Expected cost attribution to be enabled for user1")
// 	assert.False(t, manager.EnabledForUser("user2"), "Expected cost attribution to be disabled for user2")
// 	assert.False(t, manager.EnabledForUser("user4"), "Expected cost attribution to be disabled for user4")
// }

// func Test_UserAttributionLabel(t *testing.T) {
// 	manager := newTestManager()
// 	assert.Equal(t, "team", manager.UserAttributionLabel("user1"))
// 	assert.Equal(t, "", manager.UserAttributionLabel("user2"))
// 	assert.Equal(t, "department", manager.UserAttributionLabel("user3"))
// 	assert.Equal(t, 2, len(manager.attributionTracker.trackersByUserID))
// 	assert.Equal(t, "team", manager.attributionTracker.trackersByUserID["user1"].trackedLabel)
// 	assert.Equal(t, "department", manager.attributionTracker.trackersByUserID["user3"].trackedLabel)
// }

// func Test_UserAttributionLimit(t *testing.T) {
// 	manager := newTestManager()
// 	assert.Equal(t, 5, manager.UserAttributionLimit("user1"))
// 	assert.Equal(t, 0, manager.UserAttributionLimit("user2"))
// 	assert.Equal(t, 0, manager.UserAttributionLimit("user4"))
// }

// func Test_UpdateAttributionTimestamp(t *testing.T) {
// 	manager := newTestManager()

// 	lbls := labels.NewBuilder(labels.EmptyLabels())
// 	tm1, tm2, tm3 := "bar", "foo", "baz"
// 	t.Run("Should update the timestamp when limit not reached for the user attribution", func(t *testing.T) {
// 		lbls.Set("department", tm1)
// 		isOutdated, result := manager.UpdateAttributionTimestamp("user3", "department", lbls.Labels(), time.Unix(0, 0))
// 		assert.False(t, isOutdated, "Expected label to be the same as the one in the cache")
// 		assert.Equal(t, tm1, result, "Expected attribution to be returned since user is enabled for cost attribution, and limit is not reached")
// 		assert.NotNil(t, manager.attributionTracker.trackersByUserID["user3"].observed[tm1])
// 		assert.Equal(t, int64(0), manager.attributionTracker.trackersByUserID["user3"].observed[tm1].Load())

// 		lbls.Set("department", tm2)
// 		isOutdated, result = manager.UpdateAttributionTimestamp("user3", "department", lbls.Labels(), time.Unix(1, 0))
// 		assert.False(t, isOutdated)
// 		assert.Equal(t, tm2, result, "Expected attribution to be returned since user is enabled for cost attribution, and limit is not reached")
// 		assert.NotNil(t, manager.attributionTracker.trackersByUserID["user3"].observed[tm2])
// 		assert.Equal(t, int64(1), manager.attributionTracker.trackersByUserID["user3"].observed[tm2].Load())
// 	})

// 	t.Run("Should only update the timestamp of invalide when limit reached for the user attribution", func(t *testing.T) {
// 		lbls.Set("department", tm3)
// 		isOutdated, result := manager.UpdateAttributionTimestamp("user3", "department", lbls.Labels(), time.Unix(2, 0))
// 		assert.False(t, isOutdated)
// 		assert.Equal(t, manager.invalidValue, result, "Expected invalidValue to be returned since user has reached the limit of cost attribution labels")
// 		assert.NotNil(t, manager.attributionTracker.trackersByUserID["user3"].observed[manager.invalidValue])
// 		assert.Equal(t, int64(2), manager.attributionTracker.trackersByUserID["user3"].observed[manager.invalidValue].Load())

// 		lbls.Set("department", tm1)
// 		isOutdated, result = manager.UpdateAttributionTimestamp("user3", "department", lbls.Labels(), time.Unix(3, 0))
// 		assert.False(t, isOutdated)
// 		assert.Equal(t, manager.invalidValue, result, "Expected invalidValue to be returned since user has reached the limit of cost attribution labels")
// 		assert.Equal(t, int64(3), manager.attributionTracker.trackersByUserID["user3"].observed[manager.invalidValue].Load())
// 	})
// }

// func Test_SetActiveSeries(t *testing.T) {
// 	manager := newTestManager()
// 	reg := prometheus.NewRegistry()
// 	err := reg.Register(manager)
// 	require.NoError(t, err)
// 	userID := "user1"

// 	lbls := labels.NewBuilder(labels.EmptyLabels())

// 	t.Run("Should set the active series gauge for the given user and attribution", func(t *testing.T) {
// 		lbls.Set("team", "foo")
// 		isOutdated, val := manager.UpdateAttributionTimestamp(userID, "team", lbls.Labels(), time.Unix(0, 0))
// 		assert.False(t, isOutdated)
// 		manager.SetActiveSeries(userID, "team", val, 1.0)
// 		expectedMetrics := `
// 		# HELP cortex_ingester_active_series_attribution The total number of active series per user and attribution.
// 		# TYPE cortex_ingester_active_series_attribution gauge
// 		cortex_ingester_active_series_attribution{team="foo",user="user1"} 1
// 		`
// 		metricNames := []string{
// 			"cortex_ingester_active_series_attribution",
// 		}
// 		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics), metricNames...))
// 	})

// 	t.Run("Should set the active series gauge for all users and attributions enabled and ignore disabled user", func(t *testing.T) {
// 		userID = "user3"
// 		lbls.Set("department", "bar")
// 		isOutdated, val := manager.UpdateAttributionTimestamp(userID, "department", lbls.Labels(), time.Unix(0, 0))
// 		assert.False(t, isOutdated)
// 		manager.SetActiveSeries(userID, "department", val, 2.0)

// 		lbls.Set("department", "baz")
// 		isOutdated, val = manager.UpdateAttributionTimestamp(userID, "department", lbls.Labels(), time.Unix(1, 0))
// 		assert.False(t, isOutdated)
// 		manager.SetActiveSeries(userID, "department", val, 3.0)

// 		expectedMetrics := `
// 		# HELP cortex_ingester_active_series_attribution The total number of active series per user and attribution.
// 		# TYPE cortex_ingester_active_series_attribution gauge
// 		cortex_ingester_active_series_attribution{department="bar",user="user3"} 2
// 		cortex_ingester_active_series_attribution{department="baz",user="user3"} 3
// 		cortex_ingester_active_series_attribution{team="foo",user="user1"} 1
// 		`
// 		metricNames := []string{
// 			"cortex_ingester_active_series_attribution",
// 		}
// 		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics), metricNames...))
// 	})

// 	t.Run("Cleanup the active series gauge for the given user and attribution when cost attribution disabled", func(t *testing.T) {
// 		limits := manager.attributionTracker.limits
// 		defer func() { manager.attributionTracker.limits = limits }()
// 		userID = "user3"
// 		lbls.Set("department", "baz")

// 		overrides, _ := validation.NewOverrides(validation.Limits{}, validation.NewMockTenantLimits(map[string]*validation.Limits{
// 			userID: {
// 				MaxCostAttributionPerUser: 2,
// 				CostAttributionLabel:      "",
// 			},
// 		}))
// 		manager.attributionTracker.limits = overrides
// 		isOutdated, val := manager.UpdateAttributionTimestamp(userID, "department", lbls.Labels(), time.Unix(5, 0))
// 		assert.False(t, isOutdated)
// 		manager.SetActiveSeries(userID, val, "department", 3.0)

// 		expectedMetrics := `
// 		# HELP cortex_ingester_active_series_attribution The total number of active series per user and attribution.
// 		# TYPE cortex_ingester_active_series_attribution gauge
// 		cortex_ingester_active_series_attribution{team="foo",user="user1"} 1
// 		`
// 		metricNames := []string{
// 			"cortex_ingester_active_series_attribution",
// 		}
// 		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics), metricNames...))
// 	})

// 	t.Run("Should ignore setting the active series gauge for disabled user", func(t *testing.T) {
// 		userID = "user2"
// 		lbls.Set("department", "bar")
// 		isOutdated, val := manager.UpdateAttributionTimestamp(userID, "department", lbls.Labels(), time.Unix(0, 0))
// 		assert.False(t, isOutdated)
// 		manager.SetActiveSeries(userID, val, "department", 4.0)

// 		expectedMetrics := `
// 		# HELP cortex_ingester_active_series_attribution The total number of active series per user and attribution.
// 		# TYPE cortex_ingester_active_series_attribution gauge
// 		cortex_ingester_active_series_attribution{team="foo",user="user1"} 1
// 		`
// 		metricNames := []string{
// 			"cortex_ingester_active_series_attribution",
// 		}
// 		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics), metricNames...))
// 	})
// }

// func TestUpdateAttributionTimestampForUser(t *testing.T) {
// 	cooldownTimeout := 10 * time.Second
// 	t.Run("Should not update the timestamp for the user if attribution lable is not set", func(t *testing.T) {
// 		// Create mock limits
// 		limiter, err := validation.NewOverrides(validation.Limits{CostAttributionLabel: "", MaxCostAttributionPerUser: 5}, nil)
// 		assert.NoError(t, err)
// 		trackerGroup := newAttributionTrackerGroup(limiter, cooldownTimeout)
// 		assert.NotNil(t, trackerGroup)

// 		ts := time.Unix(1, 0)
// 		trackerGroup.updateAttributionCacheForUser("tenantA", "platform", "platformA", ts)
// 		trackerGroup.updateAttributionCacheForUser("tenantB", "platform", "teamB", ts)

// 		assert.Equal(t, 0, len(trackerGroup.trackersByUserID))
// 	})

// 	t.Run("Should not update the timestamp for the user if max cost attribution per user is 0", func(t *testing.T) {
// 		// Create mock limits
// 		limiter, err := validation.NewOverrides(validation.Limits{CostAttributionLabel: "platform", MaxCostAttributionPerUser: 0}, nil)
// 		assert.NoError(t, err)

// 		trackerGroup := newAttributionTrackerGroup(limiter, cooldownTimeout)
// 		assert.NotNil(t, trackerGroup)

// 		ts := time.Unix(1, 0)
// 		trackerGroup.updateAttributionCacheForUser("tenantA", "platform", "platformA", ts)
// 		trackerGroup.updateAttributionCacheForUser("tenantB", "platform", "teamB", ts)

// 		assert.Equal(t, 0, len(trackerGroup.trackersByUserID))
// 	})

// 	t.Run("Should update the timestamp for the user attribution", func(t *testing.T) {
// 		// Create mock limits
// 		limiter, err := validation.NewOverrides(validation.Limits{CostAttributionLabel: "platform", MaxCostAttributionPerUser: 5}, nil)
// 		assert.NoError(t, err)

// 		trackerGroup := newAttributionTrackerGroup(limiter, cooldownTimeout)
// 		assert.NotNil(t, trackerGroup)

// 		ts := time.Unix(1, 0)
// 		trackerGroup.updateAttributionCacheForUser("tenantA", "platform", "fooA", ts)
// 		trackerGroup.updateAttributionCacheForUser("tenantB", "platform", "barA", ts)

// 		assert.Equal(t, 2, len(trackerGroup.trackersByUserID))
// 		fmt.Println(trackerGroup.trackersByUserID)
// 		assert.NotNil(t, trackerGroup.trackersByUserID["tenantA"])
// 		assert.NotNil(t, trackerGroup.trackersByUserID["tenantA"].observed["fooA"])
// 		assert.Equal(t, int64(1), trackerGroup.trackersByUserID["tenantA"].observed["fooA"].Load())

// 		trackerGroup.updateAttributionCacheForUser("tenantB", "platform", "barA", ts.Add(time.Second))
// 		assert.Equal(t, int64(2), trackerGroup.trackersByUserID["tenantB"].observed["barA"].Load())
// 	})
// }

// func TestUserAttributionLabel(t *testing.T) {
// 	cooldownTimeout := 10 * time.Second
// 	t.Run("Should return the cost attribution label for the user", func(t *testing.T) {
// 		// Create mock limits
// 		limiter, err := validation.NewOverrides(validation.Limits{CostAttributionLabel: "platform", MaxCostAttributionPerUser: 5}, nil)
// 		assert.NoError(t, err)

// 		trackerGroup := newAttributionTrackerGroup(limiter, cooldownTimeout)
// 		assert.NotNil(t, trackerGroup)
// 		trackerGroup.updateAttributionCacheForUser("tenantA", "platform", "fooA", time.Unix(0, 0))

// 		assert.Equal(t, "platform", trackerGroup.getUserAttributionLabelFromCache("tenantA"))
// 	})

// 	t.Run("Should return the default cost attribution label for the user if it is in cache", func(t *testing.T) {
// 		// Create mock limits
// 		limiter, err := validation.NewOverrides(validation.Limits{CostAttributionLabel: "platform", MaxCostAttributionPerUser: 5}, nil)
// 		assert.NoError(t, err)

// 		trackerGroup := newAttributionTrackerGroup(limiter, cooldownTimeout)
// 		assert.NotNil(t, trackerGroup)

// 		assert.Equal(t, "platform", trackerGroup.getUserAttributionLabelFromCache("tenantA"))

// 		// update the timestamp for the user, so cache is updated
// 		trackerGroup.updateAttributionCacheForUser("tenantA", "platform", "fooA", time.Unix(0, 0))

// 		// still read the cost attribution label from cache until cache is updated by timed service
// 		assert.Equal(t, "platform", trackerGroup.getUserAttributionLabelFromCache("tenantA"))
// 	})
// }
