// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/ingester.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"context"
	"time"

	"github.com/go-kit/log/level"

	asmodel "github.com/grafana/mimir/pkg/ingester/activeseries/model"
)

// metricsUpdaterServiceRunning is the running function for the internal metrics updater service.
func (i *Ingester) metricsUpdaterServiceRunning(ctx context.Context) error {
	// Launch a dedicated goroutine for inflightRequestsTicker
	// to ensure it operates independently, unaffected by delays from other logics in this function.
	go func() {
		inflightRequestsTicker := time.NewTicker(250 * time.Millisecond)
		defer inflightRequestsTicker.Stop()

		for {
			select {
			case <-inflightRequestsTicker.C:
				i.metrics.inflightRequestsSummary.Observe(float64(i.inflightPushRequests.Load()))
			case <-ctx.Done():
				return
			}
		}
	}()

	rateUpdateTicker := time.NewTicker(i.cfg.RateUpdatePeriod)
	defer rateUpdateTicker.Stop()

	ingestionRateTicker := time.NewTicker(instanceIngestionRateTickInterval)
	defer ingestionRateTicker.Stop()

	var activeSeriesTickerChan <-chan time.Time
	var activeSeriesStartedAt time.Time
	if i.cfg.ActiveSeriesMetrics.Enabled {
		activeSeriesStartedAt = time.Now()
		t := time.NewTicker(i.cfg.ActiveSeriesMetrics.UpdatePeriod)
		activeSeriesTickerChan = t.C
		defer t.Stop()
	}

	usageStatsUpdateTicker := time.NewTicker(usageStatsUpdateInterval)
	defer usageStatsUpdateTicker.Stop()

	limitMetricsUpdateTicker := time.NewTicker(i.cfg.limitMetricsUpdatePeriod)
	defer limitMetricsUpdateTicker.Stop()

	for {
		select {
		case <-ingestionRateTicker.C:
			i.ingestionRate.Tick()
		case <-rateUpdateTicker.C:
			i.tsdbsMtx.RLock()
			for _, db := range i.tsdbs {
				db.ingestedAPISamples.Tick()
				db.ingestedRuleSamples.Tick()
			}
			i.tsdbsMtx.RUnlock()
		case <-activeSeriesTickerChan:
			now := time.Now()
			if !activeSeriesStartedAt.IsZero() && now.Sub(activeSeriesStartedAt) >= i.cfg.ActiveSeriesMetrics.IdleTimeout {
				i.metrics.activeSeriesLoading.Set(0)
				activeSeriesStartedAt = time.Time{} // Only flip once.
			}
			i.updateActiveSeries(now)
		case <-usageStatsUpdateTicker.C:
			i.updateUsageStats()
		case <-limitMetricsUpdateTicker.C:
			i.updateLimitMetrics()
		case <-ctx.Done():
			return nil
		}
	}
}

func (i *Ingester) updateActiveSeries(now time.Time) {
	for _, userID := range i.getTSDBUsers() {
		userDB := i.getTSDB(userID)
		if userDB == nil {
			continue
		}

		newMatchersConfig := i.limits.ActiveSeriesCustomTrackersConfig(userID)
		newCostAttributionActiveSeriesTracker := i.costAttributionMgr.ActiveSeriesTracker(userID)
		matchersChanged := userDB.activeSeries.MatchersDiffer(newMatchersConfig)
		catChanged := userDB.activeSeries.CostAttributionDiffers(newCostAttributionActiveSeriesTracker)

		idx := userDB.Head().MustIndex()

		var oldMatcherNames []string
		if matchersChanged || catChanged {
			level.Debug(i.logger).Log("msg", "active series config changed, reloading", "user", userID, "matchers_changed", matchersChanged, "cost_attribution_changed", catChanged)
			if matchersChanged {
				// We shouldn't delete the metrics yet, just in case a metrics scrape happens while we're reloading,
				// we don't want to trigger a staleness NaN in the metrics.
				oldMatcherNames = userDB.activeSeries.CurrentMatcherNames()
			}
			userDB.activeSeries.ReloadSeriesConfig(
				asmodel.NewMatchers(newMatchersConfig),
				newCostAttributionActiveSeriesTracker,
				matchersChanged, catChanged, idx,
			)
		}

		userDB.activeSeries.Purge(now, idx)
		idx.Close()

		allActive, activeMatching, allActiveOTLP, allActiveHistograms, activeMatchingHistograms, allActiveBuckets, activeMatchingBuckets := userDB.activeSeries.ActiveWithMatchers()
		if allActive > 0 {
			i.metrics.activeSeriesPerUser.WithLabelValues(userID).Set(float64(allActive))
		} else {
			i.metrics.activeSeriesPerUser.DeleteLabelValues(userID)
		}
		if allActiveOTLP > 0 {
			i.metrics.activeSeriesPerUserOTLP.WithLabelValues(userID).Set(float64(allActiveOTLP))
		} else {
			i.metrics.activeSeriesPerUserOTLP.DeleteLabelValues(userID)
		}
		if allActiveHistograms > 0 {
			i.metrics.activeSeriesPerUserNativeHistograms.WithLabelValues(userID).Set(float64(allActiveHistograms))
		} else {
			i.metrics.activeSeriesPerUserNativeHistograms.DeleteLabelValues(userID)
		}
		if allActiveBuckets > 0 {
			i.metrics.activeNativeHistogramBucketsPerUser.WithLabelValues(userID).Set(float64(allActiveBuckets))
		} else {
			i.metrics.activeNativeHistogramBucketsPerUser.DeleteLabelValues(userID)
		}

		attributedActiveSeriesFailure := userDB.activeSeries.ActiveSeriesAttributionFailureCount()
		if attributedActiveSeriesFailure > 0 {
			i.metrics.attributedActiveSeriesFailuresPerUser.WithLabelValues(userID).Add(attributedActiveSeriesFailure)
		}

		for idx, name := range userDB.activeSeries.CurrentMatcherNames() {
			// We only set the metrics for matchers that actually exist, to avoid increasing cardinality with zero valued metrics.
			if activeMatching[idx] > 0 {
				i.metrics.activeSeriesCustomTrackersPerUser.WithLabelValues(userID, name).Set(float64(activeMatching[idx]))
			} else {
				i.metrics.activeSeriesCustomTrackersPerUser.DeleteLabelValues(userID, name)
			}
			if activeMatchingHistograms[idx] > 0 {
				i.metrics.activeSeriesCustomTrackersPerUserNativeHistograms.WithLabelValues(userID, name).Set(float64(activeMatchingHistograms[idx]))
			} else {
				i.metrics.activeSeriesCustomTrackersPerUserNativeHistograms.DeleteLabelValues(userID, name)
			}
			if activeMatchingBuckets[idx] > 0 {
				i.metrics.activeNativeHistogramBucketsCustomTrackersPerUser.WithLabelValues(userID, name).Set(float64(activeMatchingBuckets[idx]))
			} else {
				i.metrics.activeNativeHistogramBucketsCustomTrackersPerUser.DeleteLabelValues(userID, name)
			}
		}

		// Remove the metrics belonging to old matchers now.
		if matchersChanged {
			newNames := userDB.activeSeries.CurrentMatcherNames()
			newNamesSet := make(map[string]struct{}, len(newNames))
			for _, name := range newNames {
				newNamesSet[name] = struct{}{}
			}
			for _, oldName := range oldMatcherNames {
				if _, exists := newNamesSet[oldName]; !exists {
					i.metrics.activeSeriesCustomTrackersPerUser.DeleteLabelValues(userID, oldName)
					i.metrics.activeSeriesCustomTrackersPerUserNativeHistograms.DeleteLabelValues(userID, oldName)
					i.metrics.activeNativeHistogramBucketsCustomTrackersPerUser.DeleteLabelValues(userID, oldName)
				}
			}
		}
	}
}

// updateUsageStats updated some anonymous usage statistics tracked by the ingester.
// This function is expected to be called periodically.
func (i *Ingester) updateUsageStats() {
	memoryUsersCount := int64(0)
	memorySeriesCount := int64(0)
	activeSeriesCount := int64(0)
	tenantsWithOutOfOrderEnabledCount := int64(0)
	minOutOfOrderTimeWindow := time.Duration(0)
	maxOutOfOrderTimeWindow := time.Duration(0)

	for _, userID := range i.getTSDBUsers() {
		userDB := i.getTSDB(userID)
		if userDB == nil {
			continue
		}

		// Track only tenants with at least 1 series.
		numSeries := userDB.Head().NumSeries()
		if numSeries == 0 {
			continue
		}

		memoryUsersCount++
		memorySeriesCount += int64(numSeries)

		activeSeries, _, _, _ := userDB.activeSeries.Active()
		activeSeriesCount += int64(activeSeries)

		oooWindow := i.limits.OutOfOrderTimeWindow(userID)
		if oooWindow > 0 {
			tenantsWithOutOfOrderEnabledCount++

			if minOutOfOrderTimeWindow == 0 || oooWindow < minOutOfOrderTimeWindow {
				minOutOfOrderTimeWindow = oooWindow
			}
			if oooWindow > maxOutOfOrderTimeWindow {
				maxOutOfOrderTimeWindow = oooWindow
			}
		}
	}

	// Track anonymous usage stats.
	memorySeriesStats.Set(memorySeriesCount)
	activeSeriesStats.Set(activeSeriesCount)
	memoryTenantsStats.Set(memoryUsersCount)
	tenantsWithOutOfOrderEnabledStat.Set(tenantsWithOutOfOrderEnabledCount)
	minOutOfOrderTimeWindowSecondsStat.Set(int64(minOutOfOrderTimeWindow.Seconds()))
	maxOutOfOrderTimeWindowSecondsStat.Set(int64(maxOutOfOrderTimeWindow.Seconds()))
}

func (i *Ingester) updateLimitMetrics() {
	for _, userID := range i.getTSDBUsers() {
		db := i.getTSDB(userID)
		if db == nil {
			continue
		}

		minLocalSeriesLimit := 0
		if i.cfg.UseIngesterOwnedSeriesForLimits || i.cfg.UpdateIngesterOwnedSeries {
			os := db.ownedSeriesState()
			i.metrics.ownedSeriesPerUser.WithLabelValues(userID).Set(float64(os.ownedSeriesCount))

			if i.cfg.UseIngesterOwnedSeriesForLimits {
				minLocalSeriesLimit = os.localSeriesLimit
			}
		}

		localLimit := i.limiter.maxSeriesPerUser(userID, minLocalSeriesLimit)
		i.metrics.maxLocalSeriesPerUser.WithLabelValues(userID).Set(float64(localLimit))
	}
}
