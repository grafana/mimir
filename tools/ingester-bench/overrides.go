// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

// Per-tenant limits the ingesters run with, as a multi-tenant cell sets them: a series limit and an early head
// compaction threshold above each tenant's series, a few tenants with cost attribution trackers of their own and the
// largest with additional active series trackers.
const (
	// Shares of the tenants with additional cost attribution trackers and with additional active series trackers.
	costAttributionShare = 0.02
	customTrackersShare  = 0.003
	customTrackersMax    = 45
	// The label the base cost attribution tracker, and the tenants' additional ones, attribute series by.
	costAttributionLabel           = "l0"
	additionalCostAttributionLabel = "l1"
)

type tenantLimits struct {
	MaxGlobalSeriesPerUser                  int                    `json:"max_global_series_per_user"`
	MaxActiveSeriesPerUser                  int                    `json:"max_active_series_per_user"`
	EarlyHeadCompactionOwnedSeriesThreshold int                    `json:"early_head_compaction_owned_series_threshold"`
	AdditionalCostAttributionTrackers       map[string]costTracker `json:"additional_cost_attribution_trackers,omitempty"`
	ActiveSeriesAdditionalCustomTrackers    map[string]string      `json:"active_series_additional_custom_trackers,omitempty"`
}

type costTracker struct {
	Labels []costLabel `json:"labels"`
}

type costLabel struct {
	Input  string `json:"input"`
	Output string `json:"output,omitempty"`
}

// baseCostAttributionTrackers is the cost attribution trackers of every tenant, as -validation.cost-attribution-trackers.
func baseCostAttributionTrackers() string {
	data, _ := json.Marshal(map[string]costTracker{"by-source": {Labels: []costLabel{{Input: costAttributionLabel, Output: "source"}}}})
	return string(data)
}

// writeOverrides writes the runtime configuration of the world's tenants' limits.
func writeOverrides(w *world, path string) error {
	tenants := append([]*tenantModel(nil), w.tenants...)
	// By size, the largest first: they're the ones with trackers of their own.
	sort.SliceStable(tenants, func(a, b int) bool { return tenants[a].series > tenants[b].series })
	overrides := map[string]tenantLimits{}
	for index, t := range tenants {
		// The world's series slots are about the active series; the head holds the churned ones besides.
		limits := tenantLimits{
			MaxGlobalSeriesPerUser:                  max(15_000, 3*t.series),
			MaxActiveSeriesPerUser:                  max(15_000, 2*t.series),
			EarlyHeadCompactionOwnedSeriesThreshold: max(150_000, 2*t.series),
		}
		if float64(index) < costAttributionShare*float64(len(tenants)) || index == 0 {
			limits.AdditionalCostAttributionTrackers = map[string]costTracker{"cost-attribution": {Labels: []costLabel{{Input: additionalCostAttributionLabel, Output: "team"}}}}
		}
		if float64(index) < customTrackersShare*float64(len(tenants)) || index == 0 {
			limits.ActiveSeriesAdditionalCustomTrackers = customTrackers(t)
		}
		overrides[t.id] = limits
	}
	data, err := json.MarshalIndent(map[string]any{"overrides": overrides}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

// customTrackers are selectors of a tenant's own label values, as a team's trackers of its namespaces would be.
func customTrackers(t *tenantModel) map[string]string {
	trackers := map[string]string{}
	values := t.pools[costAttributionLabel]
	for i := 0; i < len(values) && len(trackers) < customTrackersMax; i++ {
		trackers[fmt.Sprintf("group/%02d", i)] = fmt.Sprintf(`{%s=%q}`, costAttributionLabel, values[i])
	}
	return trackers
}
