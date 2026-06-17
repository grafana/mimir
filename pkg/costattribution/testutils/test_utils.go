// SPDX-License-Identifier: AGPL-3.0-only

package testutils

import (
	"time"

	"github.com/prometheus/common/model"

	"github.com/grafana/mimir/pkg/costattribution/costattributionmodel"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/validation"
)

const TestAttributionCooldown = 20 * time.Minute

func defaultTracker(labels ...costattributionmodel.Label) costattributionmodel.TrackerConfigs {
	return costattributionmodel.TrackerConfigs{
		costattributionmodel.DefaultTrackerName: {Labels: costattributionmodel.Labels(labels)},
	}
}

func NewMockCostAttributionLimits(userLabels ...[]string) *validation.Overrides {
	return NewMockCostAttributionOverrides(validation.Limits{}, nil, userLabels...)
}

func NewMockCostAttributionOverrides(limits validation.Limits, overrides map[string]*validation.Limits, userLabels ...[]string) *validation.Overrides {
	if overrides == nil {
		overrides = map[string]*validation.Limits{}
	}
	baseLimits := map[string]*validation.Limits{
		"user1": {MaxCostAttributionCardinality: 5, CostAttributionBaseTrackers: defaultTracker(costattributionmodel.Label{Input: "team", Output: "my_team"})},
		"user2": {MaxCostAttributionCardinality: 2},
		"user3": {MaxCostAttributionCardinality: 2, CostAttributionBaseTrackers: defaultTracker(
			costattributionmodel.Label{Input: "department", Output: "my_department"},
			costattributionmodel.Label{Input: "service", Output: "my_service"},
		)},
		"user4": {MaxCostAttributionCardinality: 5, CostAttributionBaseTrackers: defaultTracker(costattributionmodel.Label{Input: "platform", Output: "my_platform"})},
		"user5": {MaxCostAttributionCardinality: 10, CostAttributionBaseTrackers: defaultTracker(costattributionmodel.Label{Input: "a", Output: "a"})},
		// user6 has opted to rename team to eng_team.
		"user6": {MaxCostAttributionCardinality: 5, CostAttributionBaseTrackers: defaultTracker(costattributionmodel.Label{Input: "team", Output: "eng_team"})},
		"user7": {MaxCostAttributionCardinality: 2, CostAttributionBaseTrackers: defaultTracker(costattributionmodel.Label{Input: "team", Output: "my_team"}), CostAttributionCooldown: model.Duration(TestAttributionCooldown)},
		// user8 has a base tracker and an additional tracker.
		"user8": {
			MaxCostAttributionCardinality: 5,
			CostAttributionBaseTrackers:   defaultTracker(costattributionmodel.Label{Input: "team", Output: "my_team"}),
			AdditionalCostAttributionTrackers: costattributionmodel.TrackerConfigs{
				"by-platform": {Labels: costattributionmodel.Labels{{Input: "platform", Output: "my_platform"}}},
			},
		},
		// user9 has only additional trackers (no base).
		// Both of them are internal
		"user9": {
			MaxCostAttributionCardinality: 3,
			AdditionalCostAttributionTrackers: costattributionmodel.TrackerConfigs{
				"by-team":    {Labels: costattributionmodel.Labels{{Input: "team", Output: "my_team"}}, Internal: true},
				"by-service": {Labels: costattributionmodel.Labels{{Input: "service", Output: "my_service"}}, Internal: true},
			},
		},
	}
	for _, uls := range userLabels {
		costAttributionLabels := make(costattributionmodel.Labels, 0, len(uls)-1)
		for i := 1; i < len(uls); i++ {
			costAttributionLabels = append(costAttributionLabels, costattributionmodel.Label{Input: uls[i]})
		}
		baseLimits[uls[0]] = &validation.Limits{
			MaxCostAttributionCardinality: 10,
			CostAttributionBaseTrackers: costattributionmodel.TrackerConfigs{
				costattributionmodel.DefaultTrackerName: {Labels: costAttributionLabels},
			},
		}
	}
	for userID, l := range baseLimits {
		if overrides[userID] == nil {
			overrides[userID] = l
		} else {
			overrides[userID].MaxCostAttributionCardinality = l.MaxCostAttributionCardinality
			overrides[userID].CostAttributionBaseTrackers = l.CostAttributionBaseTrackers
			overrides[userID].AdditionalCostAttributionTrackers = l.AdditionalCostAttributionTrackers
		}
	}
	for _, l := range overrides {
		l.CostAttributionBaseTrackers.Canonicalize()
		l.AdditionalCostAttributionTrackers.Canonicalize()
		l.ComputeCostAttributionConfigHash()
	}
	return validation.NewOverrides(limits, validation.NewMockTenantLimits(overrides))
}

type Series struct {
	LabelValues  []string
	SamplesCount int
}

func CreateRequest(data []Series) *mimirpb.WriteRequest {
	timeSeries := make([]mimirpb.PreallocTimeseries, 0, len(data))
	for i := range data {
		var Labels []mimirpb.LabelAdapter
		for j := 0; j+1 < len(data[i].LabelValues); j += 2 {
			Labels = append(Labels, mimirpb.LabelAdapter{Name: data[i].LabelValues[j], Value: data[i].LabelValues[j+1]})
		}
		timeSeries = append(timeSeries, mimirpb.PreallocTimeseries{
			TimeSeries: &mimirpb.TimeSeries{
				Labels:  Labels,
				Samples: make([]mimirpb.Sample, data[i].SamplesCount),
			},
		})
	}
	return &mimirpb.WriteRequest{Timeseries: timeSeries}
}
