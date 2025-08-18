// SPDX-License-Identifier: AGPL-3.0-only

package testutils

import (
	"github.com/grafana/mimir/pkg/costattribution/costattributionmodel"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/validation"
)

func NewMockCostAttributionLimits(idx int, userLabels ...[]string) *validation.Overrides {
	return NewMockCostAttributionOverrides(validation.Limits{}, nil, idx, userLabels...)
}

func NewMockCostAttributionOverrides(limits validation.Limits, overrides map[string]*validation.Limits, idx int, userLabels ...[]string) *validation.Overrides {
	if overrides == nil {
		overrides = map[string]*validation.Limits{}
	}
	baseLimits := map[string]*validation.Limits{
		"user1": {MaxCostAttributionCardinality: 5, CostAttributionLabels: []string{"team"}},
		"user2": {MaxCostAttributionCardinality: 2, CostAttributionLabels: []string{}},
		"user3": {MaxCostAttributionCardinality: 2, CostAttributionLabels: []string{"department", "service"}},
		"user4": {MaxCostAttributionCardinality: 5, CostAttributionLabels: []string{"platform"}},
		"user5": {MaxCostAttributionCardinality: 10, CostAttributionLabels: []string{"a"}},
		// user6 has opted to rename team to eng_team.
		"user6": {MaxCostAttributionCardinality: 5, CostAttributionLabelsStructured: []costattributionmodel.Label{{Input: "team", Output: "eng_team"}}},
	}
	for _, uls := range userLabels {
		baseLimits[uls[0]] = &validation.Limits{
			MaxCostAttributionCardinality: 10,
			CostAttributionLabels:         uls[1:],
		}
	}
	for userID, l := range baseLimits {
		if overrides[userID] == nil {
			overrides[userID] = l
		} else {
			overrides[userID].MaxCostAttributionCardinality = l.MaxCostAttributionCardinality
			overrides[userID].CostAttributionLabels = l.CostAttributionLabels
			overrides[userID].CostAttributionLabelsStructured = l.CostAttributionLabelsStructured
		}
	}
	switch idx {
	case 1:
		overrides["user1"].CostAttributionLabels = []string{}
	case 2:
		overrides["user3"].CostAttributionLabels = []string{"team", "feature"}
	case 3:
		overrides["user3"].MaxCostAttributionCardinality = 3
	case 4:
		overrides["user1"].MaxCostAttributionCardinality = 2
	case 5:
		overrides["user1"].CostAttributionLabels = []string{"department"}
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
