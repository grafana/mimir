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

func NewMockCostAttributionLimits(idx int, userLabels ...[]string) *validation.Overrides {
	return NewMockCostAttributionOverrides(validation.Limits{}, nil, idx, userLabels...)
}

func NewMockCostAttributionOverrides(limits validation.Limits, overrides map[string]*validation.Limits, idx int, userLabels ...[]string) *validation.Overrides {
	if overrides == nil {
		overrides = map[string]*validation.Limits{}
	}
	baseLimits := map[string]*validation.Limits{
		"user1": {MaxCostAttributionCardinality: 5, CostAttributionLabelsStructured: costattributionmodel.Labels{{Input: "team", Output: "my_team"}}},
		"user2": {MaxCostAttributionCardinality: 2, CostAttributionLabelsStructured: costattributionmodel.Labels{}},
		"user3": {MaxCostAttributionCardinality: 2, CostAttributionLabelsStructured: costattributionmodel.Labels{
			{Input: "department", Output: "my_department"},
			{Input: "service", Output: "my_service"},
		}},
		"user4": {MaxCostAttributionCardinality: 5, CostAttributionLabelsStructured: costattributionmodel.Labels{{Input: "platform", Output: "my_platform"}}},
		"user5": {MaxCostAttributionCardinality: 10, CostAttributionLabelsStructured: costattributionmodel.Labels{{Input: "a", Output: "a"}}},
		// user6 has opted to rename team to eng_team.
		"user6": {MaxCostAttributionCardinality: 5, CostAttributionLabelsStructured: costattributionmodel.Labels{{Input: "team", Output: "eng_team"}}},
		"user7": {MaxCostAttributionCardinality: 2, CostAttributionLabelsStructured: costattributionmodel.Labels{{Input: "team", Output: "my_team"}}, CostAttributionCooldown: model.Duration(TestAttributionCooldown)},
	}
	for _, uls := range userLabels {
		costAttributionLabels := make(costattributionmodel.Labels, 0, len(uls)-1)
		for i := 1; i < len(uls); i++ {
			costAttributionLabels = append(costAttributionLabels, costattributionmodel.Label{Input: uls[i]})
		}
		baseLimits[uls[0]] = &validation.Limits{
			MaxCostAttributionCardinality:   10,
			CostAttributionLabelsStructured: costAttributionLabels,
		}
	}
	for userID, l := range baseLimits {
		if overrides[userID] == nil {
			overrides[userID] = l
		} else {
			overrides[userID].MaxCostAttributionCardinality = l.MaxCostAttributionCardinality
			overrides[userID].CostAttributionLabelsStructured = l.CostAttributionLabelsStructured
		}
	}
	switch idx {
	case 1:
		overrides["user1"].CostAttributionLabelsStructured = costattributionmodel.Labels{}
	case 2:
		overrides["user3"].CostAttributionLabelsStructured = costattributionmodel.Labels{
			{Input: "team", Output: "my_team"},
			{Input: "feature", Output: "my_feature"},
		}
	case 3:
		overrides["user3"].MaxCostAttributionCardinality = 3
	case 4:
		overrides["user1"].MaxCostAttributionCardinality = 2
	case 5:
		overrides["user1"].CostAttributionLabelsStructured = costattributionmodel.Labels{{Input: "department", Output: "my_department"}}
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
