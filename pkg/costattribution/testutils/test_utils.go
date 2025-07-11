// SPDX-License-Identifier: AGPL-3.0-only

package testutils

import (
	"github.com/grafana/mimir/pkg/costattribution/model"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/validation"
)

func NewMockCostAttributionLimits(idx int, lvs ...string) *validation.Overrides {
	baseLimits := map[string]*validation.Limits{
		"user1": {MaxCostAttributionCardinality: 5, CostAttributionLabels: []string{"team"}},
		"user2": {MaxCostAttributionCardinality: 2, CostAttributionLabels: []string{}},
		"user3": {MaxCostAttributionCardinality: 2, CostAttributionLabels: []string{"department", "service"}},
		"user4": {MaxCostAttributionCardinality: 5, CostAttributionLabels: []string{"platform"}},
		"user5": {MaxCostAttributionCardinality: 10, CostAttributionLabels: []string{"a"}},
		// user6 has opted to rename team to eng_team.
		"user6": {MaxCostAttributionCardinality: 5, CostAttributionLabelsStructured: []model.Label{{Input: "team", Output: "eng_team"}}},
	}
	if len(lvs) > 0 {
		baseLimits[lvs[0]] = &validation.Limits{
			MaxCostAttributionCardinality: 10,
			CostAttributionLabels:         lvs[1:],
		}
	}
	switch idx {
	case 1:
		baseLimits["user1"].CostAttributionLabels = []string{}
	case 2:
		baseLimits["user3"].CostAttributionLabels = []string{"team", "feature"}
	case 3:
		baseLimits["user3"].MaxCostAttributionCardinality = 3
	case 4:
		baseLimits["user1"].MaxCostAttributionCardinality = 2
	case 5:
		baseLimits["user1"].CostAttributionLabels = []string{"department"}
	}

	return validation.NewOverrides(validation.Limits{}, validation.NewMockTenantLimits(baseLimits))
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
