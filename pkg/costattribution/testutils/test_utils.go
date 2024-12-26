// SPDX-License-Identifier: AGPL-3.0-only

package testutils

import "github.com/grafana/mimir/pkg/util/validation"

func GetMockCostAttributionLimits(idx int) (*validation.Overrides, error) {
	baseLimits := map[string]*validation.Limits{
		"user1": {MaxCostAttributionCardinalityPerUser: 5, CostAttributionLabels: []string{"team"}},
		"user2": {MaxCostAttributionCardinalityPerUser: 2, CostAttributionLabels: []string{}},
		"user3": {MaxCostAttributionCardinalityPerUser: 2, CostAttributionLabels: []string{"department", "service"}},
		"user4": {MaxCostAttributionCardinalityPerUser: 5, CostAttributionLabels: []string{"platform"}},
		"user5": {MaxCostAttributionCardinalityPerUser: 10, CostAttributionLabels: []string{"a"}},
	}

	switch idx {
	case 1:
		baseLimits["user1"].CostAttributionLabels = []string{}
	case 2:
		baseLimits["user3"].CostAttributionLabels = []string{"team", "feature"}
	case 3:
		baseLimits["user3"].MaxCostAttributionCardinalityPerUser = 3
	case 4:
		baseLimits["user1"].MaxCostAttributionCardinalityPerUser = 2
	case 5:
		baseLimits["user1"].CostAttributionLabels = []string{"department"}
	}

	return validation.NewOverrides(validation.Limits{}, validation.NewMockTenantLimits(baseLimits))
}
