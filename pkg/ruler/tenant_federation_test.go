// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ruler/rulespb"
)

// TestRuler_TenantFederationFlag tests the case where Config.TenantFederation.Enabled = true but there are
// already existing federated rules in the store. We expect that these federated rules aren't being evaluated.
func TestRuler_TenantFederationFlag(t *testing.T) {
	const userID = "tenant-1"

	regularGroup := &rulespb.RuleGroupDesc{
		Namespace:     "ns",
		Name:          "non-federated",
		User:          userID,
		SourceTenants: []string{},
	}

	federatedGroupWithOneTenant := &rulespb.RuleGroupDesc{
		Namespace:     "ns",
		Name:          "federated-1",
		User:          userID,
		SourceTenants: []string{"tenant-2"},
	}

	federatedGroupWithMultipleTenants := &rulespb.RuleGroupDesc{
		Namespace:     "ns",
		Name:          "federated-2",
		User:          userID,
		SourceTenants: []string{"tenant-2", "tenant-3"},
	}

	testCases := map[string]struct {
		tenantFederationEnabled bool
		existingRules           rulespb.RuleGroupList

		expectedRunningGroupsNames []string
	}{
		"tenant federation disabled": {
			tenantFederationEnabled: false,
			existingRules:           rulespb.RuleGroupList{federatedGroupWithOneTenant, federatedGroupWithMultipleTenants},

			expectedRunningGroupsNames: []string{},
		},
		"tenant federation disabled with a federated rules and a regular rule": {
			tenantFederationEnabled: false,
			existingRules:           rulespb.RuleGroupList{federatedGroupWithOneTenant, regularGroup},

			expectedRunningGroupsNames: []string{regularGroup.Name},
		},
		"tenant federation enabled only with federated groups": {
			tenantFederationEnabled: true,
			existingRules:           rulespb.RuleGroupList{federatedGroupWithOneTenant, federatedGroupWithMultipleTenants},

			expectedRunningGroupsNames: []string{federatedGroupWithOneTenant.Name, federatedGroupWithMultipleTenants.Name},
		},
		"tenant federation enabled with federated and regular groups": {
			tenantFederationEnabled: true,
			existingRules:           rulespb.RuleGroupList{regularGroup, federatedGroupWithOneTenant, federatedGroupWithMultipleTenants},

			expectedRunningGroupsNames: []string{regularGroup.Name, federatedGroupWithOneTenant.Name, federatedGroupWithMultipleTenants.Name},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {

			cfg := defaultRulerConfig(t)
			cfg.TenantFederation.Enabled = tc.tenantFederationEnabled
			existingRules := map[string]rulespb.RuleGroupList{userID: tc.existingRules}

			r := prepareRulerManager(t, cfg)
			t.Cleanup(r.Stop)

			r.SyncRuleGroups(context.Background(), existingRules)
			r.Start()

			var loadedGroupsNames []string
			for _, g := range r.GetRules(userID) {
				loadedGroupsNames = append(loadedGroupsNames, g.Name())
			}

			require.ElementsMatch(t, tc.expectedRunningGroupsNames, loadedGroupsNames)
		})
	}
}
