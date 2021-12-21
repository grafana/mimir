package ruler

import (
	"context"
	"testing"

	"github.com/grafana/dskit/services"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

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
		"tenant federation enabled": {
			tenantFederationEnabled: true,
			existingRules:           rulespb.RuleGroupList{federatedGroupWithOneTenant, federatedGroupWithMultipleTenants},

			expectedRunningGroupsNames: []string{federatedGroupWithOneTenant.Name, federatedGroupWithMultipleTenants.Name},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {

			cfg := defaultRulerConfig(t)
			cfg.TenantFederation.Enabled = tc.tenantFederationEnabled
			existingRules := map[string]rulespb.RuleGroupList{userID: tc.existingRules}

			r := buildRuler(t, cfg, newMockRuleStore(existingRules), nil, nil)

			require.NoError(t, services.StartAndAwaitRunning(context.Background(), r))
			t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(context.Background(), r)) })

			r.syncRules(context.Background(), rulerSyncReasonInitial)

			loadedGroups, err := r.GetRules(user.InjectOrgID(context.Background(), userID))
			require.NoError(t, err)

			loadedGroupsNames := make([]string, len(loadedGroups))
			for i, g := range loadedGroups {
				loadedGroupsNames[i] = g.Group.Name
			}
			require.ElementsMatch(t, tc.expectedRunningGroupsNames, loadedGroupsNames)
		})
	}
}
