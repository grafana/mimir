// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"context"
	"testing"

	"github.com/grafana/mimir/pkg/ruler/rulespb"
	"github.com/stretchr/testify/require"
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

			r := newManager(t, cfg)
			t.Cleanup(r.Stop)

			r.SyncRuleGroups(context.Background(), existingRules)

			var loadedGroupsNames []string
			for _, g := range r.GetRules(userID) {
				loadedGroupsNames = append(loadedGroupsNames, g.Name())
			}

			require.ElementsMatch(t, tc.expectedRunningGroupsNames, loadedGroupsNames)
		})
	}
}

type groupID struct {
	user, namespace, name string
}

type mockAuthorizer struct {
	err              error
	authorizedGroups map[groupID]struct{}
}

// newMockAuthorizer returns an RuleGroupAuthorizer that will authorize any rulespb.RuleGroupDesc with User, Namespace, and Name
// that match the ones provided.
func newMockAuthorizer(err error, authorizedGroups ...rulespb.RuleGroupList) *mockAuthorizer {
	a := &mockAuthorizer{
		authorizedGroups: make(map[groupID]struct{}, len(authorizedGroups)),
		err:              err,
	}

	for _, groups := range authorizedGroups {
		for _, g := range groups {
			gID := groupID{
				user:      g.User,
				namespace: g.Namespace,
				name:      g.Name,
			}
			a.authorizedGroups[gID] = struct{}{}
		}
	}
	return a
}

func (a *mockAuthorizer) IsAuthorized(_ context.Context, d *rulespb.RuleGroupDesc) (bool, error) {
	if a.err != nil {
		// for the sake of testing return true to make sure that the non-nil error overrides the true value
		return true, a.err
	}

	gID := groupID{
		user:      d.User,
		namespace: d.Namespace,
		name:      d.Name,
	}

	if _, ok := a.authorizedGroups[gID]; !ok {
		return false, nil
	}
	return true, nil
}
