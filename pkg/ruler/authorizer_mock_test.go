// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"context"

	"github.com/grafana/mimir/pkg/ruler/rulespb"
)

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
