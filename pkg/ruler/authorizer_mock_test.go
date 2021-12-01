package ruler

import (
	"context"
	"fmt"

	"github.com/grafana/mimir/pkg/ruler/rulespb"
)

type groupID struct {
	user, namespace, name string
}

type mockAuthorizer struct {
	authorizedGroups map[groupID]struct{}
}

// newMockAuthorizer returns an Authorizer that will authorize any rulespb.RuleGroupDesc with User, Namespace, and Name
// that match the ones provided.
func newMockAuthorizer(authorizedGroups ...*rulespb.RuleGroupDesc) *mockAuthorizer {
	a := &mockAuthorizer{
		authorizedGroups: make(map[groupID]struct{}, len(authorizedGroups)),
	}

	for _, d := range authorizedGroups {
		gID := groupID{
			user:      d.User,
			namespace: d.Namespace,
			name:      d.Name,
		}
		a.authorizedGroups[gID] = struct{}{}
	}
	return a
}

func (a *mockAuthorizer) AuthorizeGroup(_ context.Context, d *rulespb.RuleGroupDesc) error {
	gID := groupID{
		user:      d.User,
		namespace: d.Namespace,
		name:      d.Name,
	}

	if _, ok := a.authorizedGroups[gID]; !ok {
		return fmt.Errorf("group is not authorized")
	}
	return nil
}
