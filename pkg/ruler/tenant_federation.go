package ruler

import (
	"context"
	"flag"

	"github.com/prometheus/prometheus/rules"
	"github.com/weaveworks/common/user"

	"github.com/grafana/mimir/pkg/ruler/rulespb"
	"github.com/grafana/mimir/pkg/tenant"
)

type TenantFederationConfig struct {
	Enabled bool `yaml:"enabled"`
}

func (cfg *TenantFederationConfig) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, "ruler.tenant-federation.enabled", false, "Enable running rule groups against multiple tenants. The tenant IDs involved need to be in the rule group's `source_tenants` field. If this flag is set to `false` when there are already created federated rule groups, then these rules groups will be skipped during evaluations.")
}

// federatedGroupContextFunc prepares the context for federated rules.
// It injects g.SourceTenants() in to the context to be used by mergeQuerier.
func federatedGroupContextFunc(ctx context.Context, g *rules.Group) context.Context {
	if len(g.SourceTenants()) == 0 {
		return ctx
	}
	return user.InjectOrgID(ctx, tenant.JoinTenantIDs(g.SourceTenants()))
}

func removeFederatedRuleGroups(groups map[string]rulespb.RuleGroupList) {
	for userID, groupList := range groups {
		var amended rulespb.RuleGroupList
		for _, group := range groupList {
			if len(group.GetSourceTenants()) > 1 {
				continue
			}
			amended = append(amended, group)
		}
		groups[userID] = amended
	}
}
