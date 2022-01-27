// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"context"
	"flag"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
	"github.com/weaveworks/common/user"

	"github.com/grafana/mimir/pkg/ruler/rulespb"
	"github.com/grafana/mimir/pkg/tenant"
)

type TenantFederationConfig struct {
	Enabled bool `yaml:"enabled"`
}

func (cfg *TenantFederationConfig) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, "ruler.tenant-federation.enabled", false, "Enable running rule groups against multiple tenants. The tenant IDs involved need to be in the rule group's 'source_tenants' field. If this flag is set to 'false' when there are already created federated rule groups, then these rules groups will be skipped during evaluations.")
}

type contextKey int

const federatedGroupSourceTenants contextKey = 1

// FederatedGroupContextFunc prepares the context for federated rules.
// It injects g.SourceTenants() in to the context to be used by mergeQuerier.
func FederatedGroupContextFunc(ctx context.Context, g *rules.Group) context.Context {
	if len(g.SourceTenants()) == 0 {
		return ctx
	}
	return context.WithValue(ctx, federatedGroupSourceTenants, g.SourceTenants())
}

func TenantFederationQueryFunc(regularQueryable, federatedQueryable rules.QueryFunc) rules.QueryFunc {
	return func(ctx context.Context, q string, t time.Time) (promql.Vector, error) {
		if sourceTenants, _ := ctx.Value(federatedGroupSourceTenants).([]string); len(sourceTenants) > 0 {
			ctx = user.InjectOrgID(ctx, tenant.JoinTenantIDs(sourceTenants))
			return federatedQueryable(ctx, q, t)
		}
		return regularQueryable(ctx, q, t)
	}
}

func RemoveFederatedRuleGroups(groups map[string]rulespb.RuleGroupList) {
	for userID, groupList := range groups {
		amended := make(rulespb.RuleGroupList, 0, len(groupList))
		for _, group := range groupList {
			if len(group.GetSourceTenants()) > 0 {
				continue
			}
			amended = append(amended, group)
		}
		groups[userID] = amended
	}
}
