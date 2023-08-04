// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"context"
	"flag"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"

	"github.com/grafana/mimir/pkg/ruler/rulespb"
)

type TenantFederationConfig struct {
	Enabled bool `yaml:"enabled"`
}

const TenantFederationFlag = "ruler.tenant-federation.enabled"

func (cfg *TenantFederationConfig) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, TenantFederationFlag, false, "Enable rule groups to query against multiple tenants. The tenant IDs involved need to be in the rule group's 'source_tenants' field. If this flag is set to 'false' when there are federated rule groups that already exist, then these rules groups will be skipped during evaluations.")
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

// ExtractTenantIDs gets the rule group org ID from the context.
func ExtractTenantIDs(ctx context.Context) (string, error) {
	if sourceTenants, _ := ctx.Value(federatedGroupSourceTenants).([]string); len(sourceTenants) > 0 {
		return tenant.JoinTenantIDs(tenant.NormalizeTenantIDs(sourceTenants)), nil
	}
	return tenant.TenantID(ctx)
}

func TenantFederationQueryFunc(regularQueryable, federatedQueryable rules.QueryFunc) rules.QueryFunc {
	return func(ctx context.Context, q string, t time.Time) (promql.Vector, error) {
		if sourceTenants, _ := ctx.Value(federatedGroupSourceTenants).([]string); len(sourceTenants) > 0 {
			ctx = user.InjectOrgID(ctx, tenant.JoinTenantIDs(tenant.NormalizeTenantIDs(sourceTenants)))
			return federatedQueryable(ctx, q, t)
		}
		return regularQueryable(ctx, q, t)
	}
}

func removeFederatedRuleGroups(groups map[string]rulespb.RuleGroupList, logger log.Logger) {
	for userID, groupList := range groups {
		amended := make(rulespb.RuleGroupList, 0, len(groupList))
		for _, group := range groupList {
			if len(group.GetSourceTenants()) > 0 {
				level.Warn(logger).Log(
					"msg", "skipping federated rule group because rule federation is disabled; "+
						"to enable the feature, configure the Mimir or GEM ruler instance with the following parameter "+
						"or contact your service administrator: set -"+TenantFederationFlag+"=true as a CLI argument, "+
						"ruler.tenant_federation.enabled: true in YAML",
					"namespace", group.Namespace,
					"group_name", group.Name,
					"user", userID,
				)
				continue
			}
			amended = append(amended, group)
		}
		groups[userID] = amended
	}
}
