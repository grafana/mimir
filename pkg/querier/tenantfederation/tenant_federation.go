// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/tenantfederation/tenant_federation.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package tenantfederation

import (
	"flag"

	"github.com/prometheus/prometheus/model/labels"
)

const (
	defaultTenantLabel   = "__tenant_id__"
	retainExistingPrefix = "original_"
	defaultConcurrency   = 16
	defaultMaxTenants    = 0
)

type Config struct {
	// Enabled switches on support for multi tenant query federation
	Enabled       bool `yaml:"enabled"`
	MaxConcurrent int  `yaml:"max_concurrent" category:"experimental"`
	MaxTenants    int  `yaml:"max_tenants"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, "tenant-federation.enabled", false, "If enabled on all services, queries can be federated across multiple tenants. The tenant IDs involved need to be specified separated by a '|' character in the 'X-Scope-OrgID' header.")
	f.IntVar(&cfg.MaxConcurrent, "tenant-federation.max-concurrent", defaultConcurrency, "The number of workers used for each tenant federated query. This setting limits the maximum number of per-tenant queries executed at a time for a tenant federated query.")
	f.IntVar(&cfg.MaxTenants, "tenant-federation.max-tenants", defaultMaxTenants, "The max number of tenant IDs that may be supplied for a federated query if enabled. 0 to disable the limit.")
}

// FilterValuesByMatchers applies matchers to inputed `idLabelName` and
// `ids`. A set of matched IDs is returned and also all label matchers not
// targeting the `idLabelName` label.
//
// In case a label matcher is set on a label conflicting with `idLabelName`, we
// need to rename this labelMatcher's name to its original name. This is used
// to as part of Select in the mergeQueryable, to ensure only relevant queries
// are considered and the forwarded matchers do not contain matchers on the
// `idLabelName`.
func FilterValuesByMatchers(idLabelName string, ids []string, matchers ...*labels.Matcher) (matchedIDs map[string]struct{}, unrelatedMatchers []*labels.Matcher) {
	// this contains the matchers which are not related to idLabelName
	unrelatedMatchers = make([]*labels.Matcher, 0, len(matchers))

	// build map of values to consider for the matchers
	matchedIDs = sliceToSet(ids)

	for _, m := range matchers {
		switch m.Name {
		// matcher has idLabelName to target a specific tenant(s)
		case idLabelName:
			for value := range matchedIDs {
				if !m.Matches(value) {
					delete(matchedIDs, value)
				}
			}

		// check if has the retained label name
		case retainExistingPrefix + idLabelName:
			// rewrite label to the original name, by copying matcher and
			// replacing the label name
			rewrittenM := *m
			rewrittenM.Name = idLabelName
			unrelatedMatchers = append(unrelatedMatchers, &rewrittenM)

		default:
			unrelatedMatchers = append(unrelatedMatchers, m)
		}
	}

	return matchedIDs, unrelatedMatchers
}

// this sets a label and preserves an existing value a new label prefixed with
// original_. It doesn't do this recursively.
func setLabelsRetainExisting(src labels.Labels, additionalLabels ...labels.Label) labels.Labels {
	lb := labels.NewBuilder(src)

	for _, additionalL := range additionalLabels {
		if oldValue := src.Get(additionalL.Name); oldValue != "" {
			lb.Set(
				retainExistingPrefix+additionalL.Name,
				oldValue,
			)
		}
		lb.Set(additionalL.Name, additionalL.Value)
	}

	return lb.Labels()
}

func sliceToSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, v := range values {
		out[v] = struct{}{}
	}
	return out
}
