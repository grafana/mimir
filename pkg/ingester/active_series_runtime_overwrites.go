// SPDX-License-Identifier: AGPL-3.0-only

package ingester

// ActiveSeriesCustomTrackersOverrides holds the definition of custom tracking rules.
type ActiveSeriesCustomTrackersOverrides struct {
	Default        *ActiveSeriesMatchers            `yaml:"default"`
	TenantSpecific map[string]*ActiveSeriesMatchers `yaml:"tenant_specific"`
}

func (asmo *ActiveSeriesCustomTrackersOverrides) MatchersForUser(userID string) *ActiveSeriesMatchers {
	if tenantspecific, ok := asmo.TenantSpecific[userID]; ok {
		return tenantspecific
	}
	return asmo.Default
}

type ActiveSeriesCustomTrackersOverridesProvider struct {
	Getter func() *ActiveSeriesCustomTrackersOverrides
}

func (p *ActiveSeriesCustomTrackersOverridesProvider) Get() *ActiveSeriesCustomTrackersOverrides {
	if p == nil || p.Getter == nil {
		return nil
	}
	return p.Getter()
}
