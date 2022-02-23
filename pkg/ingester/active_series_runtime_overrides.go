// SPDX-License-Identifier: AGPL-3.0-only

package ingester

// ActiveSeriesCustomTrackersOverrides holds the definition of custom tracking rules.
type ActiveSeriesCustomTrackersOverrides struct {
	Default        *ActiveSeriesCustomTrackersConfig            `yaml:"default"`
	TenantSpecific map[string]*ActiveSeriesCustomTrackersConfig `yaml:"tenant_specific"`
}

func (asmo *ActiveSeriesCustomTrackersOverrides) MatchersForUser(userID string) *ActiveSeriesMatchers {
	if tenantspecific, ok := asmo.TenantSpecific[userID]; ok {
		return NewActiveSeriesMatchers(tenantspecific)
	}
	if asmo.Default == nil {
		return nil
	}
	return NewActiveSeriesMatchers(asmo.Default)
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
