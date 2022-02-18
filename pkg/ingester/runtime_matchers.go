// SPDX-License-Identifier: AGPL-3.0-only

package ingester

// ActiveSeriesCustomTrackersOverrides holds the definition of custom tracking rules.
type ActiveSeriesCustomTrackersOverrides struct {
	Default   *ActiveSeriesMatchers            `yaml:"default"`
	Overrides map[string]*ActiveSeriesMatchers `yaml:"overrides"`
}

func (asmo *ActiveSeriesCustomTrackersOverrides) MatchersForUser(userID string) *ActiveSeriesMatchers {
	if overrides, ok := asmo.Overrides[userID]; ok {
		return overrides
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
