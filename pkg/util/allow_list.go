// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/allowed_tenants.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package util

// AllowList that can answer whether an item is allowed or not based on configuration.
// Default value (nil) allows all.
type AllowList struct {
	// If empty, all items are enabled. If not empty, only items in the map are enabled.
	enabled map[string]struct{}

	// If empty, no items are disabled. If not empty, items in the map are disabled.
	disabled map[string]struct{}
}

// NewAllowList builds new allowlist based on enabled and disabled items.
// If there are any enabled items, then only those are allowed.
// If there are any disabled items, then the item from that list that would normally be allowed, is disabled instead.
func NewAllowList(enabled []string, disabled []string) *AllowList {
	a := &AllowList{}

	if len(enabled) > 0 {
		a.enabled = make(map[string]struct{}, len(enabled))
		for _, u := range enabled {
			a.enabled[u] = struct{}{}
		}
	}

	if len(disabled) > 0 {
		a.disabled = make(map[string]struct{}, len(disabled))
		for _, u := range disabled {
			a.disabled[u] = struct{}{}
		}
	}

	return a
}

func (a *AllowList) IsAllowed(v string) bool {
	if a == nil {
		return true
	}

	if len(a.enabled) > 0 {
		if _, ok := a.enabled[v]; !ok {
			return false
		}
	}

	if len(a.disabled) > 0 {
		if _, ok := a.disabled[v]; ok {
			return false
		}
	}

	return true
}
