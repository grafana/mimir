// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/extprom/extprom.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package extprom

import "github.com/prometheus/client_golang/prometheus"

// WrapRegistererWithPrefix is like prometheus.WrapRegistererWithPrefix but it passes nil straight through
// which allows nil check.
func WrapRegistererWithPrefix(prefix string, reg prometheus.Registerer) prometheus.Registerer {
	if reg == nil {
		return nil
	}
	return prometheus.WrapRegistererWithPrefix(prefix, reg)
}

// WrapRegistererWith is like prometheus.WrapRegistererWith but it passes nil straight through
// which allows nil check.
func WrapRegistererWith(labels prometheus.Labels, reg prometheus.Registerer) prometheus.Registerer {
	if reg == nil {
		return nil
	}
	return prometheus.WrapRegistererWith(labels, reg)
}
