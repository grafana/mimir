// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/metrics_helper.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package util

import (
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/model/labels"
)

func MatchesSelectors(m *dto.Metric, selectors labels.Labels) bool {
	for _, l := range selectors {
		found := false
		for _, lp := range m.GetLabel() {
			if l.Name != lp.GetName() || l.Value != lp.GetValue() {
				continue
			}

			found = true
			break
		}

		if !found {
			return false
		}
	}

	return true
}
