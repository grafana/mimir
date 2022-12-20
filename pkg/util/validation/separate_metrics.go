// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"strings"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func GroupLabel(o *Overrides, userID string, timeseries []mimirpb.PreallocTimeseries) string {
	if len(timeseries) == 0 {
		return ""
	}

	groupLabel := o.SeparateMetricsLabel(userID)
	if groupLabel == "" {
		// If not set, label value will be "" and dropped by Prometheus
		return groupLabel
	}

	for _, label := range timeseries[0].Labels {
		if label.Name == groupLabel {
			return strings.Clone(label.Value)
		}
	}

	return ""
}
