// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"strings"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func FindGroupLabel(o *Overrides, userID string, labels []mimirpb.LabelAdapter) string {
	groupLabel := o.SeparateMetricsLabel(userID)
	if groupLabel == "" {
		// If not set, label value will be "" and dropped by Prometheus
		return groupLabel
	}

	for _, label := range labels {
		if label.Name == groupLabel {
			return strings.Clone(label.Value)
		}
	}

	return ""
}
