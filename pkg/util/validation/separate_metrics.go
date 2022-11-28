// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"fmt"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func FindGroupLabel(o *Overrides, userID string, labels []mimirpb.LabelAdapter) string {
	groupLabel := o.SeparateMetricsLabel(userID)
	fmt.Println("FAZ GROUP: ", groupLabel)
	if groupLabel == "" {
		// If not set, label value will be "" and dropped by Prometheus
		return groupLabel
	}

	for _, label := range labels {
		fmt.Println("FAZ LABEL: ", label.Name)
		if label.Name == groupLabel {
			// return strings.Clone(label.Value)
			return label.Value
		}
	}

	return ""
}
