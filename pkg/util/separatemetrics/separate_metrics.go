// SPDX-License-Identifier: AGPL-3.0-only

package separatemetrics

import (
	"strings"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/validation"
)

// FindGroupLabel returns the value of the group label if the separating metrics feature is
// enabled and the group label found.
func FindGroupLabel(o *validation.Overrides, userID string, labels []mimirpb.LabelAdapter) string {
	if !o.EnableSeparatingMetrics(userID) {
		// If the group label is empty, Prometheus will drop it when scraping the metrics. So a
		// blank group label is the same as having no group label.
		return ""
	}

	groupLabel := o.SeparateMetricsLabel(userID)
	for _, pair := range labels {
		if pair.Name == groupLabel {
			return strings.Clone(pair.Value)
		}
	}

	return ""
}
