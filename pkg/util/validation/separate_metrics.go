// SPDX-License-Identifier: AGPL-3.0-only

package validation

import "github.com/grafana/mimir/pkg/mimirpb"

// GroupLabel obtains the first non-empty group label from the first timeseries in the list of incoming timeseries.
func GroupLabel(o *Overrides, userID string, req mimirpb.IWriteRequest) string {
	groupLabel := o.SeparateMetricsGroupLabel(userID)
	if groupLabel == "" {
		// If not set, label value will be "" and dropped by Prometheus
		return ""
	}

	return req.Label(groupLabel)
}
