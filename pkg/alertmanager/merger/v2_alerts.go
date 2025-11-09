// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/alertmanager/merger/v2_alerts.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package merger

import (
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/go-openapi/swag/jsonutils"
	v2_models "github.com/prometheus/alertmanager/api/v2/models"
)

// V2Alerts implements the Merger interface for GET /v2/alerts. It returns the union
// of alerts over all the responses. When the same alert exists in multiple responses, the
// instance of that alert with the most recent UpdatedAt timestamp is returned in the response.
type V2Alerts struct{}

func (V2Alerts) MergeResponses(in [][]byte) ([]byte, error) {
	alerts := make(v2_models.GettableAlerts, 0)
	for _, body := range in {
		parsed := make(v2_models.GettableAlerts, 0)
		if err := jsonutils.ReadJSON(body, &parsed); err != nil {
			return nil, err
		}
		alerts = append(alerts, parsed...)
	}

	merged, err := mergeV2Alerts(alerts)
	if err != nil {
		return nil, err
	}

	return jsonutils.WriteJSON(merged)
}

func mergeV2Alerts(in v2_models.GettableAlerts) (v2_models.GettableAlerts, error) {
	// Select the most recently updated alert for each distinct alert.
	alerts := make(map[string]*v2_models.GettableAlert)
	for _, alert := range in {
		if alert.Fingerprint == nil {
			return nil, errors.New("unexpected nil fingerprint")
		}
		if alert.UpdatedAt == nil {
			return nil, errors.New("unexpected nil updatedAt")
		}

		key := *alert.Fingerprint
		if current, ok := alerts[key]; ok {
			if time.Time(*alert.UpdatedAt).After(time.Time(*current.UpdatedAt)) {
				alerts[key] = alert
			}
		} else {
			alerts[key] = alert
		}
	}

	result := make(v2_models.GettableAlerts, 0, len(alerts))
	for _, alert := range alerts {
		result = append(result, alert)
	}

	// Mimic Alertmanager which returns alerts ordered by fingerprint (as string).
	slices.SortFunc(result, func(a, b *v2_models.GettableAlert) int {
		return strings.Compare(*a.Fingerprint, *b.Fingerprint)
	})

	return result, nil
}
