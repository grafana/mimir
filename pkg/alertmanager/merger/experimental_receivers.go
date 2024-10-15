// SPDX-License-Identifier: AGPL-3.0-only

package merger

import (
	"encoding/json"
	"sort"
	"time"

	alertingmodels "github.com/grafana/alerting/models"
)

// ExperimentalReceivers implements the Merger interface for GET /api/v1/grafana/receivers.
// It returns the union of receivers and integrations from all responses, merging duplicates.
// When an integration is duplicated in multiple responses, the integration status is taken
// from the integration with the most recent notification attempt.
type ExperimentalReceivers struct{}

func (ExperimentalReceivers) MergeResponses(in [][]byte) ([]byte, error) {
	responses := make([]alertingmodels.Receiver, 0)
	for _, body := range in {
		parsed := make([]alertingmodels.Receiver, 0)
		if err := json.Unmarshal(body, &parsed); err != nil {
			return nil, err
		}
		responses = append(responses, parsed...)
	}

	merged, err := mergeReceivers(responses)
	if err != nil {
		return nil, err
	}

	return json.Marshal(merged)
}

func mergeReceivers(in []alertingmodels.Receiver) ([]alertingmodels.Receiver, error) {
	receivers := make(map[string]alertingmodels.Receiver)
	for _, recv := range in {
		name := recv.Name

		if current, ok := receivers[name]; ok {
			merged, err := mergeReceiver(current, recv)
			if err != nil {
				return nil, err
			}
			receivers[name] = merged
		} else {
			receivers[name] = recv
		}
	}

	result := make([]alertingmodels.Receiver, 0, len(receivers))
	for _, recv := range receivers {
		result = append(result, recv)
	}

	// Sort receivers by name to give a stable response.
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result, nil
}

func mergeReceiver(lhs alertingmodels.Receiver, rhs alertingmodels.Receiver) (alertingmodels.Receiver, error) {
	// Receiver is active if it's active anywhere. In theory these should never
	// be different, but perhaps during reconfiguration, one replica thinks
	// a receiver is active, and another doesn't.
	active := lhs.Active || rhs.Active

	integrations, err := mergeIntegrations(append(lhs.Integrations, rhs.Integrations...))
	if err != nil {
		return alertingmodels.Receiver{}, err
	}

	return alertingmodels.Receiver{
		Name:         lhs.Name,
		Active:       active,
		Integrations: integrations,
	}, nil
}

func mergeIntegrations(in []alertingmodels.Integration) ([]alertingmodels.Integration, error) {
	integrations := make(map[string]alertingmodels.Integration)

	for _, integration := range in {
		if current, ok := integrations[integration.Name]; ok {
			// If this is a duplicate integration, check if it has a more recent
			// notification attempt than the current integration.
			if !time.Time(integration.LastNotifyAttempt).After(time.Time(current.LastNotifyAttempt)) {
				continue
			}
		}
		integrations[integration.Name] = integration
	}

	result := make([]alertingmodels.Integration, 0, len(integrations))
	for _, integration := range integrations {
		result = append(result, integration)
	}

	// Sort integrations by name to give a stable response.
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result, nil
}
