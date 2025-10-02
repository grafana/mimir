// SPDX-License-Identifier: AGPL-3.0-only

package merger

import (
	"encoding/json"
	"slices"
	"strings"
	"time"

	alertingmodels "github.com/grafana/alerting/models"
)

// ExperimentalReceivers implements the Merger interface for GET /api/v1/grafana/receivers.
// It returns the union of receivers and integrations from all responses, merging duplicates.
// When an integration is duplicated in multiple responses, the integration status is taken
// from the integration with the most recent notification attempt.
type ExperimentalReceivers struct{}

func (ExperimentalReceivers) MergeResponses(in [][]byte) ([]byte, error) {
	responses := make([]alertingmodels.ReceiverStatus, 0)
	for _, body := range in {
		parsed := make([]alertingmodels.ReceiverStatus, 0)
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

func mergeReceivers(in []alertingmodels.ReceiverStatus) ([]alertingmodels.ReceiverStatus, error) {
	receivers := make(map[string]alertingmodels.ReceiverStatus)
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

	result := make([]alertingmodels.ReceiverStatus, 0, len(receivers))
	for _, recv := range receivers {
		result = append(result, recv)
	}

	// Sort receivers by name to give a stable response.
	slices.SortFunc(result, func(a, b alertingmodels.ReceiverStatus) int {
		return strings.Compare(a.Name, b.Name)
	})

	return result, nil
}

func mergeReceiver(lhs alertingmodels.ReceiverStatus, rhs alertingmodels.ReceiverStatus) (alertingmodels.ReceiverStatus, error) {
	// Receiver is active if it's active anywhere. In theory these should never
	// be different, but perhaps during reconfiguration, one replica thinks
	// a receiver is active, and another doesn't.
	active := lhs.Active || rhs.Active

	integrations, err := mergeIntegrations(append(lhs.Integrations, rhs.Integrations...))
	if err != nil {
		return alertingmodels.ReceiverStatus{}, err
	}

	return alertingmodels.ReceiverStatus{
		Name:         lhs.Name,
		Active:       active,
		Integrations: integrations,
	}, nil
}

func mergeIntegrations(in []alertingmodels.IntegrationStatus) ([]alertingmodels.IntegrationStatus, error) {
	integrations := make(map[string]alertingmodels.IntegrationStatus)

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

	result := make([]alertingmodels.IntegrationStatus, 0, len(integrations))
	for _, integration := range integrations {
		result = append(result, integration)
	}

	// Sort integrations by name to give a stable response.
	slices.SortFunc(result, func(a, b alertingmodels.IntegrationStatus) int {
		return strings.Compare(a.Name, b.Name)
	})

	return result, nil
}
