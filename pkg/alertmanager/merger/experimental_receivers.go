// SPDX-License-Identifier: AGPL-3.0-only

package merger

import (
	"encoding/json"
	"errors"
	"sort"
	"time"

	//	"github.com/go-openapi/swag"
	am_models "github.com/grafana/mimir/pkg/alertmanager/models"
)

// V2Alerts implements the Merger interface for GET /v2/alerts. It returns the union
// of alerts over all the responses. When the same alert exists in multiple responses, the
// instance of that alert with the most recent UpdatedAt timestamp is returned in the response.
type ExperimentalReceivers struct{}

func (ExperimentalReceivers) MergeResponses(in [][]byte) ([]byte, error) {
	responses := make([]am_models.Receiver, 0)
	for _, body := range in {
		parsed := make([]am_models.Receiver, 0)
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

func mergeReceivers(in []am_models.Receiver) ([]am_models.Receiver, error) {
	receivers := make(map[string]am_models.Receiver)
	for _, recv := range in {
		if recv.Name == nil {
			return nil, errors.New("unexpected nil receiver name")
		}
		name := *recv.Name

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

	result := make([]am_models.Receiver, 0, len(receivers))
	for _, recv := range receivers {
		result = append(result, recv)
	}

	// Sort receivers by name to give a stable response.
	sort.Slice(result, func(i, j int) bool {
		return *result[i].Name < *result[j].Name
	})

	return result, nil
}

func mergeReceiver(lhs am_models.Receiver, rhs am_models.Receiver) (am_models.Receiver, error) {
	if lhs.Name == nil || rhs.Name == nil {
		return am_models.Receiver{}, errors.New("unexpected nil name field in receiver")
	}
	if lhs.Active == nil || rhs.Active == nil {
		return am_models.Receiver{}, errors.New("unexpected nil active field in receiver")
	}

	// Receiver is active if it's active anywhere. In theory these should never
	// be different, but perhaps during reconfiguration, one replica thinks
	// a receiver is active, and another doesn't.
	active := *lhs.Active || *rhs.Active

	integrations, err := mergeIntegrations(append(lhs.Integrations, rhs.Integrations...))
	if err != nil {
		return am_models.Receiver{}, err
	}

	return am_models.Receiver{
		Name:         lhs.Name,
		Active:       &active,
		Integrations: integrations,
	}, nil
}

func mergeIntegrations(in []*am_models.Integration) ([]*am_models.Integration, error) {
	integrations := make(map[string]am_models.Integration)

	for _, integration := range in {
		if integration == nil {
			return nil, errors.New("unexpected nil integration")
		}
		if integration.Name == nil {
			return nil, errors.New("unexpected nil integration name")
		}
		if current, ok := integrations[*integration.Name]; ok {
			// If this is a duplicate integration, check if it has a more recent
			// notification attempt than the current integration.
			if time.Time(integration.LastNotifyAttempt).After(time.Time(current.LastNotifyAttempt)) {
				integrations[*integration.Name] = *integration
			}
		} else {
			integrations[*integration.Name] = *integration
		}

	}

	result := make([]*am_models.Integration, 0, len(integrations))
	for _, integration := range integrations {
		integrationCopy := integration
		result = append(result, &integrationCopy)
	}

	// Sort integrations by name to give a stable response.
	sort.Slice(result, func(i, j int) bool {
		return *result[i].Name < *result[j].Name
	})

	return result, nil
}
