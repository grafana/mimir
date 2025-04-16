// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/alertmanager/distributor.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package alertmanager

import (
	"encoding/json"
	"fmt"
	"net/url"
	"slices"
	"strings"

	"github.com/go-kit/log/level"
	"github.com/grafana/alerting/definition"

	"github.com/grafana/mimir/pkg/alertmanager/alertspb"
)

// createUsableGrafanaConfig creates an amConfig from a GrafanaAlertConfigDesc.
// If provided, it assigns the global section from the Mimir config to the Grafana config.
// The SMTP and HTTP settings in this section can be used to configure Grafana receivers.
func (am *MultitenantAlertmanager) createUsableGrafanaConfig(gCfg alertspb.GrafanaAlertConfigDesc, rawMimirConfig string) (amConfig, error) {
	externalURL, err := url.Parse(gCfg.ExternalUrl)
	if err != nil {
		return amConfig{}, err
	}

	var amCfg GrafanaAlertmanagerConfig
	if err := json.Unmarshal([]byte(gCfg.RawConfig), &amCfg); err != nil {
		return amConfig{}, fmt.Errorf("failed to unmarshal Grafana Alertmanager configuration: %w", err)
	}

	if rawMimirConfig != "" {
		cfg, err := definition.LoadCompat([]byte(rawMimirConfig))
		if err != nil {
			return amConfig{}, fmt.Errorf("failed to unmarshal Mimir Alertmanager configuration: %w", err)
		}
		amCfg.AlertmanagerConfig.Global = cfg.Global
	}

	// We want to:
	// 1. Remove duplicate receivers and keep the last receiver occurrence if there are conflicts. This is based on the upstream implementation.
	// 2. Maintain a consistent ordering and preferably original ordering. Otherwise, change detection will be impacted.
	rcvs := make([]*definition.PostableApiReceiver, 0, len(amCfg.AlertmanagerConfig.Receivers))
	nameToReceiver := make(map[string]struct{}, len(amCfg.AlertmanagerConfig.Receivers))
	// Iterate in reverse to more easily keep the last receiver.
	for i := len(amCfg.AlertmanagerConfig.Receivers) - 1; i >= 0; i-- {
		receiver := amCfg.AlertmanagerConfig.Receivers[i]
		if _, ok := nameToReceiver[receiver.Name]; ok {
			itypes := make([]string, 0, len(receiver.GrafanaManagedReceivers))
			for _, integration := range receiver.GrafanaManagedReceivers {
				itypes = append(itypes, integration.Type)
			}
			level.Debug(am.logger).Log("msg", "receiver with same name is defined multiple times, skipping", "receiver_name", receiver.Name, "skipped_integrations", strings.Join(itypes, ","))
			continue // Skip this receiver. Since we're iterating in reverse, this is not the last one.
		}
		rcvs = append(rcvs, receiver) // Insert in reverse order.
		nameToReceiver[receiver.Name] = struct{}{}
	}
	slices.Reverse(rcvs) // Reverse the result slice to restore input order.
	amCfg.AlertmanagerConfig.Receivers = rcvs

	rawCfg, err := json.Marshal(amCfg.AlertmanagerConfig)
	if err != nil {
		return amConfig{}, fmt.Errorf("failed to marshal Grafana Alertmanager configuration %w", err)
	}

	return amConfig{
		AlertConfigDesc:    alertspb.ToProto(string(rawCfg), amCfg.Templates, gCfg.User),
		tmplExternalURL:    externalURL,
		staticHeaders:      gCfg.StaticHeaders,
		usingGrafanaConfig: true,
	}, nil
}
