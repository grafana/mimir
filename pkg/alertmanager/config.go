// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/alertmanager/distributor.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package alertmanager

import (
	"encoding/json"
	"fmt"

	"github.com/grafana/alerting/definition"
	"github.com/grafana/mimir/pkg/alertmanager/alertspb"
)

// parseGrafanaConfig creates an AlertConfigDesc from a GrafanaAlertConfigDesc.
func parseGrafanaConfig(gCfg alertspb.GrafanaAlertConfigDesc, mCfg *alertspb.AlertConfigDesc) (alertspb.AlertConfigDesc, error) {
	var amCfg GrafanaAlertmanagerConfig
	if err := json.Unmarshal([]byte(gCfg.RawConfig), &amCfg); err != nil {
		return alertspb.AlertConfigDesc{}, fmt.Errorf("failed to unmarshal Grafana Alertmanager configuration %w", err)
	}

	rawCfg, err := json.Marshal(amCfg.AlertmanagerConfig)
	if err != nil {
		return alertspb.AlertConfigDesc{}, fmt.Errorf("failed to marshal Grafana Alertmanager configuration %w", err)
	}

	// Use the global section from the Mimir config.
	if mCfg != nil {
		cfg, err := definition.LoadCompat([]byte(mCfg.RawConfig))
		if err != nil {
			return alertspb.AlertConfigDesc{}, err
		}
		amCfg.AlertmanagerConfig.Config = cfg.Config
	}

	return alertspb.ToProto(string(rawCfg), amCfg.Templates, gCfg.User), nil
}
