// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/alertmanager/distributor.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package alertmanager

import (
	"encoding/json"
	"fmt"

	"github.com/grafana/mimir/pkg/alertmanager/alertspb"
)

func parseGrafanaConfig(cfg alertspb.GrafanaAlertConfigDesc) (alertspb.AlertConfigDesc, error) {
	var amCfg GrafanaAlertmanagerConfig
	if err := json.Unmarshal([]byte(cfg.RawConfig), &amCfg); err != nil {
		return alertspb.AlertConfigDesc{}, fmt.Errorf("failed to unmarshal Grafana Alertmanager configuration %w", err)
	}

	rawCfg, err := json.Marshal(amCfg.AlertmanagerConfig)
	if err != nil {
		return alertspb.AlertConfigDesc{}, fmt.Errorf("failed to marshal Grafana Alertmanager configuration %w", err)
	}

	return alertspb.ToProto(string(rawCfg), amCfg.Templates, cfg.User), nil
}
