package alertmanager

import (
	"encoding/json"
	"fmt"

	"github.com/grafana/alerting/definition"
	"github.com/grafana/mimir/pkg/alertmanager/alertspb"
	amconfig "github.com/prometheus/alertmanager/config"
	"gopkg.in/yaml.v3"
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

func loadConfig(rawCfg []byte) (*definition.PostableApiAlertingConfig, error) {
	var cfg definition.PostableApiAlertingConfig
	if err := yaml.Unmarshal(rawCfg, &cfg); err != nil {
		return nil, err
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// Note: This could be a method in the alerting package.
func grafanaToUpstreamConfig(cfg *definition.PostableApiAlertingConfig) amconfig.Config {
	// Note: We're ignoring the Grafana part of the receivers.
	// TODO: Use it.
	rcvs := make([]amconfig.Receiver, 0, len(cfg.Receivers))
	for _, r := range cfg.Receivers {
		rcvs = append(rcvs, r.Receiver)
	}

	return amconfig.Config{
		Global:            cfg.Config.Global,
		Route:             cfg.Config.Route.AsAMRoute(),
		InhibitRules:      cfg.Config.InhibitRules,
		Receivers:         rcvs,
		Templates:         cfg.Config.Templates,
		MuteTimeIntervals: cfg.Config.MuteTimeIntervals,
		TimeIntervals:     cfg.Config.TimeIntervals,
	}
}
