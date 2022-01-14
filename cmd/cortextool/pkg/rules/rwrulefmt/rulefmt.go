// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/rules/rwrulefmt/rulefmt.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package rwrulefmt

import "github.com/prometheus/prometheus/model/rulefmt"

// Wrapper around Prometheus rulefmt.

// RuleGroup is a list of sequentially evaluated recording and alerting rules.
type RuleGroup struct {
	rulefmt.RuleGroup `yaml:",inline"`
	// RWConfigs is used by the remote write forwarding ruler
	RWConfigs []RemoteWriteConfig `yaml:"remote_write,omitempty"`
}

// RemoteWriteConfig is used to specify a remote write endpoint
type RemoteWriteConfig struct {
	URL string `json:"url,omitempty"`
}
