// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/validation/notifications_limit_flag.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package validation

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"

	"github.com/grafana/mimir/pkg/util"
)

var allowedIntegrationNames = []string{
	"webhook", "email", "pagerduty", "opsgenie", "wechat", "slack", "victorops", "pushover", "sns", "webex", "telegram", "discord", "msteams",
}

type NotificationRateLimitMap map[string]float64

// String implements flag.Value
func (m NotificationRateLimitMap) String() string {
	out, err := json.Marshal(map[string]float64(m))
	if err != nil {
		return fmt.Sprintf("failed to marshal: %v", err)
	}
	return string(out)
}

// Set implements flag.Value
func (m NotificationRateLimitMap) Set(s string) error {
	newMap := map[string]float64{}
	return m.updateMap(json.Unmarshal([]byte(s), &newMap), newMap)
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (m NotificationRateLimitMap) UnmarshalYAML(value *yaml.Node) error {
	newMap := map[string]float64{}
	return m.updateMap(value.DecodeWithOptions(newMap, yaml.DecodeOptions{KnownFields: true}), newMap)
}

func (m NotificationRateLimitMap) updateMap(unmarshalErr error, newMap map[string]float64) error {
	if unmarshalErr != nil {
		return unmarshalErr
	}

	for k, v := range newMap {
		if !util.StringsContain(allowedIntegrationNames, k) {
			return errors.Errorf("unknown integration name: %s", k)
		}
		m[k] = v
	}
	return nil
}

// MarshalYAML implements yaml.Marshaler.
func (m NotificationRateLimitMap) MarshalYAML() (interface{}, error) {
	return map[string]float64(m), nil
}
