// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/validation/notifications_limit_flag.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package validation

import (
	"github.com/pkg/errors"

	"github.com/grafana/mimir/pkg/util"
)

func validateIntegrationLimit(k string, _ float64) error {
	if !util.StringsContain(allowedIntegrationNames, k) {
		return errors.Errorf("unknown integration name: %s", k)
	}
	return nil
}

// allowedIntegrationNames is a list of all the integrations that can be rate limited.
var allowedIntegrationNames = []string{
	"webhook", "email", "pagerduty", "opsgenie", "wechat", "slack", "victorops", "pushover", "sns", "webex", "telegram", "discord", "msteams",
}

// NotificationRateLimitMap returns a map that can be used as a flag for setting notification rate limits.
func NotificationRateLimitMap() LimitsMap[float64] {
	return NewLimitsMap[float64](validateIntegrationLimit)
}
