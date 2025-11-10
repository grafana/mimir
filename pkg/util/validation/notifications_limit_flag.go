// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/validation/notifications_limit_flag.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package validation

import (
	"fmt"
	"slices"

	"github.com/grafana/dskit/flagext"
)

func validateIntegrationLimit(k string, _ float64) error {
	if !slices.Contains(allowedIntegrationNames, k) {
		return fmt.Errorf("unknown integration name: %s", k)
	}
	return nil
}

// allowedIntegrationNames is a list of all the integrations that can be rate limited.
var allowedIntegrationNames = []string{
	"webhook", "email", "pagerduty", "opsgenie", "wechat", "slack", "victorops", "pushover", "sns", "webex", "telegram", "discord", "msteams", "msteamsv2",
}

// NotificationRateLimitMap returns a map that can be used as a flag for setting notification rate limits.
func NotificationRateLimitMap() flagext.LimitsMap[float64] {
	return flagext.NewLimitsMap[float64](validateIntegrationLimit)
}
