// SPDX-License-Identifier: AGPL-3.0-only

package cardinality

import (
	"encoding/json"
	"fmt"
	"net/http"

	apierror "github.com/grafana/mimir/pkg/api/error"
)

// DisabledError is the message every component refuses a tenant with when the estimate is not enabled for it.
func DisabledError(tenantID string) string {
	return fmt.Sprintf("cardinality estimate is disabled for the tenant: %s", tenantID)
}

// DisabledResponse is the endpoint's answer to such a tenant: the HTTP status and a Prometheus API error body.
func DisabledResponse(tenantID string) (int, []byte) {
	body, _ := json.Marshal(map[string]string{"status": "error", "errorType": string(apierror.TypeBadData), "error": DisabledError(tenantID)})
	return http.StatusBadRequest, append(body, '\n')
}
