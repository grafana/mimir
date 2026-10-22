// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"github.com/grafana/mimir/pkg/compartments"
)

// parseTenantIDWithReadCompartment validates tenantID, allowing an optional well-formed
// "/rc/<compartmentID>" read-compartment suffix before any tenant metadata. It does not allocate
// when tenantID is valid. The stored tenant ID keeps the suffix.
func parseTenantIDWithReadCompartment(tenantID string) error {
	_, _, err := compartments.ParseTenantIDWithMetadata(tenantID)
	return err
}
