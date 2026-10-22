// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"github.com/grafana/dskit/tenant"

	"github.com/grafana/mimir/pkg/compartments"
)

// parseTenantIDWithReadCompartment validates tenantID after removing a well-formed
// "/rc/<digits>" read-compartment suffix. The stored tenant ID keeps the suffix.
func parseTenantIDWithReadCompartment(tenantID string) error {
	_, _, err := tenant.ParseWithMetadata(compartments.TrimReadCompartmentFromTenantID(tenantID))
	return err
}
