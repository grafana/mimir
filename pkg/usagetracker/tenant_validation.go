// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"github.com/grafana/dskit/tenant"

	"github.com/grafana/mimir/pkg/compartments"
)

// parseTenantIDWithReadCompartment validates tenantID, allowing an optional well-formed
// "/rc/<compartmentID>" read-compartment suffix before any tenant metadata. It validates the tenant
// prefix, compartment ID, and metadata in place and does not allocate. The stored tenant ID keeps
// the suffix.
func parseTenantIDWithReadCompartment(tenantID string) error {
	baseTenantID, metadata, _, ok := compartments.SplitReadCompartmentTenantID(tenantID)
	if !ok {
		_, _, err := tenant.ParseWithMetadata(tenantID)
		return err
	}

	if err := tenant.ValidTenantID(baseTenantID); err != nil {
		return err
	}
	if metadata == "" {
		return nil
	}
	_, err := tenant.ParseMetadata(metadata)
	return err
}
