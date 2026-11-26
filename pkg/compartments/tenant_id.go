// SPDX-License-Identifier: AGPL-3.0-only

package compartments

import (
	"strconv"

	"github.com/grafana/dskit/tenant"
)

// readCompartmentMetadataKey is the tenant metadata key that holds the read compartment ID.
const readCompartmentMetadataKey = "rc"

// TenantIDWithReadCompartment returns tenantID with metadata and the "rc=<compartmentID>" metadata
// key-value pair. It replaces an "rc" key that is already in metadata.
func TenantIDWithReadCompartment(tenantID string, metadata tenant.Metadata, compartmentID int) string {
	return metadata.With(readCompartmentMetadataKey, strconv.Itoa(compartmentID)).WithTenant(tenantID)
}

// ReadCompartmentTenantID returns the tenant ID of the given read compartment. With
// Compartment0WithoutSuffix, read compartment 0 uses tenantID with metadata and without the "rc" key.
func (cfg Config) ReadCompartmentTenantID(tenantID string, metadata tenant.Metadata, readCompartmentID int) string {
	if readCompartmentID == 0 && cfg.Compartment0WithoutSuffix {
		return metadata.WithTenant(tenantID)
	}
	return TenantIDWithReadCompartment(tenantID, metadata, readCompartmentID)
}

// MaxTenantMetadataLength returns the maximum length of the metadata passed to
// TenantIDWithReadCompartment, so that the metadata of the result doesn't exceed
// tenant.MaxMetadataLength for any of numReadCompartments read compartments.
func MaxTenantMetadataLength(numReadCompartments int) int {
	return tenant.MaxMetadataLength - len(":"+readCompartmentMetadataKey+"=") - len(strconv.Itoa(numReadCompartments-1))
}
