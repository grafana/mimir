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
