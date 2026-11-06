// SPDX-License-Identifier: AGPL-3.0-only

package compartments

import (
	"strconv"
	"strings"

	"github.com/grafana/dskit/tenant"
)

// readCompartmentTenantIDInfix is inserted into tenant IDs as "/rc/<id>" before any metadata.
const readCompartmentTenantIDInfix = "/rc/"

// TenantIDWithReadCompartment returns tenantID with the "/rc/<compartmentID>" suffix. tenantID must
// not contain tenant metadata. Use tenant.Metadata.WithTenant to add the metadata after the suffix.
func TenantIDWithReadCompartment(tenantID string, compartmentID int) string {
	return tenantID + readCompartmentTenantIDInfix + strconv.Itoa(compartmentID)
}

// BaseTenantID returns tenantIDWithMetadata without the tenant metadata and without a well-formed
// "/rc/<compartmentID>" suffix. It does not allocate.
func BaseTenantID(tenantIDWithMetadata string) string {
	if baseTenantID, _, ok := splitReadCompartmentTenantID(tenantIDWithMetadata); ok {
		return baseTenantID
	}
	return tenant.TrimMetadata(tenantIDWithMetadata)
}

// ParseTenantIDWithMetadata parses tenantIDWithMetadata like tenant.ParseWithMetadata. It also accepts
// a well-formed "/rc/<compartmentID>" suffix before the metadata, and the returned tenantID keeps that
// suffix. It does not allocate when the input is valid.
func ParseTenantIDWithMetadata(tenantIDWithMetadata string) (tenantID string, metadata tenant.Metadata, err error) {
	baseTenantID, metadataSource, ok := splitReadCompartmentTenantID(tenantIDWithMetadata)
	if !ok {
		return tenant.ParseWithMetadata(tenantIDWithMetadata)
	}
	if err := tenant.ValidTenantID(baseTenantID); err != nil {
		return "", tenant.Metadata{}, err
	}
	if metadata, err = tenant.ParseMetadata(metadataSource); err != nil {
		return "", tenant.Metadata{}, err
	}
	return tenantIDWithMetadata[:len(tenantIDWithMetadata)-len(metadataSource)], metadata, nil
}

// splitReadCompartmentTenantID splits tenantIDWithMetadata into the tenant ID without a
// well-formed "/rc/<compartmentID>" suffix and the metadata (including the leading ':').
// ok is false when no well-formed suffix is present. The returned strings are slices of the
// input and do not allocate.
func splitReadCompartmentTenantID(tenantIDWithMetadata string) (tenantID, metadata string, ok bool) {
	// Most tenant IDs have no read compartment suffix. IndexByte finds those IDs faster than LastIndex.
	if strings.IndexByte(tenantIDWithMetadata, '/') < 0 {
		return "", "", false
	}

	tenantWithCompartment, metadata := tenantIDWithMetadata, ""
	if idx := strings.IndexByte(tenantIDWithMetadata, ':'); idx != -1 {
		tenantWithCompartment, metadata = tenantIDWithMetadata[:idx], tenantIDWithMetadata[idx:]
	}

	rcIdx := strings.LastIndex(tenantWithCompartment, readCompartmentTenantIDInfix)
	if rcIdx < 0 {
		return "", "", false
	}

	if _, err := strconv.Atoi(tenantWithCompartment[rcIdx+len(readCompartmentTenantIDInfix):]); err != nil {
		return "", "", false
	}
	return tenantWithCompartment[:rcIdx], metadata, true
}
