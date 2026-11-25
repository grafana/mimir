// SPDX-License-Identifier: AGPL-3.0-only

package compartments

import (
	"strings"
	"testing"

	"github.com/grafana/dskit/tenant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTenantIDWithReadCompartment(t *testing.T) {
	assert.Equal(t, "123:rc=2", TenantIDWithReadCompartment("123", tenant.Metadata{}, 2))
	assert.Equal(t, "user:rc=0", TenantIDWithReadCompartment("user", tenant.Metadata{}, 0))
	assert.Equal(t, "user:a=b:rc=1:source=x", TenantIDWithReadCompartment("user", tenant.NewMetadata(":a=b:source=x"), 1))
	assert.Equal(t, "user:rc=1", TenantIDWithReadCompartment("user", tenant.NewMetadata(":rc=7"), 1), "existing rc key must be replaced")
}

func TestMaxTenantMetadataLength(t *testing.T) {
	assert.Equal(t, 59, MaxTenantMetadataLength(1))
	assert.Equal(t, 59, MaxTenantMetadataLength(10))
	assert.Equal(t, 58, MaxTenantMetadataLength(11))
	assert.Equal(t, 57, MaxTenantMetadataLength(MaxReadCompartments))

	for _, numReadCompartments := range []int{1, 10, 11, MaxReadCompartments} {
		maxLength := MaxTenantMetadataLength(numReadCompartments)
		lastCompartmentID := numReadCompartments - 1

		// Metadata of the maximum length gives a valid tenant ID for the last read compartment.
		metadata := tenant.NewMetadata(":a=" + strings.Repeat("x", maxLength-len(":a=")))
		_, _, err := tenant.ParseWithMetadata(TenantIDWithReadCompartment("user", metadata, lastCompartmentID))
		require.NoError(t, err)

		// One more byte of metadata gives an invalid tenant ID.
		metadata = tenant.NewMetadata(":a=" + strings.Repeat("x", maxLength+1-len(":a=")))
		_, _, err = tenant.ParseWithMetadata(TenantIDWithReadCompartment("user", metadata, lastCompartmentID))
		require.EqualError(t, err, "metadata too long: 65")
	}
}
