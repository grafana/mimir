// SPDX-License-Identifier: AGPL-3.0-only

package compartments

import (
	"testing"

	"github.com/grafana/dskit/tenant"
	"github.com/stretchr/testify/assert"
)

func TestTenantIDWithReadCompartment(t *testing.T) {
	assert.Equal(t, "123:rc=2", TenantIDWithReadCompartment("123", tenant.Metadata{}, 2))
	assert.Equal(t, "user:rc=0", TenantIDWithReadCompartment("user", tenant.Metadata{}, 0))
	assert.Equal(t, "user:a=b:rc=1:source=x", TenantIDWithReadCompartment("user", tenant.NewMetadata(":a=b:source=x"), 1))
	assert.Equal(t, "user:rc=1", TenantIDWithReadCompartment("user", tenant.NewMetadata(":rc=7"), 1), "existing rc key must be replaced")
}
