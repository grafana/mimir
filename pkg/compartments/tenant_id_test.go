// SPDX-License-Identifier: AGPL-3.0-only

package compartments

import (
	"testing"

	"github.com/grafana/dskit/tenant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTenantIDWithReadCompartment(t *testing.T) {
	assert.Equal(t, "123/rc/2", TenantIDWithReadCompartment("123", 2))
	assert.Equal(t, "user/rc/0", TenantIDWithReadCompartment("user", 0))
}

func TestBaseTenantID(t *testing.T) {
	tests := map[string]struct {
		input    string
		expected string
	}{
		"read compartment without metadata": {
			input:    "tenant/rc/1",
			expected: "tenant",
		},
		"read compartment with metadata": {
			input:    "tenant/rc/1:source=x",
			expected: "tenant",
		},
		"compartment zero": {
			input:    "user/rc/0",
			expected: "user",
		},
		"no read compartment": {
			input:    "tenant",
			expected: "tenant",
		},
		"metadata only": {
			input:    "tenant:source=x",
			expected: "tenant",
		},
		"malformed non-digit compartment": {
			input:    "tenant/rc/x",
			expected: "tenant/rc/x",
		},
		"malformed empty compartment": {
			input:    "tenant/rc/",
			expected: "tenant/rc/",
		},
		"malformed wrong infix": {
			input:    "tenant/foo/1",
			expected: "tenant/foo/1",
		},
		"malformed non-digit compartment with metadata": {
			input:    "tenant/rc/x:source=x",
			expected: "tenant/rc/x",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, BaseTenantID(tc.input))
		})
	}
}

func TestParseTenantIDWithMetadata(t *testing.T) {
	tests := map[string]struct {
		input            string
		expectedTenantID string
		expectedMetadata string
		expectedErr      error
	}{
		"without read compartment": {
			input:            "tenant",
			expectedTenantID: "tenant",
		},
		"without read compartment, with metadata": {
			input:            "tenant:source=x",
			expectedTenantID: "tenant",
			expectedMetadata: ":source=x",
		},
		"with read compartment": {
			input:            "tenant/rc/1",
			expectedTenantID: "tenant/rc/1",
		},
		"with read compartment and metadata": {
			input:            "tenant/rc/1:source=x",
			expectedTenantID: "tenant/rc/1",
			expectedMetadata: ":source=x",
		},
		"malformed non-digit compartment": {
			input:       "tenant/rc/x",
			expectedErr: tenant.ValidTenantID("tenant/rc/x"),
		},
		"malformed wrong infix": {
			input:       "tenant/foo/1",
			expectedErr: tenant.ValidTenantID("tenant/foo/1"),
		},
		"invalid tenant ID with read compartment": {
			input:       "bad tenant/rc/1",
			expectedErr: tenant.ValidTenantID("bad tenant"),
		},
		"invalid metadata with read compartment": {
			input:       "tenant/rc/1:source=\xb5",
			expectedErr: tenant.ValidMetadata(":source=\xb5"),
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			tenantID, metadata, err := ParseTenantIDWithMetadata(tc.input)
			if tc.expectedErr != nil {
				require.EqualError(t, err, tc.expectedErr.Error())
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expectedTenantID, tenantID)
			assert.Equal(t, tenant.NewMetadata(tc.expectedMetadata), metadata)
		})
	}
}
