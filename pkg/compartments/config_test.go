// SPDX-License-Identifier: AGPL-3.0-only

package compartments

import (
	"testing"

	"github.com/grafana/dskit/tenant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		cfg         Config
		expectedErr error
	}{
		"disabled passes regardless of other values": {
			cfg: Config{Enabled: false, Read: ReadConfig{NumCompartments: 0}, Write: WriteConfig{NumCompartments: 0}},
		},
		"enabled with valid config passes": {
			cfg: Config{Enabled: true, Read: ReadConfig{NumCompartments: 2}, Write: WriteConfig{NumCompartments: 3}},
		},
		"enabled with zero read compartments is rejected": {
			cfg:         Config{Enabled: true, Read: ReadConfig{NumCompartments: 0}, Write: WriteConfig{NumCompartments: 3}},
			expectedErr: ErrInvalidNumReadCompartments,
		},
		"enabled with negative read compartments is rejected": {
			cfg:         Config{Enabled: true, Read: ReadConfig{NumCompartments: -1}, Write: WriteConfig{NumCompartments: 3}},
			expectedErr: ErrInvalidNumReadCompartments,
		},
		"enabled with zero write compartments is rejected": {
			cfg:         Config{Enabled: true, Read: ReadConfig{NumCompartments: 2}, Write: WriteConfig{NumCompartments: 0}},
			expectedErr: ErrInvalidNumWriteCompartments,
		},
		"enabled with negative write compartments is rejected": {
			cfg:         Config{Enabled: true, Read: ReadConfig{NumCompartments: 2}, Write: WriteConfig{NumCompartments: -1}},
			expectedErr: ErrInvalidNumWriteCompartments,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if tc.expectedErr != nil {
				assert.ErrorIs(t, err, tc.expectedErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestReplaceReadCompartment(t *testing.T) {
	assert.Equal(t, "ingest-rc-0", ReplaceReadCompartment("ingest-rc-<read-compartment-id>", 0))
	assert.Equal(t, "ingest-rc-3", ReplaceReadCompartment("ingest-rc-<read-compartment-id>", 3))
	// A template without the placeholder is returned unchanged.
	assert.Equal(t, "ingest", ReplaceReadCompartment("ingest", 2))
}

func TestReplaceWriteCompartment(t *testing.T) {
	assert.Equal(t, "kafka-wc-0:9092", ReplaceWriteCompartment("kafka-wc-<write-compartment-id>:9092", 0))
	assert.Equal(t, "kafka-wc-5:9092", ReplaceWriteCompartment("kafka-wc-<write-compartment-id>:9092", 5))
	// A value without the placeholder is returned unchanged.
	assert.Equal(t, "kafka:9092", ReplaceWriteCompartment("kafka:9092", 1))
}

func TestWithReadCompartmentSuffix(t *testing.T) {
	assert.Equal(t, "ingester-partitions-rc-0", WithReadCompartmentSuffix("ingester-partitions", 0))
	assert.Equal(t, "ingester-partitions-rc-3", WithReadCompartmentSuffix("ingester-partitions", 3))
	assert.Equal(t, "blocks-rc-2", WithReadCompartmentSuffix("blocks", 2))
}

func TestTenantIDWithReadCompartment(t *testing.T) {
	tests := map[string]struct {
		tenantIDWithMetadata string
		compartmentID        int
		expected             string
	}{
		"without metadata": {
			tenantIDWithMetadata: "123",
			compartmentID:        2,
			expected:             "123/rc/2",
		},
		"with metadata": {
			tenantIDWithMetadata: "123:source=k6",
			compartmentID:        2,
			expected:             "123/rc/2:source=k6",
		},
		"compartment zero": {
			tenantIDWithMetadata: "user",
			compartmentID:        0,
			expected:             "user/rc/0",
		},
		"multiple metadata keys": {
			tenantIDWithMetadata: "user:a=1:b=2",
			compartmentID:        7,
			expected:             "user/rc/7:a=1:b=2",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, TenantIDWithReadCompartment(tc.tenantIDWithMetadata, tc.compartmentID))
		})
	}
}

func TestTrimReadCompartmentFromTenantID(t *testing.T) {
	tests := map[string]struct {
		input    string
		expected string
	}{
		"without metadata": {
			input:    "tenant/rc/1",
			expected: "tenant",
		},
		"with metadata": {
			input:    "tenant/rc/1:source=x",
			expected: "tenant:source=x",
		},
		"compartment zero": {
			input:    "user/rc/0",
			expected: "user",
		},
		"no compartment suffix": {
			input:    "tenant",
			expected: "tenant",
		},
		"metadata only, no compartment": {
			input:    "tenant:source=x",
			expected: "tenant:source=x",
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
		"malformed non-digit with metadata": {
			input:    "tenant/rc/x:source=x",
			expected: "tenant/rc/x:source=x",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, TrimReadCompartmentFromTenantID(tc.input))
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
