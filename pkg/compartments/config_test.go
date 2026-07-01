// SPDX-License-Identifier: AGPL-3.0-only

package compartments

import (
	"testing"

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
