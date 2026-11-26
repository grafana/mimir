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
		"enabled with the maximum number of read compartments passes": {
			cfg: Config{Enabled: true, Read: ReadConfig{NumCompartments: MaxReadCompartments}, Write: WriteConfig{NumCompartments: 3}},
		},
		"enabled with too many read compartments is rejected": {
			cfg:         Config{Enabled: true, Read: ReadConfig{NumCompartments: MaxReadCompartments + 1}, Write: WriteConfig{NumCompartments: 3}},
			expectedErr: ErrTooManyReadCompartments,
		},
		"enabled with zero write compartments is rejected": {
			cfg:         Config{Enabled: true, Read: ReadConfig{NumCompartments: 2}, Write: WriteConfig{NumCompartments: 0}},
			expectedErr: ErrInvalidNumWriteCompartments,
		},
		"enabled with negative write compartments is rejected": {
			cfg:         Config{Enabled: true, Read: ReadConfig{NumCompartments: 2}, Write: WriteConfig{NumCompartments: -1}},
			expectedErr: ErrInvalidNumWriteCompartments,
		},
		"enabled with compartment 0 without suffix passes": {
			cfg: Config{Enabled: true, Compartment0WithoutSuffix: true, Read: ReadConfig{NumCompartments: 2}, Write: WriteConfig{NumCompartments: 3}},
		},
		"disabled with compartment 0 without suffix is rejected": {
			cfg:         Config{Enabled: false, Compartment0WithoutSuffix: true},
			expectedErr: ErrCompartment0WithoutSuffixRequiresCompartments,
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

func TestWithWriteCompartmentSuffix(t *testing.T) {
	assert.Equal(t, "ingester-0-wc-0", WithWriteCompartmentSuffix("ingester-0", 0))
	assert.Equal(t, "ingester-0-wc-3", WithWriteCompartmentSuffix("ingester-0", 3))
}

func TestConfig_CompartmentNames(t *testing.T) {
	tests := map[string]struct {
		cfg                          Config
		compartmentID                int
		expectedReadCompartmentName  string
		expectedWriteCompartmentName string
		expectedBlocksCacheBucketID  string
	}{
		"compartments disabled": {
			cfg:                          Config{},
			compartmentID:                0,
			expectedReadCompartmentName:  "store-gateway-rc-0",
			expectedWriteCompartmentName: "ingester-0-wc-0",
			expectedBlocksCacheBucketID:  "",
		},
		"compartment 0 with suffix": {
			cfg:                          Config{Enabled: true},
			compartmentID:                0,
			expectedReadCompartmentName:  "store-gateway-rc-0",
			expectedWriteCompartmentName: "ingester-0-wc-0",
			expectedBlocksCacheBucketID:  "blocks-rc-0",
		},
		"compartment 1 with suffix": {
			cfg:                          Config{Enabled: true},
			compartmentID:                1,
			expectedReadCompartmentName:  "store-gateway-rc-1",
			expectedWriteCompartmentName: "ingester-0-wc-1",
			expectedBlocksCacheBucketID:  "blocks-rc-1",
		},
		"compartment 0 without suffix": {
			cfg:                          Config{Enabled: true, Compartment0WithoutSuffix: true},
			compartmentID:                0,
			expectedReadCompartmentName:  "store-gateway",
			expectedWriteCompartmentName: "ingester-0",
			expectedBlocksCacheBucketID:  "",
		},
		"compartment 1 keeps its suffix when compartment 0 is without suffix": {
			cfg:                          Config{Enabled: true, Compartment0WithoutSuffix: true},
			compartmentID:                1,
			expectedReadCompartmentName:  "store-gateway-rc-1",
			expectedWriteCompartmentName: "ingester-0-wc-1",
			expectedBlocksCacheBucketID:  "blocks-rc-1",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expectedReadCompartmentName, tc.cfg.ReadCompartmentName("store-gateway", tc.compartmentID))
			assert.Equal(t, tc.expectedWriteCompartmentName, tc.cfg.WriteCompartmentName("ingester-0", tc.compartmentID))
			assert.Equal(t, tc.expectedBlocksCacheBucketID, tc.cfg.BlocksCacheBucketID(tc.compartmentID))
		})
	}
}
