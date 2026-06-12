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
			cfg: Config{Enabled: false, Read: ReadConfig{NumCompartments: 0, KafkaTopicFormat: ""}},
		},
		"enabled with valid config passes": {
			cfg: Config{Enabled: true, Read: ReadConfig{NumCompartments: 2, KafkaTopicFormat: "mimir-rc-<compartment-id>"}},
		},
		"enabled with zero compartments is rejected": {
			cfg:         Config{Enabled: true, Read: ReadConfig{NumCompartments: 0, KafkaTopicFormat: "mimir-rc-<compartment-id>"}},
			expectedErr: ErrInvalidNumCompartments,
		},
		"enabled with negative compartments is rejected": {
			cfg:         Config{Enabled: true, Read: ReadConfig{NumCompartments: -1, KafkaTopicFormat: "mimir-rc-<compartment-id>"}},
			expectedErr: ErrInvalidNumCompartments,
		},
		"enabled with empty topic format is rejected": {
			cfg:         Config{Enabled: true, Read: ReadConfig{NumCompartments: 2, KafkaTopicFormat: ""}},
			expectedErr: ErrEmptyKafkaTopicFormat,
		},
		"enabled with topic format missing the placeholder is rejected": {
			cfg:         Config{Enabled: true, Read: ReadConfig{NumCompartments: 2, KafkaTopicFormat: "mimir-rc"}},
			expectedErr: ErrKafkaTopicFormatPlacehold,
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
