// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"fmt"
	"testing"

	"github.com/grafana/dskit/flagext"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompartmentsConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     CompartmentsConfig
		wantErr error
	}{
		{
			name: "disabled config is always valid",
			cfg: CompartmentsConfig{
				Enabled: false,
			},
			wantErr: nil,
		},
		{
			name: "valid enabled config",
			cfg: CompartmentsConfig{
				Enabled:         true,
				NumCompartments: 3,
			},
			wantErr: nil,
		},
		{
			name: "enabled with num_compartments <= 0",
			cfg: CompartmentsConfig{
				Enabled:         true,
				NumCompartments: 0,
			},
			wantErr: ErrCompartmentsInvalidNumCompartments,
		},
		{
			name: "enabled with negative num_compartments",
			cfg: CompartmentsConfig{
				Enabled:         true,
				NumCompartments: -1,
			},
			wantErr: ErrCompartmentsInvalidNumCompartments,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCompartmentRouter_CompartmentForMetric(t *testing.T) {
	cfg := CompartmentsConfig{
		Enabled:         true,
		NumCompartments: 4,
	}
	router := NewCompartmentRouter(cfg)

	assert.Equal(t, 4, router.NumCompartments())

	t.Run("deterministic assignment", func(t *testing.T) {
		c1 := router.CompartmentForMetric("tenant-1", "up")
		c2 := router.CompartmentForMetric("tenant-1", "up")
		assert.Equal(t, c1, c2, "same user+metric should always map to the same compartment")
	})

	t.Run("returns valid compartment index", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			idx := router.CompartmentForMetric("tenant-1", fmt.Sprintf("metric_%d", i))
			assert.GreaterOrEqual(t, idx, 0)
			assert.Less(t, idx, 4)
		}
	})

	t.Run("different tenants may get different compartments for the same metric", func(t *testing.T) {
		seen := map[int]bool{}
		for i := 0; i < 1000; i++ {
			c := router.CompartmentForMetric(fmt.Sprintf("tenant-%d", i), "up")
			seen[c] = true
		}
		assert.Equal(t, 4, len(seen), "expected all 4 compartments to be assigned across 1000 tenants with the same metric")
	})

	t.Run("all compartments get assigned with enough metrics", func(t *testing.T) {
		seen := map[int]bool{}
		for i := 0; i < 1000; i++ {
			c := router.CompartmentForMetric("tenant-1", fmt.Sprintf("metric_%d", i))
			seen[c] = true
		}
		assert.Equal(t, 4, len(seen), "expected all 4 compartments to be assigned at least one metric")
	})
}

func TestKafkaConfigForCompartment(t *testing.T) {
	t.Run("replaces placeholder in all fields", func(t *testing.T) {
		base := KafkaConfig{
			Address:  flagext.StringSliceCSV{"kafka-<compartment-id>.example.com:9092", "kafka-<compartment-id>.backup.com:9092"},
			Topic:    "mimir-ingest-<compartment-id>",
			SASL: KafkaAuthConfig{
				Username: "user-<compartment-id>",
				Password: flagext.SecretWithValue("pass-<compartment-id>"),
			},
		}

		cfg := KafkaConfigForCompartment(base, 3)

		assert.Equal(t, flagext.StringSliceCSV{"kafka-3.example.com:9092", "kafka-3.backup.com:9092"}, cfg.Address)
		assert.Equal(t, "mimir-ingest-3", cfg.Topic)
		assert.Equal(t, "user-3", cfg.SASL.Username)
		assert.Equal(t, "pass-3", cfg.SASL.Password.String())
	})

	t.Run("no placeholder leaves values unchanged", func(t *testing.T) {
		base := KafkaConfig{
			Address: flagext.StringSliceCSV{"kafka.example.com:9092"},
			Topic:   "mimir-ingest",
			SASL: KafkaAuthConfig{
				Username: "user",
				Password: flagext.SecretWithValue("pass"),
			},
		}

		cfg := KafkaConfigForCompartment(base, 5)

		assert.Equal(t, flagext.StringSliceCSV{"kafka.example.com:9092"}, cfg.Address)
		assert.Equal(t, "mimir-ingest", cfg.Topic)
		assert.Equal(t, "user", cfg.SASL.Username)
		assert.Equal(t, "pass", cfg.SASL.Password.String())
	})

	t.Run("does not alias the original address slice", func(t *testing.T) {
		base := KafkaConfig{
			Address: flagext.StringSliceCSV{"kafka-<compartment-id>.example.com:9092"},
			Topic:   "topic",
		}

		cfg := KafkaConfigForCompartment(base, 1)
		assert.Equal(t, "kafka-1.example.com:9092", string(cfg.Address[0]))
		assert.Equal(t, "kafka-<compartment-id>.example.com:9092", string(base.Address[0]), "original should not be modified")
	})
}
