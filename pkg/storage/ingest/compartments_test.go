// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"fmt"
	"testing"

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
				TopicFormat:     "mimir-read-comp-<compartment-id>",
			},
			wantErr: nil,
		},
		{
			name: "enabled with num_compartments <= 0",
			cfg: CompartmentsConfig{
				Enabled:         true,
				NumCompartments: 0,
				TopicFormat:     "mimir-read-comp-<compartment-id>",
			},
			wantErr: ErrCompartmentsInvalidNumCompartments,
		},
		{
			name: "enabled with negative num_compartments",
			cfg: CompartmentsConfig{
				Enabled:         true,
				NumCompartments: -1,
				TopicFormat:     "mimir-read-comp-<compartment-id>",
			},
			wantErr: ErrCompartmentsInvalidNumCompartments,
		},
		{
			name: "enabled with missing placeholder in topic_format",
			cfg: CompartmentsConfig{
				Enabled:         true,
				NumCompartments: 3,
				TopicFormat:     "mimir-read-comp",
			},
			wantErr: ErrCompartmentsInvalidTopicFormat,
		},
		{
			name: "enabled with empty topic_format",
			cfg: CompartmentsConfig{
				Enabled:         true,
				NumCompartments: 3,
				TopicFormat:     "",
			},
			wantErr: ErrCompartmentsInvalidTopicFormat,
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

func TestCompartmentRouter_Topic(t *testing.T) {
	cfg := CompartmentsConfig{
		Enabled:         true,
		NumCompartments: 3,
		TopicFormat:     "mimir-read-comp-<compartment-id>",
	}
	router := NewCompartmentRouter(cfg)

	assert.Equal(t, 3, router.NumCompartments())
	assert.Equal(t, "mimir-read-comp-0", router.Topic(0))
	assert.Equal(t, "mimir-read-comp-1", router.Topic(1))
	assert.Equal(t, "mimir-read-comp-2", router.Topic(2))
}

func TestCompartmentRouter_CompartmentForMetric(t *testing.T) {
	cfg := CompartmentsConfig{
		Enabled:         true,
		NumCompartments: 4,
		TopicFormat:     "mimir-read-comp-<compartment-id>",
	}
	router := NewCompartmentRouter(cfg)

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

	t.Run("consistent with TopicForMetric", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			metric := fmt.Sprintf("metric_%d", i)
			idx := router.CompartmentForMetric("tenant-1", metric)
			topic := router.TopicForMetric("tenant-1", metric)
			assert.Equal(t, router.Topic(idx), topic)
		}
	})
}

func TestCompartmentRouter_TopicForMetric(t *testing.T) {
	cfg := CompartmentsConfig{
		Enabled:         true,
		NumCompartments: 4,
		TopicFormat:     "mimir-read-comp-<compartment-id>",
	}
	router := NewCompartmentRouter(cfg)

	t.Run("deterministic assignment", func(t *testing.T) {
		topic1 := router.TopicForMetric("tenant-1", "up")
		topic2 := router.TopicForMetric("tenant-1", "up")
		assert.Equal(t, topic1, topic2, "same user+metric should always map to the same topic")
	})

	t.Run("different tenants may get different compartments for the same metric", func(t *testing.T) {
		seen := map[string]bool{}
		for i := 0; i < 1000; i++ {
			topic := router.TopicForMetric(fmt.Sprintf("tenant-%d", i), "up")
			seen[topic] = true
		}
		assert.Equal(t, 4, len(seen), "expected all 4 compartments to be assigned across 1000 tenants with the same metric")
	})

	t.Run("all compartments get assigned with enough metrics", func(t *testing.T) {
		seen := map[string]bool{}
		// With enough different metrics, all compartments should be hit.
		for i := 0; i < 1000; i++ {
			topic := router.TopicForMetric("tenant-1", fmt.Sprintf("metric_%d", i))
			seen[topic] = true
		}
		assert.Equal(t, 4, len(seen), "expected all 4 compartments to be assigned at least one metric")
	})

	t.Run("returned topics are valid", func(t *testing.T) {
		validTopics := map[string]bool{
			"mimir-read-comp-0": true,
			"mimir-read-comp-1": true,
			"mimir-read-comp-2": true,
			"mimir-read-comp-3": true,
		}
		topic := router.TopicForMetric("user-a", "http_requests_total")
		assert.True(t, validTopics[topic], "topic %q should be one of the pre-computed topics", topic)
	})
}
