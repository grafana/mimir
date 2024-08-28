package blockbuilder

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfig_Validate(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		cfg, _ := blockBuilderConfig(t, "kafka:9092")
		require.NoError(t, cfg.Validate())
	})

	t.Run("empty partition assignment", func(t *testing.T) {
		cfg, _ := blockBuilderConfig(t, "kafka:9092")

		cfg.PartitionAssignment = map[string][]int32{}
		require.Error(t, cfg.Validate())
	})

	t.Run("empty instance-id assignment", func(t *testing.T) {
		cfg, _ := blockBuilderConfig(t, "kafka:9092")

		cfg.InstanceID = ""
		// Instance 0 isn't present in the assignment
		cfg.PartitionAssignment = map[string][]int32{
			"block-builder-0": {1},
		}
		require.Error(t, cfg.Validate())
	})

	t.Run("bad partition assignment", func(t *testing.T) {
		cfg, _ := blockBuilderConfig(t, "kafka:9092")

		cfg.InstanceID = "block-builder-0"
		// Instance 0 isn't present in the assignment
		cfg.PartitionAssignment = map[string][]int32{
			"block-builder-1":   {1},
			"block-builder-100": {10},
		}
		require.Error(t, cfg.Validate())
	})

	t.Run("bad consume-interval", func(t *testing.T) {
		cfg, _ := blockBuilderConfig(t, "kafka:9092")

		cfg.ConsumeInterval = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("bad lookback_on_no_commit", func(t *testing.T) {
		cfg, _ := blockBuilderConfig(t, "kafka:9092")

		cfg.LookbackOnNoCommit = -1
		require.Error(t, cfg.Validate())
	})
}
