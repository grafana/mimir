// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"flag"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/scallop"
)

func TestScallopConfigFlagsBuildRoundPolicy(t *testing.T) {
	var cfg Config
	flags := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg.RegisterFlagsWithPrefix("nautilus-rebalancer.", flags)
	require.NoError(t, flags.Parse([]string{
		"-nautilus-rebalancer.scallop.replica-balance-weight=0.2",
		"-nautilus-rebalancer.scallop.transition-events-weight=0.3",
		"-nautilus-rebalancer.scallop.transition-load-weight=0.4",
		"-nautilus-rebalancer.scallop.transition-hash-space-weight=0.5",
		"-nautilus-rebalancer.scallop.locality-miss-weight=0.6",
		"-nautilus-rebalancer.scallop.fragmentation-weight=0.7",
		"-nautilus-rebalancer.scallop.resolution-weight=0.8",
		"-nautilus-rebalancer.scallop.move-partition-multiplier=44.668359",
	}))

	policy := cfg.Scallop.policy()
	require.Equal(t, scallop.Weights{
		ReplicaBalance:      0.2,
		TransitionEvents:    0.3,
		TransitionLoad:      0.4,
		TransitionHashSpace: 0.5,
		LocalityMiss:        0.6,
		Fragmentation:       0.7,
		Resolution:          0.8,
	}, policy.Weights)
	require.Equal(t, 44.668359, policy.ActionMultipliers.MovePartition)
	require.Equal(t, scallop.DefaultPolicy().CandidateSearch, policy.CandidateSearch)
	require.Equal(t, scallop.DefaultPolicy().ActionLimits, policy.ActionLimits)
}

func TestScallopConfigUsesPlannerDefaultsWhenUnconfigured(t *testing.T) {
	require.Equal(t, scallop.DefaultPolicy(), (ScallopConfig{}).policy())

	var cfg Config
	flags := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg.RegisterFlagsWithPrefix("nautilus-rebalancer.", flags)
	require.NoError(t, flags.Parse(nil))
	require.Equal(t, scallop.DefaultPolicy(), cfg.Scallop.policy())
}

func TestScallopConfigAllowsExplicitZeroCosts(t *testing.T) {
	var cfg Config
	flags := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg.RegisterFlagsWithPrefix("nautilus-rebalancer.", flags)
	require.NoError(t, flags.Parse([]string{
		"-nautilus-rebalancer.scallop.replica-balance-weight=0",
		"-nautilus-rebalancer.scallop.transition-events-weight=0",
		"-nautilus-rebalancer.scallop.transition-load-weight=0",
		"-nautilus-rebalancer.scallop.transition-hash-space-weight=0",
		"-nautilus-rebalancer.scallop.locality-miss-weight=0",
		"-nautilus-rebalancer.scallop.fragmentation-weight=0",
		"-nautilus-rebalancer.scallop.resolution-weight=0",
		"-nautilus-rebalancer.scallop.move-partition-multiplier=0",
	}))
	require.Equal(t, scallop.Weights{}, cfg.Scallop.policy().Weights)
	require.Zero(t, cfg.Scallop.policy().ActionMultipliers.MovePartition)
}

func TestScallopConfigRejectsInvalidCosts(t *testing.T) {
	for name, mutate := range map[string]func(*ScallopConfig){
		"negative weight": func(cfg *ScallopConfig) {
			cfg.TransitionLoadWeight = -1
		},
		"NaN weight": func(cfg *ScallopConfig) {
			cfg.ResolutionWeight = math.NaN()
		},
		"infinite multiplier": func(cfg *ScallopConfig) {
			cfg.MovePartitionMultiplier = math.Inf(1)
		},
	} {
		t.Run(name, func(t *testing.T) {
			scallopConfig := defaultScallopConfig()
			mutate(&scallopConfig)
			cfg := Config{
				Planner:        plannerScallop,
				Scallop:        scallopConfig,
				PartitionCount: 1,
			}
			require.ErrorContains(t, cfg.Validate(), "must be finite and non-negative")
		})
	}
}
