// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"flag"
	"fmt"
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
		"-nautilus-rebalancer.scallop.max-moves-per-round=21",
		"-nautilus-rebalancer.scallop.max-moves-per-tenant=11",
		"-nautilus-rebalancer.scallop.max-merges-per-round=777",
		"-nautilus-rebalancer.scallop.max-merges-per-tenant=17",
		"-nautilus-rebalancer.scallop.max-splits-per-round=99",
		"-nautilus-rebalancer.scallop.max-splits-per-tenant=33",
		"-nautilus-rebalancer.scallop.max-range-partition-share=0.25",
	}))

	policy := cfg.Scallop.policy()
	require.Equal(t, 0.25, policy.MaxRangePartitionShare)
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
	expectedActionLimits := scallop.DefaultPolicy().ActionLimits
	expectedActionLimits.Move = 21
	expectedActionLimits.MovePerTenant = 11
	expectedActionLimits.Merge = 777
	expectedActionLimits.MergePerTenant = 17
	expectedActionLimits.Split = 99
	expectedActionLimits.SplitPerTenant = 33
	require.Equal(t, expectedActionLimits, policy.ActionLimits)
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
		"-nautilus-rebalancer.scallop.max-range-partition-share=0",
	}))
	require.Equal(t, scallop.Weights{}, cfg.Scallop.policy().Weights)
	require.Zero(t, cfg.Scallop.policy().ActionMultipliers.MovePartition)
	require.Zero(t, cfg.Scallop.policy().MaxRangePartitionShare)
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
		"negative range partition share": func(cfg *ScallopConfig) {
			cfg.MaxRangePartitionShare = -0.1
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

func TestScallopConfigRejectsNonPositiveMaxMergesPerTenant(t *testing.T) {
	for _, value := range []int{-1, 0} {
		t.Run(fmt.Sprintf("%d", value), func(t *testing.T) {
			scallopConfig := defaultScallopConfig()
			scallopConfig.MaxMergesPerTenant = value
			cfg := Config{
				Planner:        plannerScallop,
				Scallop:        scallopConfig,
				PartitionCount: 1,
			}
			require.ErrorContains(t, cfg.Validate(), "scallop.max-merges-per-tenant must be positive")
		})
	}
}

func TestScallopConfigValidatesMaxMergesPerRound(t *testing.T) {
	scallopConfig := defaultScallopConfig()
	scallopConfig.MaxMergesPerRound = -1
	cfg := Config{
		Planner:        plannerScallop,
		Scallop:        scallopConfig,
		PartitionCount: 1,
	}
	require.ErrorContains(t, cfg.Validate(), "scallop.max-merges-per-round must be non-negative")

	scallopConfig.MaxMergesPerRound = 0
	cfg.Scallop = scallopConfig
	require.NoError(t, cfg.Validate(), "zero disables merges")
	require.Zero(t, cfg.Scallop.policy().ActionLimits.Merge)
}

func TestScallopConfigValidatesMoveLimits(t *testing.T) {
	for _, value := range []int{-1, 0} {
		t.Run(fmt.Sprintf("per tenant %d", value), func(t *testing.T) {
			scallopConfig := defaultScallopConfig()
			scallopConfig.MaxMovesPerTenant = value
			cfg := Config{Planner: plannerScallop, Scallop: scallopConfig, PartitionCount: 1}
			require.ErrorContains(t, cfg.Validate(), "scallop.max-moves-per-tenant must be positive")
		})
	}

	scallopConfig := defaultScallopConfig()
	scallopConfig.MaxMovesPerRound = -1
	cfg := Config{Planner: plannerScallop, Scallop: scallopConfig, PartitionCount: 1}
	require.ErrorContains(t, cfg.Validate(), "scallop.max-moves-per-round must be non-negative")

	scallopConfig.MaxMovesPerRound = 0
	cfg.Scallop = scallopConfig
	require.NoError(t, cfg.Validate(), "zero disables range moves")
	require.Zero(t, cfg.Scallop.policy().ActionLimits.Move)
}

func TestScallopConfigValidatesSplitLimits(t *testing.T) {
	for _, value := range []int{-1, 0} {
		t.Run(fmt.Sprintf("per tenant %d", value), func(t *testing.T) {
			scallopConfig := defaultScallopConfig()
			scallopConfig.MaxSplitsPerTenant = value
			cfg := Config{Planner: plannerScallop, Scallop: scallopConfig, PartitionCount: 1}
			require.ErrorContains(t, cfg.Validate(), "scallop.max-splits-per-tenant must be positive")
		})
	}

	scallopConfig := defaultScallopConfig()
	scallopConfig.MaxSplitsPerRound = -1
	cfg := Config{Planner: plannerScallop, Scallop: scallopConfig, PartitionCount: 1}
	require.ErrorContains(t, cfg.Validate(), "scallop.max-splits-per-round must be non-negative")

	scallopConfig.MaxSplitsPerRound = 0
	cfg.Scallop = scallopConfig
	require.NoError(t, cfg.Validate(), "zero disables splits")
	require.Zero(t, cfg.Scallop.policy().ActionLimits.Split)
}
