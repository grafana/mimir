// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"flag"
	"fmt"
	"math"

	"github.com/grafana/mimir/pkg/nautilus/scallop"
)

// ScallopConfig exposes the calibrated Scallop cost parameters used in production rounds.
type ScallopConfig struct {
	ReplicaBalanceWeight      float64 `yaml:"replica_balance_weight"`
	TransitionEventsWeight    float64 `yaml:"transition_events_weight"`
	TransitionLoadWeight      float64 `yaml:"transition_load_weight"`
	TransitionHashSpaceWeight float64 `yaml:"transition_hash_space_weight"`
	LocalityMissWeight        float64 `yaml:"locality_miss_weight"`
	FragmentationWeight       float64 `yaml:"fragmentation_weight"`
	ResolutionWeight          float64 `yaml:"resolution_weight"`
	MovePartitionMultiplier   float64 `yaml:"move_partition_multiplier"`

	registered bool
}

// defaultScallopConfig copies the evidence-based planner defaults into rebalancer configuration.
func defaultScallopConfig() ScallopConfig {
	policy := scallop.DefaultPolicy()
	return ScallopConfig{
		ReplicaBalanceWeight:      policy.Weights.ReplicaBalance,
		TransitionEventsWeight:    policy.Weights.TransitionEvents,
		TransitionLoadWeight:      policy.Weights.TransitionLoad,
		TransitionHashSpaceWeight: policy.Weights.TransitionHashSpace,
		LocalityMissWeight:        policy.Weights.LocalityMiss,
		FragmentationWeight:       policy.Weights.Fragmentation,
		ResolutionWeight:          policy.Weights.Resolution,
		MovePartitionMultiplier:   policy.ActionMultipliers.MovePartition,
	}
}

// RegisterFlagsWithPrefix registers tunable Scallop costs under the supplied prefix.
func (cfg *ScallopConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	*cfg = defaultScallopConfig()
	cfg.registered = true
	f.Float64Var(&cfg.ReplicaBalanceWeight, prefix+"replica-balance-weight", cfg.ReplicaBalanceWeight, "Scallop weight for logical readcache-replica max/mean load imbalance.")
	f.Float64Var(&cfg.TransitionEventsWeight, prefix+"transition-events-weight", cfg.TransitionEventsWeight, "Scallop weight for fixed per-action control-plane overhead.")
	f.Float64Var(&cfg.TransitionLoadWeight, prefix+"transition-load-weight", cfg.TransitionLoadWeight, "Scallop weight for the fraction of observed load relocated by an action.")
	f.Float64Var(&cfg.TransitionHashSpaceWeight, prefix+"transition-hash-space-weight", cfg.TransitionHashSpaceWeight, "Scallop weight for the fraction of tenant hash space relocated by an action.")
	f.Float64Var(&cfg.LocalityMissWeight, prefix+"locality-miss-weight", cfg.LocalityMissWeight, "Scallop weight for load moved to a readcache replica without recent tenant history.")
	f.Float64Var(&cfg.FragmentationWeight, prefix+"fragmentation-weight", cfg.FragmentationWeight, "Scallop weight for mean hash-range count per tenant.")
	f.Float64Var(&cfg.ResolutionWeight, prefix+"resolution-weight", cfg.ResolutionWeight, "Scallop weight for coarse hot ranges, computed from load times hash width.")
	f.Float64Var(&cfg.MovePartitionMultiplier, prefix+"move-partition-multiplier", cfg.MovePartitionMultiplier, "Multiplier applied to Scallop's transition-event cost for moving a Kafka partition between readcache replicas.")
}

// policy overlays configured cost parameters onto Scallop's non-cost defaults and limits.
func (cfg ScallopConfig) policy() scallop.Policy {
	if !cfg.registered && cfg == (ScallopConfig{}) {
		return scallop.DefaultPolicy()
	}
	policy := scallop.DefaultPolicy()
	policy.Weights.ReplicaBalance = cfg.ReplicaBalanceWeight
	policy.Weights.TransitionEvents = cfg.TransitionEventsWeight
	policy.Weights.TransitionLoad = cfg.TransitionLoadWeight
	policy.Weights.TransitionHashSpace = cfg.TransitionHashSpaceWeight
	policy.Weights.LocalityMiss = cfg.LocalityMissWeight
	policy.Weights.Fragmentation = cfg.FragmentationWeight
	policy.Weights.Resolution = cfg.ResolutionWeight
	policy.ActionMultipliers.MovePartition = cfg.MovePartitionMultiplier
	return policy
}

// validate rejects cost parameters that would make planner comparisons invalid.
func (cfg ScallopConfig) validate() error {
	policy := cfg.policy()
	values := []struct {
		name  string
		value float64
	}{
		{"replica-balance-weight", policy.Weights.ReplicaBalance},
		{"transition-events-weight", policy.Weights.TransitionEvents},
		{"transition-load-weight", policy.Weights.TransitionLoad},
		{"transition-hash-space-weight", policy.Weights.TransitionHashSpace},
		{"locality-miss-weight", policy.Weights.LocalityMiss},
		{"fragmentation-weight", policy.Weights.Fragmentation},
		{"resolution-weight", policy.Weights.Resolution},
		{"move-partition-multiplier", policy.ActionMultipliers.MovePartition},
	}
	for _, parameter := range values {
		if math.IsNaN(parameter.value) || math.IsInf(parameter.value, 0) || parameter.value < 0 {
			return fmt.Errorf("scallop.%s must be finite and non-negative, got %v", parameter.name, parameter.value)
		}
	}
	return nil
}
