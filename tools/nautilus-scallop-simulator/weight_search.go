// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"fmt"
	"math"
	"runtime"
	"sort"
	"strings"

	"github.com/grafana/mimir/pkg/nautilus/scallop"
)

type SearchConfig struct {
	BeamWidth       int       `json:"beam_width"`
	Log10StepByPass []float64 `json:"log10_step_by_pass"`
	MinimumPolicies int       `json:"minimum_policies"`
}

// defaultSearchConfig defines the checked-in deterministic beam shape and minimum experiment size.
func defaultSearchConfig() SearchConfig {
	return SearchConfig{
		BeamWidth:       3,
		Log10StepByPass: []float64{0.75, 0.35, 0.15},
		MinimumPolicies: 100,
	}
}

type PolicyEvaluation struct {
	Generation       int                `json:"generation"`
	Policy           scallop.Policy     `json:"policy"`
	Fixtures         []SimulationResult `json:"fixtures"`
	Eligible         bool               `json:"eligible"`
	ConstraintErrors []string           `json:"constraint_errors,omitempty"`
	MeanUtility      float64            `json:"mean_utility"`
	WorstUtility     float64            `json:"worst_utility"`
	AggregateUtility float64            `json:"aggregate_utility"`
}

type SearchResult struct {
	Config            SearchConfig       `json:"config"`
	EvaluatedPolicies int                `json:"evaluated_policies"`
	Recommended       PolicyEvaluation   `json:"recommended"`
	Ranked            []PolicyEvaluation `json:"ranked"`
}

// runWeightSearch completes every configured beam generation and returns all policies in stable rank order.
func runWeightSearch(fixtures []Fixture, seed scallop.Policy, config SearchConfig) (SearchResult, error) {
	if config.BeamWidth <= 0 || len(config.Log10StepByPass) == 0 {
		return SearchResult{}, fmt.Errorf("invalid search configuration")
	}
	evaluated := map[string]PolicyEvaluation{}
	beam := []scallop.Policy{seed}

	for generation, step := range config.Log10StepByPass {
		candidatesByKey := map[string]scallop.Policy{}
		for _, policy := range beam {
			for _, candidate := range policyNeighbors(policy, step) {
				candidatesByKey[policyKey(candidate)] = candidate
			}
		}
		keys := make([]string, 0, len(candidatesByKey))
		for key := range candidatesByKey {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		pendingKeys := make([]string, 0, len(keys))
		for _, key := range keys {
			if _, exists := evaluated[key]; !exists {
				pendingKeys = append(pendingKeys, key)
			}
		}
		generationResults, err := evaluatePolicyBatch(fixtures, candidatesByKey, pendingKeys, generation)
		if err != nil {
			return SearchResult{}, err
		}
		for key, result := range generationResults {
			evaluated[key] = result
		}

		ranked := make([]PolicyEvaluation, 0, len(candidatesByKey))
		for key := range candidatesByKey {
			ranked = append(ranked, evaluated[key])
		}
		sortPolicyEvaluations(ranked)
		beam = beam[:0]
		for i := 0; i < min(config.BeamWidth, len(ranked)); i++ {
			beam = append(beam, ranked[i].Policy)
		}
	}

	ranked := make([]PolicyEvaluation, 0, len(evaluated))
	for _, result := range evaluated {
		ranked = append(ranked, result)
	}
	sortPolicyEvaluations(ranked)
	if len(ranked) < config.MinimumPolicies {
		return SearchResult{}, fmt.Errorf("weight search evaluated %d policies, want at least %d", len(ranked), config.MinimumPolicies)
	}
	if !ranked[0].Eligible {
		return SearchResult{}, fmt.Errorf("weight search found no policy satisfying explicit fixture constraints")
	}
	return SearchResult{
		Config:            config,
		EvaluatedPolicies: len(ranked),
		Recommended:       ranked[0],
		Ranked:            ranked,
	}, nil
}

type policyEvaluationResult struct {
	key        string
	evaluation PolicyEvaluation
	err        error
}

// evaluatePolicyBatch evaluates one generation concurrently while returning results in deterministic key space.
func evaluatePolicyBatch(
	fixtures []Fixture,
	policies map[string]scallop.Policy,
	keys []string,
	generation int,
) (map[string]PolicyEvaluation, error) {
	if len(keys) == 0 {
		return map[string]PolicyEvaluation{}, nil
	}
	workers := min(runtime.GOMAXPROCS(0), len(keys))
	jobs := make(chan string)
	results := make(chan policyEvaluationResult, len(keys))
	for range workers {
		go func() {
			for key := range jobs {
				evaluation, err := evaluatePolicy(fixtures, policies[key], generation)
				results <- policyEvaluationResult{key: key, evaluation: evaluation, err: err}
			}
		}()
	}
	go func() {
		for _, key := range keys {
			jobs <- key
		}
		close(jobs)
	}()

	evaluated := make(map[string]PolicyEvaluation, len(keys))
	errorsByKey := make(map[string]error)
	for range keys {
		result := <-results
		if result.err != nil {
			errorsByKey[result.key] = result.err
			continue
		}
		evaluated[result.key] = result.evaluation
	}
	for _, key := range keys {
		if err := errorsByKey[key]; err != nil {
			return nil, fmt.Errorf("evaluate policy %s: %w", key, err)
		}
	}
	return evaluated, nil
}

// evaluatePolicy runs one policy against every fixture and combines mean and worst-case utility.
func evaluatePolicy(fixtures []Fixture, policy scallop.Policy, generation int) (PolicyEvaluation, error) {
	out := PolicyEvaluation{
		Generation: generation,
		Policy:     policy,
		Fixtures:   make([]SimulationResult, 0, len(fixtures)),
		Eligible:   true,
	}
	for _, fixture := range fixtures {
		result, err := simulateFixture(fixture, policy)
		if err != nil {
			return PolicyEvaluation{}, err
		}
		out.Fixtures = append(out.Fixtures, result)
		out.MeanUtility += result.Utility
		out.WorstUtility = math.Max(out.WorstUtility, result.Utility)
		if fixture.SettledRanges > 0 && result.Evaluation.StructuralFootprint.UnsettledTenants > 0 {
			out.Eligible = false
			out.ConstraintErrors = append(out.ConstraintErrors, fmt.Sprintf(
				"%s: %d tenants did not reach the explicit %d-range settlement target",
				fixture.Name,
				result.Evaluation.StructuralFootprint.UnsettledTenants,
				fixture.SettledRanges,
			))
		}
	}
	out.MeanUtility /= float64(len(fixtures))
	out.AggregateUtility = out.MeanUtility + out.WorstUtility
	return out, nil
}

// policyNeighbors varies each weight and adds one joint structural tradeoff around the retained base policy.
func policyNeighbors(base scallop.Policy, log10Step float64) []scallop.Policy {
	out := []scallop.Policy{base}
	factor := math.Pow(10, log10Step)
	for dimension := 0; dimension < policyDimensions; dimension++ {
		lower := base
		setPolicyDimension(&lower, dimension, policyDimension(base, dimension)/factor)
		out = append(out, lower)

		upper := base
		setPolicyDimension(&upper, dimension, policyDimension(base, dimension)*factor)
		out = append(out, upper)
	}
	structuralTradeoff := base
	structuralTradeoff.Weights.Fragmentation /= factor
	structuralTradeoff.Weights.Resolution *= factor
	out = append(out, structuralTradeoff)
	return out
}

const policyDimensions = 7

// policyDimension reads one searchable weight by stable index for generic beam operations.
func policyDimension(policy scallop.Policy, dimension int) float64 {
	switch dimension {
	case 0:
		return policy.Weights.ReplicaBalance
	case 1:
		return policy.Weights.TransitionEvents
	case 2:
		return policy.Weights.TransitionLoad
	case 3:
		return policy.Weights.TransitionHashSpace
	case 4:
		return policy.Weights.LocalityMiss
	case 5:
		return policy.Weights.Fragmentation
	case 6:
		return policy.Weights.Resolution
	default:
		panic("invalid policy dimension")
	}
}

// setPolicyDimension writes one searchable weight by the same stable index used by policyDimension.
func setPolicyDimension(policy *scallop.Policy, dimension int, value float64) {
	switch dimension {
	case 0:
		policy.Weights.ReplicaBalance = value
	case 1:
		policy.Weights.TransitionEvents = value
	case 2:
		policy.Weights.TransitionLoad = value
	case 3:
		policy.Weights.TransitionHashSpace = value
	case 4:
		policy.Weights.LocalityMiss = value
	case 5:
		policy.Weights.Fragmentation = value
	case 6:
		policy.Weights.Resolution = value
	default:
		panic("invalid policy dimension")
	}
}

// policyKey produces a deterministic identity and tie-break key from all searched weights.
func policyKey(policy scallop.Policy) string {
	values := make([]string, policyDimensions)
	for i := range values {
		values[i] = fmt.Sprintf("%.12g", policyDimension(policy, i))
	}
	limits := policy.CandidateSearch
	actions := policy.ActionLimits
	return fmt.Sprintf("%s/%d/%d/%d/%d/%d/%d/%d/%d/%d/%d/%d/%d/%d/%d",
		strings.Join(values, "/"),
		limits.MaxMoveSources,
		limits.MaxDestinationsPerRange,
		limits.MaxSplitCandidates,
		limits.MaxMergeCandidates,
		limits.MaxPartitionMoveSources,
		limits.MaxDestinationsPerPartition,
		limits.MaxPartitionMoveCandidates,
		limits.MaxFullyScored,
		actions.Total,
		actions.Move,
		actions.Split,
		actions.Merge,
		actions.MergePerTenant,
		actions.MovePartition,
	)
}

// sortPolicyEvaluations orders policies by external utility and then deterministic weight key.
func sortPolicyEvaluations(results []PolicyEvaluation) {
	sort.Slice(results, func(i, j int) bool {
		if results[i].Eligible != results[j].Eligible {
			return results[i].Eligible
		}
		if math.Abs(results[i].AggregateUtility-results[j].AggregateUtility) > 1e-12 {
			return results[i].AggregateUtility < results[j].AggregateUtility
		}
		return policyKey(results[i].Policy) < policyKey(results[j].Policy)
	})
}
