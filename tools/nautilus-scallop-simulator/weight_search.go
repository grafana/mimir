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
		Log10StepByPass: []float64{1.0, 0.75, 0.35, 0.15},
		MinimumPolicies: 100,
	}
}

type PolicyEvaluation struct {
	Generation       int                `json:"generation"`
	Policy           scallop.Policy     `json:"policy"`
	Fixtures         []SimulationResult `json:"fixtures"`
	Replays          []ReplayEvaluation `json:"replays"`
	Eligible         bool               `json:"eligible"`
	ConstraintErrors []string           `json:"constraint_errors,omitempty"`
	MeanUtility      float64            `json:"mean_utility"`
	WorstUtility     float64            `json:"worst_utility"`
	AggregateUtility float64            `json:"aggregate_utility"`
}

type SearchResult struct {
	Config            SearchConfig        `json:"config"`
	EvaluatedPolicies int                 `json:"evaluated_policies"`
	Recommended       PolicyEvaluation    `json:"recommended"`
	Ranked            []PolicyEvaluation  `json:"ranked"`
	Sensitivity       SensitivityAnalysis `json:"sensitivity"`
}

type SensitivityAnalysis struct {
	Log10Step float64                 `json:"log10_step"`
	Cases     []SensitivityEvaluation `json:"cases"`
}

type SensitivityEvaluation struct {
	Label                 string                    `json:"label"`
	Generation            int                       `json:"generation"`
	Policy                scallop.Policy            `json:"policy"`
	Eligible              bool                      `json:"eligible"`
	ConstraintErrors      []string                  `json:"constraint_errors,omitempty"`
	MeanUtility           float64                   `json:"mean_utility"`
	WorstUtility          float64                   `json:"worst_utility"`
	AggregateUtility      float64                   `json:"aggregate_utility"`
	AggregateUtilityDelta float64                   `json:"aggregate_utility_delta"`
	FixtureDeltas         []SensitivityFixtureDelta `json:"fixture_deltas"`
	WinsFixtures          []string                  `json:"wins_fixtures,omitempty"`
}

type SensitivityFixtureDelta struct {
	Name                     string                      `json:"name"`
	Utility                  float64                     `json:"utility"`
	UtilityDelta             float64                     `json:"utility_delta"`
	ExactDeltaScoresDelta    int                         `json:"exact_delta_scores_delta"`
	CompleteProjectionsDelta int                         `json:"complete_projections_delta"`
	FinalWeightedCost        float64                     `json:"final_weighted_cost"`
	FinalWeightedCostDelta   float64                     `json:"final_weighted_cost_delta"`
	CandidateWork            scallop.CandidateSearchWork `json:"candidate_work"`
	BaselineCandidateWork    scallop.CandidateSearchWork `json:"baseline_candidate_work"`
	RoundCosts               []SensitivityRoundCosts     `json:"round_costs"`
}

type SensitivityRoundCosts struct {
	Tick                int                   `json:"tick"`
	InitialCost         scallop.CostBreakdown `json:"initial_cost"`
	BaselineInitialCost scallop.CostBreakdown `json:"baseline_initial_cost"`
	FinalCost           scallop.CostBreakdown `json:"final_cost"`
	BaselineFinalCost   scallop.CostBreakdown `json:"baseline_final_cost"`
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
		sortPolicyEvaluations(ranked, seed)
		beam = beam[:0]
		for i := 0; i < min(config.BeamWidth, len(ranked)); i++ {
			beam = append(beam, ranked[i].Policy)
		}
	}

	ranked := make([]PolicyEvaluation, 0, len(evaluated))
	for _, result := range evaluated {
		ranked = append(ranked, result)
	}
	sortPolicyEvaluations(ranked, seed)
	if len(ranked) < config.MinimumPolicies {
		return SearchResult{}, fmt.Errorf("weight search evaluated %d policies, want at least %d", len(ranked), config.MinimumPolicies)
	}
	if !ranked[0].Eligible {
		return SearchResult{}, fmt.Errorf(
			"weight search found no policy satisfying explicit fixture constraints; best policy %s failed: %s",
			policyKey(ranked[0].Policy),
			strings.Join(ranked[0].ConstraintErrors, "; "),
		)
	}
	sensitivity, err := runSensitivityAnalysis(
		fixtures,
		ranked[0],
		config.Log10StepByPass[len(config.Log10StepByPass)-1],
		len(config.Log10StepByPass),
		evaluated,
	)
	if err != nil {
		return SearchResult{}, err
	}
	return SearchResult{
		Config:            config,
		EvaluatedPolicies: len(ranked),
		Recommended:       ranked[0],
		Ranked:            ranked,
		Sensitivity:       sensitivity,
	}, nil
}

func runSensitivityAnalysis(
	fixtures []Fixture,
	base PolicyEvaluation,
	log10Step float64,
	generation int,
	evaluated map[string]PolicyEvaluation,
) (SensitivityAnalysis, error) {
	type sensitivityPolicy struct {
		label  string
		policy scallop.Policy
	}
	factor := math.Pow(10, log10Step)
	policies := make([]sensitivityPolicy, 0, policyDimensions*2+6)
	for dimension := 0; dimension < policyDimensions; dimension++ {
		for _, direction := range []struct {
			name   string
			factor float64
		}{
			{"down", 1 / factor},
			{"up", factor},
		} {
			policy := base.Policy
			setPolicyDimension(&policy, dimension, policyDimension(base.Policy, dimension)*direction.factor)
			policies = append(policies, sensitivityPolicy{
				label:  fmt.Sprintf("%s_%s", policyDimensionName(dimension), direction.name),
				policy: policy,
			})
		}
	}
	for _, pair := range []struct {
		label                     string
		first, second             int
		firstFactor, secondFactor float64
	}{
		{"fragmentation_down_resolution_down", 5, 6, 1 / factor, 1 / factor},
		{"fragmentation_up_resolution_up", 5, 6, factor, factor},
		{"fragmentation_down_resolution_up", 5, 6, 1 / factor, factor},
		{"fragmentation_up_resolution_down", 5, 6, factor, 1 / factor},
		{"transition_events_down_partition_multiplier_up", 1, 7, 1 / factor, factor},
		{"transition_events_up_partition_multiplier_down", 1, 7, factor, 1 / factor},
	} {
		policy := base.Policy
		setPolicyDimension(&policy, pair.first, policyDimension(base.Policy, pair.first)*pair.firstFactor)
		setPolicyDimension(&policy, pair.second, policyDimension(base.Policy, pair.second)*pair.secondFactor)
		policies = append(policies, sensitivityPolicy{label: pair.label, policy: policy})
	}

	out := SensitivityAnalysis{Log10Step: log10Step, Cases: make([]SensitivityEvaluation, 0, len(policies))}
	for _, item := range policies {
		evaluation, ok := evaluated[policyKey(item.policy)]
		if !ok {
			var err error
			evaluation, err = evaluatePolicy(fixtures, item.policy, generation)
			if err != nil {
				return SensitivityAnalysis{}, fmt.Errorf("evaluate sensitivity %s: %w", item.label, err)
			}
		}
		out.Cases = append(out.Cases, buildSensitivityEvaluation(item.label, base, evaluation))
	}
	return out, nil
}

func buildSensitivityEvaluation(label string, base, nearby PolicyEvaluation) SensitivityEvaluation {
	baseByFixture := make(map[string]SimulationResult, len(base.Fixtures))
	for _, fixture := range base.Fixtures {
		baseByFixture[fixture.FixtureName] = fixture
	}
	out := SensitivityEvaluation{
		Label:                 label,
		Generation:            nearby.Generation,
		Policy:                nearby.Policy,
		Eligible:              nearby.Eligible,
		ConstraintErrors:      nearby.ConstraintErrors,
		MeanUtility:           nearby.MeanUtility,
		WorstUtility:          nearby.WorstUtility,
		AggregateUtility:      nearby.AggregateUtility,
		AggregateUtilityDelta: nearby.AggregateUtility - base.AggregateUtility,
		FixtureDeltas:         make([]SensitivityFixtureDelta, 0, len(nearby.Fixtures)),
	}
	for _, fixture := range nearby.Fixtures {
		baseline := baseByFixture[fixture.FixtureName]
		finalCost, baselineFinalCost := finalRoundWeightedCost(fixture), finalRoundWeightedCost(baseline)
		roundCosts := make([]SensitivityRoundCosts, 0, min(len(fixture.Rounds), len(baseline.Rounds)))
		for i := 0; i < min(len(fixture.Rounds), len(baseline.Rounds)); i++ {
			roundCosts = append(roundCosts, SensitivityRoundCosts{
				Tick:                fixture.Rounds[i].Tick,
				InitialCost:         fixture.Rounds[i].InitialCost,
				BaselineInitialCost: baseline.Rounds[i].InitialCost,
				FinalCost:           fixture.Rounds[i].FinalCost,
				BaselineFinalCost:   baseline.Rounds[i].FinalCost,
			})
		}
		out.FixtureDeltas = append(out.FixtureDeltas, SensitivityFixtureDelta{
			Name:                     fixture.FixtureName,
			Utility:                  fixture.Utility,
			UtilityDelta:             fixture.Utility - baseline.Utility,
			ExactDeltaScoresDelta:    fixture.CandidateSearch.Work.ExactDeltaScores - baseline.CandidateSearch.Work.ExactDeltaScores,
			CompleteProjectionsDelta: fixture.CandidateSearch.Work.CompleteProjections - baseline.CandidateSearch.Work.CompleteProjections,
			FinalWeightedCost:        finalCost,
			FinalWeightedCostDelta:   finalCost - baselineFinalCost,
			CandidateWork:            fixture.CandidateSearch.Work,
			BaselineCandidateWork:    baseline.CandidateSearch.Work,
			RoundCosts:               roundCosts,
		})
		if fixture.Utility < baseline.Utility {
			out.WinsFixtures = append(out.WinsFixtures, fixture.FixtureName)
		}
	}
	return out
}

func finalRoundWeightedCost(result SimulationResult) float64 {
	if len(result.Rounds) == 0 {
		return 0
	}
	return result.Rounds[len(result.Rounds)-1].FinalCost.WeightedTotal
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
		for _, constraintError := range fixtureConstraintErrors(fixture, result) {
			out.Eligible = false
			out.ConstraintErrors = append(out.ConstraintErrors, fixture.Name+": "+constraintError)
		}
	}
	replays, err := evaluateReplayCorpus(policy)
	if err != nil {
		return PolicyEvaluation{}, err
	}
	out.Replays = replays
	for _, replay := range replays {
		if replay.Passed {
			continue
		}
		out.Eligible = false
		for _, gateError := range replay.GateErrors {
			out.ConstraintErrors = append(out.ConstraintErrors, fmt.Sprintf(
				"replay %s: %s",
				replay.Name,
				gateError,
			))
		}
	}
	out.MeanUtility /= float64(len(fixtures))
	out.AggregateUtility = out.MeanUtility + out.WorstUtility
	return out, nil
}

func fixtureConstraintErrors(fixture Fixture, result SimulationResult) []string {
	var errors []string
	evaluation := result.Evaluation
	structure := evaluation.StructuralFootprint
	if fixture.SettledRanges > 0 && structure.UnsettledTenants > 0 {
		errors = append(errors, fmt.Sprintf(
			"%d tenants did not reach the explicit %d-range settlement target",
			structure.UnsettledTenants,
			fixture.SettledRanges,
		))
	}
	if result.CandidateSearch.Work.CompleteProjections > result.CandidateSearch.Iterations {
		errors = append(errors, fmt.Sprintf(
			"%d complete projections exceeded %d bounded search iterations",
			result.CandidateSearch.Work.CompleteProjections,
			result.CandidateSearch.Iterations,
		))
	}

	switch fixture.Name {
	case "balanced-no-op":
		if totalActions(evaluation.RebalancingWork) != 0 {
			errors = append(errors, "balanced placement produced actions")
		}
	case "dev30-fluctuating":
		if evaluation.AdaptationStability.ReversedPartitionMoves > 0 {
			errors = append(errors, fmt.Sprintf(
				"%d partition moves reversed within the churn window",
				evaluation.AdaptationStability.ReversedPartitionMoves,
			))
		}
	case "temporal-burst":
		if !hasTenantActionAtOrAfter(
			result.Rounds,
			"bursting-tenant",
			fixture.Ticks/3,
			scallop.ActionMove,
			scallop.ActionSplit,
		) {
			errors = append(errors, "bursting tenant did not adapt after the burst began")
		}
	case "hotspot-shift":
		if !hasTenantActionAtOrAfter(
			result.Rounds,
			"shifting-tenant",
			fixture.Ticks/4,
			scallop.ActionMove,
			scallop.ActionSplit,
		) {
			errors = append(errors, "shifting tenant did not adapt after the hotspot moved")
		}
	case "mixed-heavy-tiny":
		for _, tenantID := range []string{"heavy-a", "heavy-b"} {
			if !hasTenantActionAtOrAfter(result.Rounds, tenantID, 0, scallop.ActionSplit) {
				errors = append(errors, tenantID+" received no useful split")
			}
		}
	case "many-tiny-tenants-consolidating", "dev30-steady-state":
		errors = append(errors, partitionBalanceGateErrors(fixture, result.Rounds)...)
	case "dev30-overfragmented-recovery":
		errors = append(errors, partitionBalanceGateErrors(fixture, result.Rounds)...)
		if evaluation.RebalancingWork.Merges <= evaluation.RebalancingWork.Splits {
			errors = append(errors, "net structural direction was not consolidation")
		}
		for _, tenantID := range []string{"dominant-45k", "dominant-30k"} {
			if structure.MergesPerTenant[tenantID] < 1_000 {
				errors = append(errors, fmt.Sprintf(
					"%s performed only %d merges; want at least 1000 to rule out action-limit starvation",
					tenantID,
					structure.MergesPerTenant[tenantID],
				))
			}
			if !strictlyDecreasing(structure.TenantRangeCountTrajectories[tenantID]) {
				errors = append(errors, tenantID+" did not consolidate in every recovery round")
			}
		}
	}
	return errors
}

// partitionBalanceGateErrors rejects trajectories that concentrate true load onto
// some partitions while draining others, which the peak excess alone cannot see.
func partitionBalanceGateErrors(fixture Fixture, rounds []RoundRecord) []string {
	if len(rounds) == 0 {
		return nil
	}
	var errors []string
	threshold := fixture.ImbalanceThreshold
	worst := 0.0
	for _, round := range rounds {
		worst = math.Max(worst, round.PostPlan.Partition)
	}
	if worst > 2*threshold {
		errors = append(errors, fmt.Sprintf(
			"worst partition imbalance %.3f exceeded %.3f", worst, 2*threshold,
		))
	}
	last := rounds[len(rounds)-1].PostPlan
	if last.Partition > threshold {
		errors = append(errors, fmt.Sprintf(
			"final partition imbalance %.3f exceeded %.3f", last.Partition, threshold,
		))
	}
	if last.PartitionFloor < 1-threshold {
		errors = append(errors, fmt.Sprintf(
			"final coldest partition carried %.3f of mean load; want at least %.3f",
			last.PartitionFloor, 1-threshold,
		))
	}
	return errors
}

func totalActions(work RebalancingWorkEvaluation) int {
	return work.Moves + work.Splits + work.Merges + work.PartitionMoves
}

func hasTenantActionAtOrAfter(
	rounds []RoundRecord,
	tenantID string,
	minimumTick int,
	kinds ...scallop.ActionKind,
) bool {
	allowed := make(map[scallop.ActionKind]struct{}, len(kinds))
	for _, kind := range kinds {
		allowed[kind] = struct{}{}
	}
	for _, round := range rounds {
		if round.Tick < minimumTick {
			continue
		}
		for _, action := range round.Actions {
			_, kindAllowed := allowed[action.Kind]
			if action.TenantID == tenantID && (len(allowed) == 0 || kindAllowed) {
				return true
			}
		}
	}
	return false
}

func strictlyDecreasing(values []int) bool {
	if len(values) == 0 {
		return false
	}
	for i := 1; i < len(values); i++ {
		if values[i] >= values[i-1] {
			return false
		}
	}
	return true
}

// policyNeighbors varies each weight and adds joint structural scale and tradeoff cases.
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
	for _, adjustment := range []struct {
		fragmentationFactor float64
		resolutionFactor    float64
	}{
		{1 / factor, 1 / factor},
		{factor, factor},
		{1 / factor, factor},
		{factor, 1 / factor},
	} {
		joint := base
		joint.Weights.Fragmentation *= adjustment.fragmentationFactor
		joint.Weights.Resolution *= adjustment.resolutionFactor
		out = append(out, joint)
	}
	return out
}

const policyDimensions = 9

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
	case 7:
		return policy.ActionMultipliers.MovePartition
	case 8:
		return policy.MaxRangePartitionShare
	default:
		panic("invalid policy dimension")
	}
}

func policyDimensionName(dimension int) string {
	switch dimension {
	case 0:
		return "replica_balance"
	case 1:
		return "transition_events"
	case 2:
		return "transition_load"
	case 3:
		return "transition_hash_space"
	case 4:
		return "locality_miss"
	case 5:
		return "fragmentation"
	case 6:
		return "resolution"
	case 7:
		return "move_partition_multiplier"
	case 8:
		return "max_range_partition_share"
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
	case 7:
		policy.ActionMultipliers.MovePartition = value
	case 8:
		policy.MaxRangePartitionShare = value
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
	return fmt.Sprintf("%s/%d/%d/%d/%d/%d/%d/%d/%d/%d/%d/%d/%d/%d/%d/%d/%d/%d/%d/%d/%d/%d/%d",
		strings.Join(values, "/"),
		limits.MaxMoveSources,
		limits.MaxDestinationsPerRange,
		limits.MaxMergeCandidates,
		limits.MaxPartitionMoveSources,
		limits.MaxDestinationsPerPartition,
		limits.MaxPartitionMoveCandidates,
		limits.MaxFullyScored,
		limits.MaxPartitionOffenders,
		limits.MaxReplicaOffenders,
		limits.MaxTenantOffenders,
		limits.MaxRangesPerOffender,
		limits.MaxAdjacencyPerTenant,
		limits.MaxDestinationPartitions,
		limits.MaxDestinationReplicas,
		actions.Total,
		actions.Move,
		actions.MovePerTenant,
		actions.Split,
		actions.SplitPerTenant,
		actions.Merge,
		actions.MergePerTenant,
		actions.MovePartition,
	)
}

// sortPolicyEvaluations orders policies by external utility, retaining the
// seed on utility-flat dimensions before applying the deterministic key.
func sortPolicyEvaluations(results []PolicyEvaluation, seed scallop.Policy) {
	sort.Slice(results, func(i, j int) bool {
		if results[i].Eligible != results[j].Eligible {
			return results[i].Eligible
		}
		if len(results[i].ConstraintErrors) != len(results[j].ConstraintErrors) {
			return len(results[i].ConstraintErrors) < len(results[j].ConstraintErrors)
		}
		if math.Abs(results[i].AggregateUtility-results[j].AggregateUtility) > 1e-12 {
			return results[i].AggregateUtility < results[j].AggregateUtility
		}
		leftDistance := policyLogDistance(results[i].Policy, seed)
		rightDistance := policyLogDistance(results[j].Policy, seed)
		if math.Abs(leftDistance-rightDistance) > 1e-12 {
			return leftDistance < rightDistance
		}
		return policyKey(results[i].Policy) < policyKey(results[j].Policy)
	})
}

func policyLogDistance(policy, seed scallop.Policy) float64 {
	distance := 0.0
	for dimension := 0; dimension < policyDimensions; dimension++ {
		value, seedValue := policyDimension(policy, dimension), policyDimension(seed, dimension)
		if value == seedValue {
			continue
		}
		if value <= 0 || seedValue <= 0 {
			return math.Inf(1)
		}
		distance += math.Abs(math.Log10(value / seedValue))
	}
	return distance
}
