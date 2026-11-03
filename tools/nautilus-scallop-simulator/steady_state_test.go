// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/scallop"
)

func TestSpreadInitialPlacementStartsBalancedAcrossEveryPartition(t *testing.T) {
	fixture := Fixture{
		Name:               "spread",
		Ticks:              2,
		TickSeconds:        1,
		Partitions:         6,
		Readcaches:         2,
		InitialRanges:      1,
		InitialPlacement:   initialPlacementSpread,
		ImbalanceThreshold: 0.2,
		Tenants: []TenantWorkload{
			{ID: "dominant", InitialRanges: 12, Baseline: 120},
			{ID: "small-a", Baseline: 1},
			{ID: "small-b", Baseline: 1},
		},
	}
	require.NoError(t, fixture.validate())

	sim, err := newSimulator(fixture)
	require.NoError(t, err)
	partitionsByTenant := map[string]map[int32]struct{}{}
	for _, entry := range sim.assignment.Entries {
		if partitionsByTenant[entry.TenantID] == nil {
			partitionsByTenant[entry.TenantID] = map[int32]struct{}{}
		}
		partitionsByTenant[entry.TenantID][entry.PartitionID] = struct{}{}
	}
	assert.Len(t, partitionsByTenant["dominant"], fixture.Partitions)
	assert.NotEqual(t, partitionsByTenant["small-a"], partitionsByTenant["small-b"],
		"the spread cursor continues between tenants")

	view, err := sim.observe(sim.assignment, 0)
	require.NoError(t, err)
	point := imbalancePoint(view)
	assert.Less(t, point.Partition, 0.1)
	assert.Greater(t, point.PartitionFloor, 0.9)

	fixture.InitialPlacement = "random"
	require.EqualError(t, fixture.validate(), `unknown initial placement "random"`)
}

func TestObservationNoisePerturbsOnlyThePlannerViewDeterministically(t *testing.T) {
	fixture := Fixture{
		Name:               "observed",
		Ticks:              2,
		TickSeconds:        1,
		Partitions:         100,
		Readcaches:         10,
		InitialRanges:      1,
		InitialPlacement:   initialPlacementSpread,
		ImbalanceThreshold: 0.2,
		ObservationNoise:   &ObservationNoise{PartitionCoefficientOfVariation: 0.03, Seed: 7},
		Tenants:            []TenantWorkload{{ID: "dominant", InitialRanges: 20_000, Baseline: 1_000}},
	}
	require.NoError(t, fixture.validate())
	sim, err := newSimulator(fixture)
	require.NoError(t, err)
	exact, err := sim.observe(sim.assignment, 0)
	require.NoError(t, err)

	first := sim.observedRangeLoads(exact, 0)
	again := sim.observedRangeLoads(exact, 0)
	nextTick := sim.observedRangeLoads(exact, 1)
	require.Equal(t, first, again)
	require.NotEqual(t, first, nextTick)

	observedByPartition := map[int32]float64{}
	for _, entry := range sim.assignment.Entries {
		key := scallop.RangeKey{TenantID: entry.TenantID, Range: entry.Range}
		require.GreaterOrEqual(t, first[key], 0.0)
		observedByPartition[entry.PartitionID] += first[key]
	}
	var sumSquaredError float64
	for partitionID, observed := range observedByPartition {
		relativeError := observed/exact.PartitionLoads[partitionID] - 1
		sumSquaredError += relativeError * relativeError
	}
	partitionCV := math.Sqrt(sumSquaredError / float64(len(observedByPartition)))
	assert.InDelta(t, 0.03, partitionCV, 0.01)

	fixture.ObservationNoise.PartitionCoefficientOfVariation = math.NaN()
	require.Error(t, fixture.validate())
}

func TestPartitionBalanceGateRejectsDrainedPartitions(t *testing.T) {
	fixture := Fixture{ImbalanceThreshold: 0.2}
	healthy := []RoundRecord{
		{PostPlan: ImbalancePoint{Partition: 0.3, PartitionFloor: 0.9}},
		{PostPlan: ImbalancePoint{Partition: 0.1, PartitionFloor: 0.85}},
	}
	assert.Empty(t, partitionBalanceGateErrors(fixture, healthy))

	drained := []RoundRecord{
		{PostPlan: ImbalancePoint{Partition: 0.1, PartitionFloor: 0.9}},
		{PostPlan: ImbalancePoint{Partition: 0.15, PartitionFloor: 0.2}},
	}
	assert.Equal(t, []string{
		"final coldest partition carried 0.200 of mean load; want at least 0.800",
	}, partitionBalanceGateErrors(fixture, drained))

	spiked := []RoundRecord{
		{PostPlan: ImbalancePoint{Partition: 0.5, PartitionFloor: 0.9}},
		{PostPlan: ImbalancePoint{Partition: 0.25, PartitionFloor: 0.9}},
	}
	assert.Equal(t, []string{
		"worst partition imbalance 0.500 exceeded 0.400",
		"final partition imbalance 0.250 exceeded 0.200",
	}, partitionBalanceGateErrors(fixture, spiked))
}

func TestFloorRatio(t *testing.T) {
	assert.Equal(t, 1.0, floorRatio(map[int]float64{}))
	assert.Equal(t, 1.0, floorRatio(map[int]float64{0: 0, 1: 0}))
	assert.Equal(t, 0.5, floorRatio(map[int]float64{0: 1, 1: 3}))
}

func TestPolicyRankingPrefersFewerConstraintFailuresAmongIneligible(t *testing.T) {
	seed := scallop.DefaultPolicy()
	other := seed
	other.Weights.Fragmentation /= 10
	results := []PolicyEvaluation{
		{Policy: seed, AggregateUtility: 1, ConstraintErrors: []string{"a", "b"}},
		{Policy: other, AggregateUtility: 100, ConstraintErrors: []string{"a"}},
	}

	sortPolicyEvaluations(results, seed)
	require.Equal(t, other, results[0].Policy)
}

func TestDev30SteadyStateFixtureMirrorsTheCell(t *testing.T) {
	fixtures, err := loadEmbeddedFixtures()
	require.NoError(t, err)
	fixture := fixtureByName(t, fixtures, "dev30-steady-state")
	require.Equal(t, 300, fixture.Partitions)
	require.Equal(t, 100, fixture.Readcaches)
	require.Len(t, fixture.Tenants, 254)
	require.Equal(t, initialPlacementSpread, fixture.InitialPlacement)
	require.NotNil(t, fixture.ObservationNoise)

	total := 0.0
	for _, tenant := range fixture.Tenants {
		total += tenant.Baseline
	}
	assert.InDelta(t, 0.60, fixture.Tenants[0].Baseline/total, 0.01)
	assert.InDelta(t, 0.39, fixture.Tenants[1].Baseline/total, 0.01)
}
