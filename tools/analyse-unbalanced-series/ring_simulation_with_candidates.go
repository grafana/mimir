package main

import (
	"fmt"
	"math"
	"os"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"golang.org/x/exp/slices"

	tokdistr "github.com/grafana/mimir/tools/analyse-unbalanced-series/tokendistributor"
)

const (
	maxTokenValue        = tokdistr.Token(math.MaxUint32)
	initialInstanceCount = 66
)

func generateRingWithZoneAwareness(logger log.Logger, numTokensPerInstanceScenarios []int, replicationFactor, instancesPerZone int, zones []tokdistr.Zone, seedGeneratorProvider func(zones []tokdistr.Zone, replicationFactor, tokensPerInstance int, maxTokenValue tokdistr.Token) tokdistr.SeedGenerator) error {
	//analysisName := "Generating ring by choosing the best candidate"
	iterations := 10
	// newPerfectlySpacedSeedGenerator(zones, replicationFactor, tokensPerInstance, maxTokenValue)
	stats := make([]tokdistr.Statistics, 0, iterations)
	result := make(map[tokdistr.Instance][]float64)

	for _, numTokensPerInstance := range numTokensPerInstanceScenarios {
		stats = stats[:0]
		for it := 0; it < iterations; it++ {
			level.Info(logger).Log("tokensPerInstance", numTokensPerInstance, "iteration", it)
			replicationStrategy := tokdistr.NewZoneAwareReplicationStrategy(replicationFactor, make(map[tokdistr.Instance]tokdistr.Zone, initialInstanceCount), nil, nil)
			tokenDistributor := tokdistr.NewTokenDistributor(numTokensPerInstance, len(zones), maxTokenValue, replicationStrategy, seedGeneratorProvider(zones, replicationFactor, numTokensPerInstance, maxTokenValue))

			for i := 0; i < instancesPerZone; i++ {
				for j := 0; j < len(zones); j++ {
					instance := tokdistr.Instance(fmt.Sprintf("%s-%d", string(rune('A'+j)), i))
					_, _, stat := tokenDistributor.AddInstance(instance, zones[j])
					stats = append(stats, stat)
				}
			}
		}
		statistics := tokdistr.GetAverageStatistics(stats)
		for instance, ownership := range statistics.RegisteredTokenOwnershipByInstance {
			resultByInstance, ok := result[instance]
			if !ok {
				resultByInstance = make([]float64, 0, len(numTokensPerInstanceScenarios))
			}
			resultByInstance = append(resultByInstance, ownership)
			result[instance] = resultByInstance
		}
	}

	instances := make([]tokdistr.Instance, 0, len(result))
	allIterationsResult := make([][]string, 0, len(result))
	for instance := range result {
		instances = append(instances, instance)
	}
	slices.Sort(instances)
	for _, instance := range instances {
		resultsPerInstance := make([]string, 0, len(numTokensPerInstanceScenarios)+1)
		resultsPerInstance = append(resultsPerInstance, string(instance))
		for _, ownership := range result[instance] {
			resultsPerInstance = append(resultsPerInstance, fmt.Sprintf("%.ff", ownership))
		}
		allIterationsResult = append(allIterationsResult, resultsPerInstance)
	}

	//Generate CSV header.
	csvHeader := make([]string, 0, len(numTokensPerInstanceScenarios)+1)
	csvHeader = append(csvHeader, "pod")
	for _, numTokensPerInstance := range numTokensPerInstanceScenarios {
		csvHeader = append(csvHeader, fmt.Sprintf("%d tokens per instance", numTokensPerInstance))
	}

	// Write result to CSV.
	w := newCSVWriter[[]string]()
	w.setHeader(csvHeader)
	w.setData(allIterationsResult, func(entry []string) []string {
		return entry
	})
	if err := w.writeCSV("simulated-ring-with-different-tokens-per-instance-with-candidates-selection.csv"); err != nil {
		return err
	}
	return nil
}

func main() {
	logger := log.NewLogfmtLogger(os.Stdout)
	level.Info(logger).Log("msg", "Generating ring with the best candidate approach")
	numTokensPerInstanceScenarios := []int{4, 16, 64, 128}
	replicationFactor := 3
	zones := []tokdistr.Zone{tokdistr.Zone("zone-a"), tokdistr.Zone("zone-b"), tokdistr.Zone("zone-c")}
	instancesPerZone := initialInstanceCount / len(zones)
	seedGenerator := func(zones []tokdistr.Zone, replicationFactor, tokensPerInstance int, maxTokenValue tokdistr.Token) tokdistr.SeedGenerator {
		return tokdistr.NewPerfectlySpacedSeedGenerator(zones, replicationFactor, tokensPerInstance, maxTokenValue)
	}
	generateRingWithZoneAwareness(logger, numTokensPerInstanceScenarios, replicationFactor, instancesPerZone, zones, seedGenerator)
}
