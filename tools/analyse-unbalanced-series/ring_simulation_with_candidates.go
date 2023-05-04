package main

import (
	"fmt"
	"math"
	"os"
	"path/filepath"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	tokdistr "github.com/grafana/mimir/tools/analyse-unbalanced-series/tokendistributor"
)

const (
	maxTokenValue        = tokdistr.Token(math.MaxUint32)
	initialInstanceCount = 66
	iterations           = 30
)

func generateRingWithZoneAwareness(logger log.Logger, numTokensPerInstanceScenarios []int, replicationFactor, instancesPerZone int, zones []tokdistr.Zone, seedGeneratorProvider func(zones []tokdistr.Zone, replicationFactor, tokensPerInstance int, maxTokenValue tokdistr.Token) tokdistr.SeedGenerator) error {
	result := make([][]float64, 0, iterations)

	for it := 0; it < iterations; it++ {
		result = append(result, make([]float64, 0, len(numTokensPerInstanceScenarios)))
	}

	for _, numTokensPerInstance := range numTokensPerInstanceScenarios {
		for it := 0; it < iterations; it++ {
			level.Info(logger).Log("tokensPerInstance", numTokensPerInstance, "iteration", it)
			replicationStrategy := tokdistr.NewZoneAwareReplicationStrategy(replicationFactor, make(map[tokdistr.Instance]tokdistr.Zone, initialInstanceCount), nil, nil)
			tokenDistributor := tokdistr.NewTokenDistributor(numTokensPerInstance, len(zones), maxTokenValue, replicationStrategy, seedGeneratorProvider(zones, replicationFactor, numTokensPerInstance, maxTokenValue))
			var stat *tokdistr.Statistics
			for i := 0; i < instancesPerZone; i++ {
				for j := 0; j < len(zones); j++ {
					instance := tokdistr.Instance(fmt.Sprintf("%s-%d", string(rune('A'+j)), i))
					_, _, stat, _ = tokenDistributor.AddInstance(instance, zones[j])
				}
			}
			result[it] = append(result[it], stat.CombinedStatistics[tokdistr.InstanceStatKey].Spread)
		}
	}

	//Generate CSV header.
	csvHeader := make([]string, 0, len(numTokensPerInstanceScenarios))
	for _, numTokensPerInstance := range numTokensPerInstanceScenarios {
		csvHeader = append(csvHeader, fmt.Sprintf("%d tokens per instance", numTokensPerInstance))
	}

	// Write result to CSV.
	w := newCSVWriter[[]float64]()
	w.setHeader(csvHeader)
	w.setData(result, func(entry []float64) []string {
		res := make([]string, 0, len(numTokensPerInstanceScenarios))
		for _, value := range entry {
			res = append(res, fmt.Sprintf("%.3f", value))
		}
		return res
	})
	output := fmt.Sprintf("simulated-ring-with-different-tokens-per-instance-with-candidates-selection-rf-%d-zone-awareness-%s.csv", replicationFactor, formatEnabled(len(zones) > 1))
	filename := filepath.Join("tools", "analyse-unbalanced-series", "tokendistributor", output)
	if err := w.writeCSV(filename); err != nil {
		return err
	}
	return nil
}

func main() {
	logger := log.NewLogfmtLogger(os.Stdout)
	level.Info(logger).Log("msg", "Generating ring with the best candidate approach")
	numTokensPerInstanceScenarios := []int{4, 16, 64, 128, 256, 512}
	replicationFactor := 3
	zones := []tokdistr.Zone{tokdistr.Zone("zone-a"), tokdistr.Zone("zone-b"), tokdistr.Zone("zone-c")}
	instancesPerZone := initialInstanceCount / len(zones)
	seedGenerator := func(zones []tokdistr.Zone, replicationFactor, tokensPerInstance int, maxTokenValue tokdistr.Token) tokdistr.SeedGenerator {
		return tokdistr.NewPerfectlySpacedSeedGenerator(zones, replicationFactor, tokensPerInstance, maxTokenValue)
	}

	// generate ring with different tokens per instance with replication and zone-awareness enabled
	generateRingWithZoneAwareness(logger, numTokensPerInstanceScenarios, replicationFactor, instancesPerZone, zones, seedGenerator)
}
