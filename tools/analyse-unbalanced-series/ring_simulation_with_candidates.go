package main

import (
	"math"

	tokdistr "github.com/grafana/mimir/tools/analyse-unbalanced-series/tokendistributor"
)

const (
	maxTokenValue        = tokdistr.Token(math.MaxUint32)
	initialInstanceCount = 66
)

/*
func generateRingWithZoneAwareness(numTokensPerIngesterScenarios []int, replicationFactor, instancesPerZone, tokensPerInstance int, zones []tokdistr.Zone, seedGenerator tokdistr.SeedGenerator) {
	analysisName := ""
	iterations := 10
	stats := make([]tokdistr.Statistics, 0, iterations)
	// newPerfectlySpacedSeedGenerator(zones, replicationFactor, tokensPerInstance, maxTokenValue)

	for it := 0; it < iterations; it++ {
		replicationStrategy := tokdistr.NewZoneAwareReplicationStrategy(replicationFactor, make(map[tokdistr.Instance]tokdistr.Zone, initialInstanceCount), nil, nil)
		tokenDistributor := tokdistr.NewTokenDistributor(tokensPerInstance, len(zones), maxTokenValue, replicationStrategy, seedGenerator)

		for i := 0; i < instancesPerZone; i++ {
			for j := 0; j < len(zones); j++ {
				instance := tokdistr.Instance(fmt.Sprintf("%s-%d", string(rune('A'+j)), i))
				_, _, stat := tokenDistributor.AddInstance(instance, zones[j])
				stats = append(stats, stat)
			}
		}
	}
	statistics := tokdistr.GetAverageStatistics(stats)
	statistics.Print()

	// Generate CSV header.
	var csvHeader []string
	for _, numTokensPerIngester := range numTokensPerIngesterScenarios {
		csvHeader = append(csvHeader, fmt.Sprintf("%d tokens per ingester", numTokensPerIngester))
	}

	// Write result to CSV.
	w := newCSVWriter([[]float64])
	w.setHeader(csvHeader)
	w.setData(allIterationsResults, func(entry []float64) []string {
		formatted := make([]string, 0, len(entry))
		for _, value := range entry {
			formatted = append(formatted, fmt.Sprintf("%.3f", value))
		}

		return formatted
	})
	if err := w.writeCSV(fmt.Sprintf("simulated-ingesters-ring-ownership-spread-on-different-tokens-per-ingester.csv")); err != nil {
		return err
	}
}*/
