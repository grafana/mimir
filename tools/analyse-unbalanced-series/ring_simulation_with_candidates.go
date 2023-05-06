package main

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"golang.org/x/exp/slices"

	tokdistr "github.com/grafana/mimir/tools/analyse-unbalanced-series/tokendistributor"
)

const (
	maxTokenValue        = tokdistr.Token(math.MaxUint32)
	initialInstanceCount = 66
	iterations           = 15
)

func generateRingWithZoneAwareness(logger log.Logger, numTokensPerInstanceScenarios []int, replicationFactor, instancesPerZone int, zones []tokdistr.Zone, seedGeneratorProvider func(zones []tokdistr.Zone, replicationFactor, tokensPerInstance int, maxTokenValue tokdistr.Token) tokdistr.SeedGenerator) error {
	level.Info(logger).Log("test", "Generate token ring with zone-awareness enabled", "status", "start")
	ownershipInfos := make([]*tokdistr.OwnershipInfo, 0, iterations)
	instanceOwnershipByInstanceMap := make(map[tokdistr.Instance][]string, instancesPerZone*len(zones))

	for _, numTokensPerInstance := range numTokensPerInstanceScenarios {
		ownershipInfos = ownershipInfos[:0]
		for it := 0; it < iterations; it++ {
			level.Info(logger).Log("tokensPerInstance", numTokensPerInstance, "iteration", it)
			replicationStrategy := tokdistr.NewZoneAwareReplicationStrategy(replicationFactor, make(map[tokdistr.Instance]tokdistr.Zone, initialInstanceCount), nil, nil)
			tokenDistributor := tokdistr.NewTokenDistributor(numTokensPerInstance, len(zones), maxTokenValue, replicationStrategy, seedGeneratorProvider(zones, replicationFactor, numTokensPerInstance, maxTokenValue))
			var ownershipInfo *tokdistr.OwnershipInfo
			for i := 0; i < instancesPerZone; i++ {
				for j := 0; j < len(zones); j++ {
					instance := tokdistr.Instance(fmt.Sprintf("%s-%d", string(rune('A'+j)), i))
					_, _, ownershipInfo, _ = tokenDistributor.AddInstance(instance, zones[j])
				}
			}
			ownershipInfos = append(ownershipInfos, ownershipInfo)
		}
		ownershipInfosIntoInstanceStringMap(ownershipInfos, iterations, len(numTokensPerInstanceScenarios), instanceOwnershipByInstanceMap)
	}

	w := createCSVWriter(instanceOwnershipByInstanceMap, numTokensPerInstanceScenarios)
	output := fmt.Sprintf("%s-simulated-ring-with-different-tokens-per-instance-with-candidates-selection-rf-%d-zone-awareness-%s.csv", time.Now().Local(), replicationFactor, formatEnabled(len(zones) > 1))
	filename := filepath.Join("tools", "analyse-unbalanced-series", "tokendistributor", output)
	if err := w.writeCSV(filename); err != nil {
		return err
	}
	level.Info(logger).Log("test", "Generate token ring with zone-awareness enabled", "status", "end")
	return nil
}

func generateRingWithoutZoneAwareness(logger log.Logger, numTokensPerInstanceScenarios []int, replicationFactor, instancesCount int, seedGeneratorProvider func(zones []tokdistr.Zone, replicationFactor int, tokensPerInstance int, maxTokenValue tokdistr.Token) tokdistr.SeedGenerator) error {
	level.Info(logger).Log("test", "Generate token ring with zone-awareness enabled", "status", "start")
	ownershipInfos := make([]*tokdistr.OwnershipInfo, 0, iterations)
	instanceOwnershipByInstanceMap := make(map[tokdistr.Instance][]string, instancesCount)

	zones := []tokdistr.Zone{tokdistr.SingleZone}

	for _, numTokensPerInstance := range numTokensPerInstanceScenarios {
		for it := 0; it < iterations; it++ {
			level.Info(logger).Log("tokensPerInstance", numTokensPerInstance, "iteration", it)
			replicationStrategy := tokdistr.NewSimpleReplicationStrategy(replicationFactor, nil)
			tokenDistributor := tokdistr.NewTokenDistributor(numTokensPerInstance, len(zones), maxTokenValue, replicationStrategy, seedGeneratorProvider(zones, replicationFactor, numTokensPerInstance, maxTokenValue))
			var ownershipInfo *tokdistr.OwnershipInfo
			for i := 0; i < instancesCount; i++ {
				instance := tokdistr.Instance(fmt.Sprintf("I-%d", i))
				_, _, ownershipInfo, _ = tokenDistributor.AddInstance(instance, zones[0])
			}
			ownershipInfos = append(ownershipInfos, ownershipInfo)
		}
		ownershipInfosIntoInstanceStringMap(ownershipInfos, iterations, len(numTokensPerInstanceScenarios), instanceOwnershipByInstanceMap)
	}

	w := createCSVWriter(instanceOwnershipByInstanceMap, numTokensPerInstanceScenarios)
	output := fmt.Sprintf("%s-simulated-ring-with-different-tokens-per-instance-with-candidates-selection-rf-%d-zone-awareness-%s.csv", time.Now().Local(), replicationFactor, formatEnabled(len(zones) > 1))
	filename := filepath.Join("tools", "analyse-unbalanced-series", "tokendistributor", output)
	if err := w.writeCSV(filename); err != nil {
		return err
	}
	return nil
}

func ownershipInfosIntoInstanceStringMap(ownershipInfos []*tokdistr.OwnershipInfo, iterationCount, samplesCount int, result map[tokdistr.Instance][]string) {
	maxInstanceStDev, _ := tokdistr.CalculateMaxStdevAndBounds(ownershipInfos)
	for i, ownershipInfo := range ownershipInfos {
		for instance, ownership := range ownershipInfo.InstanceOwnershipMap {
			currResult, ok := result[instance]
			if !ok {
				currResult = make([]string, 0, samplesCount*(iterationCount+3)+2)
				currResult = append(currResult, string(instance))
				currResult = append(currResult, fmt.Sprintf("%f", ownershipInfo.OptimaInstanceOwnership))
			}
			currResult = append(currResult, fmt.Sprintf("%f", ownership))
			if i == len(ownershipInfos)-1 {
				currResult = append(currResult, fmt.Sprintf("%f", maxInstanceStDev))
				currResult = append(currResult, fmt.Sprintf("%f", ownershipInfo.OptimaInstanceOwnership-maxInstanceStDev))
				currResult = append(currResult, fmt.Sprintf("%f", ownershipInfo.OptimaInstanceOwnership+maxInstanceStDev))
			}
			result[instance] = currResult
		}
	}
}

func createCSVWriter(instanceOwnershipByInstanceMap map[tokdistr.Instance][]string, numTokensPerInstanceScenarios []int) *csvWriter[[]string] {
	//Generate CSV header.
	csvHeader := make([]string, 0, (iterations+3)*len(numTokensPerInstanceScenarios)+2)
	csvHeader = append(csvHeader, "instance")
	csvHeader = append(csvHeader, "optimal ownership")
	for _, numTokensPerInstance := range numTokensPerInstanceScenarios {
		for i := 0; i < iterations; i++ {
			csvHeader = append(csvHeader, fmt.Sprintf("it:%d-tokens:%d", i+1, numTokensPerInstance))
		}
		csvHeader = append(csvHeader, fmt.Sprintf("tokens:%d-maxStDev", numTokensPerInstance))
		csvHeader = append(csvHeader, fmt.Sprintf("tokens:%d-lowerBound", numTokensPerInstance))
		csvHeader = append(csvHeader, fmt.Sprintf("tokens:%d-upperBound", numTokensPerInstance))
	}

	instances := make([]tokdistr.Instance, 0, len(instanceOwnershipByInstanceMap))
	for instance := range instanceOwnershipByInstanceMap {
		instances = append(instances, instance)
	}
	slices.Sort(instances)

	result := make([][]string, 0, len(instanceOwnershipByInstanceMap))
	for _, instance := range instances {
		result = append(result, instanceOwnershipByInstanceMap[instance])
	}

	// Write result to CSV.
	w := newCSVWriter[[]string]()
	w.setHeader(csvHeader)
	w.setData(result, func(entry []string) []string {
		return entry
	})
	return w
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

	// generate ring with different tokens per instance with RF 1 and zone-awareness disabled
	generateRingWithoutZoneAwareness(logger, numTokensPerInstanceScenarios, 1, initialInstanceCount, seedGenerator)
	// generate ring with different tokens per instance with replication and zone-awareness enabled
	generateRingWithoutZoneAwareness(logger, numTokensPerInstanceScenarios, replicationFactor, initialInstanceCount, seedGenerator)
	// generate ring with different tokens per instance with replication and zone-awareness enabled
	generateRingWithZoneAwareness(logger, numTokensPerInstanceScenarios, replicationFactor, instancesPerZone, zones, seedGenerator)
}
