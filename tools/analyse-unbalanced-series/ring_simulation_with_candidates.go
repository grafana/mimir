package main

import (
	"fmt"
	"math"
	"math/rand"
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
	iterations           = 5
)

func generateRingWithZoneAwareness(logger log.Logger, numTokensPerInstanceScenarios []int, replicationFactor, instancesPerZone int, zones []tokdistr.Zone, seedGeneratorProvider func(zones []tokdistr.Zone, replicationFactor, tokensPerInstance int, maxTokenValue tokdistr.Token) tokdistr.SeedGenerator) error {
	level.Info(logger).Log("test", "Generate token ring with candidate selection with zone-awareness enabled", "zones count", len(zones), "status", "start")
	ownershipInfos := make([]*tokdistr.OwnershipInfo, 0, iterations)
	instanceOwnershipByInstanceMap := make(map[tokdistr.Instance][]string, instancesPerZone*len(zones))
	runtimesPerScenario := make(map[int]time.Duration, len(numTokensPerInstanceScenarios))

	for _, numTokensPerInstance := range numTokensPerInstanceScenarios {
		ownershipInfos = ownershipInfos[:0]
		start := time.Now()
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
		runtimesPerScenario[numTokensPerInstance] = time.Since(start)
		ownershipInfosIntoInstanceStringMap(ownershipInfos, iterations, len(numTokensPerInstanceScenarios), instanceOwnershipByInstanceMap)
	}

	w := createCSVWriter(instanceOwnershipByInstanceMap, numTokensPerInstanceScenarios, iterations)
	output := fmt.Sprintf("%s-simulated-ring-with-different-tokens-per-instance-with-candidates-selection-rf-%d-zone-awareness-%s.csv", time.Now().Local(), replicationFactor, formatEnabled(len(zones) > 1))
	filename := filepath.Join("tools", "analyse-unbalanced-series", "tokendistributor", output)
	if err := w.writeCSV(filename); err != nil {
		return err
	}
	for scenario, runtime := range runtimesPerScenario {
		level.Info(logger).Log("number of tokens", scenario, "runtime", runtime/iterations)
	}
	level.Info(logger).Log("test", "Generate token ring with candidate selection with zone-awareness enabled", "zones count", len(zones), "status", "end")
	return nil
}

func generateRingWithoutZoneAwareness(logger log.Logger, numTokensPerInstanceScenarios []int, replicationFactor, instancesCount int, seedGeneratorProvider func(zones []tokdistr.Zone, replicationFactor int, tokensPerInstance int, maxTokenValue tokdistr.Token) tokdistr.SeedGenerator) error {
	level.Info(logger).Log("test", "Generate token ring with candidate selection with zone-awareness disabled", "replication factor", replicationFactor, "status", "start")
	ownershipInfos := make([]*tokdistr.OwnershipInfo, 0, iterations)
	instanceOwnershipByInstanceMap := make(map[tokdistr.Instance][]string, instancesCount)
	runtimesPerScenario := make(map[int]time.Duration, len(numTokensPerInstanceScenarios))

	zones := []tokdistr.Zone{tokdistr.SingleZone}

	for _, numTokensPerInstance := range numTokensPerInstanceScenarios {
		ownershipInfos = ownershipInfos[:0]
		start := time.Now()
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
		runtimesPerScenario[numTokensPerInstance] = time.Since(start)
		ownershipInfosIntoInstanceStringMap(ownershipInfos, iterations, len(numTokensPerInstanceScenarios), instanceOwnershipByInstanceMap)
	}

	w := createCSVWriter(instanceOwnershipByInstanceMap, numTokensPerInstanceScenarios, iterations)
	output := fmt.Sprintf("%s-simulated-ring-with-different-tokens-per-instance-with-candidates-selection-rf-%d-zone-awareness-%s.csv", time.Now().Local(), replicationFactor, formatEnabled(len(zones) > 1))
	filename := filepath.Join("tools", "analyse-unbalanced-series", "tokendistributor", output)
	if err := w.writeCSV(filename); err != nil {
		return err
	}
	for scenario, runtime := range runtimesPerScenario {
		level.Info(logger).Log("number of tokens", scenario, "runtime", runtime/iterations)
	}
	level.Info(logger).Log("test", "Generate token ring with candidate selection with zone-awareness disabled", "replication factor", replicationFactor, "status", "end")
	return nil
}

func generateRingWithZoneAwarenessAndRandomTokens(logger log.Logger, numTokensPerInstanceScenarios []int, replicationFactor, instancesPerZone int, zones []tokdistr.Zone, seedGeneratorProvider func(zones []tokdistr.Zone, replicationFactor, tokensPerInstance int, maxTokenValue tokdistr.Token) tokdistr.SeedGenerator) error {
	level.Info(logger).Log("test", "Generate token ring with random tokens with zone-awareness enabled", "zones count", len(zones), "status", "start")
	ownershipInfos := make([]*tokdistr.OwnershipInfo, 0, iterations)
	instanceOwnershipByInstanceMap := make(map[tokdistr.Instance][]string, instancesPerZone*len(zones))
	runtimesPerScenario := make(map[int]time.Duration, len(numTokensPerInstanceScenarios))

	for _, numTokensPerInstance := range numTokensPerInstanceScenarios {
		start := time.Now()
		ownershipInfos = ownershipInfos[:0]
		for it := 0; it < iterations; it++ {
			level.Info(logger).Log("tokensPerInstance", numTokensPerInstance, "iteration", it)
			replicationStrategy := tokdistr.NewZoneAwareReplicationStrategy(replicationFactor, make(map[tokdistr.Instance]tokdistr.Zone, initialInstanceCount), nil, nil)
			tokenDistributor := tokdistr.NewRandomTokenDistributor(numTokensPerInstance, len(zones), maxTokenValue, replicationStrategy, seedGeneratorProvider(zones, replicationFactor, numTokensPerInstance, maxTokenValue))
			var ownershipInfo *tokdistr.OwnershipInfo
			for i := 0; i < instancesPerZone; i++ {
				for j := 0; j < len(zones); j++ {
					instance := tokdistr.Instance(fmt.Sprintf("%s-%d", string(rune('A'+j)), i))
					_, _, ownershipInfo, _ = tokenDistributor.AddInstance(instance, zones[j])
				}
			}
			ownershipInfos = append(ownershipInfos, ownershipInfo)
		}
		runtimesPerScenario[numTokensPerInstance] = time.Since(start)
		ownershipInfosIntoInstanceStringMap(ownershipInfos, iterations, len(numTokensPerInstanceScenarios), instanceOwnershipByInstanceMap)
	}

	w := createCSVWriter(instanceOwnershipByInstanceMap, numTokensPerInstanceScenarios, iterations)
	output := fmt.Sprintf("%s-simulated-ring-with-different-tokens-per-instance-with-random-tokens-rf-%d-zone-awareness-%s.csv", time.Now().Local(), replicationFactor, formatEnabled(len(zones) > 1))
	filename := filepath.Join("tools", "analyse-unbalanced-series", "tokendistributor", output)
	if err := w.writeCSV(filename); err != nil {
		return err
	}
	for scenario, runtime := range runtimesPerScenario {
		level.Info(logger).Log("number of tokens", scenario, "runtime", runtime/iterations)
	}
	level.Info(logger).Log("test", "Generate token with random tokens ring with zone-awareness enabled", "zones count", len(zones), "status", "end")
	return nil
}

func generateRingWithoutZoneAwarenessAndRandomTokens(logger log.Logger, numTokensPerInstanceScenarios []int, replicationFactor, instancesCount int, seedGeneratorProvider func(zones []tokdistr.Zone, replicationFactor int, tokensPerInstance int, maxTokenValue tokdistr.Token) tokdistr.SeedGenerator) error {
	level.Info(logger).Log("test", "Generate token ring with random tokens with zone-awareness disabled", "replication factor", replicationFactor, "status", "start")
	ownershipInfos := make([]*tokdistr.OwnershipInfo, 0, iterations)
	instanceOwnershipByInstanceMap := make(map[tokdistr.Instance][]string, instancesCount)
	runtimesPerScenario := make(map[int]time.Duration, len(numTokensPerInstanceScenarios))

	zones := []tokdistr.Zone{tokdistr.SingleZone}

	for _, numTokensPerInstance := range numTokensPerInstanceScenarios {
		start := time.Now()
		ownershipInfos = ownershipInfos[:0]
		for it := 0; it < iterations; it++ {
			level.Info(logger).Log("tokensPerInstance", numTokensPerInstance, "iteration", it)
			replicationStrategy := tokdistr.NewSimpleReplicationStrategy(replicationFactor, nil)
			tokenDistributor := tokdistr.NewRandomTokenDistributor(numTokensPerInstance, len(zones), maxTokenValue, replicationStrategy, seedGeneratorProvider(zones, replicationFactor, numTokensPerInstance, maxTokenValue))
			var ownershipInfo *tokdistr.OwnershipInfo
			for i := 0; i < instancesCount; i++ {
				instance := tokdistr.Instance(fmt.Sprintf("I-%d", i))
				_, _, ownershipInfo, _ = tokenDistributor.AddInstance(instance, zones[0])
			}
			ownershipInfos = append(ownershipInfos, ownershipInfo)
		}
		runtimesPerScenario[numTokensPerInstance] = time.Since(start)
		ownershipInfosIntoInstanceStringMap(ownershipInfos, iterations, len(numTokensPerInstanceScenarios), instanceOwnershipByInstanceMap)
	}

	w := createCSVWriter(instanceOwnershipByInstanceMap, numTokensPerInstanceScenarios, iterations)
	output := fmt.Sprintf("%s-simulated-ring-with-different-tokens-per-instance-with-random-tokens-rf-%d-zone-awareness-%s.csv", time.Now().Local(), replicationFactor, formatEnabled(len(zones) > 1))
	filename := filepath.Join("tools", "analyse-unbalanced-series", "tokendistributor", output)
	if err := w.writeCSV(filename); err != nil {
		return err
	}
	level.Info(logger).Log("test", "Generate token ring with random tokens with zone-awareness enabled", "status", "end")
	for scenario, runtime := range runtimesPerScenario {
		level.Info(logger).Log("number of tokens", scenario, "runtime", runtime/iterations)
	}
	level.Info(logger).Log("test", "Generate token ring with random tokens with zone-awareness disabled", "replication factor", replicationFactor, "status", "end")
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

func createCSVWriter(instanceOwnershipByInstanceMap map[tokdistr.Instance][]string, numTokensPerInstanceScenarios []int, iterations int) *csvWriter[[]string] {
	//Generate CSV header.
	csvHeader := make([]string, 0, (iterations+3)*len(numTokensPerInstanceScenarios)+2)
	csvHeader = append(csvHeader, "instance")
	csvHeader = append(csvHeader, "optimal ownership")
	for _, numTokensPerInstance := range numTokensPerInstanceScenarios {
		if iterations == 1 {
			csvHeader = append(csvHeader, fmt.Sprintf("tokens:%d", numTokensPerInstance))
		} else {
			for i := 0; i < iterations; i++ {
				csvHeader = append(csvHeader, fmt.Sprintf("it:%d-tokens:%d", i+1, numTokensPerInstance))
			}
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

func formatCandidateSelectionMode(candidateSelectionMode bool) string {
	if candidateSelectionMode {
		return "candidates selection"
	}
	return "random tokens"
}

func createTokenDistributor(replicationFactor, tokensPerInstance, instancesPerZone int, zones []tokdistr.Zone) tokdistr.TokenDistributorInterface {
	totalInstances := instancesPerZone * len(zones)
	perfectlySpacedSeedGenerator := tokdistr.NewPerfectlySpacedSeedGenerator(zones, replicationFactor, tokensPerInstance, maxTokenValue)
	var replicationStrategy tokdistr.ReplicationStrategy
	if len(zones) == 1 {
		replicationStrategy = tokdistr.NewSimpleReplicationStrategy(replicationFactor, nil)
	} else {
		replicationStrategy = tokdistr.NewZoneAwareReplicationStrategy(replicationFactor, make(map[tokdistr.Instance]tokdistr.Zone, totalInstances), nil, nil)
	}
	return tokdistr.NewTokenDistributor(tokensPerInstance, len(zones), maxTokenValue, replicationStrategy, perfectlySpacedSeedGenerator)
}

func createRandomTokenDistributor(replicationFactor, tokensPerInstance, instancesPerZone int, zones []tokdistr.Zone) tokdistr.TokenDistributorInterface {
	totalInstances := instancesPerZone * len(zones)
	randomSeedGenerator := tokdistr.NewRandomSeedGenerator(zones, replicationFactor, tokensPerInstance, maxTokenValue)
	var replicationStrategy tokdistr.ReplicationStrategy
	if len(zones) == 1 {
		replicationStrategy = tokdistr.NewSimpleReplicationStrategy(replicationFactor, nil)
	} else {
		replicationStrategy = tokdistr.NewZoneAwareReplicationStrategy(replicationFactor, make(map[tokdistr.Instance]tokdistr.Zone, totalInstances), nil, nil)
	}
	return tokdistr.NewRandomTokenDistributor(tokensPerInstance, len(zones), maxTokenValue, replicationStrategy, randomSeedGenerator)
}

func generateRing(logger log.Logger, candidateSelectionMode bool, replicationFactor, tokensPerInstance, instancesPerZone int, zones []tokdistr.Zone) (tokdistr.TokenDistributorInterface, *tokdistr.OwnershipInfo, error) {
	level.Info(logger).Log("test", "Generate token ring", "mode", formatCandidateSelectionMode(candidateSelectionMode), "replication factor", replicationFactor, "zone-awareness", formatEnabled(len(zones) > 1), "tokens per instance", tokensPerInstance, "total tokens", tokensPerInstance*instancesPerZone, "status", "start")

	var initial rune
	if len(zones) == 1 {
		initial = 'I'
	} else {
		initial = 'A'
	}
	var tokenDistributor tokdistr.TokenDistributorInterface
	if candidateSelectionMode {
		tokenDistributor = createTokenDistributor(replicationFactor, tokensPerInstance, instancesPerZone, zones)
	} else {
		tokenDistributor = createRandomTokenDistributor(replicationFactor, tokensPerInstance, instancesPerZone, zones)
	}
	var ownershipInfo *tokdistr.OwnershipInfo
	var err error
	start := time.Now()
	for i := 0; i < instancesPerZone; i++ {
		for j := 0; j < len(zones); j++ {
			instance := tokdistr.Instance(fmt.Sprintf("%s-%d", string(initial+rune(j)), i))
			_, _, ownershipInfo, err = tokenDistributor.AddInstance(instance, zones[j])
			if err != nil {
				level.Error(logger).Log("test", "Generate token ring", "mode", formatCandidateSelectionMode(candidateSelectionMode), "replication factor", replicationFactor, "zone-awareness", formatEnabled(len(zones) > 1), "tokens per instance", tokensPerInstance, "total instances", tokensPerInstance*instancesPerZone, "runtime", time.Since(start), "err", err)
				return nil, nil, err
			}
		}
	}
	level.Info(logger).Log("test", "Generate token ring", "mode", formatCandidateSelectionMode(candidateSelectionMode), "replication factor", replicationFactor, "zone-awareness", formatEnabled(len(zones) > 1), "tokens per instance", tokensPerInstance, "total tokens", tokensPerInstance*instancesPerZone, "runtime", time.Since(start), "status", "end")
	return tokenDistributor, ownershipInfo, nil
}

func simulateTimeSeriesDistribution(logger log.Logger, candidateSelectionMode bool, numTokensPerInstanceScenarios []int, timeSeriesCount, replicationFactor, instancesPerZone int, zones []tokdistr.Zone) error {
	instancesCount := instancesPerZone * len(zones)
	level.Info(logger).Log("test", "Distribute tokens in a simulated ring", "mode", formatCandidateSelectionMode(candidateSelectionMode), "replication factor", replicationFactor, "zone-awareness", formatEnabled(len(zones) > 1), "total instances", instancesCount, "status", "start")
	instanceOwnershipByInstanceMap := make(map[tokdistr.Instance][]string, instancesCount)
	ownershipInfos := make([]*tokdistr.OwnershipInfo, 0, 1)
	for _, tokensPerInstance := range numTokensPerInstanceScenarios {
		tokenDistribution, _, err := generateRing(logger, candidateSelectionMode, replicationFactor, tokensPerInstance, instancesPerZone, zones)
		if err != nil {
			panic(err)
		}
		ownershipMap := make(map[tokdistr.Instance]float64, instancesCount)
		for i := 0; i < timeSeriesCount; i++ {
			timeSeriesToken := tokdistr.Token(rand.Uint32())
			replicaSet, err := tokenDistribution.GetReplicaSet(timeSeriesToken)
			if err != nil {
				panic(err)
			}
			for _, instance := range replicaSet {
				ownership, ok := ownershipMap[instance]
				if !ok {
					ownership = 0.0
				}
				ownership++
				ownershipMap[instance] = ownership
			}
		}
		optimalOwnership := float64(timeSeriesCount*replicationFactor) / float64(instancesCount)
		ownershipInfos = ownershipInfos[:0]
		ownershipInfo := &tokdistr.OwnershipInfo{
			OptimaInstanceOwnership: optimalOwnership,
			InstanceOwnershipMap:    ownershipMap,
		}
		ownershipInfos = append(ownershipInfos, ownershipInfo)
		ownershipInfosIntoInstanceStringMap(ownershipInfos, 1, len(numTokensPerInstanceScenarios), instanceOwnershipByInstanceMap)
	}

	w := createCSVWriter(instanceOwnershipByInstanceMap, numTokensPerInstanceScenarios, 1)
	var output string
	if candidateSelectionMode {
		output = fmt.Sprintf("%s-timeseries-distribution-in-ring-with-different-tokens-per-instance-with-candidate-selection-instances-%d-rf-%d-zone-awareness-%s.csv", time.Now().Local(), instancesCount, replicationFactor, formatEnabled(len(zones) > 1))
	} else {
		output = fmt.Sprintf("%s-timeseries-distribution-in-ring-with-different-tokens-per-instance-with-random-tokens-instances-%d-rf-%d-zone-awareness-%s.csv", time.Now().Local(), instancesCount, replicationFactor, formatEnabled(len(zones) > 1))
	}
	filename := filepath.Join("tools", "analyse-unbalanced-series", "tokendistributor", output)
	if err := w.writeCSV(filename); err != nil {
		return err
	}
	level.Info(logger).Log("test", "Distribute tokens in a simulated ring", "mode", formatCandidateSelectionMode(candidateSelectionMode), "replication factor", replicationFactor, "zone-awareness", formatEnabled(len(zones) > 1), "total instances", instancesCount, "status", "end")
	return nil
}

func simulateAddingAndRemovingInstances(logger log.Logger, candidateSelectionMode bool, numTokensPerInstanceScenarios []int, deltaCount, replicationFactor, instancesPerZone int, zones []tokdistr.Zone, lifoZones bool) error {
	instancesCount := instancesPerZone * len(zones)
	level.Info(logger).Log("test", "Add and Remove Instances in a simulated ring", "mode", formatCandidateSelectionMode(candidateSelectionMode), "replication factor", replicationFactor, "zone-awareness", formatEnabled(len(zones) > 1), "total instances", instancesCount, "status", "start")
	standardDeviationEvolutionByScenario := make(map[int][]*tokdistr.OwnershipInfo)
	ownershipInfos := make([]*tokdistr.OwnershipInfo, 0, 1+2*deltaCount)
	var initial rune
	if len(zones) == 1 {
		initial = 'I'
	} else {
		initial = 'A'
	}
	for _, tokensPerInstance := range numTokensPerInstanceScenarios {
		ownershipInfos = ownershipInfos[:0]
		tokenDistributor, ownershipInfo, err := generateRing(logger, candidateSelectionMode, replicationFactor, tokensPerInstance, instancesPerZone, zones)
		ownershipInfos = append(ownershipInfos, ownershipInfo)
		if err != nil {
			panic(err)
		}
		currIndex := instancesPerZone - 1
		for i := 0; i < deltaCount; i++ {
			zoneIndex := i % len(zones)
			if zoneIndex == 0 {
				currIndex++
			}
			instance := tokdistr.Instance(fmt.Sprintf("%s-%d", string(initial+rune(zoneIndex)), currIndex))
			_, _, ownershipInfo, _ = tokenDistributor.AddInstance(instance, zones[zoneIndex])
			ownershipInfos = append(ownershipInfos, ownershipInfo)
		}

		currIndex++
		for i := 0; i < deltaCount; i++ {
			zoneIndex := i % len(zones)
			if zoneIndex == 0 {
				currIndex--
			}
			if lifoZones {
				zoneIndex = len(zones) - 1 - zoneIndex
			}
			instance := tokdistr.Instance(fmt.Sprintf("%s-%d", string(initial+rune(zoneIndex)), currIndex))
			ownershipInfo, _ := tokenDistributor.RemoveInstance(instance)
			ownershipInfos = append(ownershipInfos, ownershipInfo)
		}
		standardDeviationEvolution := make([]*tokdistr.OwnershipInfo, 0, 1+2*deltaCount)
		standardDeviationEvolution = append(standardDeviationEvolution, ownershipInfos...)
		standardDeviationEvolutionByScenario[tokensPerInstance] = standardDeviationEvolution
	}

	//Generate CSV header.
	csvHeader := make([]string, 0, 3*len(numTokensPerInstanceScenarios))
	for _, numTokensPerInstance := range numTokensPerInstanceScenarios {
		csvHeader = append(csvHeader, fmt.Sprintf("tokens:%d-instances count", numTokensPerInstance))
		csvHeader = append(csvHeader, fmt.Sprintf("tokens:%d-optimal ownership", numTokensPerInstance))
		csvHeader = append(csvHeader, fmt.Sprintf("tokens:%d-stdev", numTokensPerInstance))
	}

	result := make([][]string, 0, 1+2*deltaCount)
	for i := 0; i < 1+2*deltaCount; i++ {
		result = append(result, make([]string, 0, 3*len(numTokensPerInstanceScenarios)))
	}
	for _, numTokensPerInstance := range numTokensPerInstanceScenarios {
		standardDeviationEvolution := standardDeviationEvolutionByScenario[numTokensPerInstance]
		for i, ownershipInfo := range standardDeviationEvolution {
			stdev := result[i]
			stdev = append(stdev, fmt.Sprintf("%d", len(ownershipInfo.InstanceOwnershipMap)))
			stdev = append(stdev, fmt.Sprintf("%.3f", ownershipInfo.OptimaInstanceOwnership))
			instanceStDev, _ := ownershipInfo.StDev()
			stdev = append(stdev, fmt.Sprintf("%.3f", instanceStDev))
			result[i] = stdev
		}
	}

	// Write result to CSV.
	w := newCSVWriter[[]string]()
	w.setHeader(csvHeader)
	w.setData(result, func(entry []string) []string {
		return entry
	})

	var output string
	if candidateSelectionMode {
		output = fmt.Sprintf("%s-add-remove-stdev-with-different-tokens-per-instance-with-candidate-selection-instances-%d-rf-%d-zone-awareness-%s.csv", time.Now().Local(), instancesCount, replicationFactor, formatEnabled(len(zones) > 1))
	} else {
		output = fmt.Sprintf("%s-add-remove-stdev-with-different-tokens-per-instance-with-random-tokens-instances-%d-rf-%d-zone-awareness-%s.csv", time.Now().Local(), instancesCount, replicationFactor, formatEnabled(len(zones) > 1))
	}
	filename := filepath.Join("tools", "analyse-unbalanced-series", "tokendistributor", output)
	if err := w.writeCSV(filename); err != nil {
		return err
	}
	level.Info(logger).Log("test", "Add and Remove Instances in a simulated ring", "mode", formatCandidateSelectionMode(candidateSelectionMode), "replication factor", replicationFactor, "zone-awareness", formatEnabled(len(zones) > 1), "total instances", instancesCount, "status", "end")
	return nil
}

func main() {
	logger := log.NewLogfmtLogger(os.Stdout)
	numTokensPerInstanceScenarios := []int{4, 16, 64, 128}
	replicationFactor := 3
	zones := []tokdistr.Zone{tokdistr.Zone("zone-a"), tokdistr.Zone("zone-b"), tokdistr.Zone("zone-c")}
	singleZones := []tokdistr.Zone{tokdistr.SingleZone}
	instancesPerZone := initialInstanceCount / len(zones)
	perfectlySpacedSeedGenerator := func(zones []tokdistr.Zone, replicationFactor, tokensPerInstance int, maxTokenValue tokdistr.Token) tokdistr.SeedGenerator {
		return tokdistr.NewPerfectlySpacedSeedGenerator(zones, replicationFactor, tokensPerInstance, maxTokenValue)
	}
	randomSeedGenerator := func(zones []tokdistr.Zone, replicationFactor, tokensPerInstance int, maxTokenValue tokdistr.Token) tokdistr.SeedGenerator {
		return tokdistr.NewRandomSeedGenerator(zones, replicationFactor, tokensPerInstance, maxTokenValue)
	}

	// simulation of ring creation with different number of tokens per instance, and with candidate selection or random token modes
	// generate ring with different tokens per instance with candidate selection with RF 1 and zone-awareness disabled
	generateRingWithoutZoneAwareness(logger, numTokensPerInstanceScenarios, 1, initialInstanceCount, perfectlySpacedSeedGenerator)
	// generate ring with different tokens per instance with candidate selection with replication and zone-awareness enabled
	generateRingWithoutZoneAwareness(logger, numTokensPerInstanceScenarios, replicationFactor, initialInstanceCount, perfectlySpacedSeedGenerator)
	// generate ring with different tokens per instance with candidate selection with replication and zone-awareness enabled
	generateRingWithZoneAwareness(logger, numTokensPerInstanceScenarios, replicationFactor, instancesPerZone, zones, perfectlySpacedSeedGenerator)
	// generate ring with different tokens per instance with random tokens with RF 1 and zone-awareness disabled
	generateRingWithoutZoneAwarenessAndRandomTokens(logger, numTokensPerInstanceScenarios, 1, initialInstanceCount, randomSeedGenerator)
	// generate ring with different tokens per instance with random tokens and zone-awareness enabled
	generateRingWithoutZoneAwarenessAndRandomTokens(logger, numTokensPerInstanceScenarios, replicationFactor, initialInstanceCount, randomSeedGenerator)
	// generate ring with different tokens per instance with random tokens and zone-awareness enabled
	generateRingWithZoneAwarenessAndRandomTokens(logger, numTokensPerInstanceScenarios, replicationFactor, instancesPerZone, zones, randomSeedGenerator)

	totalInstanceCount := initialInstanceCount
	timeSeriesCount := 10_000_000
	// simulation of timeseries distribution in a simulated ring with different number of tokens per instance, and with candidate selection or random token modes
	// candidate selection, RF = 1, zone-awareness disabled
	simulateTimeSeriesDistribution(logger, true, numTokensPerInstanceScenarios, timeSeriesCount, 1, totalInstanceCount, singleZones)
	// candidate selection, RF = 3, zone-awareness disabled
	simulateTimeSeriesDistribution(logger, true, numTokensPerInstanceScenarios, timeSeriesCount, 3, totalInstanceCount, singleZones)
	// candidate selection, RF = 3, zone-awareness enabled
	simulateTimeSeriesDistribution(logger, true, numTokensPerInstanceScenarios, timeSeriesCount, 3, totalInstanceCount/len(zones), zones)
	// random tokens, RF = 1, zone-awareness disabled
	simulateTimeSeriesDistribution(logger, false, numTokensPerInstanceScenarios, timeSeriesCount, 1, totalInstanceCount, singleZones)
	// random tokens, RF = 3, zone-awareness disabled
	simulateTimeSeriesDistribution(logger, false, numTokensPerInstanceScenarios, timeSeriesCount, 3, totalInstanceCount, singleZones)
	// random tokens, RF = 3, zone-awareness enabled
	simulateTimeSeriesDistribution(logger, false, numTokensPerInstanceScenarios, timeSeriesCount, 3, totalInstanceCount/len(zones), zones)

	// simulation of changing standard deviation by adding 100% of instances one-by-one and then removing the added instances one-by-one
	// in a simulated ring with different number of tokens per instance, and with candidate selection or random token modes
	// candidate selection, RF = 1, zone-awareness disabled
	simulateAddingAndRemovingInstances(logger, true, numTokensPerInstanceScenarios, totalInstanceCount, 1, totalInstanceCount, singleZones, true)
	// candidate selection, RF = 3, zone-awareness disabled
	simulateAddingAndRemovingInstances(logger, true, numTokensPerInstanceScenarios, totalInstanceCount, 3, totalInstanceCount, singleZones, true)
	// candidate selection, RF = 3, zone-awareness enabled
	simulateAddingAndRemovingInstances(logger, true, numTokensPerInstanceScenarios, totalInstanceCount, 3, totalInstanceCount/len(zones), zones, true)
	// random tokens, RF = 1, zone-awareness disabled
	simulateAddingAndRemovingInstances(logger, false, numTokensPerInstanceScenarios, totalInstanceCount, 1, totalInstanceCount, zones, true)
	// random tokens, RF = 3, zone-awareness disabled
	simulateAddingAndRemovingInstances(logger, false, numTokensPerInstanceScenarios, totalInstanceCount, 3, totalInstanceCount, zones, true)
	// random tokens, RF = 3, zone-awareness enabled
	simulateAddingAndRemovingInstances(logger, false, numTokensPerInstanceScenarios, totalInstanceCount, 3, totalInstanceCount/len(zones), zones, true)
}
