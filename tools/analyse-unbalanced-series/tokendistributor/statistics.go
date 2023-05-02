package tokendistributor

import (
	"fmt"
	"math"

	"golang.org/x/exp/slices"
)

const (
	tokenStatKey    = "token"
	instanceStatKey = "instance"
)

type Statistics struct {
	CombinedStatistics                 map[string]StatisticType
	RegisteredTokenOwnershipByInstance map[Instance]float64
}

type StatisticType struct {
	optimalTokenOwnership                float64
	minDistanceFromOptimalTokenOwnership float64
	maxDistanceFromOptimalTokenOwnership float64
	minOwnership                         float64
	maxOwnership                         float64
	standardDeviation                    float64
	sum                                  float64
}

func (s Statistics) Print() {
	tokenStat, tokenStatFound := s.CombinedStatistics[tokenStatKey]
	instanceStat, instanceStatFound := s.CombinedStatistics[instanceStatKey]
	fmt.Printf("Optimal token ownership: per token %.2f, per instance %.2f\n", tokenStat.optimalTokenOwnership, instanceStat.optimalTokenOwnership)
	if tokenStatFound {
		fmt.Printf("Token    - new min dist from opt: %6.2f, new max dist from opt: %6.2f, new min ownership: %6.2f%%, new max ownership: %6.2f%%, new stdev: %6.2f, new sum: %6.2f\n", tokenStat.minDistanceFromOptimalTokenOwnership, tokenStat.maxDistanceFromOptimalTokenOwnership, tokenStat.minOwnership, tokenStat.maxOwnership, tokenStat.standardDeviation, tokenStat.sum)
	}
	if instanceStatFound {
		fmt.Printf("Instance - new min dist from opt: %6.2f, new max dist from opt: %6.2f, new min ownership: %6.2f%%, new max ownership: %6.2f%%, new stdev: %6.2f, new sum: %6.2f\n", instanceStat.minDistanceFromOptimalTokenOwnership, instanceStat.maxDistanceFromOptimalTokenOwnership, instanceStat.minOwnership, instanceStat.maxOwnership, instanceStat.standardDeviation, instanceStat.sum)
	}
}

func GetAverageStatistics(stats []Statistics) Statistics {
	combinedType := make(map[string]StatisticType)
	combinedType[tokenStatKey] = StatisticType{}
	combinedType[instanceStatKey] = StatisticType{}
	size := float64(len(stats))
	perInstanceRegisteredTokenOwnership := make(map[Instance]float64, len(stats[0].RegisteredTokenOwnershipByInstance))

	for _, singleStatistics := range stats {
		for key, statisticType := range singleStatistics.CombinedStatistics {
			var combinedResult StatisticType
			if key == tokenStatKey {
				combinedResult = combinedType[tokenStatKey]
			} else {
				combinedResult = combinedType[instanceStatKey]
			}
			combinedResult.optimalTokenOwnership = statisticType.optimalTokenOwnership
			combinedResult.minDistanceFromOptimalTokenOwnership += statisticType.minDistanceFromOptimalTokenOwnership / size
			combinedResult.maxDistanceFromOptimalTokenOwnership += statisticType.maxDistanceFromOptimalTokenOwnership / size
			combinedResult.minOwnership += statisticType.minOwnership / size
			combinedResult.maxOwnership += statisticType.maxOwnership / size
			combinedResult.standardDeviation += statisticType.standardDeviation / size
			combinedResult.sum = statisticType.sum
			if key == tokenStatKey {
				combinedType[tokenStatKey] = combinedResult
			} else {
				combinedType[instanceStatKey] = combinedResult
			}
		}
		for instance, registeredTokenOwnership := range singleStatistics.RegisteredTokenOwnershipByInstance {
			ownership, ok := perInstanceRegisteredTokenOwnership[instance]
			if !ok {
				ownership = 0.0
			}
			ownership += registeredTokenOwnership / size
			perInstanceRegisteredTokenOwnership[instance] = ownership
		}
	}
	return Statistics{
		CombinedStatistics:                 combinedType,
		RegisteredTokenOwnershipByInstance: perInstanceRegisteredTokenOwnership,
	}
}

func getTimeseriesStatistics(tokenDistributor *TokenDistributor, ownershipMap map[Instance]int, optimalTimeseriesOwnership float64, totalTimeseries int) Statistics {
	statisticType := make(map[string]StatisticType, 1)
	instance := StatisticType{}

	instance.minDistanceFromOptimalTokenOwnership = math.MaxFloat64
	instance.maxDistanceFromOptimalTokenOwnership = math.SmallestNonzeroFloat64
	instance.minOwnership = math.MaxFloat64
	instance.maxOwnership = math.SmallestNonzeroFloat64
	instance.optimalTokenOwnership = float64(tokenDistributor.tokensPerInstance) * optimalTimeseriesOwnership
	instance.standardDeviation = 0.0
	instance.sum = 0.0
	for _, ownership := range ownershipMap {
		dist := float64(ownership) / (optimalTimeseriesOwnership * float64(tokenDistributor.tokensPerInstance))
		currTokensPercentage := float64(ownership) * 100.00 / float64(totalTimeseries)
		if instance.minDistanceFromOptimalTokenOwnership > dist {
			instance.minDistanceFromOptimalTokenOwnership = dist
		}
		if instance.maxDistanceFromOptimalTokenOwnership < dist {
			instance.maxDistanceFromOptimalTokenOwnership = dist
		}
		if instance.minOwnership > currTokensPercentage {
			instance.minOwnership = currTokensPercentage
		}
		if instance.maxOwnership < currTokensPercentage {
			instance.maxOwnership = currTokensPercentage
		}
		instance.standardDeviation += +sq(dist - 1.0)
		instance.sum += float64(ownership)
	}
	statisticType[instanceStatKey] = instance
	return Statistics{CombinedStatistics: statisticType}
}

type SingleInstanceStatistics struct {
	registeredTokenOwnership    float64
	assignedTimeseriesOwnership float64
}

func getTimeseriesPerInstanceStatistics(timeSeriesOwnershipMap map[Instance]int, totalTimeseries int) []float64 {
	instances := make([]Instance, 0, len(timeSeriesOwnershipMap))
	for instance := range timeSeriesOwnershipMap {
		instances = append(instances, instance)
	}
	slices.Sort(instances)

	perInstanceStatistics := make([]float64, 0, len(instances))
	for _, instance := range instances {
		assignedTimeseriesOwnership := float64(timeSeriesOwnershipMap[instance]) * 100.00 / float64(totalTimeseries)
		perInstanceStatistics = append(perInstanceStatistics, assignedTimeseriesOwnership)
	}

	return perInstanceStatistics
}
