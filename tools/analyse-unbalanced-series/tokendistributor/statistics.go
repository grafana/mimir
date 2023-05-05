package tokendistributor

import (
	"fmt"
	"math"

	"golang.org/x/exp/slices"
)

const (
	TokenStatKey    = "token"
	InstanceStatKey = "instance"
)

type Statistics struct {
	MaxToken                           uint32
	ReplicationFactor                  int
	CombinedStatistics                 map[string]StatisticType
	RegisteredTokenOwnershipByInstance map[Instance]float64
}

type StatisticType struct {
	OptimalTokenOwnership                float64
	MinDistanceFromOptimalTokenOwnership float64
	MaxDistanceFromOptimalTokenOwnership float64
	MinOwnership                         float64
	MaxOwnership                         float64
	StandardDeviation                    float64
	Spread                               float64
	Sum                                  float64
}

func (s Statistics) Print() {
	tokenStat, tokenStatFound := s.CombinedStatistics[TokenStatKey]
	instanceStat, instanceStatFound := s.CombinedStatistics[InstanceStatKey]
	fmt.Printf("Toke space size: %d, replication factor: %d\n", s.MaxToken, s.ReplicationFactor)
	if tokenStatFound {
		fmt.Printf("Token    - opt: %10.3f, min dist from opt: %6.3f, max dist from opt: %6.3f, min ownership: %10.3f, max ownership: %10.3f, stdev: %10.3f, Spread: %6.3f, Sum: %10.3f\n", tokenStat.OptimalTokenOwnership, tokenStat.MinDistanceFromOptimalTokenOwnership, tokenStat.MaxDistanceFromOptimalTokenOwnership, tokenStat.MinOwnership, tokenStat.MaxOwnership, tokenStat.StandardDeviation, tokenStat.Spread, tokenStat.Sum)
	}
	if instanceStatFound {
		fmt.Printf("Instance - opt: %10.3f min dist from opt: %6.3f, max dist from opt: %6.3f, min ownership: %10.3f, max ownership: %10.3f, stdev: %10.3f, Spread: %6.3f, Sum: %10.3f\n", instanceStat.OptimalTokenOwnership, instanceStat.MinDistanceFromOptimalTokenOwnership, instanceStat.MaxDistanceFromOptimalTokenOwnership, instanceStat.MinOwnership, instanceStat.MaxOwnership, instanceStat.StandardDeviation, instanceStat.Spread, instanceStat.Sum)
	}
	/*for instance, ownership := range s.RegisteredTokenOwnershipByInstance {
		fmt.Printf("%10s: %6.3f\n", instance, ownership)
	}*/
}

func GetAverageStatistics(stats []Statistics) Statistics {
	combinedType := make(map[string]StatisticType)
	combinedType[TokenStatKey] = StatisticType{}
	combinedType[InstanceStatKey] = StatisticType{}
	size := float64(len(stats))

	for _, singleStatistics := range stats {
		for key, statisticType := range singleStatistics.CombinedStatistics {
			var combinedResult StatisticType
			if key == TokenStatKey {
				combinedResult = combinedType[TokenStatKey]
			} else {
				combinedResult = combinedType[InstanceStatKey]
			}
			combinedResult.OptimalTokenOwnership = statisticType.OptimalTokenOwnership
			combinedResult.MinDistanceFromOptimalTokenOwnership += statisticType.MinDistanceFromOptimalTokenOwnership / size
			combinedResult.MaxDistanceFromOptimalTokenOwnership += statisticType.MaxDistanceFromOptimalTokenOwnership / size
			combinedResult.MinOwnership += statisticType.MinOwnership / size
			combinedResult.MaxOwnership += statisticType.MaxOwnership / size
			combinedResult.StandardDeviation += statisticType.StandardDeviation / size
			combinedResult.Spread += statisticType.Spread / size
			combinedResult.Sum = statisticType.Sum
			if key == TokenStatKey {
				combinedType[TokenStatKey] = combinedResult
			} else {
				combinedType[InstanceStatKey] = combinedResult
			}
		}
	}
	return Statistics{
		CombinedStatistics: combinedType,
	}
}

func getTimeseriesStatistics(tokenDistributor *TokenDistributor, ownershipMap map[Instance]int, optimalTimeseriesOwnership float64, totalTimeseries int) *Statistics {
	statisticType := make(map[string]StatisticType, 1)
	instance := StatisticType{}

	instance.MinDistanceFromOptimalTokenOwnership = math.MaxFloat64
	instance.MaxDistanceFromOptimalTokenOwnership = math.SmallestNonzeroFloat64
	instance.MinOwnership = math.MaxFloat64
	instance.MaxOwnership = math.SmallestNonzeroFloat64
	instance.OptimalTokenOwnership = float64(tokenDistributor.tokensPerInstance) * optimalTimeseriesOwnership
	instance.StandardDeviation = 0.0
	instance.Sum = 0.0
	for _, ownership := range ownershipMap {
		dist := float64(ownership) / (optimalTimeseriesOwnership * float64(tokenDistributor.tokensPerInstance))
		currTokensPercentage := float64(ownership) * 100.00 / float64(totalTimeseries)
		if instance.MinDistanceFromOptimalTokenOwnership > dist {
			instance.MinDistanceFromOptimalTokenOwnership = dist
		}
		if instance.MaxDistanceFromOptimalTokenOwnership < dist {
			instance.MaxDistanceFromOptimalTokenOwnership = dist
		}
		if instance.MinOwnership > currTokensPercentage {
			instance.MinOwnership = currTokensPercentage
		}
		if instance.MaxOwnership < currTokensPercentage {
			instance.MaxOwnership = currTokensPercentage
		}
		instance.StandardDeviation += +sq(dist - 1.0)
		instance.Sum += float64(ownership)
	}
	statisticType[InstanceStatKey] = instance
	return &Statistics{CombinedStatistics: statisticType}
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
