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
	UpperBound                           float64
	LowerBound                           float64
	Spread                               float64
	Sum                                  float64
}

func (s Statistics) Print() {
	tokenStat, tokenStatFound := s.CombinedStatistics[TokenStatKey]
	instanceStat, instanceStatFound := s.CombinedStatistics[InstanceStatKey]
	fmt.Printf("Token space size: %d, replication factor: %d\n", s.MaxToken, s.ReplicationFactor)
	if tokenStatFound {
		fmt.Printf("Instance - opt: %15.3f, sum: %15.3f\n"+
			"\tmin dist from opt: %15.3f, max dist from opt: %15.3f\n"+
			"\t    min ownership: %15.3f,     max ownership: %15.3f,      spread: %6.3f\n"+
			"\t            stdev: %15.3f,       lower bound: %15.3f, upper bound: %15.3f\n", tokenStat.OptimalTokenOwnership, tokenStat.Sum, tokenStat.MinDistanceFromOptimalTokenOwnership, tokenStat.MaxDistanceFromOptimalTokenOwnership, tokenStat.MinOwnership, tokenStat.MaxOwnership, tokenStat.Spread, tokenStat.StandardDeviation, tokenStat.LowerBound, tokenStat.UpperBound)
	}
	if instanceStatFound {
		fmt.Printf("Instance - opt: %15.3f, sum: %15.3f\n"+
			"\tmin dist from opt: %15.3f, max dist from opt: %15.3f\n"+
			"\t    min ownership: %15.3f,     max ownership: %15.3f,      spread: %6.3f\n"+
			"\t            stdev: %15.3f,       lower bound: %15.3f, upper bound: %15.3f\n", instanceStat.OptimalTokenOwnership, instanceStat.Sum, instanceStat.MinDistanceFromOptimalTokenOwnership, instanceStat.MaxDistanceFromOptimalTokenOwnership, instanceStat.MinOwnership, instanceStat.MaxOwnership, instanceStat.Spread, instanceStat.StandardDeviation, instanceStat.LowerBound, instanceStat.UpperBound)
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

type OwnershipInfo struct {
	InstanceOwnershipMap    map[Instance]float64
	TokenOwnershipMap       map[Token]float64
	OptimaInstanceOwnership float64
	OptimalTokenOwnership   float64
}

func calculateMaxStdevAndBounds(input []*OwnershipInfo) (float64, float64) {
	maxInstanceStDev := 0.0
	maxTokenStDev := 0.0
	for _, ownershipInfo := range input {
		instanceStDev, tokenStDev := stDev(ownershipInfo)
		maxInstanceStDev = math.Max(maxInstanceStDev, instanceStDev)
		maxTokenStDev = math.Max(maxTokenStDev, tokenStDev)
	}
	return maxInstanceStDev, maxTokenStDev
}

func stDev(ownershipInfo *OwnershipInfo) (float64, float64) {
	instanceStDev := 0.0
	tokenStDev := 0.0
	for _, ownership := range ownershipInfo.InstanceOwnershipMap {
		instanceStDev += math.Pow(ownership-ownershipInfo.OptimaInstanceOwnership, 2.0)
	}
	instanceStDev = math.Sqrt(instanceStDev / float64(len(ownershipInfo.InstanceOwnershipMap)))
	for _, ownership := range ownershipInfo.TokenOwnershipMap {
		tokenStDev += math.Pow(ownership-ownershipInfo.OptimalTokenOwnership, 2.0)
	}
	tokenStDev = math.Sqrt(tokenStDev / float64(len(ownershipInfo.TokenOwnershipMap)))
	return instanceStDev, tokenStDev
}
