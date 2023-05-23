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

type OwnershipInfo struct {
	InstanceOwnershipMap    map[Instance]float64
	TokenOwnershipMap       map[Token]float64
	OptimaInstanceOwnership float64
	OptimalTokenOwnership   float64
}

func CalculateMaxStdevAndBounds(input []*OwnershipInfo) (float64, float64) {
	maxInstanceStDev := 0.0
	maxTokenStDev := 0.0
	for _, ownershipInfo := range input {
		instanceStDev, tokenStDev := ownershipInfo.StDev()
		maxInstanceStDev = math.Max(maxInstanceStDev, instanceStDev)
		maxTokenStDev = math.Max(maxTokenStDev, tokenStDev)
	}
	return maxInstanceStDev, maxTokenStDev
}

func (o *OwnershipInfo) StDev() (float64, float64) {
	instanceStDev := 0.0
	tokenStDev := 0.0
	for _, ownership := range o.InstanceOwnershipMap {
		instanceStDev += math.Pow(ownership-o.OptimaInstanceOwnership, 2.0)
	}
	instanceStDev = math.Sqrt(instanceStDev / float64(len(o.InstanceOwnershipMap)))
	for _, ownership := range o.TokenOwnershipMap {
		tokenStDev += math.Pow(ownership-o.OptimalTokenOwnership, 2.0)
	}
	tokenStDev = math.Sqrt(tokenStDev / float64(len(o.TokenOwnershipMap)))
	return instanceStDev, tokenStDev
}

func (o *OwnershipInfo) getStat() (float64, float64, float64, float64) {
	min := math.MaxFloat64
	max := 0.0
	instanceStDev := 0.0
	sum := 0.0
	for _, ownership := range o.InstanceOwnershipMap {
		if ownership < min {
			min = ownership
		}
		if ownership > max {
			max = ownership
		}
		instanceStDev += math.Pow(ownership-o.OptimaInstanceOwnership, 2.0)
		sum += ownership
	}
	instanceStDev = math.Sqrt(instanceStDev / float64(len(o.InstanceOwnershipMap)))
	return min, max, sum, instanceStDev
}

func (o *OwnershipInfo) Print() {
	min, max, sum, stdev := o.getStat()
	spread := (max - min) / max
	fmt.Printf("Instance - opt: %15.3f, sum: %15.3f\n"+
		"\tmin ownership: %15.3f, max ownership: %15.3f, spread: %6.3f, stdev: %15.3f\n", o.OptimalTokenOwnership, sum, min, max, spread, stdev)

	instances := make([]Instance, 0, len(o.InstanceOwnershipMap))
	for instance, _ := range o.InstanceOwnershipMap {
		instances = append(instances, instance)
	}

	slices.Sort(instances)

	for _, instance := range instances {
		fmt.Printf("%10s,%15.3f\n", instance, o.InstanceOwnershipMap[instance])
	}
}
