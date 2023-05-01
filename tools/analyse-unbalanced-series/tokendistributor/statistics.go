package tokendistributor

import "fmt"

type Statistics struct {
	types map[string]StatisticType
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

func (s Statistics) print() {
	tokenStat := s.types["token"]
	instanceStat := s.types["instance"]
	fmt.Printf("Optimal token ownership: per token %.2f, per instance %.2f\n", tokenStat.optimalTokenOwnership, instanceStat.optimalTokenOwnership)
	fmt.Printf("Token    - new min dist from opt: %6.2f, new max dist from opt: %6.2f, new min ownership: %6.2f%%, new max ownership: %6.2f%%, new stdev: %6.2f, new sum: %6.2f\n", tokenStat.minDistanceFromOptimalTokenOwnership, tokenStat.maxDistanceFromOptimalTokenOwnership, tokenStat.minOwnership, tokenStat.maxOwnership, tokenStat.standardDeviation, tokenStat.sum)
	fmt.Printf("Instance - new min dist from opt: %6.2f, new max dist from opt: %6.2f, new min ownership: %6.2f%%, new max ownership: %6.2f%%, new stdev: %6.2f, new sum: %6.2f\n", instanceStat.minDistanceFromOptimalTokenOwnership, instanceStat.maxDistanceFromOptimalTokenOwnership, instanceStat.minOwnership, instanceStat.maxOwnership, instanceStat.standardDeviation, instanceStat.sum)
}
