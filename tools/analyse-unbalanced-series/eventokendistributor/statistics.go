package eventokendistributor

import (
	"fmt"
	"math"
)

type RingStatistics struct {
	zones                  []string
	minByZone              []float64
	maxByZone              []float64
	spreadByZone           []float64
	optimalOwnershipByZone []float64
	stdevByZone            []float64
}

func NewRingStatistics(ownershipByInstanceByZone map[string]map[string]float64, maxTokenValue uint32) RingStatistics {
	zonesCount := len(ownershipByInstanceByZone)
	zones := make([]string, 0, zonesCount)
	minByZone := make([]float64, 0, zonesCount)
	maxByZone := make([]float64, 0, zonesCount)
	spreadByZone := make([]float64, 0, zonesCount)
	optimalOwnershipByZone := make([]float64, 0, zonesCount)
	stdevByZone := make([]float64, 0, zonesCount)
	for zone, ownershipByInstance := range ownershipByInstanceByZone {
		zones = append(zones, zone)
		min := float64(math.MaxUint32)
		max := 0.0
		optimalOwnership := float64(maxTokenValue) / float64(len(ownershipByInstance))
		optimalOwnershipByZone = append(optimalOwnershipByZone, optimalOwnership)
		stdev := 0.0
		for _, ownership := range ownershipByInstance {
			min = math.Min(min, ownership)
			max = math.Max(max, ownership)
			stdev += math.Pow(ownership-optimalOwnership, 2)
		}
		stdev = math.Sqrt(stdev / float64(len(ownershipByInstance)))
		stdev /= optimalOwnership
		stdev *= 100
		minByZone = append(minByZone, min)
		maxByZone = append(maxByZone, max)
		spreadByZone = append(spreadByZone, (max-min)*100/max)
		stdevByZone = append(stdevByZone, stdev)
	}

	statistics := RingStatistics{
		zones:                  zones,
		minByZone:              minByZone,
		maxByZone:              maxByZone,
		spreadByZone:           spreadByZone,
		optimalOwnershipByZone: optimalOwnershipByZone,
		stdevByZone:            spreadByZone,
	}
	return statistics
}

func (s *RingStatistics) Print() {
	for z, zone := range s.zones {
		fmt.Printf("zone: %10s, optimalOwnership: %15.3f, min: %15.3f, max: %15.3f, spread: %6.3f%%, stdev: %6.3f%%\n", zone, s.optimalOwnershipByZone[z], s.minByZone[z], s.maxByZone[z], s.spreadByZone[z], s.stdevByZone[z])
	}
}

type TimeseriesDistributionStatistics struct {
	OwnershipByInstanceByZone map[string]map[string]float64
}

func NewTimeseriesDistributionStatistics(r *Ring) *TimeseriesDistributionStatistics {
	ownershipByInstanceByZone := make(map[string]map[string]float64, len(r.zones))
	instancesByZone := r.getInstancesByZone()
	for zone, instances := range instancesByZone {
		ownershipByInstanceByZone[zone] = make(map[string]float64, len(instances))
	}
	return &TimeseriesDistributionStatistics{
		OwnershipByInstanceByZone: ownershipByInstanceByZone,
	}
}

func (s *TimeseriesDistributionStatistics) AddOwnership(replicationSet []InstanceInfo) {
	for _, instanceInfo := range replicationSet {
		ownershipByInstance := s.OwnershipByInstanceByZone[instanceInfo.Zone]
		ownershipByInstance[instanceInfo.InstanceID]++
		s.OwnershipByInstanceByZone[instanceInfo.Zone] = ownershipByInstance
	}
}

func (s *TimeseriesDistributionStatistics) Print(verbose bool) {
	for zone, ownershipByInstance := range s.OwnershipByInstanceByZone {
		min := math.MaxFloat64
		max := math.SmallestNonzeroFloat64
		fmt.Printf("Distribution of timeseries in zone %s\n", zone)
		for instance, ownerhsip := range ownershipByInstance {
			min = math.Min(min, ownerhsip)
			max = math.Max(max, ownerhsip)
			if verbose {
				fmt.Printf("\tinstance: %30s, ownership: %15.3f\n", instance, ownerhsip)
			}
		}
		spread := (max - min) * 100.00 / max
		fmt.Printf("min ownership: %15.3f, max ownership: %15.3f, spread: %6.3f%%\n\n", min, max, spread)
	}
}
