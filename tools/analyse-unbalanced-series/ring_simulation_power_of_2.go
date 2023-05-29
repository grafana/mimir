package main

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/ring"
	"golang.org/x/exp/slices"

	"github.com/grafana/mimir/tools/analyse-unbalanced-series/eventokendistributor"
)

type instanceOwnership struct {
	instanceID string
	percentage float64
}

func getTokensOwnership(r *eventokendistributor.Ring, timeseriesCount int) (*eventokendistributor.TimeseriesDistributionStatistics, error) {
	var (
		bufDescs [ring.GetBufferSize]eventokendistributor.InstanceInfo
		bufHosts [ring.GetBufferSize]string
		bufZones [ring.GetBufferSize]string
	)
	statistics := eventokendistributor.NewTimeseriesDistributionStatistics(r)
	ringTokens := r.GetTokens()
	instanceByToken := r.GetInstanceByToken()

	for i := 0; i < timeseriesCount; i++ {
		token := rand.Uint32()
		//fmt.Printf("Processing %dth timeseries with token %d...\n", i+1, token)
		replicationSet, err := r.GetReplicationSet(token, ringTokens, instanceByToken, ring.WriteNoExtend, bufDescs[:0], bufHosts[:0], bufZones[:0])
		if err != nil {
			return nil, err
		}
		statistics.AddOwnership(replicationSet)
		//fmt.Printf("%dth timeseries with token %d processed\n", i+1, token)
	}
	statistics.Print(false)
	return statistics, nil
}

func generateAndAnalyzeRing(analysisName string, timeseriesCount, instancesPerZone int, zones []string, maxTokenValue uint32, tokenDistributor eventokendistributor.TokenDistributorInterface) (*eventokendistributor.TimeseriesDistributionStatistics, error) {
	ringDesc := &ring.Desc{}
	r := eventokendistributor.NewRing(ringDesc, zones, maxTokenValue, tokenDistributor)
	for i := 0; i < instancesPerZone; i++ {
		for _, zone := range zones {
			err := r.AddInstance(i, zone)
			if err != nil {
				panic(err)
			}
		}
	}

	statistics, err := getTokensOwnership(r, timeseriesCount)
	if err != nil {
		return nil, err
	}
	generateCombinedCSV(analysisName, instancesPerZone, timeseriesCount, statistics)
	//generateCSV(analysisName, statistics, logger)
	return statistics, nil
}

func generateCSV(analysisName string, statistics *eventokendistributor.TimeseriesDistributionStatistics, logger log.Logger) error {
	for zone, ownershipByInstance := range statistics.OwnershipByInstanceByZone {
		//Generate CSV header.
		csvHeader := []string{"instance", "ownership"}
		// Write result to CSV.
		w := newCSVWriter[[]string]()
		w.setHeader(csvHeader)
		result := make([][]string, 0, len(ownershipByInstance))
		instances := make([]string, 0, len(ownershipByInstance))
		for instance := range ownershipByInstance {
			instances = append(instances, instance)
		}
		slices.Sort(instances)
		for _, instance := range instances {
			result = append(result, []string{instance, fmt.Sprintf("%f", ownershipByInstance[instance])})
		}
		w.setData(result, func(entry []string) []string {
			return entry
		})

		output := fmt.Sprintf("%s-%s-%s", analysisName, zone, time.Now().Local())
		filename := filepath.Join("tools", "analyse-unbalanced-series", "eventokendistributor", output)
		if err := w.writeCSV(filename); err != nil {
			return err
		}
	}
	return nil
}

func generateCombinedCSV(analysisName string, instancesPerZone int, timeseriesCount int, statistics *eventokendistributor.TimeseriesDistributionStatistics) error {
	zones := make([]string, 0, len(statistics.OwnershipByInstanceByZone))
	for zone := range statistics.OwnershipByInstanceByZone {
		zones = append(zones, zone)
	}
	slices.Sort(zones)
	csvHeader := make([]string, 0, 2+len(zones))
	csvHeader = append(csvHeader, "instance")
	csvHeader = append(csvHeader, zones...)
	csvHeader = append(csvHeader, "optimal ownership")
	result := make([][]string, 0, instancesPerZone)
	for i := 0; i < instancesPerZone; i++ {
		ownerships := make([]string, 0, 2+len(zones))
		ownerships = append(ownerships, fmt.Sprintf("instance-%d", i))
		result = append(result, ownerships)
	}
	optimalOwnership := float64(timeseriesCount) / float64(instancesPerZone)
	for z, zone := range zones {
		ownershipByInstance := statistics.OwnershipByInstanceByZone[zone]
		i := 0
		instances := make([]string, 0, instancesPerZone)
		for instance := range ownershipByInstance {
			instances = append(instances, instance)
		}
		slices.Sort(instances)
		for _, instance := range instances {
			ownerships := result[i]
			ownerships = append(ownerships, fmt.Sprintf("%f", ownershipByInstance[instance]))
			if z == len(zones)-1 {

				ownerships = append(ownerships, fmt.Sprintf("%15.3f", optimalOwnership))
			}
			result[i] = ownerships
			i++
		}
	}

	w := newCSVWriter[[]string]()
	w.setHeader(csvHeader)
	w.setData(result, func(entry []string) []string {
		return entry
	})
	output := fmt.Sprintf("%s-%s", analysisName, time.Now().Local())
	filename := filepath.Join("tools", "analyse-unbalanced-series", "eventokendistributor", "results", output)
	if err := w.writeCSV(filename); err != nil {
		return err
	}
	return nil
}

func generateSpreadCSV(analysisName string, spreadByZoneByInstancePerZone map[int]map[string]float64) error {
	instancesPerZoneScenarios := make([]int, 0, len(spreadByZoneByInstancePerZone))
	for instancesPerZone := range spreadByZoneByInstancePerZone {
		instancesPerZoneScenarios = append(instancesPerZoneScenarios, instancesPerZone)
	}
	slices.Sort(instancesPerZoneScenarios)
	var csvHeader []string
	result := make([][]string, 0, len(instancesPerZoneScenarios))
	for _, instancesPerZone := range instancesPerZoneScenarios {
		spreadByZone := spreadByZoneByInstancePerZone[instancesPerZone]
		zones := make([]string, 0, len(spreadByZone))
		if csvHeader == nil || len(csvHeader) == 0 {
			csvHeader = make([]string, 0, 1+len(spreadByZone))
			csvHeader = append(csvHeader, "instances per zone")
		}
		for zone := range spreadByZone {
			zones = append(zones, zone)
		}
		slices.Sort(zones)
		if len(csvHeader) == 1 {
			csvHeader = append(csvHeader, zones...)
		}
		spreads := make([]string, 0, 1+len(zones))
		spreads = append(spreads, fmt.Sprintf("%d", instancesPerZone))
		for _, zone := range zones {
			spreads = append(spreads, fmt.Sprintf("%6.3f%%", spreadByZone[zone]))
		}
		result = append(result, spreads)
	}

	w := newCSVWriter[[]string]()
	w.setHeader(csvHeader)
	w.setData(result, func(entry []string) []string {
		return entry
	})

	output := fmt.Sprintf("%s-%s", analysisName, time.Now().Local())
	filename := filepath.Join("tools", "analyse-unbalanced-series", "eventokendistributor", "results", output)
	if err := w.writeCSV(filename); err != nil {
		return err
	}
	return nil
}

func generateAndAnalyzePowerOf2Ring(timeseriesCount, tokensPerInstance, instancesPerZone int, zones []string, maxTokenValue uint32, logger log.Logger) (*eventokendistributor.TimeseriesDistributionStatistics, error) {
	level.Info(logger).Log("test", "Analysis of a distribution of timeseries in a powerOf2 ring", "timeseriesCount", timeseriesCount, "tokensPerInstance", tokensPerInstance, "instancesPerZone", instancesPerZone, "phase", "Generating CSV files", "status", "start")
	tokenDistributor := eventokendistributor.NewPower2TokenDistributor(zones, tokensPerInstance, maxTokenValue)
	analysisName := fmt.Sprintf("timeseries-in-powerOf2Ring-tokensPerInstance-%d-instancesPerZone-%d", tokensPerInstance, instancesPerZone)
	statistics, err := generateAndAnalyzeRing(analysisName, timeseriesCount, instancesPerZone, zones, maxTokenValue, tokenDistributor)
	if err != nil {
		return nil, err
	}
	level.Info(logger).Log("test", "Analysis of a distribution of timeseries in a powerOf2 ring", "timeseriesCount", timeseriesCount, "tokensPerInstance", tokensPerInstance, "instancesPerZone", instancesPerZone, "phase", "Generating CSV files", "status", "end")
	return statistics, nil
}

func generateAndAnalyzeRandomTokenRing(timeseriesCount, tokensPerInstance, instancesPerZone int, zones []string, maxTokenValue uint32, logger log.Logger) (*eventokendistributor.TimeseriesDistributionStatistics, error) {
	level.Info(logger).Log("test", "Analysis of a distribution of in a randomToken ring", "timeseriesCount", timeseriesCount, "tokensPerInstance", tokensPerInstance, "instancesPerZone", instancesPerZone, "phase", "Generating CSV files", "status", "start")
	tokenDistributor := eventokendistributor.NewRandomTokenDistributor(tokensPerInstance, maxTokenValue)
	analysisName := fmt.Sprintf("timeseries-in-randomTokenRing-tokensPerInstance-%d-instancesPerZone-%d-", tokensPerInstance, instancesPerZone)
	statistics, err := generateAndAnalyzeRing(analysisName, timeseriesCount, instancesPerZone, zones, maxTokenValue, tokenDistributor)
	if err != nil {
		return nil, err
	}
	level.Info(logger).Log("test", "Analysis of a distribution of timeseries in a randomToken ring", "timeseriesCount", timeseriesCount, "tokensPerInstance", tokensPerInstance, "instancesPerZone", instancesPerZone, "phase", "Generating CSV files", "status", "end")
	return statistics, nil
}

func generateAndAnalyzeRandomBucketTokenRing(timeseriesCount, tokensPerInstance, instancesPerZone int, zones []string, maxTokenValue uint32, logger log.Logger) (*eventokendistributor.TimeseriesDistributionStatistics, error) {
	level.Info(logger).Log("test", "Analysis of a distribution of in a randomBucketToken ring", "timeseriesCount", timeseriesCount, "tokensPerInstance", tokensPerInstance, "instancesPerZone", instancesPerZone, "phase", "Generating CSV files", "status", "start")
	tokenDistributor := eventokendistributor.NewRandomBucketTokenDistributor(tokensPerInstance, maxTokenValue)
	analysisName := fmt.Sprintf("timeseries-in-randomBucketTokenRing-tokensPerInstance-%d-instancesPerZone-%d-", tokensPerInstance, instancesPerZone)
	statistics, err := generateAndAnalyzeRing(analysisName, timeseriesCount, instancesPerZone, zones, maxTokenValue, tokenDistributor)
	if err != nil {
		return nil, err
	}
	level.Info(logger).Log("test", "Analysis of a distribution of timeseries in a randomBucketToken ring", "timeseriesCount", timeseriesCount, "tokensPerInstance", tokensPerInstance, "instancesPerZone", instancesPerZone, "phase", "Generating CSV files", "status", "end")
	return statistics, nil
}

func generateAndAnalyzeRandomModuloTokenRing(timeseriesCount, tokensPerInstance, instancesPerZone int, zones []string, maxTokenValue uint32, logger log.Logger) (*eventokendistributor.TimeseriesDistributionStatistics, error) {
	level.Info(logger).Log("test", "Analysis of a distribution of in a randomModuloToken ring", "timeseriesCount", timeseriesCount, "tokensPerInstance", tokensPerInstance, "instancesPerZone", instancesPerZone, "phase", "Generating CSV files", "status", "start")
	tokenDistributor := eventokendistributor.NewRandomModuloTokenDistributor(zones, tokensPerInstance, maxTokenValue)
	analysisName := fmt.Sprintf("timeseries-in-randomModuloTokenRing-tokensPerInstance-%d-instancesPerZone-%d-", tokensPerInstance, instancesPerZone)
	statistics, err := generateAndAnalyzeRing(analysisName, timeseriesCount, instancesPerZone, zones, maxTokenValue, tokenDistributor)
	if err != nil {
		return nil, err
	}
	level.Info(logger).Log("test", "Analysis of a distribution of timeseries in a randomModuloToken ring", "timeseriesCount", timeseriesCount, "tokensPerInstance", tokensPerInstance, "instancesPerZone", instancesPerZone, "phase", "Generating CSV files", "status", "end")
	return statistics, nil
}

func generateAndAnalyzeFixedTokenRing(timeseriesCount, tokensPerInstance, instancesPerZone int, zones []string, maxTokenValue uint32, logger log.Logger) (*eventokendistributor.TimeseriesDistributionStatistics, error) {
	level.Info(logger).Log("test", "Analysis of a distribution of in a fixedToken ring", "timeseriesCount", timeseriesCount, "tokensPerInstance", tokensPerInstance, "instancesPerZone", instancesPerZone, "phase", "Generating CSV files", "status", "start")
	tokenDistributor := eventokendistributor.Fixed{}
	analysisName := fmt.Sprintf("timeseries-in-fixedTokenRing-tokensPerInstance-%d-instancesPerZone-%d-", tokensPerInstance, instancesPerZone)
	statistics, err := generateAndAnalyzeRing(analysisName, timeseriesCount, instancesPerZone, zones, maxTokenValue, tokenDistributor)
	if err != nil {
		return nil, err
	}
	level.Info(logger).Log("test", "Analysis of a distribution of timeseries in a randomModuloToken ring", "timeseriesCount", timeseriesCount, "tokensPerInstance", tokensPerInstance, "instancesPerZone", instancesPerZone, "phase", "Generating CSV files", "status", "end")
	return statistics, nil
}

func main() {
	logger := log.NewLogfmtLogger(os.Stdout)
	zones := []string{"zone-a", "zone-b", "zone-c"}
	tokensPerInstance := 512
	instancesPerZoneScenarios := []int{1, 2, 3, 4, 5, 15, 16, 17, 63, 64, 65, 127, 128, 129} // []int{18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31} //
	timeseriesCountScenarios := []int{10_000_000}
	analyses := map[string]func(int, int, int, []string, uint32, log.Logger) (*eventokendistributor.TimeseriesDistributionStatistics, error){
		/*"powerOf2":     generateAndAnalyzePowerOf2Ring,
		"random":       generateAndAnalyzeRandomTokenRing,
		"randomBucket": generateAndAnalyzeRandomBucketTokenRing,
		"randomModulo": generateAndAnalyzeRandomModuloTokenRing,*/
		"fixed": generateAndAnalyzeFixedTokenRing,
	}
	for analysisName, analysis := range analyses {
		spreadsByZoneByInstancePerZone := make(map[int]map[string]float64, len(instancesPerZoneScenarios))
		for _, timeseriesCount := range timeseriesCountScenarios {
			for _, instancesPerZone := range instancesPerZoneScenarios {
				statistics, err := analysis(timeseriesCount, tokensPerInstance, instancesPerZone, zones, math.MaxUint32, logger)
				if err != nil {
					panic(err)
				}
				spreadByZone := statistics.GetSpreadByZone()
				spreadsByZoneByInstancePerZone[instancesPerZone] = spreadByZone
				fmt.Println()
			}
			generateSpreadCSV(fmt.Sprintf("spreads-%s-timeseries-%d", analysisName, timeseriesCount), spreadsByZoneByInstancePerZone)
		}
	}
}

func main7() {
	logger := log.NewLogfmtLogger(os.Stdout)
	zones := []string{"zone-a", "zone-b", "zone-c"}
	tokensPerInstance := 512
	instancesPerZoneScenarios := []int{1, 2, 3, 4, 5, 15, 16, 17, 63, 64, 65, 127, 128, 129} // []int{18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31} //
	timeseriesCountScenarios := []int{10_000_000}
	powerOf2SpreadsByZoneByInstancesPerZone := make(map[int]map[string]float64, len(instancesPerZoneScenarios))
	randomTokenSpreadsByZoneByInstancesPerZone := make(map[int]map[string]float64, len(instancesPerZoneScenarios))
	randomBucketTokenSpreadsByZoneByInstancesPerZone := make(map[int]map[string]float64, len(instancesPerZoneScenarios))
	for _, timeseriesCount := range timeseriesCountScenarios {
		for _, instancesPerZone := range instancesPerZoneScenarios {
			statistics, err := generateAndAnalyzePowerOf2Ring(timeseriesCount, tokensPerInstance, instancesPerZone, zones, math.MaxUint32, logger)
			if err != nil {
				panic(err)
			}
			spreadByZone := statistics.GetSpreadByZone()
			powerOf2SpreadsByZoneByInstancesPerZone[instancesPerZone] = spreadByZone
			fmt.Println()
			statistics, err = generateAndAnalyzeRandomTokenRing(timeseriesCount, tokensPerInstance, instancesPerZone, zones, math.MaxUint32, logger)
			if err != nil {
				panic(err)
			}
			spreadByZone = statistics.GetSpreadByZone()
			randomTokenSpreadsByZoneByInstancesPerZone[instancesPerZone] = spreadByZone
			fmt.Println()
			statistics, err = generateAndAnalyzeRandomBucketTokenRing(timeseriesCount, tokensPerInstance, instancesPerZone, zones, math.MaxUint32, logger)
			if err != nil {
				panic(err)
			}
			spreadByZone = statistics.GetSpreadByZone()
			randomBucketTokenSpreadsByZoneByInstancesPerZone[instancesPerZone] = spreadByZone
			fmt.Println(strings.Repeat("-", 50))
		}
		generateSpreadCSV(fmt.Sprintf("spread-by-instancePer-zone-powerOf2-timeseries-%d", timeseriesCount), powerOf2SpreadsByZoneByInstancesPerZone)
		generateSpreadCSV(fmt.Sprintf("spread-by-instancePer-zone-random-timeseries-%d", timeseriesCount), randomTokenSpreadsByZoneByInstancesPerZone)
		generateSpreadCSV(fmt.Sprintf("spread-by-instancePer-zone-randomBucket-timeseries-%d", timeseriesCount), randomBucketTokenSpreadsByZoneByInstancesPerZone)
	}
}

func main5() {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	instancesPerZone := 16
	tokensPerInstance := 512
	ringDesc := &ring.Desc{}
	td := eventokendistributor.NewPower2TokenDistributor(zones, tokensPerInstance, math.MaxUint32)
	r := eventokendistributor.NewRing(ringDesc, zones, math.MaxUint32, td)
	for i := 0; i < instancesPerZone; i++ {
		for _, zone := range zones {
			err := r.AddInstance(i, zone)
			if err != nil {
				panic(err)
			}
		}
	}

	getTokensOwnership(r, 1000000)
}
