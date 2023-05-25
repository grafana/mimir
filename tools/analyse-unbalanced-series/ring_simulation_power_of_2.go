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

func generateAndAnalyzeRing(analysisName string, timeseriesCount, instancesPerZone int, zones []string, maxTokenValue uint32, tokenDistributor eventokendistributor.TokenDistributorInterface, logger log.Logger) {
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
		panic(err)
	}
	generateCSV(analysisName, statistics, logger)
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

func generateAndAnalyzePowerOf2Ring(timeseriesCount, tokensPerInstance, instancesPerZone int, zones []string, maxTokenValue uint32, logger log.Logger) {
	level.Info(logger).Log("test", "Analysis of a distribution of timeseries in a powerOf2 ring", "timeseriesCount", timeseriesCount, "tokensPerInstance", tokensPerInstance, "instancesPerZone", instancesPerZone, "phase", "Generating CSV files", "status", "start")
	tokenDistributor := eventokendistributor.NewPower2TokenDistributor(zones, tokensPerInstance, maxTokenValue)
	analysisName := fmt.Sprintf("timeseries-in-powerOf2Ring-tokensPerInstance-%d-instancesPerZone-%d", tokensPerInstance, instancesPerZone)
	generateAndAnalyzeRing(analysisName, timeseriesCount, instancesPerZone, zones, maxTokenValue, tokenDistributor, logger)
	level.Info(logger).Log("test", "Analysis of a distribution of timeseries in a powerOf2 ring", "timeseriesCount", timeseriesCount, "tokensPerInstance", tokensPerInstance, "instancesPerZone", instancesPerZone, "phase", "Generating CSV files", "status", "end")
}

func generateAndAnalyzeRandomTokenRing(timeseriesCount, tokensPerInstance, instancesPerZone int, zones []string, maxTokenValue uint32, logger log.Logger) {
	level.Info(logger).Log("test", "Analysis of a distribution of in a randomToken ring", "timeseriesCount", timeseriesCount, "tokensPerInstance", tokensPerInstance, "instancesPerZone", instancesPerZone, "phase", "Generating CSV files", "status", "start")
	tokenDistributor := eventokendistributor.NewRandomTokenDistributor(zones, tokensPerInstance, maxTokenValue)
	analysisName := fmt.Sprintf("timeseries-in-randomTokenRing-tokensPerInstance-%d-instancesPerZone-%d-", tokensPerInstance, instancesPerZone)
	generateAndAnalyzeRing(analysisName, timeseriesCount, instancesPerZone, zones, maxTokenValue, tokenDistributor, logger)
	level.Info(logger).Log("test", "Analysis of a distribution of timeseries in a randomToken ring", "timeseriesCount", timeseriesCount, "tokensPerInstance", tokensPerInstance, "instancesPerZone", instancesPerZone, "phase", "Generating CSV files", "status", "end")
}

func main() {
	logger := log.NewLogfmtLogger(os.Stdout)
	zones := []string{"zone-a", "zone-b", "zone-c"}
	tokensPerInstance := 512
	instancesPerZoneScenarios := []int{1, 2, 3, 4, 5, 15, 16, 17, 63, 64, 65, 127, 128, 129}
	timeseriesCountScenarios := []int{10_000_000}
	for _, timeseriesCount := range timeseriesCountScenarios {
		for _, instancesPerZone := range instancesPerZoneScenarios {
			generateAndAnalyzePowerOf2Ring(timeseriesCount, tokensPerInstance, instancesPerZone, zones, math.MaxUint32, logger)
			fmt.Println()
			generateAndAnalyzeRandomTokenRing(timeseriesCount, tokensPerInstance, instancesPerZone, zones, math.MaxUint32, logger)
			fmt.Println(strings.Repeat("-", 50))
		}
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
