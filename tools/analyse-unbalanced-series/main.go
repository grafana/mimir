package main

import (
	"fmt"
	"math"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

func main() {
	mimirDistributorURL := "http://localhost:8080"

	logger := log.NewLogfmtLogger(os.Stdout)

	// Build dataset.
	level.Info(logger).Log("msg", "Building dataset")
	ringStatus, err := getRingStatus(mimirDistributorURL)
	if err != nil {
		level.Error(logger).Log("msg", "Failed to build dataset", "err", err)
		os.Exit(1)
	}
	level.Info(logger).Log("msg", "Successfully built dataset")

	// Run analysis on real ring.
	analyseRing("real-ingesters-ring", ringStatus.toRingModel(), logger)

	// Run analysis on simulated ring.
	const (
		numIngesters         = 444
		numZones             = 3
		numTokensPerIngester = 512
	)

	//ringDesc := generateRingWithPerfectlySpacedTokens(numIngesters, numZones, numTokensPerIngester, math.MaxUint32, time.Now())
	//analyseRing("simulated-ingesters-ring-with-perfectly-spaced-tokens", ringDesc, logger)

	//ringDesc = generateRingWithRandomTokens(numIngesters, numZones, numTokensPerIngester, math.MaxUint32, time.Now())
	//analyseRing("simulated-ingesters-ring-with-random-tokens", ringDesc, logger)

	//analyzeRingOwnershipSpreadOnDifferentTokensPerIngester(numIngesters, numZones, []int{64, 128, 256, 512, 1024, 2048, 4096, 8192}, logger)

	analysesPerZone(63, numZones, 64, logger)

}

func analysesPerZone(numIngesters, numZones, numTokensPerIngester int, logger log.Logger) {
	// analyze ring creation by optimizing token distribution per-zone with zone-awareness disabled by considering different numbers of tokens per ingester
	analyzeRingOwnershipSpreadOnDifferentTokensPerZone(numIngesters, 1, []int{4, 16, 64, 128, 256, 512}, logger)

	// analyze ring creation by optimizing token distribution per-zone with zone-awareness enabled by considering different numbers of tokens per ingester
	analyzeRingOwnershipSpreadOnDifferentTokensPerZone(numIngesters, numZones, []int{4, 16, 64, 128, 256, 512}, logger)

	// simulate traffic in a ring created by optimizing token distribution per zone with zone-awareness disabled by considering 512 ingesters and 64 tokens per ingester
	//analysesPerZoneSingleZone(numIngesters, numTokensPerIngester, logger)

	// simulate traffic in a ring created by optimizing token distribution per zone with zone-awareness disabled by considering 512 ingesters and 64 tokens per ingester
	analysesPerZoneMultiZone(numIngesters, numZones, numTokensPerIngester, logger)
}

func analysesPerZoneSingleZone(numIngesters, numTokensPerIngester int, logger log.Logger) error {
	var (
		ringDesc, _         = generateRingWithEvenlyDistributedTokensPerZone(numIngesters, 1, numTokensPerIngester, math.MaxUint32, time.Now())
		ringTokens          = ringDesc.GetTokens()
		ringInstanceByToken = getRingInstanceByToken(ringDesc)
	)

	// Analyze owned tokens.
	if err := analyzeActualTokensOwnership(fmt.Sprintf("simulated-ingesters-ring-with-evenly-distributed-tokens-per-zone-analysis-ingesters-%d-tokens-per-ingester-%d-rf-%d-zone-aware-%s", numIngesters, numTokensPerIngester, 1, "disabled"), ringDesc, ringTokens, ringInstanceByToken, 1, false, logger); err != nil {
		return err
	}
	if err := analyzeActualTokensOwnership(fmt.Sprintf("simulated-ingesters-ring-with-evenly-distributed-tokens-per-zone-analysis-ingesters-%d-tokens-per-ingester-%d-rf-%d-zone-aware-%s", numIngesters, numTokensPerIngester, 3, "disabled"), ringDesc, ringTokens, ringInstanceByToken, 3, false, logger); err != nil {
		return err
	}

	// Analyze the registered tokens percentage (no replication factor or zone-aware replication
	// taken in account).
	if err := analyzeRegisteredTokensOwnership(fmt.Sprintf("simulated-ingesters-ring-with-evenly-distributed-tokens-per-zone-ingesters-%d-tokens-per-ingester-%d-zone-aware-disabled", numIngesters, numTokensPerIngester), ringTokens, ringInstanceByToken, logger); err != nil {
		return err
	}
	return nil
}

func analysesPerZoneMultiZone(numIngesters, numZones, numTokensPerIngester int, logger log.Logger) error {

	var (
		ringDesc, _         = generateRingWithEvenlyDistributedTokensPerZone(numIngesters, 1, numTokensPerIngester, math.MaxUint32, time.Now())
		ringTokens          = ringDesc.GetTokens()
		ringInstanceByToken = getRingInstanceByToken(ringDesc)
	)

	if err := analyzeActualTokensOwnership(fmt.Sprintf("simulated-ingesters-ring-with-evenly-distributed-tokens-per-zone-analysis-ingesters-%d-tokens-per-ingester-%d-rf-%d-zone-aware-%s", numIngesters, numTokensPerIngester, numZones, "enabled"), ringDesc, ringTokens, ringInstanceByToken, numZones, true, logger); err != nil {
		return err
	}

	// Analyze the registered tokens percentage (no replication factor or zone-aware replication
	// taken in account).
	if err := analyzeRegisteredTokensOwnership(fmt.Sprintf("simulated-ingesters-ring-with-evenly-distributed-tokens-per-zone-ingesters-%d-tokens-per-ingester-%d-zone-aware-enabled", numIngesters, numTokensPerIngester), ringTokens, ringInstanceByToken, logger); err != nil {
		return err
	}
	return nil
}
