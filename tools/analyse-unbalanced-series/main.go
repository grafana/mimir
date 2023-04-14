package main

import (
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

	ringDesc := generateRingWithPerfectlySpacedTokens(numIngesters, numZones, numTokensPerIngester, math.MaxUint32, time.Now())
	analyseRing("simulated-ingesters-ring-with-perfectly-spaced-tokens", ringDesc, logger)

	ringDesc = generateRingWithRandomTokens(numIngesters, numZones, numTokensPerIngester, math.MaxUint32, time.Now())
	analyseRing("simulated-ingesters-ring-with-random-tokens", ringDesc, logger)

	analyzeRingOwnershipSpreadOnDifferentTokensPerIngester(numIngesters, numZones, []int{64, 128, 256, 512, 1024, 2048, 4096, 8192}, logger)
}
