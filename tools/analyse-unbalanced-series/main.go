package main

import (
	"math"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

func main() {
	const (
		mimirRingPageURL    = "http://localhost:8080/store-gateway/ring"
		bucketIndexFilepath = "229572-bucket-index.json.gz"
	)

	logger := log.NewLogfmtLogger(os.Stdout)

	// Build dataset.
	level.Info(logger).Log("msg", "Building dataset")
	ringStatus, err := getRingStatus(mimirRingPageURL)
	if err != nil {
		level.Error(logger).Log("msg", "Failed to build dataset", "err", err)
		os.Exit(1)
	}
	level.Info(logger).Log("msg", "Successfully built dataset")

	// Run analysis on real ring.
	if false {
		analyseRing("real-instances-ring", ringStatus.toRingModel(), logger)
	}

	// Run analysis on simulated ring.
	if false {
		const (
			numInstances         = 444
			numZones             = 3
			numTokensPerInstance = 512
		)

		ringDesc := generateRingWithPerfectlySpacedTokens(numInstances, numZones, numTokensPerInstance, math.MaxUint32, time.Now())
		analyseRing("simulated-instances-ring-with-perfectly-spaced-tokens", ringDesc, logger)

		ringDesc = generateRingWithRandomTokens(numInstances, numZones, numTokensPerInstance, math.MaxUint32, time.Now())
		analyseRing("simulated-instances-ring-with-random-tokens", ringDesc, logger)

		analyzeRingOwnershipSpreadOnDifferentTokensPerInstance(numInstances, numZones, []int{64, 128, 256, 512, 1024, 2048, 4096, 8192}, logger)
	}

	// Store-gateway ring analysis.
	if true {
		bucketIndex, err := readBucketIndex(bucketIndexFilepath, logger)
		if err != nil {
			level.Error(logger).Log("msg", "Failed to load bucket index", "filepath", bucketIndexFilepath, "err", err)
		}

		analyzeStoreGatewayActualBlocksOwnership(bucketIndex.Blocks, ringStatus.toRingModel(), logger)
	}
}
