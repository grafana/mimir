package main

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
	"strconv"
	"time"

	"github.com/cespare/xxhash"
	"github.com/dgryski/go-shardedkv/choosers/jump"
	"github.com/dgryski/go-shardedkv/choosers/ketama"
	"github.com/dgryski/go-shardedkv/choosers/maglev"
	"github.com/dgryski/go-shardedkv/choosers/mpc"
	"github.com/dgryski/go-shardedkv/choosers/rendezvous"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/ring"
)

type chooser interface {
	SetBuckets(buckets []string) error
	Choose(key string) string
}

type algorithmTestConfig struct {
	name    string
	chooser chooser
}

type algorithmTestScenario struct {
	numInstances    int
	numShardedItems int
}

type algorithmTestResult struct {
	algorithm               algorithmTestConfig
	varianceForEachScenario []float64
}

func analyzeAndCompareHashingAlgorithms(logger log.Logger) error {
	algorithms := []algorithmTestConfig{
		{
			name:    "jump",
			chooser: jump.New(xxhash.Sum64),
		}, {
			name:    "maglev",
			chooser: maglev.New(),
		}, {
			name:    "ketama",
			chooser: ketama.New(),
		}, {
			name: "mpc-with-k-21",
			chooser: func() chooser {
				seeds := [2]uint64{1, 2}
				k := 21

				return mpc.New(func(b []byte, seed uint64) uint64 {
					// Not sure this is a valid way to apply the seed.
					rawSeed := make([]byte, 8)
					binary.LittleEndian.PutUint64(rawSeed, seed)

					dataWithSeed := append(b, rawSeed...)
					return xxhash.Sum64(dataWithSeed)
				}, seeds, k)
			}(),
		}, {
			name:    "rendezvous",
			chooser: rendezvous.New(),
		}, {
			name:    "mimir-ring-with-128-tokens",
			chooser: newMimirRingChooser(128),
		}, {
			name:    "mimir-ring-with-256-tokens",
			chooser: newMimirRingChooser(256),
		}, {
			name:    "mimir-ring-with-512-tokens",
			chooser: newMimirRingChooser(512),
		}, {
			name:    "mimir-ring-with-1024-tokens",
			chooser: newMimirRingChooser(1024),
		}, {
			name:    "mimir-ring-with-2048-tokens",
			chooser: newMimirRingChooser(2048),
		},
	}

	// Generate the scenarios used to test each algorithm.
	var scenarios []algorithmTestScenario
	for _, numInstances := range []int{3, 30, 300} {
		for _, numShardedItems := range []int{1000, 100_00, 100_000, 1_000_000} {
			scenarios = append(scenarios, algorithmTestScenario{
				numInstances:    numInstances,
				numShardedItems: numShardedItems,
			})
		}
	}

	// Test each algorithm.
	// TODO run each algorithm N times
	results := make([]algorithmTestResult, 0, len(algorithms))
	for _, algorithm := range algorithms {
		varianceForEachScenario := make([]float64, 0, len(scenarios))

		for _, scenario := range scenarios {
			level.Info(logger).Log("msg", "Analyzing hashing algorithm", "algorithm", algorithm.name, "instances", scenario.numInstances, "items", scenario.numShardedItems)

			variance := analyzeHashingAlgorithm(algorithm, scenario.numInstances, scenario.numShardedItems)
			varianceForEachScenario = append(varianceForEachScenario, variance)
		}

		results = append(results, algorithmTestResult{
			algorithm:               algorithm,
			varianceForEachScenario: varianceForEachScenario,
		})
	}

	// Prepare CSV header.
	header := []string{"Algorithm"}
	for _, scenario := range scenarios {
		header = append(header, fmt.Sprintf("Variance %% - instances = %d sharded items = %d", scenario.numInstances, scenario.numShardedItems))
	}

	// Write result to CSV.
	w := newCSVWriter[algorithmTestResult]()
	w.setHeader(header)
	w.setData(results, func(entry algorithmTestResult) []string {
		values := []string{entry.algorithm.name}

		for _, variance := range entry.varianceForEachScenario {
			values = append(values, fmt.Sprintf("%.3f", variance))
		}

		return values
	})
	if err := w.writeCSV("compare-hashing-algorithms.csv"); err != nil {
		return err
	}

	return nil
}

func analyzeHashingAlgorithm(algorithm algorithmTestConfig, numInstances, numShardedItems int) (variance float64) {
	// Ensure each test run on the same set of (random) items.
	rand.Seed(0)

	// Generate the instance IDs.
	instanceIDs := make([]string, 0, numInstances)
	ownedItemsPerInstance := make(map[string]int, numInstances)
	for i := 0; i < numInstances; i++ {
		id := fmt.Sprintf("instance-%d", i)

		instanceIDs = append(instanceIDs, id)
		ownedItemsPerInstance[id] = 0
	}

	c := algorithm.chooser
	c.SetBuckets(instanceIDs)

	// Run the simulation.
	for i := 0; i < numShardedItems; i++ {
		instanceID := c.Choose(strconv.Itoa(rand.Int()))
		ownedItemsPerInstance[instanceID]++
	}

	// Compute statistics.
	ownershipDistribution := make([]float64, 0, len(ownedItemsPerInstance))
	minOwnedItems := math.MaxInt
	maxOwnedItems := 0

	for _, num := range ownedItemsPerInstance {
		ownershipDistribution = append(ownershipDistribution, float64(num))

		if num < minOwnedItems {
			minOwnedItems = num
		}
		if num > maxOwnedItems {
			maxOwnedItems = num
		}
	}

	variance = ((float64(maxOwnedItems) - float64(minOwnedItems)) / float64(maxOwnedItems)) * 100

	//mean, _ := stats.Mean(ownershipDistribution)
	//peakToMeanRatio := float64(maxOwnedItems) / mean

	return
}

type mimirRingChooser struct {
	numTokensPerInstance int
	ringDesc             *ring.Desc
	ringTokens           []uint32
	ringInstanceByToken  map[uint32]instanceInfo

	bufDescs [ring.GetBufferSize]string
	bufHosts [ring.GetBufferSize]string
	bufZones [ring.GetBufferSize]string
}

func newMimirRingChooser(numTokensPerInstance int) *mimirRingChooser {
	return &mimirRingChooser{
		numTokensPerInstance: numTokensPerInstance,
	}
}

func (c *mimirRingChooser) SetBuckets(buckets []string) error {
	// Ensure each test run on the same set of (random) tokens.
	rand.Seed(1)

	c.ringDesc = &ring.Desc{Ingesters: map[string]ring.InstanceDesc{}}

	for _, instanceID := range buckets {
		c.ringDesc.Ingesters[instanceID] = ring.InstanceDesc{
			Addr:                instanceID,
			Timestamp:           time.Now().Unix(),
			State:               ring.ACTIVE,
			Tokens:              ring.GenerateTokens(c.numTokensPerInstance, nil),
			Zone:                "",
			RegisteredTimestamp: time.Now().Unix(),
		}
	}

	c.ringTokens = c.ringDesc.GetTokens()
	c.ringInstanceByToken = getRingInstanceByToken(c.ringDesc)

	return nil
}

func (c *mimirRingChooser) Choose(key string) string {
	const (
		replicationFactor    = 1
		zoneAwarenessEnabled = false
	)

	ids, _ := ringGet(mimirHash(key), c.ringDesc, c.ringTokens, c.ringInstanceByToken, ring.WriteNoExtend, replicationFactor, zoneAwarenessEnabled, c.bufDescs[:0], c.bufHosts[:0], c.bufZones[:0])
	return ids[0]
}

func mimirHash(key string) uint32 {
	// Hash the key.
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(key))
	hash := hasher.Sum32()

	return hash
}
