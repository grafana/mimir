package main

import (
	crypto_rand "crypto/rand"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"sort"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/ring"
)

type tokensGenerator func(instanceID int, ringDesc *ring.Desc) []uint32

func generateRingWithPerfectlySpacedTokens(numInstances, numZones, numTokensPerInstance int, maxTokenValue uint32, now time.Time) *ring.Desc {
	return generateRingWithTokensGenerator(numInstances, numZones, now, func(instanceID int, ringDesc *ring.Desc) []uint32 {
		tokens := make([]uint32, 0, numTokensPerInstance)

		tokensOffset := maxTokenValue / uint32(numTokensPerInstance)
		startToken := (tokensOffset / uint32(numInstances)) * uint32(instanceID)
		for t := uint32(0); t < uint32(numTokensPerInstance); t++ {
			tokens = append(tokens, startToken+(t*tokensOffset))
		}

		return tokens
	})
}

func generateRingWithRandomTokens(numInstances, numZones, numTokensPerInstance int, maxTokenValue uint32, now time.Time) *ring.Desc {
	return generateRingWithTokensGenerator(numInstances, numZones, now, func(instanceID int, ringDesc *ring.Desc) []uint32 {
		return ring.GenerateTokens(numTokensPerInstance, nil)
	})
}

func generateRingWithCryptoRandomTokens(numInstances, numZones, numTokensPerInstance int, maxTokenValue uint32, now time.Time) *ring.Desc {
	max := big.NewInt(int64(maxTokenValue))

	return generateRingWithTokensGenerator(numInstances, numZones, now, func(instanceID int, ringDesc *ring.Desc) []uint32 {
		tokens := make([]uint32, 0, numTokensPerInstance)

		for i := uint32(0); i < uint32(numTokensPerInstance); i++ {
			value, _ := crypto_rand.Int(crypto_rand.Reader, max)
			tokens = append(tokens, uint32(value.Int64()))
		}

		// Ensure returned tokens are sorted.
		sort.Slice(tokens, func(i, j int) bool {
			return tokens[i] < tokens[j]
		})

		return tokens
	})
}

// TODO IGNORE THIS IMPLEMENTATION - IT'S WRONG
// TODO This implementation should also be zone-aware.
func generateRingWithBucketedFillerTokens(numInstances, numZones, numTokensPerInstance int, maxTokenValue uint32, now time.Time) *ring.Desc {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	return generateRingWithTokensGenerator(numInstances, numZones, now, func(instanceID int, ringDesc *ring.Desc) []uint32 {
		// If the ring is empty, generate random bucketed tokens.
		if len(ringDesc.Ingesters) == 0 {
			return randomBucketedTokens(numTokensPerInstance, maxTokenValue, r)
		}

		ringTokens := ringDesc.GetTokens()
		myTokens := make([]uint32, 0, numTokensPerInstance)

		// TODO DEBUG
		//fmt.Println("Generating tokens for", instanceID)
		//fmt.Println("  ring tokens:", ringTokens)

		// Analyze each bucket.
		bucketOffset := maxTokenValue / uint32(numTokensPerInstance)

		nextBucketTokenOffset := 0

		for i := uint32(0); i < uint32(numTokensPerInstance); i++ {
			//bucketStart := i * bucketOffset
			bucketEnd := (i + 1) * bucketOffset

			// If it's the last bucket, make sure we generate values up to the max value.
			if i == uint32(numTokensPerInstance)-1 {
				bucketEnd = maxTokenValue
			}

			// Find the largest gap.
			largestGapLength := uint32(0)
			largestGapStart := uint32(0)

			for t := nextBucketTokenOffset; t < len(ringTokens); t++ {
				//if ringTokens[t] < bucketStart {
				//	continue
				//}

				currToken := ringTokens[t]
				prevToken := uint32(0)
				//nextToken := uint32(math.MaxUint32)
				if t > 0 {
					prevToken = ringTokens[t-1]
				}
				//if t < len(ringTokens)-1 {
				//	nextToken = ringTokens[t+1]
				//}

				// If the current token is after the current bucket end
				// TODO continue description
				if currToken > bucketEnd && (currToken-bucketEnd) > (bucketEnd-prevToken) && largestGapLength > 0 {
					break
				}

				gapStart := prevToken //util_math.Max(bucketStart, prevToken)
				gapEnd := currToken   // util_math.Min(bucketEnd, currToken)
				gapLength := gapEnd - gapStart

				// TODO DEBUG
				//fmt.Println("  range:", prevToken, "-", currToken, "length:", gapLength)

				if gapLength > largestGapLength {
					largestGapStart = gapStart
					largestGapLength = gapLength
				}

				nextBucketTokenOffset = t + 1

				if ringTokens[t] >= bucketEnd {
					// Since tokens are sorted, if we're here it means we reached the end of the bucket.
					break
				}
			}

			// Take in account the range starting from the last token.
			if bucketEnd == maxTokenValue {
				gapStart := ringTokens[len(ringTokens)-1]
				gapEnd := maxTokenValue
				gapLength := gapEnd - gapStart

				if gapLength > largestGapLength {
					largestGapStart = gapStart
					largestGapLength = gapLength
				}
			}

			// Now that we've found the largest gap, we fill it with a token in the middle.
			newToken := largestGapStart + (largestGapLength / 2)
			myTokens = append(myTokens, newToken)

			// TODO DEBUG
			//fmt.Println("  => new token:", newToken)
			//fmt.Print("\n")
		}

		// Ensure returned tokens are sorted.
		sort.Slice(myTokens, func(i, j int) bool {
			return myTokens[i] < myTokens[j]
		})

		return myTokens
	})
}

func generateRingWithBucketedRandomTokens(numInstances, numZones, numTokensPerInstance int, maxTokenValue uint32, now time.Time) *ring.Desc {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	return generateRingWithTokensGenerator(numInstances, numZones, now, func(instanceID int, ringDesc *ring.Desc) []uint32 {
		return randomBucketedTokens(numTokensPerInstance, maxTokenValue, r)
	})
}

func randomBucketedTokens(numTokensPerInstance int, maxTokenValue uint32, r *rand.Rand) []uint32 {
	tokens := make([]uint32, 0, numTokensPerInstance)
	offset := maxTokenValue / uint32(numTokensPerInstance)

	for i := uint32(0); i < uint32(numTokensPerInstance); i++ {
		start := i * offset
		end := (i + 1) * offset

		// If it's the last bucket, make sure we generate values up to the max value.
		if i == uint32(numTokensPerInstance)-1 {
			end = maxTokenValue
		}

		value := start + uint32(r.Intn(int(end-start)))
		tokens = append(tokens, value)
	}

	// No need to sort tokens because they're already guaranteed to be sorted.
	return tokens
}

func generateRingWithTokensGenerator(numInstances, numZones int, now time.Time, generateTokens tokensGenerator) *ring.Desc {
	desc := &ring.Desc{Ingesters: map[string]ring.InstanceDesc{}}

	for i := 0; i < numInstances; i++ {
		// Get the zone letter starting from "a".
		zoneID := "zone-" + string(rune('a'+(i%numZones)))
		instanceID := fmt.Sprintf("ingester-%s-%d", zoneID, i/numZones)

		desc.Ingesters[instanceID] = ring.InstanceDesc{
			Addr:                instanceID,
			Timestamp:           now.Unix(),
			State:               ring.ACTIVE,
			Tokens:              generateTokens(i, desc),
			Zone:                zoneID,
			RegisteredTimestamp: now.Unix(),
		}
	}

	return desc
}

func analyzeRingOwnershipSpreadOnDifferentTokensPerInstance(numInstances, numZones int, numTokensPerInstanceScenarios []int, logger log.Logger) error {
	const (
		numIterations = 10
	)

	var (
		allIterationsResults [][]float64
	)

	level.Info(logger).Log("msg", "Analyzing ring tokens ownership spread on different tokens per instance")

	for i := 0; i < numIterations; i++ {
		iterationResults := make([]float64, 0, len(numTokensPerInstanceScenarios))

		for _, numTokensPerInstance := range numTokensPerInstanceScenarios {
			level.Info(logger).Log("msg", "Analysis iteration", "iteration", i, "num tokens per instance", numTokensPerInstance)

			ringDesc := generateRingWithRandomTokens(numInstances, numZones, numTokensPerInstance, math.MaxUint32, time.Now())
			ringTokens := ringDesc.GetTokens()
			ringInstanceByToken := getRingInstanceByToken(ringDesc)

			_, _, spread := getRegisteredTokensOwnershipStatistics(ringTokens, ringInstanceByToken)
			iterationResults = append(iterationResults, spread)
		}

		allIterationsResults = append(allIterationsResults, iterationResults)
	}

	// Generate CSV header.
	var csvHeader []string
	for _, numTokensPerInstance := range numTokensPerInstanceScenarios {
		csvHeader = append(csvHeader, fmt.Sprintf("%d tokens per instance", numTokensPerInstance))
	}

	// Write result to CSV.
	w := newCSVWriter[[]float64]()
	w.setHeader(csvHeader)
	w.setData(allIterationsResults, func(entry []float64) []string {
		formatted := make([]string, 0, len(entry))
		for _, value := range entry {
			formatted = append(formatted, fmt.Sprintf("%.3f", value))
		}

		return formatted
	})
	if err := w.writeCSV(fmt.Sprintf("simulated-instances-ring-ownership-spread-on-different-tokens-per-instance.csv")); err != nil {
		return err
	}

	return nil
}
