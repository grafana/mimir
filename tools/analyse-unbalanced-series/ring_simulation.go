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

type tokensGenerator func(ingesterID int, ringDesc *ring.Desc) []uint32

func generateRingWithPerfectlySpacedTokens(numIngesters, numZones, numTokensPerIngester int, maxTokenValue uint32, now time.Time) *ring.Desc {
	return generateRingWithTokensGenerator(numIngesters, numZones, now, func(ingesterID int, ringDesc *ring.Desc) []uint32 {
		tokens := make([]uint32, 0, numTokensPerIngester)

		tokensOffset := maxTokenValue / uint32(numTokensPerIngester)
		startToken := (tokensOffset / uint32(numIngesters)) * uint32(ingesterID)
		for t := uint32(0); t < uint32(numTokensPerIngester); t++ {
			tokens = append(tokens, startToken+(t*tokensOffset))
		}

		return tokens
	})
}

func generateRingWithRandomTokens(numIngesters, numZones, numTokensPerIngester int, maxTokenValue uint32, now time.Time) *ring.Desc {
	return generateRingWithTokensGenerator(numIngesters, numZones, now, func(ingesterID int, ringDesc *ring.Desc) []uint32 {
		return ring.GenerateTokens(numTokensPerIngester, nil)
	})
}

func generateRingWithCryptoRandomTokens(numIngesters, numZones, numTokensPerIngester int, maxTokenValue uint32, now time.Time) *ring.Desc {
	max := big.NewInt(int64(maxTokenValue))

	return generateRingWithTokensGenerator(numIngesters, numZones, now, func(ingesterID int, ringDesc *ring.Desc) []uint32 {
		tokens := make([]uint32, 0, numTokensPerIngester)

		for i := uint32(0); i < uint32(numTokensPerIngester); i++ {
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
func generateRingWithBucketedFillerTokens(numIngesters, numZones, numTokensPerIngester int, maxTokenValue uint32, now time.Time) *ring.Desc {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	return generateRingWithTokensGenerator(numIngesters, numZones, now, func(ingesterID int, ringDesc *ring.Desc) []uint32 {
		// If the ring is empty, generate random bucketed tokens.
		if len(ringDesc.Ingesters) == 0 {
			return randomBucketedTokens(numTokensPerIngester, maxTokenValue, r)
		}

		ringTokens := ringDesc.GetTokens()
		myTokens := make([]uint32, 0, numTokensPerIngester)

		// TODO DEBUG
		//fmt.Println("Generating tokens for", ingesterID)
		//fmt.Println("  ring tokens:", ringTokens)

		// Analyze each bucket.
		bucketOffset := maxTokenValue / uint32(numTokensPerIngester)

		nextBucketTokenOffset := 0

		for i := uint32(0); i < uint32(numTokensPerIngester); i++ {
			//bucketStart := i * bucketOffset
			bucketEnd := (i + 1) * bucketOffset

			// If it's the last bucket, make sure we generate values up to the max value.
			if i == uint32(numTokensPerIngester)-1 {
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

func generateRingWithBucketedRandomTokens(numIngesters, numZones, numTokensPerIngester int, maxTokenValue uint32, now time.Time) *ring.Desc {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	return generateRingWithTokensGenerator(numIngesters, numZones, now, func(ingesterID int, ringDesc *ring.Desc) []uint32 {
		return randomBucketedTokens(numTokensPerIngester, maxTokenValue, r)
	})
}

func randomBucketedTokens(numTokensPerIngester int, maxTokenValue uint32, r *rand.Rand) []uint32 {
	tokens := make([]uint32, 0, numTokensPerIngester)
	offset := maxTokenValue / uint32(numTokensPerIngester)

	for i := uint32(0); i < uint32(numTokensPerIngester); i++ {
		start := i * offset
		end := (i + 1) * offset

		// If it's the last bucket, make sure we generate values up to the max value.
		if i == uint32(numTokensPerIngester)-1 {
			end = maxTokenValue
		}

		value := start + uint32(r.Intn(int(end-start)))
		tokens = append(tokens, value)
	}

	// No need to sort tokens because they're already guaranteed to be sorted.
	return tokens
}

func generateRingWithTokensGenerator(numIngesters, numZones int, now time.Time, generateTokens tokensGenerator) *ring.Desc {
	desc := &ring.Desc{Ingesters: map[string]ring.InstanceDesc{}}

	for i := 0; i < numIngesters; i++ {
		// Get the zone letter starting from "a".
		zoneID := "zone-" + string(rune('a'+(i%numZones)))
		ingesterID := fmt.Sprintf("ingester-%s-%d", zoneID, i/numZones)

		desc.Ingesters[ingesterID] = ring.InstanceDesc{
			Addr:                ingesterID,
			Timestamp:           now.Unix(),
			State:               ring.ACTIVE,
			Tokens:              generateTokens(i, desc),
			Zone:                zoneID,
			RegisteredTimestamp: now.Unix(),
		}
	}

	return desc
}

func analyzeRingOwnershipSpreadOnDifferentTokensPerIngester(numIngesters, numZones int, numTokensPerIngesterScenarios []int, logger log.Logger) error {
	const (
		numIterations = 10
	)

	var (
		allIterationsResults [][]float64
	)

	level.Info(logger).Log("msg", "Analyzing ring tokens ownership spread on different tokens per ingester")

	for i := 0; i < numIterations; i++ {
		iterationResults := make([]float64, 0, len(numTokensPerIngesterScenarios))

		for _, numTokensPerIngester := range numTokensPerIngesterScenarios {
			level.Info(logger).Log("msg", "Analysis iteration", "iteration", i, "num tokens per ingester", numTokensPerIngester)

			ringDesc := generateRingWithRandomTokens(numIngesters, numZones, numTokensPerIngester, math.MaxUint32, time.Now())
			ringTokens := ringDesc.GetTokens()
			ringInstanceByToken := getRingInstanceByToken(ringDesc)

			_, _, spread := getRegisteredTokensOwnershipStatistics(ringTokens, ringInstanceByToken)
			iterationResults = append(iterationResults, spread)
		}

		allIterationsResults = append(allIterationsResults, iterationResults)
	}

	// Generate CSV header.
	var csvHeader []string
	for _, numTokensPerIngester := range numTokensPerIngesterScenarios {
		csvHeader = append(csvHeader, fmt.Sprintf("%d tokens per ingester", numTokensPerIngester))
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
	if err := w.writeCSV(fmt.Sprintf("simulated-ingesters-ring-ownership-spread-on-different-tokens-per-ingester.csv")); err != nil {
		return err
	}

	return nil
}
