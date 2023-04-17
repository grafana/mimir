package main

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/grafana/dskit/ring"
	"github.com/stretchr/testify/assert"
)

func TestGenerateRingWithPerfectlySpacedTokens(t *testing.T) {
	now := time.Now()

	mockInstanceDesc := func(addr, zone string, tokens []uint32) ring.InstanceDesc {
		return ring.InstanceDesc{
			Addr:                addr,
			Timestamp:           now.Unix(),
			State:               ring.ACTIVE,
			Tokens:              tokens,
			Zone:                zone,
			RegisteredTimestamp: now.Unix(),
		}
	}

	// To simplify the test and make it easier to understand, we reduce
	// the max token value to 120, which is a multiple of 6 ingesters * 2
	// tokens per ingester.
	actual := generateRingWithPerfectlySpacedTokens(6, 3, 2, 120, now)

	expected := &ring.Desc{
		Ingesters: map[string]ring.InstanceDesc{
			"ingester-zone-a-0": mockInstanceDesc("ingester-zone-a-0", "zone-a", []uint32{0, 60}),
			"ingester-zone-b-0": mockInstanceDesc("ingester-zone-b-0", "zone-b", []uint32{10, 70}),
			"ingester-zone-c-0": mockInstanceDesc("ingester-zone-c-0", "zone-c", []uint32{20, 80}),
			"ingester-zone-a-1": mockInstanceDesc("ingester-zone-a-1", "zone-a", []uint32{30, 90}),
			"ingester-zone-b-1": mockInstanceDesc("ingester-zone-b-1", "zone-b", []uint32{40, 100}),
			"ingester-zone-c-1": mockInstanceDesc("ingester-zone-c-1", "zone-c", []uint32{50, 110}),
		},
	}

	assert.Equal(t, expected, actual)
}

func TestGenerateRingWithBucketedRandomTokens(t *testing.T) {
	actual := generateRingWithBucketedRandomTokens(6, 3, 5, 1000, time.Now())
	for id, ingester := range actual.Ingesters {
		fmt.Println(id, "\t", ingester.Tokens)
	}
}

func TestGenerateRingWithBucketedFillerTokens(t *testing.T) {
	actual := generateRingWithBucketedFillerTokens(6, 3, 10, 1000, time.Now())

	printRingTokens(actual)
}

func TestGenerateRingWithEvenlyDistributedTokensPerZone(t *testing.T) {
	ringDesc, tokenInfoByZone := generateRingWithEvenlyDistributedTokensPerZone(6, 3, 4, math.MaxUint32, time.Now())

	printRingTokensPerZone(ringDesc, tokenInfoByZone)
}

func printRingTokens(ringDesc *ring.Desc) {
	const verbose = true

	if verbose {
		// Print the tokens for each ingester.
		for id, ingester := range ringDesc.Ingesters {
			fmt.Println(id, "\t", ingester.Tokens)
		}

		// Print all tokens with their gap.
		fmt.Println("")
		prevToken := uint32(0)
		for _, token := range ringDesc.GetTokens() {
			fmt.Println(fmt.Sprintf("%d (+%d) ", token, token-prevToken))
			prevToken = token
		}
	}

	// Print stats.
	min, max, spread := getRegisteredTokensOwnershipStatistics(ringDesc.GetTokens(), getRingInstanceByToken(ringDesc))
	fmt.Println("")
	fmt.Println(fmt.Sprintf("min: %.3f max: %.3f spread: %.2f", min, max, spread))
}

func printRingTokensPerZone(ringDesc *ring.Desc, tokenInfoByZone map[string]*tokenInfo) {
	const verbose = true

	if verbose {
		// Print the tokens for each ingester.
		for id, ingester := range ringDesc.Ingesters {
			fmt.Println(id, "\t", ingester.Tokens)
		}

		fmt.Println("")
		for zone, tokenInfo := range tokenInfoByZone {
			fmt.Println(zone)
			first := tokenInfo
			if first != nil {
				fmt.Printf("%d (+%d)\n", first.token, first.registeredOwnership)
				for curr := tokenInfo.next; curr != first; curr = curr.next {
					fmt.Printf("%d (+%d)\n", curr.token, curr.registeredOwnership)
				}
			}
		}
	}

	// Print stats.
	statistics := getTokenInfoByZoneStatistics(tokenInfoByZone)
	fmt.Println("")
	for zone, tokenInfoStatistics := range statistics {
		fmt.Println(zone)
		fmt.Printf("min: %d max: %d spread: %.2f\n", tokenInfoStatistics.min, tokenInfoStatistics.max, tokenInfoStatistics.spread)
	}
}
