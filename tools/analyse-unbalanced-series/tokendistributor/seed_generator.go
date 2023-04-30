package tokendistributor

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"golang.org/x/exp/slices"
)

type SeedGenerator interface {
	generateSeedByZone(zones []Zone, tokensPerInstance int, maxTokenValue Token) map[Zone][]Token
}

type TestSeedGenerator struct{}

func (g TestSeedGenerator) generateSeedByZone(zones []Zone, tokensPerInstance int, maxTokenValue Token) map[Zone][]Token {
	input := "27826563 117305049 206783535 296262021 385740507 475218993 564697479 654175965 743654451 833132937 922611423 1012089909 1101568395 1191046881 1280525367 1370003853 1459482339 1548960825 1638439278 1727917764 1817396250 1906874736 1996353222 2085831708 2175310194 2264788680 2354267166 2443745652 2533224138 2622702624 2712181110 2801659596 2891138082 2980616568 3070095054 3159573540 3249052026 3338530512 3428008998 3517487484 3606965970 3696444456 3785922942 3875401428 3964879914 4054358400 4143836886 4233315372"
	strSeeds := strings.Split(input, " ")
	allSeeds := make([]Token, 0, len(strSeeds))
	for _, strSeed := range strSeeds {
		seed, err := strconv.ParseUint(strSeed, 10, 32)
		if err != nil {
			panic("Bad conversion " + strSeed)
		}
		allSeeds = append(allSeeds, Token(seed))
	}
	//allSeeds := []Token{17, 101, 185, 269, 353, 437, 513, 597, 681, 765, 849, 933}

	slices.Sort(allSeeds)
	fmt.Println(allSeeds)

	return distributeSortedTokensByZone(allSeeds, zones, tokensPerInstance)
}

type PerfectlySpacedSeedGenerator struct{}

func (g PerfectlySpacedSeedGenerator) generateSeedByZone(zones []Zone, tokensPerInstance int, maxTokenValue Token) map[Zone][]Token {
	allSeeds := make([]Token, 0, tokensPerInstance*len(zones))
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	tokensCount := tokensPerInstance * len(zones)
	offset := uint32(math.Ceil(float64(maxTokenValue) / float64(tokensCount)))
	curr := uint32(r.Intn(int(maxTokenValue)))
	for i := 0; i < tokensCount; i++ {
		allSeeds = append(allSeeds, Token(curr))
		if uint32(maxTokenValue)-offset < curr {
			curr -= uint32(maxTokenValue) - offset
		} else {
			curr += offset
		}
	}

	slices.Sort(allSeeds)
	fmt.Println(allSeeds)

	return distributeSortedTokensByZone(allSeeds, zones, tokensPerInstance)
}

type RandomSeedGenerator struct{}

func (g RandomSeedGenerator) generateSeedByZone(zones []Zone, tokensPerInstance int, maxTokenValue Token) map[Zone][]Token {
	allSeeds := make([]Token, 0, tokensPerInstance*len(zones))
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	tokensCount := tokensPerInstance * len(zones)
	for {
		token := Token(r.Intn(int(maxTokenValue)))
		if slices.Contains(allSeeds, token) {
			continue
		}
		allSeeds = append(allSeeds, token)
		if len(allSeeds) == tokensCount {
			break
		}
	}

	slices.Sort(allSeeds)

	return distributeSortedTokensByZone(allSeeds, zones, tokensPerInstance)
}

func distributeSortedTokensByZone(allSeeds []Token, zones []Zone, tokensPerInstance int) map[Zone][]Token {
	seedByZone := make(map[Zone][]Token, len(zones))
	for i, token := range allSeeds {
		zone := zones[i%len(zones)]
		currSeed, ok := seedByZone[zone]
		if !ok {
			currSeed = make([]Token, 0, tokensPerInstance)
		}
		currSeed = append(currSeed, token)
		seedByZone[zone] = currSeed
	}
	return seedByZone
}
