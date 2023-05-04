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
	hasNextSeed(zone Zone) bool
	getNextSeed(zone Zone) ([]Token, bool)
}

type BaseSeedGenerator struct {
	seedsByZone       map[Zone][][]Token
	lastReplica       int
	replicationFactor int
}

func (g *BaseSeedGenerator) hasNextSeed(zone Zone) bool {
	_, ok := g.seedsByZone[zone]
	return ok && g.lastReplica < g.replicationFactor
}

func (g *BaseSeedGenerator) getNextSeed(zone Zone) ([]Token, bool) {
	if g.hasNextSeed(zone) {
		seed := make([]Token, 0, len(g.seedsByZone[zone][g.lastReplica]))
		seed = append(seed, g.seedsByZone[zone][g.lastReplica]...)
		g.seedsByZone[zone][g.lastReplica] = nil
		g.lastReplica = g.lastReplica + 1
		if g.lastReplica == len(g.seedsByZone[zone]) {
			delete(g.seedsByZone, zone)
			g.lastReplica = 0
		}
		return seed, true
	}
	return nil, false
}

type PerfectlySpacedSeedGenerator struct {
	*BaseSeedGenerator
}

func NewPerfectlySpacedSeedGenerator(zones []Zone, replicationFactor, tokensPerInstance int, maxTokenValue Token) *PerfectlySpacedSeedGenerator {
	tokensCount := int(math.Max(float64(len(zones)), float64(replicationFactor))) * tokensPerInstance

	allSeeds := make([]Token, 0, tokensCount)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
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
	seedsByZone := distributeSortedTokensByZone(allSeeds, zones, replicationFactor, tokensPerInstance)
	return &PerfectlySpacedSeedGenerator{
		&BaseSeedGenerator{
			seedsByZone:       seedsByZone,
			lastReplica:       0,
			replicationFactor: replicationFactor,
		},
	}
}

type RandomSeedGenerator struct {
	*BaseSeedGenerator
}

func newRandomSeedGenerator(zones []Zone, replicationFactor, tokensPerInstance int, maxTokenValue Token) *RandomSeedGenerator {
	tokensCount := int(math.Max(float64(len(zones)), float64(replicationFactor))) * tokensPerInstance

	allSeeds := make([]Token, 0, tokensCount)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
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
	seedsByZone := distributeSortedTokensByZone(allSeeds, zones, replicationFactor, tokensPerInstance)
	return &RandomSeedGenerator{
		&BaseSeedGenerator{
			seedsByZone:       seedsByZone,
			lastReplica:       0,
			replicationFactor: replicationFactor,
		},
	}
}

type TestSeedGenerator struct {
	*BaseSeedGenerator
}

func newTestSeedGenerator(zones []Zone, replicationFactor, tokensPerInstance int, maxTokenValue Token) *TestSeedGenerator {
	input := "77 177 277 377 477 577 677 777 877 977 1077 1177"
	strSeeds := strings.Split(input, " ")
	allSeeds := make([]Token, 0, len(strSeeds))
	for _, strSeed := range strSeeds {
		seed, err := strconv.ParseUint(strSeed, 10, 32)
		if err != nil {
			panic("Bad conversion " + strSeed)
		}
		allSeeds = append(allSeeds, Token(seed))
	}

	slices.Sort(allSeeds)
	fmt.Println(allSeeds)

	slices.Sort(allSeeds)
	seedsByZone := distributeSortedTokensByZone(allSeeds, zones, replicationFactor, tokensPerInstance)
	return &TestSeedGenerator{
		&BaseSeedGenerator{
			seedsByZone:       seedsByZone,
			lastReplica:       0,
			replicationFactor: replicationFactor,
		},
	}
}

func distributeSortedTokensByZone(allSeeds []Token, zones []Zone, replicationFactor, tokensPerInstance int) map[Zone][][]Token {
	seedByZone := make(map[Zone][][]Token, len(zones))
	if len(zones) > 1 {
		for i, token := range allSeeds {
			zone := zones[i%len(zones)]
			currSeed, ok := seedByZone[zone]
			if !ok {
				currSeed = make([][]Token, 0, 1)
				currSeed = append(currSeed, make([]Token, 0, tokensPerInstance))
			}
			currSeed[0] = append(currSeed[0], token)
			seedByZone[zone] = currSeed
		}
	} else {
		seeds := make([][]Token, 0, replicationFactor)
		for i := 0; i < replicationFactor; i++ {
			seeds = append(seeds, make([]Token, 0, tokensPerInstance))
		}
		for i, token := range allSeeds {
			seed := seeds[i%replicationFactor]
			seed = append(seed, token)
			seeds[i%replicationFactor] = seed
		}
		seedByZone[zones[0]] = seeds
	}
	return seedByZone
}
