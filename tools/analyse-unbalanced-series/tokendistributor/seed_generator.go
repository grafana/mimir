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
	//fmt.Println(allSeeds)
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

func NewRandomSeedGenerator(zones []Zone, replicationFactor, tokensPerInstance int, maxTokenValue Token) *RandomSeedGenerator {
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
	input := "1987118 24356740 46726362 69095984 91465606 113835228 136204850 158574472 180944094 203313716 225683338 248052960 270422582 292792204 315161826 337531448 359901070 382270692 404640314 427009936 449379558 471749180 494118802 516488424 538858046 561227668 583597290 605966912 628336534 650706156 673075778 695445400 717815022 740184644 762554266 784923888 807293510 829663132 852032754 874402376 896771998 919141620 941511242 963880864 986250486 1008620108 1030989730 1053359352 1075728974 1098098596 1120468218 1142837840 1165207462 1187577084 1209946706 1232316328 1254685950 1277055572 1299425194 1321794816 1344164438 1366534060 1388903682 1411273304 1433642926 1456012548 1478382170 1500751792 1523121414 1545491036 1567860658 1590230280 1612599902 1634969524 1657339146 1679708768 1702078390 1724448012 1746817634 1769187256 1791556878 1813926500 1836296122 1858665744 1881035366 1903404988 1925774610 1948144232 1970513854 1992883476 2015253098 2037622720 2059992342 2082361964 2104731586 2127101208 2149470830 2171840452 2194210074 2216579696 2238949318 2261318940 2283688562 2306058184 2328427806 2350797428 2373167050 2395536672 2417906294 2440275916 2462645538 2485015160 2507384782 2529754404 2552124026 2574493648 2596863270 2619232892 2641602514 2663972136 2686341758 2708711380 2731081002 2753450624 2775820246 2798189868 2820559490 2842929112 2865298734 2887668356 2910037978 2932407600 2954777222 2977146844 2999516466 3021886088 3044255710 3066625332 3088994954 3111364576 3133734198 3156103820 3178473442 3200843064 3223212686 3245582308 3267951930 3290321552 3312691174 3335060796 3357430418 3379800040 3402169662 3424539284 3446908906 3469278528 3491648150 3514017772 3536387394 3558757016 3581126638 3603496260 3625865882 3648235504 3670605126 3692974748 3715344370 3737713992 3760083614 3782453236 3804822858 3827192480 3849562102 3871931724 3894301346 3916670968 3939040590 3961410212 3983779834 4006149456 4028519078 4050888571 4073258193 4095627815 4117997437 4140367059 4162736681 4185106303 4207475925 4229845547 4252215169 4274584791"
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
