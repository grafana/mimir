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
	input := "4183135 26552757 48922379 71292001 93661623 116031245 138400867 160770489 183140111 205509733 227879355 250248977 272618599 294988221 317357843 339727465 362097087 384466709 406836331 429205953 451575575 473945197 496314819 518684441 541053934 563423556 585793178 608162800 630532422 652902044 675271666 697641288 720010910 742380532 764750154 787119776 809489398 831859020 854228642 876598264 898967886 921337508 943707130 966076752 988446374 1010815996 1033185618 1055555240 1077924862 1100294484 1122664106 1145033728 1167403350 1189772972 1212142594 1234512216 1256881838 1279251460 1301621082 1323990704 1346360326 1368729948 1391099570 1413469192 1435838814 1458208436 1480578058 1502947680 1525317302 1547686924 1570056546 1592426168 1614795790 1637165412 1659535034 1681904656 1704274278 1726643900 1749013522 1771383144 1793752766 1816122388 1838492010 1860861632 1883231254 1905600876 1927970498 1950340120 1972709742 1995079364 2017448986 2039818608 2062188230 2084557852 2106927474 2129297096 2151666718 2174036340 2196405962 2218775584 2241145206 2263514828 2285884450 2308254072 2330623694 2352993316 2375362938 2397732560 2420102182 2442471804 2464841426 2487211048 2509580670 2531950292 2554319914 2576689536 2599059158 2621428780 2643798402 2666168024 2688537646 2710907268 2733276890 2755646512 2778016134 2800385756 2822755378 2845125000 2867494622 2889864244 2912233866 2934603488 2956973110 2979342732 3001712354 3024081976 3046451598 3068821220 3091190842 3113560464 3135930086 3158299708 3180669330 3203038952 3225408574 3247778196 3270147818 3292517440 3314887062 3337256684 3359626306 3381995928 3404365550 3426735172 3449104794 3471474416 3493844038 3516213660 3538583282 3560952904 3583322526 3605692148 3628061770 3650431392 3672801014 3695170636 3717540258 3739909880 3762279502 3784649124 3807018746 3829388368 3851757990 3874127612 3896497234 3918866856 3941236478 3963606100 3985975722 4008345344 4030714966 4053084588 4075454210 4097823832 4120193454 4142563076 4164932698 4187302320 4209671942 4232041564 4254411186 4276780808"
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
