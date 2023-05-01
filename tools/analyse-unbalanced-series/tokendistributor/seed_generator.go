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
	input := "11781580 34151202 56520824 78890446 101260068 123629690 145999312 168368934 190738556 213108178 235477800 257847422 280217044 302586666 324956288 347325910 369695532 392065154 414434776 436804398 459174020 481543642 503913264 526282757 548652379 571022001 593391623 615761245 638130867 660500489 682870111 705239733 727609355 749978977 772348599 794718221 817087843 839457465 861827087 884196709 906566331 928935953 951305575 973675197 996044819 1018414441 1040784063 1063153685 1085523307 1107892929 1130262551 1152632173 1175001795 1197371417 1219741039 1242110661 1264480283 1286849905 1309219527 1331589149 1353958771 1376328393 1398698015 1421067637 1443437259 1465806881 1488176503 1510546125 1532915747 1555285369 1577654991 1600024613 1622394235 1644763857 1667133479 1689503101 1711872723 1734242345 1756611967 1778981589 1801351211 1823720833 1846090455 1868460077 1890829699 1913199321 1935568943 1957938565 1980308187 2002677809 2025047431 2047417053 2069786675 2092156297 2114525919 2136895541 2159265163 2181634785 2204004407 2226374029 2248743651 2271113273 2293482895 2315852517 2338222139 2360591761 2382961383 2405331005 2427700627 2450070249 2472439871 2494809493 2517179115 2539548737 2561918359 2584287981 2606657603 2629027225 2651396847 2673766469 2696136091 2718505713 2740875335 2763244957 2785614579 2807984201 2830353823 2852723445 2875093067 2897462689 2919832311 2942201933 2964571555 2986941177 3009310799 3031680421 3054050043 3076419665 3098789287 3121158909 3143528531 3165898153 3188267775 3210637397 3233007019 3255376641 3277746263 3300115885 3322485507 3344855129 3367224751 3389594373 3411963995 3434333617 3456703239 3479072861 3501442483 3523812105 3546181727 3568551349 3590920971 3613290593 3635660215 3658029837 3680399459 3702769081 3725138703 3747508325 3769877947 3792247569 3814617191 3836986813 3859356435 3881726057 3904095679 3926465301 3948834923 3971204545 3993574167 4015943789 4038313411 4060683033 4083052655 4105422277 4127791899 4150161521 4172531143 4194900765 4217270387 4239640009 4262009631 4284379253"
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
