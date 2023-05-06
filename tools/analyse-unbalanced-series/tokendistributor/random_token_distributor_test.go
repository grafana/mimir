package tokendistributor

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

func createRandomTokenDistributor(maxToken Token) *RandomTokenDistributor {
	tokenDistributor := createTokenDistributor(maxToken)
	return &RandomTokenDistributor{
		tokenDistributor,
	}
}

func TestRandomTokenDistributor_CalculateCandidateToken(t *testing.T) {
	randomTokenDistributor := createRandomTokenDistributor(maxToken)
	newInstanceInfo := newInstanceInfo(newInstance, newZoneInfo(newInstanceZone), tokensPerInstance)
	tokenInfoCircularList := createTokenInfoCircularList(randomTokenDistributor.TokenDistributor, newInstanceInfo)
	fmt.Println(tokenInfoCircularList)
	candidateToken, err := randomTokenDistributor.calculateCandidateToken()
	require.Nil(t, err)
	require.False(t, slices.Contains(randomTokenDistributor.sortedTokens, candidateToken))
	hostIndexFloor := searchTokenFloor(randomTokenDistributor.sortedTokens, candidateToken)
	hostIndex := searchToken(randomTokenDistributor.sortedTokens, candidateToken)
	fmt.Println(candidateToken, hostIndexFloor, randomTokenDistributor.sortedTokens[hostIndexFloor], hostIndex, randomTokenDistributor.sortedTokens[hostIndex])
	hostIndexFloor = searchTokenFloor(randomTokenDistributor.sortedTokens, Token(910))
	hostIndex = searchToken(randomTokenDistributor.sortedTokens, Token(910))
	fmt.Println(910, hostIndexFloor, randomTokenDistributor.sortedTokens[hostIndexFloor], hostIndex, randomTokenDistributor.sortedTokens[hostIndex])
	hostIndexFloor = searchTokenFloor(randomTokenDistributor.sortedTokens, Token(20))
	hostIndex = searchToken(randomTokenDistributor.sortedTokens, Token(20))
	fmt.Println(20, hostIndexFloor, randomTokenDistributor.sortedTokens[hostIndexFloor], hostIndex, randomTokenDistributor.sortedTokens[hostIndex])
}
