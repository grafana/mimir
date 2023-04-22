package tokendistributor

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func createTokenDistributor() *TokenDistributor {
	sortedRingTokens, ringInstanceByToken, zoneByInstance := createRingTokensInstancesZones()
	replicationStrategy := newZoneAwareReplicationStrategy(3, zoneByInstance, nil, nil)
	tokenDistributor := newTokenDistributorFromInitializedInstances(sortedRingTokens, ringInstanceByToken, 4, replicationStrategy)
	tokenDistributor.maxTokenValue = 1000
	return tokenDistributor
}

func TestTokenDistributor_CreateTokenInfoCircularList(t *testing.T) {
	tokenDistributor := createTokenDistributor()
	infoInstanceByInstance, zoneInfoByZone := tokenDistributor.createInstanceAndZoneInfos()
	tokenInfoCircularList := tokenDistributor.createTokenInfoCircularList(infoInstanceByInstance, zoneInfoByZone["zone-b"])
	for _, token := range tokenDistributor.sortedTokens {
		head := tokenInfoCircularList.head
		fmt.Println(head.getData())
		require.Equal(t, head.getData().getToken(), token)
		tokenInfoCircularList.remove(head)
	}
}
