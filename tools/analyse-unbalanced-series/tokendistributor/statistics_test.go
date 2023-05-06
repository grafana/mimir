package tokendistributor

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func createOwnershipInfo() *OwnershipInfo {
	ownership := []float64{190141778, 223696210, 223696209, 167772156, 212511400, 234881023, 190141778, 190141781, 212511402, 201326592, 201326589, 190141781, 212511402, 167772161, 201326593, 162179755, 145402539, 190141783, 223696212, 178956971, 190141781, 178956972, 212511402, 181753171, 201326591, 145402537, 190141781, 167772157, 145402539, 156587350, 150994943, 148198740, 190141781, 167772160, 201326592, 178956971, 184549378, 190141783, 156587350, 201326594, 223696214, 201326592, 201326593, 223696215, 167772159, 190141783, 212511406, 218103806, 212511402, 206918994, 167772160, 206918999, 212511404, 206918999, 212511404, 234881026, 195734188, 212511405, 195734187, 184549377, 234881026, 212511404, 234881024, 223696215, 209715201, 220900014}
	optimalInstanceOwnership := float64(math.MaxUint32) * 3.0 / float64(len(ownership))
	ownershipMap := make(map[Instance]float64, len(ownership))
	for i := range ownership {
		instance := Instance(fmt.Sprintf("I-%d", i))
		ownershipMap[instance] = ownership[i]
	}
	return &OwnershipInfo{
		InstanceOwnershipMap:    ownershipMap,
		OptimaInstanceOwnership: optimalInstanceOwnership,
		TokenOwnershipMap:       make(map[Token]float64, 0),
		OptimalTokenOwnership:   0,
	}
}

func TestOwnershipInfo_CalculateMaxStDev(t *testing.T) {
	ownershipInfo := createOwnershipInfo()
	maxInstanceStDev, _ := calculateMaxStdevAndBounds([]*OwnershipInfo{ownershipInfo})
	require.InDelta(t, 2.3969524061154366e+07, maxInstanceStDev, 0.0001)
}
