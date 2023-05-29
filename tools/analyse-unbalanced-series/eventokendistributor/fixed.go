package eventokendistributor

import (
	"fmt"
	"math"

	"github.com/grafana/dskit/ring"
)

type Fixed struct{}

func (f Fixed) CalculateTokens(instanceID, zoneID int, _ ring.Tokens) (ring.Tokens, error) {
	if instanceID == 0 {
		return ring.Tokens{0 + uint32(zoneID)*100}, nil
	}

	// = 1 - 1/2 -  1/6 - 1/12 - 1/20 - 1/30 - 1/42
	// = 1 - 1/2 -  1/6 - 1/12 - 1/20 - 1/42
	// = 1 - 1/2 -  1/6 - 1/42
	// = 1 - 1/6 - 1/12 - 1/20 - 1/30 - 1/42
	// = 1 - 1/6 - 1/42
	// = 1 - 1/6 - 1/42

	// = 1 - 1/2 -  1/6 - 1/12 - 1/20 - 1/30 - 1/42
	// = 1       -  1/6 - 1/12 - 1/20 - 1/30 - 1/42
	// = 1 - 1/2 -  1/6 - 1/12 - 1/20        - 1/42
	// = 1                     - 1/20        - 1/42
	// = 1 - 1/2 -  1/6                      - 1/42
	// = 1       -  1/6                      - 1/42

	// =1 - 1/2  -  1/6 - 1/12 - 1/20 - 1/30
	// =1         - 1/6 - 1/12 - 1/20 - 1/30
	// =1 - 1/2  -  1/6 - 1/12 - 1/20
	// =1 - 1/2  -  1/6
	// =1 - 1/6
	// =1                      - 1/20

	// First token
	// = 1 - 1/2
	// = 1 - 1/2 - 1/6
	// = 1 - 1/2 - 1/6 - 1/12
	// = 1 - 1/2 - 1/6 - 1/12 - 1/20
	// = 1 - 1/2 - 1/6 - 1/12 - 1/20 - 1/30
	// = 1 - 1/2 - 1/6 - 1/12 - 1/20 - 1/30 - 1/42

	// Second token
	// = 1       - 1/6
	// = 1 - 1/2       - 1/12
	// = 1 - 1/2 - 1/6        - 1/20
	// = 1 - 1/2 - 1/6 - 1/12        - 1/30
	// = 1 - 1/2 - 1/6 - 1/12 - 1/20         - 1/42

	// Third token
	// = 1       - 1/6 - 1/12
	// = 1       - 1/6 - 1/12 - 1/20
	// = 1 - 1/2       - 1/12         - 1/30
	// = 1 - 1/2 - 1/6                       - 1/42

	// Fourth token
	// = 1              - 1/20
	// = 1 - 1/6 - 1/12 - 1/20 - 1/30
	// = 1 - 1/6 - 1/12 - 1/20 - 1/30 - 1/42

	tokens := make(ring.Tokens, 0, instanceID+1)
	token := uint32(math.MaxUint32)
	str := ""
	for n := instanceID; n >= 1; n-- {
		str = fmt.Sprintf("%s-1/%d", str, n*(n+1))
		token = token - uint32((math.MaxUint32)/float64(n*(n+1)))
		tokens = append(tokens, token)
		fmt.Printf("token: %10d 1%s\n", token, str)
	}
	// Reverse tokens list, because we generated it from the end.
	for i, j := 0, len(tokens)-1; i < j; i, j = i+1, j-1 {
		tokens[i], tokens[j] = tokens[j], tokens[i]
	}

	return tokens, nil
}
