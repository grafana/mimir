package eventokendistributor

import (
	"math"

	"github.com/grafana/dskit/ring"
	"golang.org/x/exp/slices"
)

type token struct {
	start    uint32
	next     *token
	instance int
	other    *token
}

func (t *token) size() uint32 {
	end := t.end()
	return end - t.start
}

func (t *token) end() uint32 {
	if t.next != nil {
		return t.next.start
	}
	return math.MaxUint32
}

type Fixed struct {
	zoneOffset int
}

func (f Fixed) CalculateTokens(instanceID, zoneID int, _ ring.Tokens) (ring.Tokens, error) {
	first := make([]*token, 0, instanceID)
	first = append(first, &token{start: 0, instance: 0})
	first = append(first, &token{start: math.MaxUint32 / 2, instance: 1})
	first[0].next = first[1]

	for instance := 2; instance <= instanceID; instance++ {
		fraction := uint32(math.MaxUint32 / (instance * (instance + 1)))
		first = append(first, nil)
		var last *token

		for stolen := 0; stolen < instance; stolen++ {
			assigned := false
			for candidate := first[stolen]; !assigned && candidate != nil; candidate = candidate.other {
				if candidate.size() > fraction {
					assigned = true
					token := &token{start: candidate.end() - fraction, next: candidate.next, instance: instance}
					candidate.next = token
					if first[instance] == nil {
						first[instance] = token
					} else {
						last.other = token
					}
					last = token
				}
			}
			if !assigned {
				panic("not assigned")
			}
		}
	}

	ret := make([]uint32, 0, instanceID)
	for token := first[instanceID]; token != nil; token = token.other {
		ret = append(ret, (token.start+uint32(zoneID*f.zoneOffset))%math.MaxUint32)
	}
	slices.Sort(ret)
	return ret, nil
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

// 2=     0     1
// 3=     1     0     1
// 4=     1     1     0     1
// 5=     1     1     1     0     1
// 6=     1     1     1     1     0     1

// Third token
// = 1       - 1/6 - 1/12
// = 1       - 1/6 - 1/12 - 1/20
// = 1 - 1/2       - 1/12         - 1/30
// = 1 - 1/2 - 1/6                       - 1/42

// 3=      0     1     1
// 4=      0     1     1     1
// 5=      1     0     1     0     1
// 6=      1     1     0     1     0

// Fourth token
// = 1                    - 1/20
// = 1       - 1/6 - 1/12 - 1/20 - 1/30
// = 1       - 1/6 - 1/12 - 1/20 - 1/30 - 1/42

// 4=      0     0     0     1
// 5=      0     1     1     1     1
// 6=      0     1     1     1     1     1
