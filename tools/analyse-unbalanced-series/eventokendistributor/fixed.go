package eventokendistributor

import (
	"container/heap"
	"fmt"
	"math"

	"github.com/grafana/dskit/ring"
	"golang.org/x/exp/slices"
)

type Fixed struct{}

// CalculateTokens calculates tokens for instance n in zone z, regardless of the existing tokens in the ring.
// It returns n tokens for instance n, except for n=0, where it also returns 1 token.
func (f Fixed) CalculateTokens(n, z int, _ ring.Tokens) (ring.Tokens, error) {
	if n == 0 {
		return ring.Tokens{0 + uint32(z)}, nil
	}

	rs := make([]tr, 1, 1+n*(n+1)/2)
	rs[0] = tr{0, math.MaxUint32}

	tokens := make([]uint32, 0, n)

	// i is the instance we're going to calculate the tokens for.
	// We need to calculate all previous tokens first.
	for i := 1; i <= n; i++ {
		// The denominator of the fraction of tokens we're going to steal.
		// Every new instances steals 1/(n(n+1)) tokens from each one of the previous instances.
		denominator := i * (i + 1)
		// fraction is the amount of tokens we're going to steal from each previous instance.
		fraction := uint32(float64(math.MaxUint32) / float64(denominator))

		// si is the instance we're going to steal some range from.
		// We have to steal a 'fraction' from every previous instance.
		for si := 0; si < i; si++ {
			// s is the index of the first token of instance si.
			// This is also the token with the biggest share.
			s := 0
			if si > 0 {
				s = (si-1)*si/2 + 1
			}
			// l is the number of tokens of instance si.
			l := 1
			if si > 0 {
				l = si
			}

			// Make sure this token has enough space to steal from.
			if rs[s].end-rs[s].start < fraction {
				return nil, fmt.Errorf(
					"not enough tokens to steal from instance %d when building instance %d: range starts at %d ends at %d and has %d tokens, but we want to steal %d",
					si, i, rs[s].start, rs[s].end, rs[s].end-rs[s].start, fraction,
				)
			}

			// Create the new range at the end of the range we're going to steal from.
			r := tr{rs[s].end - fraction, rs[s].end}
			rs = append(rs, r)

			// Update the existing range to end where we start.
			rs[s].end = r.start
			heap.Fix(trh(rs[s:s+l]), s-s)

			// For last instance, store the tokens we've generated.
			if i == n {
				tokens = append(tokens, r.start+uint32(z))
			}
		}

		// Fix the heap.
		heap.Init(trh(rs[len(rs)-i:]))
	}

	slices.Sort(tokens)
	return tokens, nil
}

// tr is a token range, closed at start and opened at end.
type tr struct{ start, end uint32 }

// trh is a max-heap implementation for token ranges, based on the range size.
type trh []tr

func (t trh) Len() int           { return len(t) }
func (t trh) Less(i, j int) bool { return t[i].end-t[i].start > t[j].end-t[j].start }
func (t trh) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t trh) Push(x any)         { panic("implement me") } // not used.
func (t trh) Pop() any           { panic("implement me") } // not used.
