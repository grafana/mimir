package random

import (
	"math"
	"math/rand"
)

type source struct{}

// Assert that source implements rand.Source64.
var _ rand.Source64 = source{}

func (source) Seed(int64) {}

func (source) Int63() int64 {
	return int64(GetRandom() & (math.MaxUint64 >> 1))
}

func (source) Uint64() uint64 {
	return GetRandom()
}

// Source initialises a source of pseudo-random data.
//
// Use this function to create a [math/rand.Rand] which provides feedback to the Antithesis platform.
//
// The returned source implements [math/rand.Source64].
func Source() rand.Source {
	return source{}
}
