package random

import "math"

// RandomChoice returns a randomly chosen item from a list of options. You should not store this value, but should use it immediately.
//
// This function is not purely for convenience. Signaling to the Antithesis platform that you intend to use a random value in a structured way enables it to provide more interesting choices over time.
//
// The second result is false for an empty slice.
func RandomChoice[T any](things []T) (T, bool) {
	numThings := len(things)
	if numThings == 0 {
		var none T
		return none, false
	}
	if numThings == 1 {
		return things[0], true
	}

	n := uint64(numThings)
	ceiling := (math.MaxUint64 / n) * n
	v := GetRandom()
	for v >= ceiling {
		v = GetRandom()
	}
	return things[v%n], true
}
