// SPDX-License-Identifier: AGPL-3.0-only

package streaminglabelvalues

// JaroMatcher holds the pre-parsed runes of a reference string so that Jaro
// similarity scores against many candidate strings avoid re-converting the
// reference string on every call.
type JaroMatcher struct {
	ref []rune
}

// NewJaroMatcher returns a JaroMatcher for the given reference string.
// The reference is converted to runes once at construction time.
func NewJaroMatcher(reference string) JaroMatcher {
	return JaroMatcher{ref: []rune(reference)}
}

// Similarity returns the Jaro similarity score between the reference string
// and s, in the range [0, 1]. A score of 1 means the strings are identical.
func (j JaroMatcher) Similarity(s string) float64 {
	return jaroSimilarity(j.ref, []rune(s))
}

// jaroSimilarity computes the Jaro similarity between two rune slices.
func jaroSimilarity(s1, s2 []rune) float64 {
	l1, l2 := len(s1), len(s2)
	if l1 == 0 && l2 == 0 {
		return 1.0
	}
	if l1 == 0 || l2 == 0 {
		return 0.0
	}

	// Characters are considered matching only if they lie within this window
	// of each other: floor(max(l1, l2) / 2) - 1.
	maxLen := l1
	if l2 > maxLen {
		maxLen = l2
	}
	window := maxLen/2 - 1
	if window < 0 {
		window = 0
	}

	matched1 := make([]bool, l1)
	matched2 := make([]bool, l2)
	matches := 0

	for i := 0; i < l1; i++ {
		lo := i - window
		if lo < 0 {
			lo = 0
		}
		hi := i + window + 1
		if hi > l2 {
			hi = l2
		}
		for k := lo; k < hi; k++ {
			if !matched2[k] && s1[i] == s2[k] {
				matched1[i] = true
				matched2[k] = true
				matches++
				break
			}
		}
	}

	if matches == 0 {
		return 0.0
	}

	// Count transpositions: matched characters that appear in a different order.
	transpositions := 0
	k := 0
	for i := 0; i < l1; i++ {
		if !matched1[i] {
			continue
		}
		for !matched2[k] {
			k++
		}
		if s1[i] != s2[k] {
			transpositions++
		}
		k++
	}

	m := float64(matches)
	t := float64(transpositions) / 2.0
	return (m/float64(l1) + m/float64(l2) + (m-t)/m) / 3.0
}

// WinklerBoost adds the Jaro-Winkler prefix boost to a pre-computed Jaro score.
// The boost is proportional to the length of the common prefix of s1 and s2,
// capped at 4 characters as per the original Winkler formulation:
//
//	boosted = jaroScore + l*p*(1 − jaroScore)
//
// where l is the common prefix length and p is the scaling factor (standard value 0.1).
func WinklerBoost(jaroScore float64, s1, s2 string, p float64) float64 {
	r1, r2 := []rune(s1), []rune(s2)
	maxPrefix := 4
	if len(r1) < maxPrefix {
		maxPrefix = len(r1)
	}
	if len(r2) < maxPrefix {
		maxPrefix = len(r2)
	}
	l := 0
	for l < maxPrefix && r1[l] == r2[l] {
		l++
	}
	return jaroScore + float64(l)*p*(1-jaroScore)
}
