package streaminglabelvalues

import (
	"math"
	"strings"

	"github.com/grafana/mimir/pkg/storage"
)

type ComparerAlpha struct {
}

func (c *ComparerAlpha) Compare(a storage.FilteredResult, b storage.FilteredResult) int {
	return strings.Compare(a.Value, b.Value)
}

type ComparerAlphaDesc struct {
}

func (c *ComparerAlphaDesc) Compare(a storage.FilteredResult, b storage.FilteredResult) int {
	return strings.Compare(b.Value, a.Value)
}

type CompareScore struct {
	jaros []JaroMatcher
}

func NewCompareScore(terms []string) *CompareScore {
	jaros := make([]JaroMatcher, 0, len(terms))
	for _, term := range terms {
		jaros = append(jaros, NewJaroMatcher(term))
	}
	return &CompareScore{jaros}
}

func (c *CompareScore) Compare(a storage.FilteredResult, b storage.FilteredResult) int {
	leftScore := a.Score
	if leftScore == unscored {
		leftScore = c.bestScore(a.Value)
	}
	leftScore = c.bestWithBoost(a.Value, leftScore)

	rightScore := b.Score
	if rightScore == unscored {
		rightScore = c.bestScore(a.Value)
	}
	rightScore = c.bestWithBoost(b.Value, rightScore)

	if leftScore > rightScore {
		return -1
	}
	if leftScore < rightScore {
		return 1
	}
	return 0
}

func (c *CompareScore) bestWithBoost(s string, jaroScore float64) float64 {
	best := unscored
	for _, jaro := range c.jaros {
		best = math.Max(best, WinklerBoost(jaroScore, string(jaro.ref), s, 0.1))
	}
	return best
}

func (c *CompareScore) bestScore(s string) float64 {
	best := unscored
	for _, jaro := range c.jaros {
		best = math.Max(best, jaro.Similarity(s))
	}
	return best
}
