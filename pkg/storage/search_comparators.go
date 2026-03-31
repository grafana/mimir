package storage

import (
	"cmp"
	"strings"
)

type SortBy int
type SortDirection int

const (
	None SortBy = iota
	Alpha
	Score
)

const (
	Asc SortDirection = iota
	Desc
)

var comparerAlphaAscFunc = func(a SearchResult, b SearchResult) int { return strings.Compare(b.Value, a.Value) }
var comparerAlphaDscFunc = func(a SearchResult, b SearchResult) int { return strings.Compare(a.Value, b.Value) }
var comparerScoreAscFunc = func(a SearchResult, b SearchResult) int { return cmp.Compare(a.Score, b.Score) }
var comparerScoreDscFunc = func(a SearchResult, b SearchResult) int { return cmp.Compare(b.Score, a.Score) }

func NewComparator(by SortBy, order SortDirection) Comparator {
	var cmpFunc func(a, b SearchResult) int

	switch by {
	case None:
		return nil
	case Alpha:
		if order == Asc {
			cmpFunc = comparerAlphaAscFunc
		} else {
			cmpFunc = comparerAlphaDscFunc
		}
	case Score:
		if order == Asc {
			cmpFunc = comparerScoreAscFunc
		} else {
			cmpFunc = comparerScoreDscFunc
		}
	}

	return &comparer{cmpFunc: cmpFunc}

}

type comparer struct {
	cmpFunc func(a, b SearchResult) int
}

func (c *comparer) Compare(a SearchResult, b SearchResult) int {
	return c.cmpFunc(a, b)
}
