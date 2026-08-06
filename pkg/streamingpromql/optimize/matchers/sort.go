package matchers

import (
	"slices"
	"strings"

	"github.com/prometheus/prometheus/model/labels"
)

func Sort(matchers []*labels.Matcher) {
	slices.SortFunc(matchers, Compare)
}

func Compare(a, b *labels.Matcher) int {
	if a.Name != b.Name {
		return strings.Compare(a.Name, b.Name)
	}

	if a.Type != b.Type {
		return int(a.Type - b.Type)
	}

	return strings.Compare(a.Value, b.Value)
}
