package labels

import (
	"strings"

	"github.com/prometheus/prometheus/pkg/labels"
)

type matchersWrapper struct {
	matchers []*labels.Matcher
}

func (w matchersWrapper) String() string {
	var b strings.Builder
	for _, m := range w.matchers {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		b.WriteString(m.String())
	}

	return b.String()
}

// WrapMatchers wraps a set of *labels.Matcher in a stringable type.
func WrapMatchers(matchers []*labels.Matcher) matchersWrapper {
	return matchersWrapper{matchers}
}
