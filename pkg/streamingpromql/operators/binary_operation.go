// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"slices"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

// vectorMatchingGroupKeyFunc returns a function that computes the grouping key of the output group a series belongs to.
//
// The return value from the function is valid until it is called again.
func vectorMatchingGroupKeyFunc(vectorMatching parser.VectorMatching) func(labels.Labels) []byte {
	buf := make([]byte, 0, 1024)

	if vectorMatching.On {
		slices.Sort(vectorMatching.MatchingLabels)

		return func(l labels.Labels) []byte {
			return l.BytesWithLabels(buf, vectorMatching.MatchingLabels...)
		}
	}

	if len(vectorMatching.MatchingLabels) == 0 {
		// Fast path for common case for expressions like "a + b" with no 'on' or 'without' labels.
		return func(l labels.Labels) []byte {
			return l.BytesWithoutLabels(buf, labels.MetricName)
		}
	}

	lbls := make([]string, 0, len(vectorMatching.MatchingLabels)+1)
	lbls = append(lbls, labels.MetricName)
	lbls = append(lbls, vectorMatching.MatchingLabels...)
	slices.Sort(lbls)

	return func(l labels.Labels) []byte {
		return l.BytesWithoutLabels(buf, lbls...)
	}
}
