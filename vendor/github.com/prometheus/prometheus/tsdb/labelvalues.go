package tsdb

import (
	"context"
	"fmt"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/index"
)

func labelValuesForMatchersStream(ctx context.Context, r IndexReader, name string, matchers []*labels.Matcher) storage.LabelValues {
	// See which labels must be non-empty.
	// Optimization for case like {l=~".", l!="1"}.
	labelMustBeSet := make(map[string]bool, len(matchers))
	for _, m := range matchers {
		if !m.Matches("") {
			labelMustBeSet[m.Name] = true
		}
	}

	var its, notIts []index.Postings
	for _, m := range matchers {
		switch {
		case labelMustBeSet[m.Name]:
			// If this matcher must be non-empty, we can be smarter.
			matchesEmpty := m.Matches("")
			isNot := m.Type == labels.MatchNotEqual || m.Type == labels.MatchNotRegexp
			switch {
			case isNot && matchesEmpty: // l!="foo"
				// If the label can't be empty and is a Not and the inner matcher
				// doesn't match empty, then subtract it out at the end.
				inverse, err := m.Inverse()
				if err != nil {
					return storage.ErrLabelValues(err)
				}

				it := r.PostingsForMatcher(ctx, inverse)
				if it.Err() != nil {
					return storage.ErrLabelValues(it.Err())
				}
				notIts = append(notIts, it)
			case isNot && !matchesEmpty: // l!=""
				// If the label can't be empty and is a Not, but the inner matcher can
				// be empty we need to use inversePostingsForMatcher.
				inverse, err := m.Inverse()
				if err != nil {
					return storage.ErrLabelValues(err)
				}

				it, err := inversePostingsForMatcher(ctx, r, inverse)
				if err != nil {
					return storage.ErrLabelValues(err)
				}
				if index.IsEmptyPostingsType(it) {
					return storage.EmptyLabelValues()
				}
				its = append(its, it)
			default: // l="a"
				// Non-Not matcher, use normal postingsForMatcher.
				it := r.PostingsForMatcher(ctx, m)
				if it.Err() != nil {
					return storage.ErrLabelValues(it.Err())
				}
				if index.IsEmptyPostingsType(it) {
					return storage.EmptyLabelValues()
				}
				its = append(its, it)
			}
		default: // l=""
			// If a matcher for a labelname selects an empty value, it selects all
			// the series which don't have the label name set too. See:
			// https://github.com/prometheus/prometheus/issues/3575 and
			// https://github.com/prometheus/prometheus/pull/3578#issuecomment-351653555
			it, err := inversePostingsForMatcher(ctx, r, m)
			if err != nil {
				return storage.ErrLabelValues(err)
			}
			notIts = append(notIts, it)
		}
	}

	if len(its) == 0 && len(notIts) > 0 {
		pit := index.Merge(ctx, notIts...)
		return r.LabelValuesExcluding(pit, name)
	}

	pit := index.Intersect(its...)
	for _, n := range notIts {
		pit = index.Without(pit, n)
	}
	pit = expandPostings(pit)

	return r.LabelValuesFor(pit, name)
}

// expandPostings expands postings up to a certain limit, to reduce runtime complexity when filtering label values.
// If the limit is reached, the rest is unexpanded.
func expandPostings(postings index.Postings) index.Postings {
	const expandPostingsLimit = 10_000_000
	var expanded []storage.SeriesRef
	// Go one beyond the limit, so we can tell if the iterator is exhausted
	for len(expanded) <= expandPostingsLimit && postings.Next() {
		expanded = append(expanded, postings.At())
	}
	if postings.Err() != nil {
		return index.ErrPostings(fmt.Errorf("expanding postings for matchers: %w", postings.Err()))
	}
	if len(expanded) > expandPostingsLimit {
		// Couldn't exhaust the iterator
		postings = index.NewPrependPostings(expanded, postings)
	} else {
		postings = index.NewListPostings(expanded)
	}

	return postings
}
