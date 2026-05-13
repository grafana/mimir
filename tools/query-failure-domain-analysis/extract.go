// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"regexp"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

// parsedQuery is the structural information we need from a single PromQL query
// in order to simulate how that query would route to partitions/ingesters under
// alternative hashing schemes.
type parsedQuery struct {
	// EqualityNames are the distinct __name__="..." matches in the query.
	EqualityNames []string

	// NameRegexes are compiled regex matchers on __name__ (both =~ and !~).
	// Negative matchers are recorded separately so callers can decide how to
	// treat them; in practice a negative regex on __name__ alone leaves the
	// possible matches open to "everything else" so the query is unbucketable.
	NameRegexes          []*regexp.Regexp
	HasNegativeNameRegex bool

	// HasUnnamedSelector is true when any vector selector in the query has no
	// __name__ constraint at all (e.g. count({namespace="prod"})). Such queries
	// are unbucketable and always touch the full shuffle shard.
	HasUnnamedSelector bool
}

// fullShard reports whether this query is guaranteed to touch the entire
// shuffle shard under any name-based routing scheme.
func (p parsedQuery) fullShard() bool {
	return p.HasUnnamedSelector || p.HasNegativeNameRegex
}

// promqlParser is reused for every input query. Enabling the experimental
// flags is permissive — we want to parse as many real-world queries as
// possible, including ones using newer language features.
var promqlParser = parser.NewParser(parser.Options{
	EnableExperimentalFunctions:  true,
	ExperimentalDurationExpr:     true,
	EnableExtendedRangeSelectors: true,
	EnableBinopFillModifiers:     true,
})

// extractMetricSelectors parses a PromQL expression and collects the
// information needed to estimate failure domains. Returns an error when the
// expression cannot be parsed.
func extractMetricSelectors(expr string) (parsedQuery, error) {
	root, err := promqlParser.ParseExpr(expr)
	if err != nil {
		return parsedQuery{}, err
	}

	var (
		pq      parsedQuery
		nameSet = map[string]struct{}{}
	)

	parser.Inspect(root, func(n parser.Node, _ []parser.Node) error {
		vs, ok := n.(*parser.VectorSelector)
		if !ok {
			return nil
		}
		classify(vs, &pq, nameSet)
		return nil
	})

	for name := range nameSet {
		pq.EqualityNames = append(pq.EqualityNames, name)
	}
	return pq, nil
}

// classify inspects a single vector selector and updates the parsedQuery
// accordingly. It is its own function so the logic is easy to unit-test.
func classify(vs *parser.VectorSelector, pq *parsedQuery, nameSet map[string]struct{}) {
	// vs.Name is the shorthand form (e.g. "cpu_usage{...}") and is set in
	// addition to LabelMatchers, so we don't need to special-case it here:
	// the parser always synthesises a __name__ matcher when the shorthand is
	// present. But we walk LabelMatchers anyway because it's the source of
	// truth and handles {__name__="..."} too.
	var sawNameMatcher bool
	for _, m := range vs.LabelMatchers {
		if m.Name != labels.MetricName {
			continue
		}
		sawNameMatcher = true
		switch m.Type {
		case labels.MatchEqual:
			if m.Value != "" {
				nameSet[m.Value] = struct{}{}
			} else {
				// __name__="" is effectively no constraint.
				sawNameMatcher = false
			}
		case labels.MatchRegexp:
			if re, err := regexp.Compile("^(?:" + m.Value + ")$"); err == nil {
				pq.NameRegexes = append(pq.NameRegexes, re)
			} else {
				// If we can't compile the regex, be conservative.
				pq.HasUnnamedSelector = true
			}
		case labels.MatchNotEqual, labels.MatchNotRegexp:
			// A negative constraint on __name__ keeps the set of possible
			// matching names open-ended (everything else in the universe),
			// so we treat this selector as targeting the whole shard.
			pq.HasNegativeNameRegex = true
		}
	}
	if !sawNameMatcher {
		pq.HasUnnamedSelector = true
	}
}
