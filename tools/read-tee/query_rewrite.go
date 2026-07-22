// SPDX-License-Identifier: AGPL-3.0-only

package readtee

import (
	"regexp"
	"strconv"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/util/promqlext"
)

// promQLParser is configured identically to Mimir's query-frontend/querier so read-tee parses
// exactly the queries the real read path accepts (experimental functions, extended range
// selectors, duration expressions). parser.Parser instances are safe for concurrent use.
var promQLParser = promqlext.NewPromQLParser()

// ampVariantsSuffix matches the original value optionally followed by any write-tee amplification
// suffix (_amp{N}). It is appended (as a regex) to negative matchers so they exclude the value
// across the base series and every amplified replica, not just a single one. The digit class
// (rather than .*) keeps the exclusion tight to genuine _amp{N} variants.
const ampVariantsSuffix = "(?:_amp[0-9]+)?"

// rewriteOptions controls how matchers are rewritten.
type rewriteOptions struct {
	// excludeAmplifiedNegative changes how negative matchers are rewritten: a negative matcher
	// excludes the original value and all _amp{N} variants (independent of the replica) instead of
	// only the _amp{replica} form. When false, negative matchers are suffixed _amp{replica} like
	// positive matchers (the original behaviour).
	excludeAmplifiedNegative bool
	// matchAllReplicas rewrites every matcher (positive and negative) to match/exclude the value
	// across the base series and all _amp{N} variants at once, ignoring the replica index. Used for
	// the amp.*-mode heavy copy that targets all replicas in a single query.
	matchAllReplicas bool
}

// ampSuffix returns the label-value suffix for amplification replica k. It must match exactly the
// suffix write-tee stamps on amplified series (value + "_amp" + N), so read replica k lands on
// write replica k's data.
func ampSuffix(k int) string {
	return "_amp" + strconv.Itoa(k)
}

// rewriteQuery parses a PromQL expression, suffixes every leaf vector selector's label-value
// matchers for the given amplification replica, and returns the re-serialized expression.
//
// Only leaf VectorSelector matchers are rewritten. Aggregation grouping (by/without/on), function
// arguments, offsets, @ modifiers, subqueries and binary operators are left untouched because they
// reference label NAMES (or non-selector syntax), not label values. MatrixSelector embeds a
// VectorSelector, so parser.Inspect visits the inner VectorSelector and its matchers are rewritten
// too.
func rewriteQuery(query string, replica int, opts rewriteOptions) (string, error) {
	expr, err := promQLParser.ParseExpr(query)
	if err != nil {
		return "", err
	}

	parser.Inspect(expr, func(node parser.Node, _ []parser.Node) error {
		if vs, ok := node.(*parser.VectorSelector); ok {
			vs.LabelMatchers = rewriteMatchers(vs.LabelMatchers, replica, opts)
		}
		return nil
	})

	return expr.String(), nil
}

// rewriteSelector parses a single metric selector (e.g. the value of a match[] parameter),
// suffixes its label-value matchers for the given replica, and re-serializes it. Serialization
// goes through a VectorSelector so a present __name__ matcher renders as name{...} and all matchers
// render with correct escaping.
func rewriteSelector(sel string, replica int, opts rewriteOptions) (string, error) {
	m, err := promQLParser.ParseMetricSelector(sel)
	if err != nil {
		return "", err
	}

	m = rewriteMatchers(m, replica, opts)

	// Populate Name from an equality __name__ matcher so the selector renders in the canonical
	// name{...} form (matching how parser.ParseExpr renders vector selectors). Without this the
	// printer would emit the equivalent {__name__="name",...} form.
	vs := &parser.VectorSelector{LabelMatchers: m}
	for _, mm := range m {
		if mm.Name == labels.MetricName && mm.Type == labels.MatchEqual {
			vs.Name = mm.Value
			break
		}
	}
	return vs.String(), nil
}

// rewriteMatchers returns a new matcher slice with label-value matchers suffixed for the given
// replica. See the design doc / spec for the per-matcher rules:
//
//   - __name__ matchers are never suffixed (metric names are shared across replicas).
//   - empty-value matchers (absence l="" / presence l!="") are left unchanged (write-tee only
//     suffixes labels that exist, so absent labels stay absent).
//   - =  and != : value -> value + suffix
//   - =~ and !~ : value -> "(?:" + value + ")" + suffix (regexes are fully anchored, so grouping
//     the original regex and appending the literal suffix matches "<original>_amp{k}" exactly).
//
// When opts.excludeAmplifiedNegative is set, negative matchers are instead rewritten to exclude the
// value across the base series and every amplified replica (value + ampVariantsSuffix), rather than
// only the _amp{replica} form. A != matcher becomes a !~ matcher (its literal value is regex-quoted)
// because a single != can only exclude one exact string.
//
// Regexp matchers must be rebuilt via labels.NewMatcher so the compiled *regexp is regenerated;
// mutating Matcher.Value in place would leave a stale compiled regex. For consistency we rebuild
// every changed matcher via NewMatcher.
func rewriteMatchers(ms []*labels.Matcher, replica int, opts rewriteOptions) []*labels.Matcher {
	suffix := ampSuffix(replica)
	out := make([]*labels.Matcher, len(ms))
	for i, mm := range ms {
		// Never suffix metric names or empty-value (absence/presence) matchers.
		if mm.Name == labels.MetricName || mm.Value == "" {
			out[i] = mm
			continue
		}

		newType := mm.Type
		var newValue string
		switch mm.Type {
		case labels.MatchEqual:
			if opts.matchAllReplicas {
				// Match the value across the base series and all _amp{N} variants. = can only match
				// one string, so switch to =~ and regex-quote the (literal) value.
				newType = labels.MatchRegexp
				newValue = regexp.QuoteMeta(mm.Value) + ampVariantsSuffix
			} else {
				newValue = mm.Value + suffix
			}
		case labels.MatchRegexp:
			if opts.matchAllReplicas {
				newValue = "(?:" + mm.Value + ")" + ampVariantsSuffix
			} else {
				newValue = "(?:" + mm.Value + ")" + suffix
			}
		case labels.MatchNotEqual:
			if opts.matchAllReplicas || opts.excludeAmplifiedNegative {
				// Exclude the literal value and all its _amp{N} variants. != can only exclude one
				// string, so switch to !~ and regex-quote the (literal) value.
				newType = labels.MatchNotRegexp
				newValue = regexp.QuoteMeta(mm.Value) + ampVariantsSuffix
			} else {
				newValue = mm.Value + suffix
			}
		case labels.MatchNotRegexp:
			if opts.matchAllReplicas || opts.excludeAmplifiedNegative {
				newValue = "(?:" + mm.Value + ")" + ampVariantsSuffix
			} else {
				newValue = "(?:" + mm.Value + ")" + suffix
			}
		default:
			out[i] = mm
			continue
		}

		// Rebuild so the compiled regex (for regexp matchers) is regenerated. NewMatcher only
		// fails if the (already-valid) regex fails to recompile with our wrapper, which can't
		// happen for a well-formed group; fall back to the original matcher if it ever does.
		nm, err := labels.NewMatcher(newType, mm.Name, newValue)
		if err != nil {
			out[i] = mm
			continue
		}
		out[i] = nm
	}
	return out
}
