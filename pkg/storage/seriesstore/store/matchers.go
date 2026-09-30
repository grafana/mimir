// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"errors"
	"fmt"
	"regexp/syntax"
	"slices"
	"strconv"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	prometheuslabels "github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/labels"
)

// Matcher types, as cortex.LabelMatcher numbers them.
const (
	MatchEqual    int32 = 0
	MatchNotEqual int32 = 1
	MatchRegex    int32 = 2
	MatchNotRegex int32 = 3
)

// LabelMatcher is a query's label matcher, as the querier sends it.
type LabelMatcher struct {
	Type  int32
	Name  string
	Value string
}

type matcherKind uint8

const (
	kindEqual matcherKind = iota
	kindNotEqual
	kindRegex
	kindNotRegex
	kindShard
)

// compiledMatcher is a matcher ready to check values. Shard matchers select series by
// `labels.StableHash` modulo count.
type compiledMatcher struct {
	kind  matcherKind
	name  string
	value string
	re    *compiledRegex
	// The zero-based shard index and the shard count.
	shardIndex, shardCount uint64
}

func (m *compiledMatcher) labelName() (string, bool) {
	if m.kind == kindShard {
		return "", false
	}
	return m.name, true
}

func (m *compiledMatcher) matchesValue(value string) bool {
	switch m.kind {
	case kindEqual:
		return value == m.value
	case kindNotEqual:
		return value != m.value
	case kindRegex:
		return m.re.isMatch(value)
	case kindNotRegex:
		return !m.re.isMatch(value)
	default:
		return true
	}
}

// compiledRegex is a matcher's regex, anchored like Prometheus's, with every value it accepts
// when there are few, sorted. Queriers send alternations of literals, like the `a|b|c` of
// dashboard variables: like Prometheus's set matches, their values are looked up in the index
// instead of the regex checking every value.
type compiledRegex struct {
	matcher *prometheuslabels.FastRegexMatcher
	pattern string
	values  []string
	finite  bool
}

func (r *compiledRegex) isMatch(value string) bool {
	if r.finite {
		_, found := slices.BinarySearch(r.values, value)
		return found
	}
	return r.matcher.MatchString(value)
}

// acceptedValues returns every value the regex accepts, when there are few.
func (r *compiledRegex) acceptedValues() ([]string, bool) {
	return r.values, r.finite
}

// The most values a regex is looked up by: beyond, checking the index's values costs less.
const maxAcceptedValues = 256

// acceptedValues returns every value pattern, matched as a whole, accepts, sorted, when it accepts
// at most maxAcceptedValues: its language is finite and built from literals, classes and bounded
// repetitions. It parses like Prometheus's regex matcher, where `.` also matches a newline.
func acceptedValues(pattern string) ([]string, bool) {
	parsed, err := syntax.Parse(pattern, syntax.Perl|syntax.DotNL)
	if err != nil {
		return nil, false
	}
	values, ok := language(parsed)
	if !ok || len(values) > maxAcceptedValues {
		return nil, false
	}
	slices.Sort(values)
	return slices.Compact(values), true
}

func language(re *syntax.Regexp) ([]string, bool) {
	switch re.Op {
	case syntax.OpEmptyMatch:
		return []string{""}, true
	case syntax.OpNoMatch:
		return []string{}, true
	case syntax.OpLiteral:
		values := []string{""}
		for _, r := range re.Rune {
			variants := []rune{r}
			if re.Flags&syntax.FoldCase != 0 {
				for f := unicode.SimpleFold(r); f != r; f = unicode.SimpleFold(f) {
					variants = append(variants, f)
				}
			}
			next, ok := productRunes(values, variants)
			if !ok {
				return nil, false
			}
			values = next
		}
		return values, true
	case syntax.OpCharClass:
		var values []string
		for index := 0; index+1 < len(re.Rune); index += 2 {
			for r := re.Rune[index]; r <= re.Rune[index+1]; r++ {
				if len(values) == maxAcceptedValues {
					return nil, false
				}
				if !utf8.ValidRune(r) {
					return nil, false
				}
				values = append(values, string(r))
			}
		}
		return values, true
	case syntax.OpCapture:
		return language(re.Sub[0])
	case syntax.OpQuest:
		return repeat(re.Sub[0], 0, 1)
	case syntax.OpRepeat:
		if re.Max < 0 {
			return nil, false
		}
		return repeat(re.Sub[0], re.Min, re.Max)
	case syntax.OpConcat:
		values := []string{""}
		for _, sub := range re.Sub {
			subValues, ok := language(sub)
			if !ok {
				return nil, false
			}
			if values, ok = product(values, subValues); !ok {
				return nil, false
			}
		}
		return values, true
	case syntax.OpAlternate:
		var values []string
		for _, sub := range re.Sub {
			subValues, ok := language(sub)
			if !ok {
				return nil, false
			}
			values = append(values, subValues...)
			if len(values) > maxAcceptedValues {
				return nil, false
			}
		}
		return values, true
	default:
		// Unbounded repetitions, any character, and anchors and word boundaries, which depend on
		// what surrounds a value.
		return nil, false
	}
}

func repeat(sub *syntax.Regexp, minCount, maxCount int) ([]string, bool) {
	subValues, ok := language(sub)
	if !ok {
		return nil, false
	}
	var values []string
	repeated := []string{""}
	for count := 0; count <= maxCount; count++ {
		if count >= minCount {
			values = append(values, repeated...)
		}
		if count < maxCount {
			if repeated, ok = product(repeated, subValues); !ok {
				return nil, false
			}
		}
		if len(values) > maxAcceptedValues {
			return nil, false
		}
	}
	return values, true
}

func product(prefixes, suffixes []string) ([]string, bool) {
	if len(prefixes)*len(suffixes) > maxAcceptedValues {
		return nil, false
	}
	values := make([]string, 0, len(prefixes)*len(suffixes))
	for _, prefix := range prefixes {
		for _, suffix := range suffixes {
			values = append(values, prefix+suffix)
		}
	}
	return values, true
}

func productRunes(prefixes []string, runes []rune) ([]string, bool) {
	suffixes := make([]string, len(runes))
	for index, r := range runes {
		suffixes[index] = string(r)
	}
	return product(prefixes, suffixes)
}

// Queriers send the same few patterns with every request: like Go's matcher cache, compiled
// regexes are kept, up to a bound so arbitrary queries can't grow it.
const cachedRegexes = 4096

var regexCache = struct {
	sync.Mutex
	byPattern map[string]*compiledRegex
}{byPattern: map[string]*compiledRegex{}}

// anchoredRegex compiles a matcher's regex, anchored like Prometheus's.
func anchoredRegex(pattern string) (*compiledRegex, error) {
	regexCache.Lock()
	cached, ok := regexCache.byPattern[pattern]
	regexCache.Unlock()
	if ok {
		return cached, nil
	}
	if countersEnabled {
		regexCompiles.Add(1)
	}
	matcher, err := prometheuslabels.NewFastRegexMatcher(pattern)
	if err != nil {
		return nil, err
	}
	values, finite := acceptedValues(pattern)
	compiled := &compiledRegex{matcher: matcher, pattern: "^(?:" + pattern + ")$", values: values, finite: finite}
	regexCache.Lock()
	if len(regexCache.byPattern) >= cachedRegexes {
		clear(regexCache.byPattern)
	}
	regexCache.byPattern[pattern] = compiled
	regexCache.Unlock()
	return compiled, nil
}

func compileMatchers(matchers []LabelMatcher) ([]compiledMatcher, error) {
	compiled := make([]compiledMatcher, 0, len(matchers))
	for _, matcher := range matchers {
		if matcher.Name == shardLabel && matcher.Type == MatchEqual {
			index, count, err := parseShard(matcher.Value)
			if err != nil {
				return nil, err
			}
			compiled = append(compiled, compiledMatcher{kind: kindShard, shardIndex: index, shardCount: count})
			continue
		}
		switch matcher.Type {
		case MatchEqual:
			compiled = append(compiled, compiledMatcher{kind: kindEqual, name: matcher.Name, value: matcher.Value})
		case MatchNotEqual:
			compiled = append(compiled, compiledMatcher{kind: kindNotEqual, name: matcher.Name, value: matcher.Value})
		case MatchRegex, MatchNotRegex:
			re, err := anchoredRegex(matcher.Value)
			if err != nil {
				return nil, err
			}
			kind := kindRegex
			if matcher.Type == MatchNotRegex {
				kind = kindNotRegex
			}
			compiled = append(compiled, compiledMatcher{kind: kind, name: matcher.Name, re: re})
		default:
			return nil, fmt.Errorf("invalid matcher type %d", matcher.Type)
		}
	}
	return compiled, nil
}

const shardLabel = "__query_shard__"

func parseShard(value string) (uint64, uint64, error) {
	parts := strings.Split(value, "_")
	if len(parts) != 3 || parts[1] != "of" {
		return 0, 0, fmt.Errorf("invalid shard ID %q", value)
	}
	oneBased, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid shard ID %q: %w", value, err)
	}
	count, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid shard ID %q: %w", value, err)
	}
	if oneBased == 0 || count == 0 || oneBased > count {
		return 0, 0, fmt.Errorf("invalid shard ID %q", value)
	}
	return oneBased - 1, count, nil
}

// matches checks every matcher against labels, which read missing labels as empty.
func matches(ls labels.LabelSet, matchers []compiledMatcher) bool {
	for index := range matchers {
		matcher := &matchers[index]
		if matcher.kind == kindShard {
			if labels.StableHash(ls)%matcher.shardCount != matcher.shardIndex {
				return false
			}
			continue
		}
		countLabelValueRead()
		if !matcher.matchesValue(ls.Get(matcher.name)) {
			return false
		}
	}
	return true
}

// inQueryShard reports whether a series with shardHash is in every query shard of shard, which
// holds only shard matchers.
func inQueryShard(shardHash uint64, shard []compiledMatcher) bool {
	for index := range shard {
		if shardHash%shard[index].shardCount != shard[index].shardIndex {
			return false
		}
	}
	return true
}

// nameMatcherKey identifies a name matcher other than an equality, by type and pattern.
type nameMatcherKey struct {
	kind    matcherKind
	pattern string
}

func keyOf(matcher *compiledMatcher) nameMatcherKey {
	switch matcher.kind {
	case kindNotEqual:
		return nameMatcherKey{kindNotEqual, matcher.value}
	case kindRegex, kindNotRegex:
		return nameMatcherKey{matcher.kind, matcher.re.pattern}
	default:
		panic(errors.New("key of an equality or shard matcher"))
	}
}
