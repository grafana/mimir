// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"cmp"
	"errors"
	"fmt"
	"math"
	"regexp/syntax"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/DmitriyVTitov/size"
	"github.com/cespare/xxhash/v2"
	prometheuslabels "github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
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
	// Every value a large alternation of literals accepts, as info()'s of every target, which the regex would check one
	// alternative after another: only native resources lookups compile one, without a matcher (lookupRegex).
	set map[string]struct{}
	// That a native resources lookup found the pattern doesn't make a set, so that the next ones don't parse it again.
	notSet atomic.Bool
	// What every accepted value starts with: with it, the values of a sorted dictionary that can
	// match are one range.
	prefix string
	// The regex's verdicts on the values lookups checked it against, once it proved slow.
	verdicts regexMemo
}

func (r *compiledRegex) isMatch(value string) bool {
	if r.set != nil {
		_, found := r.set[value]
		return found
	}
	if r.finite {
		_, found := slices.BinarySearch(r.values, value)
		return found
	}
	return r.matcher.MatchString(value)
}

// A slow regex, like the case-insensitive search for a word in any value a query without a metric name sends, checked
// every value of the label in each of a tenant's store shards, and again in each query shard of the query: its verdict on
// a value never changes, so they are kept for the next ones, which the cache of compiled regexes shares. A regex the
// matcher answers in less than slowRegexNs, as for a literal or a prefix, isn't worth a lookup. A regex keeps the verdicts
// of at most maxRegexMemoValues values, and all of them of at most maxRegexMemoTotal: past it, the ones used the longest
// ago drop theirs, as queries searching for one word after another each want their own for a few seconds.
const (
	regexMemoSamples   = 64
	maxRegexMemoValues = 1 << 16
	maxRegexMemoTotal  = 1 << 20
	regexMemoStripes   = 256
)

// slowRegexNs is how long a regex takes to answer on average, from its first answers, to keep its verdicts. A variable for
// tests.
var slowRegexNs int64 = 250

var (
	regexMemoTotal atomic.Int64
	// The memos of the regexes that proved slow, which the total is of.
	regexMemos struct {
		sync.Mutex
		slow []*regexMemo
	}
)

// evictRegexMemos drops the verdicts of the slow regexes used the longest ago, until the total is half its bound.
func evictRegexMemos() {
	if !regexMemos.TryLock() {
		return
	}
	defer regexMemos.Unlock()
	slices.SortFunc(regexMemos.slow, func(a, b *regexMemo) int { return cmp.Compare(a.used.Load(), b.used.Load()) })
	for _, memo := range regexMemos.slow {
		if regexMemoTotal.Load() < maxRegexMemoTotal/2 {
			return
		}
		memo.reset()
	}
}

// reset drops the memo's verdicts: lookups that have its table keep reading it.
func (m *regexMemo) reset() {
	m.table.Store(new(regexMemoTable))
	regexMemoTotal.Add(-m.values.Swap(0))
}

const (
	regexMemoSampling int32 = iota
	regexMemoFast
	regexMemoSlow
)

type regexMemo struct {
	state             atomic.Int32
	samples, sampleNs atomic.Int64
	values            atomic.Int64
	// When a lookup last missed the verdicts, in Unix nanoseconds.
	used atomic.Int64
	// Made once the regex proved slow, which most never do.
	table atomic.Pointer[regexMemoTable]
}

// regexMemoTable is a slow regex's verdicts, in many stripes, each on a cache line of its own: the query shards of a
// query look values up at once, and taking a stripe's read lock writes it.
type regexMemoTable [regexMemoStripes]struct {
	sync.RWMutex
	verdicts map[string]bool
	_        [32]byte
}

func countRegexEvaluation() {
	if countersEnabled {
		regexEvaluations.Add(1)
	}
}

// memoMatch is isMatch, from the regex's kept verdicts when it is slow: for the values of a label lookups check.
func (r *compiledRegex) memoMatch(value string) bool {
	if r.finite || r.set != nil {
		return r.isMatch(value)
	}
	memo := &r.verdicts
	switch memo.state.Load() {
	case regexMemoFast:
		countRegexEvaluation()
		return r.isMatch(value)
	case regexMemoSampling:
		countRegexEvaluation()
		start := time.Now()
		matched := r.isMatch(value)
		took := time.Since(start)
		if samples, total := memo.samples.Add(1), memo.sampleNs.Add(int64(took)); samples == regexMemoSamples {
			state := regexMemoFast
			if total/samples >= slowRegexNs {
				state = regexMemoSlow
				memo.table.Store(new(regexMemoTable))
				regexMemos.Lock()
				regexMemos.slow = append(regexMemos.slow, memo)
				regexMemos.Unlock()
			}
			memo.state.Store(state)
		}
		return matched
	}
	stripe := &memo.table.Load()[xxhash.Sum64String(value)%regexMemoStripes]
	stripe.RLock()
	matched, ok := stripe.verdicts[value]
	stripe.RUnlock()
	if ok {
		return matched
	}
	countRegexEvaluation()
	matched = r.isMatch(value)
	// On a miss rather than every lookup: a regex in use meets values it hasn't seen, and the clock costs more than a hit.
	memo.used.Store(time.Now().UnixNano())
	if regexMemoTotal.Load() >= maxRegexMemoTotal {
		evictRegexMemos()
	}
	if memo.values.Load() < maxRegexMemoValues && regexMemoTotal.Load() < maxRegexMemoTotal {
		stripe.Lock()
		if stripe.verdicts == nil {
			stripe.verdicts = map[string]bool{}
		}
		if _, ok := stripe.verdicts[value]; !ok {
			// Its own copy: the value is a series' labels', which may be removed.
			stripe.verdicts[strings.Clone(value)] = matched
			memo.values.Add(1)
			regexMemoTotal.Add(1)
		}
		stripe.Unlock()
	}
	return matched
}

// acceptedValues returns every value the regex accepts, when there are few.
func (r *compiledRegex) acceptedValues() ([]string, bool) {
	return r.values, r.finite
}

// The most values a regex is looked up by: beyond, checking the index's values costs less.
const maxAcceptedValues = 256

// languageBound is how far language enumerates a pattern's values: at most values of them, of at most bytes in all.
// strict also refuses case folding, which multiplies a literal's values by its letters' cases, and U+FFFD, which Go's
// regexp reads an invalid byte as, so that a set of the values accepts exactly what the regex does.
type languageBound struct {
	values, bytes int
	strict        bool
}

// The bound of the values a regex is looked up by.
var acceptedValuesBound = languageBound{values: maxAcceptedValues, bytes: math.MaxInt}

// acceptedValues returns every value pattern, matched as a whole, accepts, sorted, when it accepts
// at most maxAcceptedValues: its language is finite and built from literals, classes and bounded
// repetitions. It parses like Prometheus's regex matcher, where `.` also matches a newline.
func acceptedValues(pattern string) ([]string, bool) {
	parsed, err := syntax.Parse(pattern, syntax.Perl|syntax.DotNL)
	if err != nil {
		return nil, false
	}
	values, ok := language(parsed, acceptedValuesBound)
	if !ok || len(values) > maxAcceptedValues {
		return nil, false
	}
	slices.Sort(values)
	return slices.Compact(values), true
}

func language(re *syntax.Regexp, bound languageBound) ([]string, bool) {
	switch re.Op {
	case syntax.OpEmptyMatch:
		return []string{""}, true
	case syntax.OpNoMatch:
		return []string{}, true
	case syntax.OpLiteral:
		if re.Flags&syntax.FoldCase == 0 {
			// In one step: a rune at a time copies the literal over and over, quadratic in its length.
			literal := string(re.Rune)
			if len(literal) > bound.bytes || (bound.strict && slices.Contains(re.Rune, utf8.RuneError)) {
				return nil, false
			}
			return []string{literal}, true
		}
		if bound.strict {
			return nil, false
		}
		values := []string{""}
		for _, r := range re.Rune {
			variants := []rune{r}
			for f := unicode.SimpleFold(r); f != r; f = unicode.SimpleFold(f) {
				variants = append(variants, f)
			}
			next, ok := productRunes(values, variants, bound)
			if !ok {
				return nil, false
			}
			values = next
		}
		return values, true
	case syntax.OpCharClass:
		var values []string
		bytes := 0
		for index := 0; index+1 < len(re.Rune); index += 2 {
			if bound.strict && re.Rune[index] <= utf8.RuneError && utf8.RuneError <= re.Rune[index+1] {
				return nil, false
			}
			for r := re.Rune[index]; r <= re.Rune[index+1]; r++ {
				if len(values) == bound.values {
					return nil, false
				}
				if !utf8.ValidRune(r) {
					return nil, false
				}
				if bytes += utf8.RuneLen(r); bytes > bound.bytes {
					return nil, false
				}
				values = append(values, string(r))
			}
		}
		return values, true
	case syntax.OpCapture:
		return language(re.Sub[0], bound)
	case syntax.OpQuest:
		return repeat(re.Sub[0], 0, 1, bound)
	case syntax.OpRepeat:
		if re.Max < 0 {
			return nil, false
		}
		return repeat(re.Sub[0], re.Min, re.Max, bound)
	case syntax.OpConcat:
		values := []string{""}
		for _, sub := range re.Sub {
			subValues, ok := language(sub, bound)
			if !ok {
				return nil, false
			}
			if values, ok = product(values, subValues, bound); !ok {
				return nil, false
			}
		}
		return values, true
	case syntax.OpAlternate:
		var values []string
		bytes := 0
		for _, sub := range re.Sub {
			subValues, ok := language(sub, bound)
			if !ok {
				return nil, false
			}
			values = append(values, subValues...)
			if bytes += totalBytes(subValues); len(values) > bound.values || bytes > bound.bytes {
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

func repeat(sub *syntax.Regexp, minCount, maxCount int, bound languageBound) ([]string, bool) {
	subValues, ok := language(sub, bound)
	if !ok {
		return nil, false
	}
	var values []string
	bytes := 0
	repeated := []string{""}
	for count := 0; count <= maxCount; count++ {
		if count >= minCount {
			values = append(values, repeated...)
			bytes += totalBytes(repeated)
		}
		if count < maxCount {
			if repeated, ok = product(repeated, subValues, bound); !ok {
				return nil, false
			}
		}
		if len(values) > bound.values || bytes > bound.bytes {
			return nil, false
		}
	}
	return values, true
}

// product concatenates every prefix with every suffix, unless that is more values or bytes than bound, which it
// checks before it allocates them.
func product(prefixes, suffixes []string, bound languageBound) ([]string, bool) {
	if len(prefixes)*len(suffixes) > bound.values {
		return nil, false
	}
	// Each prefix appears once with every suffix, and each suffix with every prefix. Short of overflowing: both counts
	// are within the values' bound, and each sum within the bytes' of the values the factors are.
	if bound.bytes != math.MaxInt && len(suffixes)*totalBytes(prefixes)+len(prefixes)*totalBytes(suffixes) > bound.bytes {
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

func productRunes(prefixes []string, runes []rune, bound languageBound) ([]string, bool) {
	suffixes := make([]string, len(runes))
	for index, r := range runes {
		suffixes[index] = string(r)
	}
	return product(prefixes, suffixes, bound)
}

func totalBytes(values []string) int {
	bytes := 0
	for _, value := range values {
		bytes += len(value)
	}
	return bytes
}

// Queriers send the same few patterns with every request: like Go's matcher cache, compiled
// regexes are kept, up to bounds on how many and how big so arbitrary queries can't grow it.
// Most are a few KB. info() selects target_info with an alternation of every target of the
// series it joins, which is MBs compiled and changes with the targets: kept by their count
// alone, a few thousand of them filled an ingester's heap. One bigger than maxCachedRegexBytes
// is compiled for its query alone.
const cachedRegexes = 4096

// The bytes of compiled regexes kept in all, and of each. Variables for tests.
var (
	maxCachedRegexesBytes = 128 << 20
	maxCachedRegexBytes   = 2 << 20
)

// A pattern longer than this isn't sized to be kept: an alternation of literals compiles to about
// 40 times its length, more than maxCachedRegexBytes, and sizing one took a fifth of compiling it.
const maxSizedRegexPattern = 64 << 10

// regexKey is a cached regex's pattern, and whether it is a native resources lookup's set: a select or a label lookup
// mustn't be answered by a set, nor a lookup by one after its tenant's switch went off.
type regexKey struct {
	pattern string
	set     bool
}

var regexCache = struct {
	sync.Mutex
	byPattern map[regexKey]*compiledRegex
	// The bytes of the regexes byPattern has, by regexBytes.
	bytes int
}{byPattern: map[regexKey]*compiledRegex{}}

// setEntryBytes is about what a Go map takes for each of a set's values, besides the value's bytes: its string header
// and the bucket's share.
const setEntryBytes = 32

// regexBytes is how big a compiled regex is to keep, and whether it's small enough: its matcher, sized as Prometheus's
// cache of them sizes them, or its set, and the strings it adds. size.Of can't size a whole compiledRegex, as its memo
// has an unsafe.Pointer.
func regexBytes(re *compiledRegex, pattern string) (int, bool) {
	if len(pattern) > maxSizedRegexPattern {
		return 0, false
	}
	bytes := len(re.pattern) + len(re.prefix)
	if re.set != nil {
		for value := range re.set {
			bytes += len(value) + setEntryBytes
		}
	} else {
		matcher := size.Of(re.matcher)
		if matcher < 0 {
			return 0, false
		}
		bytes += matcher
	}
	for _, value := range re.values {
		bytes += len(value)
	}
	return bytes, bytes <= maxCachedRegexBytes
}

// cachedRegex returns the regex cached for key.
func cachedRegex(key regexKey) (*compiledRegex, bool) {
	regexCache.Lock()
	defer regexCache.Unlock()
	cached, ok := regexCache.byPattern[key]
	return cached, ok
}

// cacheRegex keeps compiled for key, when it's small enough, and returns the one kept.
func cacheRegex(key regexKey, compiled *compiledRegex) *compiledRegex {
	bytes, keep := regexBytes(compiled, key.pattern)
	if !keep {
		// Its verdicts would be its query's alone, and a slow regex's memo, which the list of slow ones holds until the
		// cache is cleared, would keep it.
		compiled.verdicts.state.Store(regexMemoFast)
		return compiled
	}
	regexCache.Lock()
	defer regexCache.Unlock()
	// The query shards of a query compile its regexes at once: they share the one kept first, and with it its verdicts.
	if cached, ok := regexCache.byPattern[key]; ok {
		return cached
	}
	if len(regexCache.byPattern) >= cachedRegexes || regexCache.bytes+bytes > maxCachedRegexesBytes {
		regexMemos.Lock()
		for _, memo := range regexMemos.slow {
			memo.reset()
		}
		regexMemos.slow = nil
		regexMemos.Unlock()
		clear(regexCache.byPattern)
		regexCache.bytes = 0
	}
	regexCache.byPattern[key] = compiled
	regexCache.bytes += bytes
	return compiled
}

// anchoredRegex compiles a matcher's regex, anchored like Prometheus's.
func anchoredRegex(pattern string) (*compiledRegex, error) {
	if cached, ok := cachedRegex(regexKey{pattern: pattern}); ok {
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
	compiled := &compiledRegex{matcher: matcher, pattern: "^(?:" + pattern + ")$", values: values, finite: finite, prefix: literalPrefix(pattern)}
	return cacheRegex(regexKey{pattern: pattern}, compiled), nil
}

// lookupRegex compiles a native resources lookup's regex: a large alternation of literals, as info()'s of every
// target, is matched by a set of its values, which a lookup checks every series of the tenant's shards against. Any
// other pattern is compiled as anchoredRegex compiles it, including one of at most maxAcceptedValues values, which
// stays finite so that the lookup gets postings for it.
func lookupRegex(pattern string) (*compiledRegex, error) {
	key := regexKey{pattern: pattern, set: true}
	if cached, ok := cachedRegex(key); ok {
		return cached, nil
	}
	if cached, ok := cachedRegex(regexKey{pattern: pattern}); ok && cached.notSet.Load() {
		return cached, nil
	}
	values, ok := setValues(pattern)
	if !ok {
		compiled, err := anchoredRegex(pattern)
		if err != nil {
			return nil, err
		}
		compiled.notSet.Store(true)
		return compiled, nil
	}
	if countersEnabled {
		regexSets.Add(1)
	}
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	// Patterns past maxSizedRegexPattern aren't kept: with targets changing between rounds, a set would be used within a
	// round only, and they'd fill the cache, whose clearing drops every regex and memo.
	compiled := &compiledRegex{pattern: "^(?:" + pattern + ")$", set: set, prefix: valuesPrefix(values)}
	return cacheRegex(key, compiled), nil
}

// setValues returns the values a pattern accepts, when it makes a set: a finite language of more than
// maxAcceptedValues literals, with no more values than the pattern has alternatives and no more bytes than the
// pattern has, counted in the source, since Go's parser factors an alternation of literals into classes and nested
// concatenations, and no case folding or U+FFFD.
func setValues(pattern string) ([]string, bool) {
	parsed, err := syntax.Parse(pattern, syntax.Perl|syntax.DotNL)
	if err != nil {
		return nil, false
	}
	values, ok := language(parsed, languageBound{values: strings.Count(pattern, "|") + 1, bytes: len(pattern), strict: true})
	if !ok || len(values) <= maxAcceptedValues {
		return nil, false
	}
	return values, true
}

// valuesPrefix is what every value starts with, with which a sorted dictionary's values that can match are one range.
func valuesPrefix(values []string) string {
	if len(values) == 0 {
		return ""
	}
	prefix := values[0]
	for _, value := range values[1:] {
		n := 0
		for n < len(prefix) && n < len(value) && prefix[n] == value[n] {
			n++
		}
		prefix = prefix[:n]
	}
	// Whole runes, as literalPrefix's: a byte prefix can end inside one.
	for len(prefix) > 0 && !utf8.ValidString(prefix) {
		prefix = prefix[:len(prefix)-1]
	}
	return strings.Clone(prefix)
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
// maxRegexVerdicts is how many values a regexVerdicts keeps the verdict of for each regex matcher.
const maxRegexVerdicts = 1 << 16

// regexVerdicts is a lookup's verdicts of its regex matchers on each value they were asked about: a label's values
// repeat over the series a lookup checks, where each check ran the regex again. It lives as long as the lookup, so its
// values may be in a cold block's mapping, which the lookup holds.
type regexVerdicts struct {
	byMatcher []map[string]bool
}

func newRegexVerdicts(matchers []compiledMatcher) *regexVerdicts {
	verdicts := &regexVerdicts{byMatcher: make([]map[string]bool, len(matchers))}
	for index := range matchers {
		if kind := matchers[index].kind; kind == kindRegex || kind == kindNotRegex {
			verdicts.byMatcher[index] = map[string]bool{}
		}
	}
	return verdicts
}

// matches is matches, with the regex matchers' verdicts on values seen before.
func matchesWithVerdicts[L labels.LabelSet](ls L, matchers []compiledMatcher, verdicts *regexVerdicts) bool {
	for index := range matchers {
		matcher := &matchers[index]
		known := verdicts.byMatcher[index]
		if known == nil {
			if !matches(ls, matchers[index:index+1]) {
				return false
			}
			continue
		}
		value := ls.Get(matcher.name)
		verdict, ok := known[value]
		if !ok {
			countLabelValueRead()
			verdict = matcher.matchesValue(value)
			if len(known) < maxRegexVerdicts {
				known[value] = verdict
			}
		}
		if !verdict {
			return false
		}
	}
	return true
}

// matches is generic over the labels so that a series' own, of a string type, aren't boxed into an interface, which
// allocated for every series a query checked.
func matches[L labels.LabelSet](ls L, matchers []compiledMatcher) bool {
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

// literalPrefix is what every value pattern accepts starts with.
func literalPrefix(pattern string) string {
	parsed, err := syntax.Parse(pattern, syntax.Perl|syntax.DotNL)
	if err != nil {
		return ""
	}
	prog, err := syntax.Compile(parsed.Simplify())
	if err != nil {
		return ""
	}
	prefix, _ := prog.Prefix()
	return prefix
}
