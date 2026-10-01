// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"cmp"
	"slices"
	"sync"

	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
)

// seriesEntry is a stored series with its labels and their hash.
type seriesEntry struct {
	hash   uint64
	labels labels.Labels
	// The next entry of the group with the same hash, or -1: a hash collision only adds a
	// comparison.
	next   int32
	series Series
}

// group holds the series of one metric name, found by label hash so ingesting into an existing
// series allocates nothing. The entries are one slice, which a name matcher scans in order.
type group struct {
	entries []seriesEntry
	first   map[uint64]int32
}

func (g *group) lookup(hash uint64, visit func(entry *seriesEntry) bool) bool {
	index, ok := g.first[hash]
	for ok && index >= 0 {
		entry := &g.entries[index]
		if visit(entry) {
			return true
		}
		index = entry.next
	}
	return false
}

// seriesRef locates a series: its name group and label hash.
type seriesRef struct {
	group uint32
	hash  uint64
}

func compareRefs(a, b seriesRef) int {
	if c := cmp.Compare(a.group, b.group); c != 0 {
		return c
	}
	return cmp.Compare(a.hash, b.hash)
}

// A posting is the one series with a value or, with this bit, the index of its series in
// sharedPostings. Most values of high-cardinality labels belong to one series, so every series
// pays for a few bytes per label instead of a list.
const sharedPosting = uint32(1) << 31

// Distinct values a query remembers its regex matchers' results for: labels queried by regex,
// like job, have few values, which Go's postings match once each; beyond this, each series checks.
const rememberedValues = 256

// seriesByName groups series by metric name, so a `__name__` matcher only visits its own metric,
// and keeps postings for the other labels like the Go head index, so a selective equality matcher
// only visits its series.
type seriesByName struct {
	names  map[string]uint32
	groups []group
	// Each name group's name: groups are numbered as names appear and never removed.
	groupNames []string
	// By label name id, then 32 bits of a value's hash, the ids of every series with that label as
	// a posting. Series ids index refs. A hash collision only adds candidates, which the matchers
	// then check.
	postings       []map[uint32]uint32
	sharedPostings [][]uint32
	refs           []seriesRef
	// By label name id, how many series have the label: its postings list them all, and queries
	// compare their size with other candidates' before listing them.
	labelSeries []uint32
	// The name groups each name matcher other than an equality accepted, with how many groups were
	// checked: a sharded query sends the same one in every request, and a metric appearing only
	// needs its own name checked.
	nameMatchesLock sync.Mutex
	nameMatches     map[nameMatcherKey]nameMatch
	len             int
}

type nameMatch struct {
	checked int
	groups  []uint32
}

func newSeriesByName() *seriesByName {
	return &seriesByName{names: map[string]uint32{}}
}

func postingKey(value string) uint32 {
	return uint32(labels.ValueHash(value))
}

// getOrInsert finds the series with labels pairs, sorted with hash hash, or adds it with
// newLabels. The entry stays valid until the next insertion into its group.
func (b *seriesByName) getOrInsert(name string, hash uint64, pairs [][2]string, newLabels func() labels.Labels) (*seriesEntry, bool) {
	groupID, ok := b.names[name]
	if !ok {
		groupID = uint32(len(b.groups))
		owned := string([]byte(name))
		b.names[owned] = groupID
		b.groupNames = append(b.groupNames, owned)
		b.groups = append(b.groups, group{first: map[uint64]int32{}})
	}
	g := &b.groups[groupID]
	var found *seriesEntry
	g.lookup(hash, func(entry *seriesEntry) bool {
		if entry.labels.EqPairs(pairs) {
			found = entry
			return true
		}
		return false
	})
	if found != nil {
		return found, false
	}
	stored := newLabels()
	return b.add(groupID, hash, stored, Series{}), true
}

func (b *seriesByName) add(groupID uint32, hash uint64, stored labels.Labels, series Series) *seriesEntry {
	g := &b.groups[groupID]
	next := int32(-1)
	if first, ok := g.first[hash]; ok {
		next = first
	}
	g.entries = append(g.entries, seriesEntry{hash: hash, labels: stored, next: next, series: series})
	g.first[hash] = int32(len(g.entries) - 1)
	b.len++
	b.addPostings(stored, groupID, hash)
	return &g.entries[len(g.entries)-1]
}

// insert adds a series under key unless one with the same labels exists.
func (b *seriesByName) insert(hash uint64, stored labels.Labels, series Series) bool {
	name := stored.Get(metricNameLabel)
	groupID, ok := b.names[name]
	if !ok {
		groupID = uint32(len(b.groups))
		owned := string([]byte(name))
		b.names[owned] = groupID
		b.groupNames = append(b.groupNames, owned)
		b.groups = append(b.groups, group{first: map[uint64]int32{}})
	}
	if b.groups[groupID].lookup(hash, func(entry *seriesEntry) bool { return entry.labels == stored }) {
		return false
	}
	b.add(groupID, hash, stored, series)
	return true
}

const metricNameLabel = "__name__"

var metricNameID = labels.Intern(metricNameLabel)

func (b *seriesByName) addPostings(stored labels.Labels, groupID uint32, hash uint64) {
	id := uint32(len(b.refs))
	if id&sharedPosting != 0 {
		panic("fewer than 2^31 series per tenant shard")
	}
	b.refs = append(b.refs, seriesRef{groupID, hash})
	rest := string(stored)
	for len(rest) > 0 {
		name := uint32(takeUvarintString(&rest))
		size := takeUvarintString(&rest)
		value := rest[:size]
		rest = rest[size:]
		if name == metricNameID {
			continue
		}
		for int(name) >= len(b.postings) {
			b.postings = append(b.postings, nil)
			b.labelSeries = append(b.labelSeries, 0)
		}
		values := b.postings[name]
		if values == nil {
			values = map[uint32]uint32{}
			b.postings[name] = values
		}
		b.labelSeries[name]++
		key := postingKey(value)
		posting, ok := values[key]
		switch {
		case !ok:
			values[key] = id
		case posting&sharedPosting == 0:
			b.sharedPostings = append(b.sharedPostings, []uint32{posting, id})
			values[key] = sharedPosting | uint32(len(b.sharedPostings)-1)
		default:
			index := posting &^ sharedPosting
			b.sharedPostings[index] = append(b.sharedPostings[index], id)
		}
	}
}

// posting returns the ids of a posting.
func (b *seriesByName) posting(posting uint32) (one uint32, many []uint32) {
	if posting&sharedPosting == 0 {
		return posting, nil
	}
	return 0, b.sharedPostings[posting&^sharedPosting]
}

func (b *seriesByName) postingLen(posting uint32) int {
	if posting&sharedPosting == 0 {
		return 1
	}
	return len(b.sharedPostings[posting&^sharedPosting])
}

// seriesWith returns how many series have the label name id.
func (b *seriesByName) seriesWith(name uint32, known bool) int {
	if !known || int(name) >= len(b.labelSeries) {
		return 0
	}
	return int(b.labelSeries[name])
}

// refsWith returns the refs of every series with the label, once each.
func (b *seriesByName) refsWith(name uint32, known bool) []seriesRef {
	if !known || int(name) >= len(b.postings) {
		return nil
	}
	refs := make([]seriesRef, 0, b.labelSeries[name])
	for _, posting := range b.postings[name] {
		one, many := b.posting(posting)
		if many == nil {
			refs = append(refs, b.refs[one])
			continue
		}
		for _, id := range many {
			refs = append(refs, b.refs[id])
		}
	}
	return sortRefs(refs)
}

// refsOf returns the refs of the postings, once each.
func (b *seriesByName) refsOf(postings []uint32, total int) []seriesRef {
	refs := make([]seriesRef, 0, total)
	for _, posting := range postings {
		one, many := b.posting(posting)
		if many == nil {
			refs = append(refs, b.refs[one])
			continue
		}
		for _, id := range many {
			refs = append(refs, b.refs[id])
		}
	}
	return sortRefs(refs)
}

func sortRefs(refs []seriesRef) []seriesRef {
	slices.SortFunc(refs, compareRefs)
	return slices.Compact(refs)
}

func (b *seriesByName) forEach(visit func(entry *seriesEntry)) {
	for groupID := range b.groups {
		entries := b.groups[groupID].entries
		for index := range entries {
			visit(&entries[index])
		}
	}
}

// visitRefs calls visit for every series of refs, stopping when it returns false.
func (b *seriesByName) visitRefs(refs []seriesRef, visit func(entry *seriesEntry) bool) {
	for _, ref := range refs {
		stop := b.groups[ref.group].lookup(ref.hash, func(entry *seriesEntry) bool {
			return entry.hash == ref.hash && !visit(entry)
		})
		if stop {
			return
		}
	}
}

// retain keeps the series keep accepts, rebuilding the postings when any is removed: removals are
// rare and batched by retention.
func (b *seriesByName) retain(keep func(entry *seriesEntry) bool) {
	total := 0
	for groupID := range b.groups {
		g := &b.groups[groupID]
		kept := g.entries[:0]
		for index := range g.entries {
			if keep(&g.entries[index]) {
				kept = append(kept, g.entries[index])
			}
		}
		clear(g.entries[len(kept):])
		// Head compaction and retention remove whole hours of series at once; give back their
		// slots, which are most of an entry's cost once a group lost half its series. Growing the
		// slice again copies it once, as appending to it would have anyway.
		if cap(kept) > max(len(kept)+len(kept)/4, 8) {
			kept = slices.Clone(kept)
		}
		g.entries = kept
		total += len(kept)
	}
	if total == b.len {
		return
	}
	b.postings = nil
	b.sharedPostings = nil
	b.refs = nil
	b.labelSeries = nil
	for groupID := range b.groups {
		g := &b.groups[groupID]
		g.first = make(map[uint64]int32, len(g.entries))
		for index := range g.entries {
			entry := &g.entries[index]
			entry.next = -1
			if first, ok := g.first[entry.hash]; ok {
				entry.next = first
			}
			g.first[entry.hash] = int32(index)
			b.addPostings(entry.labels, uint32(groupID), entry.hash)
		}
	}
	b.len = total
}

// nameGroups returns the name groups whose name matcher, a name matcher other than an equality,
// accepts.
func (b *seriesByName) nameGroups(matcher *compiledMatcher) []uint32 {
	if matcher.kind == kindRegex {
		if values, ok := matcher.re.acceptedValues(); ok {
			groups := make([]uint32, 0, len(values))
			for _, value := range values {
				if id, ok := b.names[value]; ok {
					groups = append(groups, id)
				}
			}
			slices.Sort(groups)
			return groups
		}
	}
	key := keyOf(matcher)
	b.nameMatchesLock.Lock()
	defer b.nameMatchesLock.Unlock()
	cached, ok := b.nameMatches[key]
	if ok && cached.checked == len(b.groupNames) {
		return cached.groups
	}
	// Only the names that appeared since are checked.
	groups := slices.Clone(cached.groups)
	for index := cached.checked; index < len(b.groupNames); index++ {
		if countersEnabled {
			nameMatchCount.Add(1)
		}
		if matcher.matchesValue(b.groupNames[index]) {
			groups = append(groups, uint32(index))
		}
	}
	// Bounded, so arbitrary queries can't grow it.
	if b.nameMatches == nil || len(b.nameMatches) >= 1024 {
		b.nameMatches = map[nameMatcherKey]nameMatch{}
	}
	b.nameMatches[key] = nameMatch{checked: len(b.groupNames), groups: groups}
	return groups
}

// resolvedMatcher is a matcher with its label name looked up once for a query, and what it
// remembers of the values it checked.
type resolvedMatcher struct {
	matcher *compiledMatcher
	nameID  uint32
	known   bool
	// For a matcher that accepts the empty value, the hashes of the series with its label when
	// few have it: the others pass without reading their labels.
	withLabel  map[uint64]struct{}
	remembered map[string]bool
}

// matching calls visit for every series that matches matchers, until it returns false.
func (b *seriesByName) matching(matchers []compiledMatcher, visit func(entry *seriesEntry) bool) {
	var nameMatcher *compiledMatcher
	for index := range matchers {
		if name, ok := matchers[index].labelName(); ok && name == metricNameLabel {
			nameMatcher = &matchers[index]
			break
		}
	}
	groupID, hasGroup := uint32(0), false
	if nameMatcher != nil && nameMatcher.kind == kindEqual {
		id, ok := b.names[nameMatcher.value]
		if !ok {
			return
		}
		groupID, hasGroup = id, true
	}
	// The smallest posting lists of a matcher, when smaller than the name group: an equality's, or
	// those of every value a regex accepts, when it accepts few and not the empty value.
	var (
		best      []uint32
		bestLen   int
		hasBest   bool
		oneValue  [1]string
		valueList []string
	)
	for index := range matchers {
		matcher := &matchers[index]
		switch {
		case matcher.kind == kindEqual && matcher.value != "":
			oneValue[0] = matcher.value
			valueList = oneValue[:]
		case matcher.kind == kindRegex && !matcher.re.isMatch(""):
			values, ok := matcher.re.acceptedValues()
			if !ok {
				continue
			}
			valueList = values
		default:
			continue
		}
		if matcher.name == metricNameLabel {
			continue
		}
		id, known := labels.Lookup(matcher.name)
		var lists []uint32
		total := 0
		if known && int(id) < len(b.postings) && b.postings[id] != nil {
			for _, value := range valueList {
				if posting, ok := b.postings[id][postingKey(value)]; ok {
					lists = append(lists, posting)
					total += b.postingLen(posting)
				}
			}
		}
		// Only series with one of the values can match.
		if total == 0 {
			return
		}
		if !hasBest || total < bestLen {
			best, bestLen, hasBest = lists, total, true
		}
	}
	groupLen := 0
	if hasGroup {
		groupLen = len(b.groups[groupID].entries)
	}
	var nameGroups []uint32
	hasNameGroups := nameMatcher != nil && !hasGroup
	if hasNameGroups {
		nameGroups = b.nameGroups(nameMatcher)
	}
	// How many series the candidates below would be: a posting list, a name group, the groups of
	// the names a name matcher accepts, or every series.
	var chosen int
	switch {
	case hasBest && (!hasGroup || bestLen < groupLen):
		chosen = bestLen
	case hasGroup:
		chosen = groupLen
	case hasNameGroups:
		for _, id := range nameGroups {
			chosen += len(b.groups[id].entries)
		}
	default:
		chosen = b.len
	}
	// Like the Go head's postings for matchers that reject the empty value: only series with the
	// label can match, and the label's postings list them. A nameless label regex visited every
	// series of the tenant otherwise.
	var (
		presentCount int
		presentName  string
		hasPresent   bool
	)
	for index := range matchers {
		matcher := &matchers[index]
		if matcher.kind == kindShard || matcher.kind == kindEqual || matcher.name == metricNameLabel || matcher.matchesValue("") {
			continue
		}
		count := b.seriesWith(labels.Lookup(matcher.name))
		if !hasPresent || count < presentCount || (count == presentCount && matcher.name < presentName) {
			presentCount, presentName, hasPresent = count, matcher.name, true
		}
	}
	if hasPresent && presentCount >= chosen {
		hasPresent = false
	}
	// Name groups hold exactly one name, so candidates taken from groups already match the name
	// matcher; checking it again read every candidate's labels, a cache miss per series.
	nameMatched := true
	candidatesLen := chosen
	if hasPresent {
		candidatesLen = presentCount
	}
	resolved := make([]resolvedMatcher, 0, len(matchers))
	var fromRefs []seriesRef
	switch {
	case hasPresent:
		nameMatched = false
		fromRefs = b.refsWith(labels.Lookup(presentName))
	case hasBest && (!hasGroup || bestLen < groupLen):
		nameMatched = false
		fromRefs = b.refsOf(best, bestLen)
	}
	for index := range matchers {
		matcher := &matchers[index]
		if nameMatched && matcher == nameMatcher {
			continue
		}
		r := resolvedMatcher{matcher: matcher}
		if name, ok := matcher.labelName(); ok {
			r.nameID, r.known = labels.Lookup(name)
			if name != metricNameLabel && matcher.matchesValue("") && b.seriesWith(r.nameID, r.known)*4 < candidatesLen {
				refs := b.refsWith(r.nameID, r.known)
				r.withLabel = make(map[uint64]struct{}, len(refs))
				for _, ref := range refs {
					r.withLabel[ref.hash] = struct{}{}
				}
			}
		}
		resolved = append(resolved, r)
	}
	check := func(entry *seriesEntry) bool {
		for index := range resolved {
			r := &resolved[index]
			matcher := r.matcher
			if matcher.kind == kindShard {
				if entry.series.shardHash%matcher.shardCount != matcher.shardIndex {
					return false
				}
				continue
			}
			// A hash collision only makes a series without the label read its labels.
			if r.withLabel != nil {
				if _, ok := r.withLabel[entry.hash]; !ok {
					continue
				}
			}
			countLabelValueRead()
			value := ""
			if r.known {
				value = entry.labels.ValueOf(r.nameID)
			}
			if matcher.kind == kindRegex || matcher.kind == kindNotRegex {
				matched, ok := r.remembered[value]
				if !ok {
					if countersEnabled {
						regexEvaluations.Add(1)
					}
					matched = matcher.matchesValue(value)
					if r.remembered == nil {
						r.remembered = map[string]bool{}
					}
					if len(r.remembered) < rememberedValues {
						r.remembered[value] = matched
					}
				}
				if !matched {
					return false
				}
				continue
			}
			if !matcher.matchesValue(value) {
				return false
			}
		}
		return true
	}
	emit := func(entry *seriesEntry) bool {
		if !check(entry) {
			return true
		}
		return visit(entry)
	}
	switch {
	case hasPresent || (hasBest && (!hasGroup || bestLen < groupLen)):
		b.visitRefs(fromRefs, emit)
	case hasGroup:
		entries := b.groups[groupID].entries
		for index := range entries {
			if !emit(&entries[index]) {
				return
			}
		}
	case hasNameGroups:
		for _, id := range nameGroups {
			entries := b.groups[id].entries
			for index := range entries {
				if !emit(&entries[index]) {
					return
				}
			}
		}
	default:
		for groupID := range b.groups {
			entries := b.groups[groupID].entries
			for index := range entries {
				if !emit(&entries[index]) {
					return
				}
			}
		}
	}
}
