// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"cmp"
	"fmt"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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
	first   hashIndex
}

func (g *group) lookup(hash uint64, visit func(entry *seriesEntry) bool) bool {
	index, ok := g.first.get(hash, g.entries)
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
	// How many series were removed since the postings were built, whose ids they still have.
	stale int
	// Bumped whenever series are removed, which invalidates what headLabels derived from them.
	removals   uint64
	headLabels headLabels
	// The hashes of the series' values of a label, by name group and label.
	columnsLock sync.Mutex
	columns     map[uint64][]uint32
	// The distinct values of a label of a name group, and each series' value among them.
	dicts       map[uint64]*valueDict
	columnBytes int
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
		b.groups = append(b.groups, group{})
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
	if first, ok := g.first.get(hash, g.entries); ok {
		next = first
	}
	g.entries = append(g.entries, seriesEntry{hash: hash, labels: stored, next: next, series: series})
	g.first.set(hash, int32(len(g.entries)-1), g.entries)
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
		b.groups = append(b.groups, group{})
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

// minColumnSeries is how many series a name group has before it is scanned by value columns, and maxColumnFilters how
// many of the lookup's labels are.
const (
	minColumnSeries  = 128
	maxColumnFilters = 3
	// maxColumnBytes is what a shard keeps of value columns: a column is 4 bytes a series of a name group.
	maxColumnBytes = 16 << 20
)

// maxDictValues is how many distinct values of a label a dictionary has before it isn't worth it: the values are
// strings, and a matcher evaluated for each of them is a scan of the series by another name.
const (
	maxDictValues   = 4096
	maxDictFilters  = 2
	dictValueBytes  = 64
	minDictSavingBy = 8
)

// valueDict is the values of one label of the series of a name group, in the order of its entries: each series has the
// id of its value in values, where id 0 is the empty value, which a series without the label has. A matcher is
// evaluated once for each value, instead of for each series, and the series are scanned by 4 bytes each, not by
// reading their labels.
type valueDict struct {
	ids    []uint32
	values []string
	index  map[string]uint32
	// How many series the group had when the label turned out to have too many values for the dictionary, or 0: it is
	// tried again once the group doubled.
	tooManyAt int
}

// dictSnapshot is what a lookup scans, which later series only append to.
type dictSnapshot struct {
	ids    []uint32
	values []string
}

// valueDictOf returns the dictionary of the label of the name group, built when first asked for and extended with the
// series added since, or false when the label has too many values.
func (b *seriesByName) valueDictOf(groupID, name uint32) (dictSnapshot, bool) {
	b.columnsLock.Lock()
	defer b.columnsLock.Unlock()
	key := uint64(groupID)<<32 | uint64(name)
	entries := b.groups[groupID].entries
	dict := b.dicts[key]
	if dict == nil {
		if b.dicts == nil {
			b.dicts = map[uint64]*valueDict{}
		}
		dict = &valueDict{values: []string{""}, index: map[string]uint32{"": 0}}
		b.dicts[key] = dict
	}
	if dict.tooManyAt > 0 {
		if len(entries) < 2*dict.tooManyAt {
			return dictSnapshot{}, false
		}
		dict = &valueDict{values: []string{""}, index: map[string]uint32{"": 0}}
		b.dicts[key] = dict
	}
	if len(dict.ids) < len(entries) {
		if b.columnBytes+4*(len(entries)-len(dict.ids)) > maxColumnBytes {
			clear(b.columns)
			clear(b.dicts)
			b.columnBytes = 0
			return dictSnapshot{}, false
		}
		before, valuesBefore := len(dict.ids), len(dict.values)
		for index := len(dict.ids); index < len(entries); index++ {
			value, _ := labelValue(entries[index].labels, name)
			id, ok := dict.index[value]
			if !ok {
				if len(dict.values) >= maxDictValues || len(dict.values) >= len(entries)/minDictSavingBy {
					b.columnBytes -= 4*before + dictValueBytes*valuesBefore
					*dict = valueDict{tooManyAt: len(entries)}
					return dictSnapshot{}, false
				}
				id = uint32(len(dict.values))
				dict.values = append(dict.values, value)
				dict.index[value] = id
			}
			dict.ids = append(dict.ids, id)
		}
		b.columnBytes += 4*(len(dict.ids)-before) + dictValueBytes*(len(dict.values)-valuesBefore)
	}
	return dictSnapshot{ids: dict.ids[:len(entries)], values: dict.values}, true
}

type dictFilter struct {
	ids      []uint32
	accepted []bool
}

type valueColumnFilter struct {
	hashes []uint32
	want   uint32
}

// valueColumn returns the hash of the value of the label of each series of the name group, in the order of its
// entries, built when first asked for and extended with the series added since. It is cleared when series are
// removed, and when a shard holds too many.
func (b *seriesByName) valueColumn(groupID, name uint32) []uint32 {
	b.columnsLock.Lock()
	defer b.columnsLock.Unlock()
	key := uint64(groupID)<<32 | uint64(name)
	entries := b.groups[groupID].entries
	column := b.columns[key]
	if len(column) < len(entries) {
		if b.columns == nil || b.columnBytes+4*(len(entries)-len(column)) > maxColumnBytes {
			clear(b.columns)
			clear(b.dicts)
			if b.columns == nil {
				b.columns = map[uint64][]uint32{}
			}
			b.columnBytes, column = 0, nil
		}
		b.columnBytes += 4 * (len(entries) - len(column))
		for index := len(column); index < len(entries); index++ {
			value, _ := labelValue(entries[index].labels, name)
			column = append(column, postingKey(value))
		}
		b.columns[key] = column
	}
	return column[:len(entries)]
}

// maxFilterPostings is how many postings a matcher filters by: each is looked up for every candidate.
const maxFilterPostings = 4

// refsOfFiltered is refsOf for the series that are also in one of the postings of each filter, and in the name
// group when there is one: a series is the candidate of every matcher, and only reading its labels says so
// otherwise.
func (b *seriesByName) refsOfFiltered(postings []uint32, total int, filters [][]uint32, groupID uint32, hasGroup bool) []seriesRef {
	if len(filters) == 0 && !hasGroup {
		return b.refsOf(postings, total)
	}
	refs := make([]seriesRef, 0, min(total, 1024))
	keep := func(id uint32) bool {
		if hasGroup && b.refs[id].group != groupID {
			return false
		}
		for _, filter := range filters {
			if !b.inPostings(id, filter) {
				return false
			}
		}
		return true
	}
	for _, posting := range postings {
		one, many := b.posting(posting)
		if many == nil {
			if keep(one) {
				refs = append(refs, b.refs[one])
			}
			continue
		}
		for _, id := range many {
			if keep(id) {
				refs = append(refs, b.refs[id])
			}
		}
	}
	return sortRefs(refs)
}

// inPostings reports whether the series id is in one of the postings.
func (b *seriesByName) inPostings(id uint32, postings []uint32) bool {
	for _, posting := range postings {
		one, many := b.posting(posting)
		if many == nil {
			if one == id {
				return true
			}
			continue
		}
		if _, found := slices.BinarySearch(many, id); found {
			return true
		}
	}
	return false
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

// retain keeps the series keep accepts, and returns the name groups that lost series. A removal costs what the removed
// series and their groups do, not what the whole shard does: the postings keep the ids of the removed series, which no
// entry answers to any more, so they only add candidates that the lookups skip, until rebuildable says to rebuild them.
func (b *seriesByName) retain(keep func(entry *seriesEntry) bool) (changed []uint32) {
	removed := 0
	for groupID := range b.groups {
		g := &b.groups[groupID]
		before := len(g.entries)
		write := 0
		for read := range g.entries {
			entry := &g.entries[read]
			if keep(entry) {
				if write != read {
					g.entries[write] = *entry
				}
				write++
				continue
			}
			b.forgetLabels(entry.labels)
		}
		if write == before {
			continue
		}
		clear(g.entries[write:])
		kept := g.entries[:write]
		// Head compaction and retention remove whole hours of series at once; give back their
		// slots, which are most of an entry's cost once a group lost half its series. Growing the
		// slice again copies it once, as appending to it would have anyway.
		if cap(kept) > max(len(kept)+len(kept)/4, 8) {
			kept = slices.Clone(kept)
		}
		g.entries = kept
		g.first.reset(len(g.entries))
		for index := range g.entries {
			entry := &g.entries[index]
			entry.next = -1
			if first, ok := g.first.get(entry.hash, g.entries); ok {
				entry.next = first
			}
			g.first.set(entry.hash, int32(index), g.entries)
		}
		changed = append(changed, uint32(groupID))
		removed += before - write
	}
	if removed == 0 {
		return nil
	}
	b.len -= removed
	b.stale += removed
	b.removals++
	// A column is by the position of the series in its group, which moved in the groups that lost series.
	b.columnsLock.Lock()
	for key, column := range b.columns {
		if slices.Contains(changed, uint32(key>>32)) {
			b.columnBytes -= 4 * len(column)
			delete(b.columns, key)
		}
	}
	for key, dict := range b.dicts {
		if slices.Contains(changed, uint32(key>>32)) {
			b.columnBytes -= 4*len(dict.ids) + dictValueBytes*len(dict.values)
			delete(b.dicts, key)
		}
	}
	b.columnsLock.Unlock()
	return changed
}

// forgetLabels takes a removed series off the count of series with each of its labels.
func (b *seriesByName) forgetLabels(stored labels.Labels) {
	rest := string(stored)
	for len(rest) > 0 {
		name := uint32(takeUvarintString(&rest))
		size := takeUvarintString(&rest)
		rest = rest[size:]
		if name != metricNameID && int(name) < len(b.labelSeries) && b.labelSeries[name] > 0 {
			b.labelSeries[name]--
		}
	}
}

// rebuildable reports whether the removed series' ids are in the postings enough to make them worth rebuilding.
func (b *seriesByName) rebuildable() bool {
	return b.stale > max(1024, b.len/4)
}

// postingsSnapshot is what a rebuild of the postings needs from the series, to build them while the shard is unlocked.
type postingsSnapshot struct {
	removals uint64
	groups   []int
	series   []snapshotSeries
}

type snapshotSeries struct {
	group  uint32
	hash   uint64
	labels labels.Labels
}

// snapshotForPostings copies what identifies each series, with the shard at least read locked.
func (b *seriesByName) snapshotForPostings() postingsSnapshot {
	snapshot := postingsSnapshot{removals: b.removals, groups: make([]int, len(b.groups)), series: make([]snapshotSeries, 0, b.len)}
	for groupID := range b.groups {
		entries := b.groups[groupID].entries
		snapshot.groups[groupID] = len(entries)
		for index := range entries {
			snapshot.series = append(snapshot.series, snapshotSeries{uint32(groupID), entries[index].hash, entries[index].labels})
		}
	}
	return snapshot
}

// buildPostings builds the postings of the snapshot's series, which needs no lock.
func buildPostings(snapshot postingsSnapshot) *seriesByName {
	built := &seriesByName{}
	for _, series := range snapshot.series {
		built.addPostings(series.labels, series.group, series.hash)
	}
	return built
}

// installPostings replaces the postings with the built ones, once the series added since the snapshot are in them, with
// the shard locked. It reports false when series were removed meanwhile: their positions in the snapshot are stale.
func (b *seriesByName) installPostings(built *seriesByName, snapshot postingsSnapshot) bool {
	if b.removals != snapshot.removals {
		return false
	}
	for groupID := range b.groups {
		from := 0
		if groupID < len(snapshot.groups) {
			from = snapshot.groups[groupID]
		}
		entries := b.groups[groupID].entries
		for index := from; index < len(entries); index++ {
			built.addPostings(entries[index].labels, uint32(groupID), entries[index].hash)
		}
	}
	b.postings, b.sharedPostings, b.refs, b.labelSeries = built.postings, built.sharedPostings, built.refs, built.labelSeries
	b.stale = 0
	// What headLabels derived from the refs is by their position.
	b.removals++
	return true
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
	// Whether the scan of the group's dictionary already decided the matcher.
	decided bool
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
	// The postings of the matchers that weren't the best, which series of the best have to be in.
	var filters [][]uint32
	// How many series a value lookup has to beat: the name group's or every series'.
	groupLimit := b.len
	if hasGroup {
		groupLimit = len(b.groups[groupID].entries)
	}
	for index := range matchers {
		matcher := &matchers[index]
		switch {
		case matcher.kind == kindEqual && matcher.value != "":
			oneValue[0] = matcher.value
			valueList = oneValue[:]
		case matcher.kind == kindRegex && !matcher.re.isMatch(""):
			values, ok := matcher.re.acceptedValues()
			if !ok && matcher.name != metricNameLabel {
				values, ok = b.dictionaryMatches(matcher, groupLimit)
			}
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
		// The other matchers' postings filter the best one's series without reading their labels.
		if !hasBest || total < bestLen {
			if hasBest && len(best) <= maxFilterPostings {
				filters = append(filters, best)
			}
			best, bestLen, hasBest = lists, total, true
		} else if len(lists) <= maxFilterPostings {
			filters = append(filters, lists)
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
		fromRefs = b.refsOfFiltered(best, bestLen, filters, groupID, hasGroup)
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
			if r.decided {
				continue
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
	var scanned, matched int
	emit := func(entry *seriesEntry) bool {
		scanned++
		if !check(entry) {
			return true
		}
		matched++
		return visit(entry)
	}
	defer func() {
		if scanned >= scanLogMinScanned {
			logScan(matchers, scanned, matched, hasGroup, groupLimit, hasBest, bestLen, len(filters))
		}
	}()
	switch {
	case hasPresent || (hasBest && (!hasGroup || bestLen < groupLen)):
		b.visitRefs(fromRefs, emit)
	case hasGroup:
		entries := b.groups[groupID].entries
		// A big group is scanned by the hashes of its series' values of the equality matchers' labels, which
		// are one array each, instead of by reading every series' labels: a series with another hash can't match.
		var columns []valueColumnFilter
		if len(entries) >= minColumnSeries {
			for index := range resolved {
				r := &resolved[index]
				if r.matcher.kind == kindEqual && r.known && r.matcher.name != metricNameLabel && len(columns) < maxColumnFilters {
					columns = append(columns, valueColumnFilter{b.valueColumn(groupID, r.nameID), postingKey(r.matcher.value)})
				}
			}
		}
		// The matchers that aren't equalities are evaluated once for each value of their label, then the series
		// are scanned by their value's id.
		var dicts []dictFilter
		if len(entries) >= minColumnSeries {
			for index := range resolved {
				r := &resolved[index]
				switch r.matcher.kind {
				case kindRegex, kindNotRegex, kindNotEqual:
				default:
					continue
				}
				if !r.known || r.withLabel != nil || r.matcher.name == metricNameLabel || len(dicts) >= maxDictFilters {
					continue
				}
				snapshot, ok := b.valueDictOf(groupID, r.nameID)
				if !ok {
					continue
				}
				accepted := make([]bool, len(snapshot.values))
				for id, value := range snapshot.values {
					accepted[id] = r.matcher.matchesValue(value)
				}
				r.decided = true
				dicts = append(dicts, dictFilter{snapshot.ids, accepted})
			}
		}
		for index := range entries {
			skip := false
			for _, column := range columns {
				if column.hashes[index] != column.want {
					skip = true
					break
				}
			}
			for _, dict := range dicts {
				if !skip && !dict.accepted[dict.ids[index]] {
					skip = true
				}
			}
			if skip {
				continue
			}
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

// minDictionarySeries is how many series a regex has to select among before the label's values
// are looked up rather than every series' value checked.
const minDictionarySeries = 512

// dictionaryMatches returns the values of a regex matcher's label that it accepts, from the sorted
// values of the label, when that costs less than checking each of the candidate series.
func (b *seriesByName) dictionaryMatches(matcher *compiledMatcher, candidates int) ([]string, bool) {
	id, known := labels.Lookup(matcher.name)
	if !known || candidates < minDictionarySeries || b.seriesWith(id, true) < minDictionarySeries {
		return nil, false
	}
	dictionary, ok := b.labelDictionary(id)
	if !ok {
		return nil, false
	}
	prefix := matcher.re.prefix
	first := sort.SearchStrings(dictionary, prefix)
	var matched []string
	for _, value := range dictionary[first:] {
		if !strings.HasPrefix(value, prefix) {
			break
		}
		if matcher.re.isMatch(value) {
			matched = append(matched, value)
			// Past this, the postings of the values are no shorter than the candidates.
			if len(matched) > candidates/2 {
				return nil, false
			}
		}
	}
	return matched, true
}

// scanLogMinScanned is how many series a lookup visits before it is logged.
const scanLogMinScanned = 3000

var lastScanLog atomic.Int64

// logScan prints the shape of a lookup that visited many series, at most every five seconds: which matchers made
// the engine read labels, to be found in the logs of a deployment.
func logScan(matchers []compiledMatcher, scanned, matched int, hasGroup bool, groupLimit int, hasBest bool, bestLen, filters int) {
	now := time.Now().UnixNano()
	last := lastScanLog.Load()
	if now-last < int64(5*time.Second) || !lastScanLog.CompareAndSwap(last, now) {
		return
	}
	var shape strings.Builder
	for _, m := range matchers {
		value := m.value
		if m.re != nil {
			value = m.re.pattern
		}
		if len(value) > 40 {
			value = value[:40]
		}
		fmt.Fprintf(&shape, " {kind=%d name=%s value=%q}", m.kind, m.name, value)
	}
	fmt.Fprintf(os.Stderr, "scanlog scanned=%d matched=%d hasGroup=%t group=%d hasBest=%t best=%d filters=%d matchers:%s\n", scanned, matched, hasGroup, groupLimit, hasBest, bestLen, filters, shape.String())
}
