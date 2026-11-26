// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"math/rand/v2"
	"strings"
	"sync/atomic"

	promlabels "github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
)

// A native resources lookup's sets are checked against the Prometheus matchers the ingester compiled for the same
// patterns, on a sample of the lookups, and a value a set answers differently from its regex is counted, by direction.
// The run's comparisons with the join can't vouch for the sets: a value one wrongly rejects shows only where every
// answering ingester rejects it, looking like a known gap, and one it wrongly accepts never changes an info() answer.
//
// A check takes the values of one shard, chosen at random, since a value's verdict depends on the value alone:
// shadowCheckEvery lookups with a set are checked in, of at most maxShadowValues values each, which at the 12.6k
// values of info()'s alternation of every target takes a few ms, at most about 50 ms for rejected values, which scan
// every alternative of the regex.
const (
	shadowCheckEvery = 10
	maxShadowValues  = 4096
)

var (
	shadowChecks                                     atomic.Uint64
	shadowValues, shadowFalseAccepts, shadowFalseRej atomic.Uint64

	// For tests: every lookup with a set checked, and a hook between fetching a shard's values and checking them.
	shadowCheckAll bool
	shadowHook     func()
)

// SetShadowStats returns how many values the native resources lookups' sets were checked on since the process
// started, and how many they answered differently from their regexes: accepted where the regex rejects, and rejected
// where it accepts, or accepted without the prefix the label's dictionary is searched by.
func SetShadowStats() (checked, falseAccepts, falseRejects uint64) {
	return shadowValues.Load(), shadowFalseAccepts.Load(), shadowFalseRej.Load()
}

// shadowCheckSets checks a sample of a lookup's sets, given the Prometheus matchers they were compiled from, in the
// same order. It reads one shard's label dictionaries under the shard's read lock, and compares after releasing it.
func (e *Engine) shadowCheckSets(ctx context.Context, matcherSets [][]*promlabels.Matcher, compiled [][]compiledMatcher) {
	if ctx.Err() != nil || (!shadowCheckAll && shadowChecks.Add(1)%shadowCheckEvery != 0) {
		return
	}
	index := rand.IntN(len(e.store.shards))
	t := e.tenant(index)
	if t == nil {
		return
	}
	for set, matchers := range compiled {
		for m := range matchers {
			re := matchers[m].re
			if re == nil || re.set == nil {
				continue
			}
			id, known := labels.Lookup(matchers[m].name)
			if !known {
				continue
			}
			shard := e.store.shards[index]
			shard.RLock()
			// The dictionary is replaced rather than changed, and its values are copies of their own.
			values, ok := t.series.labelDictionary(id)
			shard.RUnlock()
			if !ok {
				continue
			}
			if shadowHook != nil {
				shadowHook()
			}
			if len(values) > maxShadowValues {
				start := rand.IntN(len(values) - maxShadowValues + 1)
				values = values[start : start+maxShadowValues]
			}
			// No dictionary holds the empty value, which the planning and info()'s mixed labels depend on.
			checkSetValue(re, matcherSets[set][m], "")
			for _, value := range values {
				checkSetValue(re, matcherSets[set][m], value)
			}
		}
	}
}

func checkSetValue(re *compiledRegex, regex *promlabels.Matcher, value string) {
	shadowValues.Add(1)
	// The set is the regex's language; a negative matcher's Matches is its complement.
	accepted, want := re.isMatch(value), regex.Matches(value) != (regex.Type == promlabels.MatchNotRegexp)
	switch {
	case accepted && !want:
		shadowFalseAccepts.Add(1)
	case !accepted && want, accepted && !strings.HasPrefix(value, re.prefix):
		// A lookup that finds the label's values in its dictionary searches them by the prefix.
		shadowFalseRej.Add(1)
	}
}
