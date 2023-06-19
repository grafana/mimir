// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/active_series.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package activeseries

import (
	"math"
	"sync"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"go.uber.org/atomic"
)

const (
	numStripes = 512
)

// ActiveSeries is keeping track of recently active series for a single tenant.
type ActiveSeries struct {
	stripes [numStripes]seriesStripe

	// matchersMutex protects matchers and lastMatchersUpdate.
	matchersMutex      sync.RWMutex
	matchers           *Matchers
	lastMatchersUpdate time.Time

	// The duration after which series become inactive.
	// Also used to determine if enough time has passed since configuration reload for valid results.
	timeout time.Duration
}

// seriesStripe holds a subset of the series timestamps for a single tenant.
type seriesStripe struct {
	matchers *Matchers

	// Unix nanoseconds. Only used by purge. Zero = unknown.
	// Updated in purge and when old timestamp is used when updating series (in this case, oldestEntryTs is updated
	// without holding the lock -- hence the atomic).
	oldestEntryTs atomic.Int64

	mu             sync.RWMutex
	refs           map[uint64]seriesEntry
	active         int   // Number of active entries in this stripe. Only decreased during purge or clear.
	activeMatching []int // Number of active entries in this stripe matching each matcher of the configured Matchers.
}

// seriesEntry holds a timestamp for single series.
type seriesEntry struct {
	nanos   *atomic.Int64        // Unix timestamp in nanoseconds. Needs to be a pointer because we don't store pointers to entries in the stripe.
	matches preAllocDynamicSlice //  Index of the matcher matching
}

func NewActiveSeries(asm *Matchers, timeout time.Duration) *ActiveSeries {
	c := &ActiveSeries{matchers: asm, timeout: timeout}

	// Stripes are pre-allocated so that we only read on them and no lock is required.
	for i := 0; i < numStripes; i++ {
		c.stripes[i].reinitialize(asm)
	}

	return c
}

func (c *ActiveSeries) CurrentMatcherNames() []string {
	c.matchersMutex.RLock()
	defer c.matchersMutex.RUnlock()
	return c.matchers.MatcherNames()
}

func (c *ActiveSeries) ReloadMatchers(asm *Matchers, now time.Time) {
	c.matchersMutex.Lock()
	defer c.matchersMutex.Unlock()

	for i := 0; i < numStripes; i++ {
		c.stripes[i].reinitialize(asm)
	}
	c.matchers = asm
	c.lastMatchersUpdate = now
}

func (c *ActiveSeries) CurrentConfig() CustomTrackersConfig {
	c.matchersMutex.RLock()
	defer c.matchersMutex.RUnlock()
	return c.matchers.Config()
}

// UpdateSeries updates series timestamp to 'now'. Function is called to make a copy of labels if entry doesn't exist yet.
func (c *ActiveSeries) UpdateSeries(series labels.Labels, ref uint64, now time.Time) {
	stripeID := ref % numStripes

	c.stripes[stripeID].updateSeriesTimestamp(now, series, ref)
}

// Purge purges expired entries and returns true if enough time has passed since
// last reload. This should be called periodically to avoid unbounded memory
// growth.
func (c *ActiveSeries) Purge(now time.Time) bool {
	c.matchersMutex.Lock()
	defer c.matchersMutex.Unlock()
	purgeTime := now.Add(-c.timeout)
	c.purge(purgeTime)

	return !c.lastMatchersUpdate.After(purgeTime)
}

// purge removes expired entries from the cache.
func (c *ActiveSeries) purge(keepUntil time.Time) {
	for s := 0; s < numStripes; s++ {
		c.stripes[s].purge(keepUntil)
	}
}

func (c *ActiveSeries) ContainsRef(ref uint64) bool {
	stripeID := ref % numStripes
	return c.stripes[stripeID].containsRef(ref)
}

// Active returns the total number of active series. This method does not purge
// expired entries, so Purge should be called periodically.
func (c *ActiveSeries) Active() int {
	total := 0
	for s := 0; s < numStripes; s++ {
		total += c.stripes[s].getTotal()
	}
	return total
}

// ActiveWithMatchers returns the total number of active series, as well as a
// slice of active series matching each one of the custom trackers provided (in
// the same order as custom trackers are defined). This method does not purge
// expired entries, so Purge should be called periodically.
func (c *ActiveSeries) ActiveWithMatchers() (int, []int) {
	c.matchersMutex.RLock()
	defer c.matchersMutex.RUnlock()

	total := 0
	totalMatching := resizeAndClear(len(c.matchers.MatcherNames()), nil)
	for s := 0; s < numStripes; s++ {
		total += c.stripes[s].getTotalAndUpdateMatching(totalMatching)
	}

	return total, totalMatching
}

func (s *seriesStripe) containsRef(ref uint64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.refs[ref]
	return ok
}

// getTotal will return the total active series in the stripe
func (s *seriesStripe) getTotal() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.active
}

// getTotalAndUpdateMatching will return the total active series in the stripe and also update the slice provided
// with each matcher's total.
func (s *seriesStripe) getTotalAndUpdateMatching(matching []int) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// len(matching) == len(s.activeMatching) by design, and it could be nil
	for i, a := range s.activeMatching {
		matching[i] += a
	}

	return s.active
}

func (s *seriesStripe) updateSeriesTimestamp(now time.Time, series labels.Labels, ref uint64) {
	nowNanos := now.UnixNano()

	e := s.findEntryForSeries(ref)
	entryTimeSet := false
	if e == nil {
		e, entryTimeSet = s.findOrCreateEntryForSeries(ref, series, nowNanos)
	}

	if !entryTimeSet {
		if prev := e.Load(); nowNanos > prev {
			entryTimeSet = e.CompareAndSwap(prev, nowNanos)
		}
	}

	if entryTimeSet {
		for prevOldest := s.oldestEntryTs.Load(); nowNanos < prevOldest; {
			// If recent purge already removed entries older than "oldest entry timestamp", setting this to 0 will make
			// sure that next purge doesn't take the shortcut route.
			if s.oldestEntryTs.CompareAndSwap(prevOldest, 0) {
				break
			}
		}
	}
}

func (s *seriesStripe) findEntryForSeries(ref uint64) *atomic.Int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.refs[ref].nanos
}

func (s *seriesStripe) findOrCreateEntryForSeries(ref uint64, series labels.Labels, nowNanos int64) (*atomic.Int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if already exists within the entries.
	// This repeats findEntryForSeries(), but under write lock.
	entry, ok := s.refs[ref]
	if ok {
		return entry.nanos, false
	}

	matches := s.matchers.matches(series)
	matchesLen := matches.len()

	s.active++
	for i := 0; i < matchesLen; i++ {
		s.activeMatching[matches.get(i)]++
	}

	e := seriesEntry{
		nanos:   atomic.NewInt64(nowNanos),
		matches: matches,
	}

	s.refs[ref] = e

	return e.nanos, true
}

// nolint // Linter reports that this method is unused, but it is.
func (s *seriesStripe) clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.oldestEntryTs.Store(0)
	s.refs = map[uint64]seriesEntry{}
	s.active = 0
	for i := range s.activeMatching {
		s.activeMatching[i] = 0
	}
}

// Reinitialize assigns new matchers and corresponding size activeMatching slices.
func (s *seriesStripe) reinitialize(asm *Matchers) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.oldestEntryTs.Store(0)
	s.refs = map[uint64]seriesEntry{}
	s.active = 0
	s.matchers = asm
	s.activeMatching = resizeAndClear(len(asm.MatcherNames()), s.activeMatching)
}

func (s *seriesStripe) purge(keepUntil time.Time) {
	keepUntilNanos := keepUntil.UnixNano()
	if oldest := s.oldestEntryTs.Load(); oldest > 0 && keepUntilNanos <= oldest {
		// Nothing to do.
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.active = 0
	s.activeMatching = resizeAndClear(len(s.activeMatching), s.activeMatching)

	oldest := int64(math.MaxInt64)
	for ref, entry := range s.refs {
		ts := entry.nanos.Load()
		if ts < keepUntilNanos {
			delete(s.refs, ref)
			continue
		}

		s.active++
		ml := entry.matches.len()
		for i := 0; i < ml; i++ {
			s.activeMatching[entry.matches.get(i)]++
		}
		if ts < oldest {
			oldest = ts
		}
	}

	if oldest == math.MaxInt64 {
		s.oldestEntryTs.Store(0)
	} else {
		s.oldestEntryTs.Store(oldest)
	}
}

func resizeAndClear(l int, prev []int) []int {
	if cap(prev) < l {
		if l == 0 {
			return nil
		}
		// The allocation is bigger than the required capacity to save time in cases when the number of matchers are just slightly increasing.
		// In cases where the default matchers are slightly changed in size it could save from lot of reallocations, while having low memory impact.
		return make([]int, l, l*2)
	}

	p := prev[:l]
	for i := 0; i < l; i++ {
		p[i] = 0
	}
	return p
}
