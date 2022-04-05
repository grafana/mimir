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
	mu                 sync.RWMutex
	stripes            [numStripes]seriesStripe
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
	refs           map[uint64][]seriesEntry
	active         int   // Number of active entries in this stripe. Only decreased during purge or clear.
	activeMatching []int // Number of active entries in this stripe matching each matcher of the configured Matchers.
}

// seriesEntry holds a timestamp for single series.
type seriesEntry struct {
	lbs     labels.Labels
	nanos   *atomic.Int64 // Unix timestamp in nanoseconds. Needs to be a pointer because we don't store pointers to entries in the stripe.
	matches []bool        // Which matchers of Matchers does this series match
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
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.matchers.MatcherNames()
}

func (c *ActiveSeries) ReloadMatchers(asm *Matchers, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i := 0; i < numStripes; i++ {
		c.stripes[i].reinitialize(asm)
	}
	c.matchers = asm
	c.lastMatchersUpdate = now
}

func (c *ActiveSeries) CurrentConfig() CustomTrackersConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.matchers.Config()
}

// UpdateSeries updates series timestamp to 'now'. Function is called to make a copy of labels if entry doesn't exist yet.
func (c *ActiveSeries) UpdateSeries(series labels.Labels, now time.Time, labelsCopy func(labels.Labels) labels.Labels) {
	fp := series.Hash()
	stripeID := fp % numStripes

	c.stripes[stripeID].updateSeriesTimestamp(now, series, fp, labelsCopy)
}

// purge removes expired entries from the cache.
func (c *ActiveSeries) purge(keepUntil time.Time) {
	for s := 0; s < numStripes; s++ {
		c.stripes[s].purge(keepUntil)
	}
}

// Active returns the total number of active series, as well as a slice of active series matching each one of the
// custom trackers provided (in the same order as custom trackers are defined).
// The result is correct only if the third return value is true, which shows if enough time has passed since last reload.
// This should be called periodically to avoid unbounded memory growth.
func (c *ActiveSeries) Active(now time.Time) (int, []int, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	purgeTime := now.Add(-c.timeout)
	c.purge(purgeTime)

	if c.lastMatchersUpdate.After(purgeTime) {
		return 0, nil, false
	}

	total := 0
	totalMatching := resizeAndClear(len(c.matchers.MatcherNames()), nil)
	for s := 0; s < numStripes; s++ {
		total += c.stripes[s].getTotalAndUpdateMatching(totalMatching)
	}

	return total, totalMatching, true
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

func (s *seriesStripe) updateSeriesTimestamp(now time.Time, series labels.Labels, fingerprint uint64, labelsCopy func(labels.Labels) labels.Labels) {
	nowNanos := now.UnixNano()

	e := s.findEntryForSeries(fingerprint, series)
	entryTimeSet := false
	if e == nil {
		e, entryTimeSet = s.findOrCreateEntryForSeries(fingerprint, series, nowNanos, labelsCopy)
	}

	if !entryTimeSet {
		if prev := e.Load(); nowNanos > prev {
			entryTimeSet = e.CAS(prev, nowNanos)
		}
	}

	if entryTimeSet {
		for prevOldest := s.oldestEntryTs.Load(); nowNanos < prevOldest; {
			// If recent purge already removed entries older than "oldest entry timestamp", setting this to 0 will make
			// sure that next purge doesn't take the shortcut route.
			if s.oldestEntryTs.CAS(prevOldest, 0) {
				break
			}
		}
	}
}

func (s *seriesStripe) findEntryForSeries(fingerprint uint64, series labels.Labels) *atomic.Int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check if already exists within the entries.
	for _, entry := range s.refs[fingerprint] {
		if labels.Equal(entry.lbs, series) {
			return entry.nanos
		}
	}

	return nil
}

func (s *seriesStripe) findOrCreateEntryForSeries(fingerprint uint64, series labels.Labels, nowNanos int64, labelsCopy func(labels.Labels) labels.Labels) (*atomic.Int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if already exists within the entries.
	// This repeats findEntryForSeries(), but under write lock.
	for _, entry := range s.refs[fingerprint] {
		if labels.Equal(entry.lbs, series) {
			return entry.nanos, false
		}
	}

	matches := s.matchers.Matches(series)

	s.active++
	for i, ok := range matches {
		if ok {
			s.activeMatching[i]++
		}
	}

	e := seriesEntry{
		lbs:     labelsCopy(series),
		nanos:   atomic.NewInt64(nowNanos),
		matches: matches,
	}

	s.refs[fingerprint] = append(s.refs[fingerprint], e)

	return e.nanos, true
}

//nolint // Linter reports that this method is unused, but it is.
func (s *seriesStripe) clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.oldestEntryTs.Store(0)
	s.refs = map[uint64][]seriesEntry{}
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
	s.refs = map[uint64][]seriesEntry{}
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
	for fp, entries := range s.refs {
		// Since we do expect very few fingerprint collisions, we
		// have an optimized implementation for the common case.
		if len(entries) == 1 {
			ts := entries[0].nanos.Load()
			if ts < keepUntilNanos {
				delete(s.refs, fp)
				continue
			}

			s.active++
			for i, ok := range entries[0].matches {
				if ok {
					s.activeMatching[i]++
				}
			}
			if ts < oldest {
				oldest = ts
			}
			continue
		}

		// We have more entries, which means there's a collision,
		// so we have to iterate over the entries.
		for i := 0; i < len(entries); {
			ts := entries[i].nanos.Load()
			if ts < keepUntilNanos {
				entries = append(entries[:i], entries[i+1:]...)
			} else {
				if ts < oldest {
					oldest = ts
				}

				i++
			}
		}

		// Either update or delete the entries in the map
		if cnt := len(entries); cnt == 0 {
			delete(s.refs, fp)
		} else {
			s.active += cnt
			for i := range entries {
				for i, ok := range entries[i].matches {
					if ok {
						s.activeMatching[i]++
					}
				}
			}

			s.refs[fp] = entries
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
