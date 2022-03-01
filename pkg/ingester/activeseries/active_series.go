// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/active_series.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package activeseries

import (
	"math"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"go.uber.org/atomic"
)

const (
	numActiveSeriesStripes = 512
)

// ActiveSeries is keeping track of recently active series for a single tenant.
type ActiveSeries struct {
	stripes       [numActiveSeriesStripes]activeSeriesStripe
	asm           *ActiveSeriesMatchers
	lastAsmUpdate *atomic.Int64
	// The duration after series become inactive.
	timeout time.Duration
	mu      sync.RWMutex
}

// activeSeriesStripe holds a subset of the series timestamps for a single tenant.
type activeSeriesStripe struct {
	asm *ActiveSeriesMatchers

	// Unix nanoseconds. Only used by purge. Zero = unknown.
	// Updated in purge and when old timestamp is used when updating series (in this case, oldestEntryTs is updated
	// without holding the lock -- hence the atomic).
	oldestEntryTs atomic.Int64

	mu             sync.RWMutex
	refs           map[uint64][]activeSeriesEntry
	active         int   // Number of active entries in this stripe. Only decreased during purge or clear.
	activeMatching []int // Number of active entries in this stripe matching each matcher of the configured ActiveSeriesMatchers.
}

// activeSeriesEntry holds a timestamp for single series.
type activeSeriesEntry struct {
	lbs     labels.Labels
	nanos   *atomic.Int64 // Unix timestamp in nanoseconds. Needs to be a pointer because we don't store pointers to entries in the stripe.
	matches []bool        // Which matchers of ActiveSeriesMatchers does this series match
}

func NewActiveSeries(asm *ActiveSeriesMatchers, idleTimeout time.Duration) *ActiveSeries {
	c := &ActiveSeries{asm: asm, timeout: idleTimeout, lastAsmUpdate: atomic.NewInt64(0)}

	// Stripes are pre-allocated so that we only read on them and no lock is required.
	for i := 0; i < numActiveSeriesStripes; i++ {
		c.stripes[i] = activeSeriesStripe{
			asm:            asm,
			refs:           map[uint64][]activeSeriesEntry{},
			activeMatching: resizeAndClear(len(asm.MatcherNames()), nil),
		}
	}

	return c
}

func (c *ActiveSeries) CurrentMatcherNames() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.asm.MatcherNames()
}

func (c *ActiveSeries) ReloadSeriesMatchers(asm *ActiveSeriesMatchers) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i := 0; i < numActiveSeriesStripes; i++ {
		c.stripes[i].reinitialize(asm)
	}
	c.asm = asm
	c.lastAsmUpdate.Store(time.Now().UnixNano())
}

func (c *ActiveSeries) LastAsmUpdate() int64 {
	return c.lastAsmUpdate.Load()
}

func (c *ActiveSeries) CurrentConfig() *ActiveSeriesCustomTrackersConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.asm.Config()
}

// Updates series timestamp to 'now'. Function is called to make a copy of labels if entry doesn't exist yet.
func (c *ActiveSeries) UpdateSeries(series labels.Labels, now time.Time, labelsCopy func(labels.Labels) labels.Labels) {
	fp := fingerprint(series)
	stripeID := fp % numActiveSeriesStripes

	c.stripes[stripeID].updateSeriesTimestamp(now, series, fp, labelsCopy)
}

var sep = []byte{model.SeparatorByte}

func fingerprint(series labels.Labels) uint64 {
	sum := xxhash.New()

	for _, label := range series {
		_, _ = sum.WriteString(label.Name)
		_, _ = sum.Write(sep)
		_, _ = sum.WriteString(label.Value)
		_, _ = sum.Write(sep)
	}

	return sum.Sum64()
}

// Purge removes expired entries from the cache. This function is called by Active.
func (c *ActiveSeries) purge(keepUntil time.Time) {
	for s := 0; s < numActiveSeriesStripes; s++ {
		c.stripes[s].purge(keepUntil)
	}
}

//nolint // Linter reports that this method is unused, but it is.
func (c *ActiveSeries) clear() {
	for s := 0; s < numActiveSeriesStripes; s++ {
		c.stripes[s].clear()
	}
}

// Active returns the total number of active series, as well as a slice of active series matching each one of the
// custom trackers provided (in the same order as custom trackers are defined)
// This should be called periodically to avoid memory leaks.
// Active cannot be called concurrently with ReloadSeriesMatchers.
func (c *ActiveSeries) Active(now time.Time) (int, []int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.purge(now.Add(-c.timeout))
	total := 0
	totalMatching := resizeAndClear(len(c.asm.MatcherNames()), nil)
	for s := 0; s < numActiveSeriesStripes; s++ {
		total += c.stripes[s].getTotalAndUpdateMatching(totalMatching)
	}
	return total, totalMatching
}

// getTotalAndUpdateMatching will return the total active series in the stripe and also update the slice provided
// with each matcher's total.
func (s *activeSeriesStripe) getTotalAndUpdateMatching(matching []int) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// len(matching) == len(s.activeMatching) by design, and it could be nil
	for i, a := range s.activeMatching {
		matching[i] += a
	}

	return s.active
}

func (s *activeSeriesStripe) updateSeriesTimestamp(now time.Time, series labels.Labels, fingerprint uint64, labelsCopy func(labels.Labels) labels.Labels) {
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

func (s *activeSeriesStripe) findEntryForSeries(fingerprint uint64, series labels.Labels) *atomic.Int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check if already exists within the entries.
	for ix, entry := range s.refs[fingerprint] {
		if labels.Equal(entry.lbs, series) {
			return s.refs[fingerprint][ix].nanos
		}
	}

	return nil
}

func (s *activeSeriesStripe) findOrCreateEntryForSeries(fingerprint uint64, series labels.Labels, nowNanos int64, labelsCopy func(labels.Labels) labels.Labels) (*atomic.Int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if already exists within the entries.
	for ix, entry := range s.refs[fingerprint] {
		if labels.Equal(entry.lbs, series) {
			return s.refs[fingerprint][ix].nanos, false
		}
	}

	matches := s.asm.Matches(series)

	s.active++
	for i, ok := range matches {
		if ok {
			s.activeMatching[i]++
		}
	}

	e := activeSeriesEntry{
		lbs:     labelsCopy(series),
		nanos:   atomic.NewInt64(nowNanos),
		matches: matches,
	}

	s.refs[fingerprint] = append(s.refs[fingerprint], e)

	return e.nanos, true
}

//nolint // Linter reports that this method is unused, but it is.
func (s *activeSeriesStripe) clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.oldestEntryTs.Store(0)
	s.refs = map[uint64][]activeSeriesEntry{}
	s.active = 0
	for i := range s.activeMatching {
		s.activeMatching[i] = 0
	}
}

// Reinitalize is more than clear that it assigns new matchers and corresponding size activeMatching slices.
func (s *activeSeriesStripe) reinitialize(asm *ActiveSeriesMatchers) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.oldestEntryTs.Store(0)
	s.refs = map[uint64][]activeSeriesEntry{}
	s.active = 0
	s.asm = asm
	s.activeMatching = resizeAndClear(len(asm.MatcherNames()), s.activeMatching)
}

func (s *activeSeriesStripe) purge(keepUntil time.Time) {
	keepUntilNanos := keepUntil.UnixNano()
	if oldest := s.oldestEntryTs.Load(); oldest > 0 && keepUntilNanos <= oldest {
		// Nothing to do.
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	active := 0
	activeMatching := resizeAndClear(len(s.activeMatching), s.activeMatching)

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

			active++
			for i, ok := range entries[0].matches {
				if ok {
					activeMatching[i]++
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
			active += cnt
			for i := range entries {
				for i, ok := range entries[i].matches {
					if ok {
						activeMatching[i]++
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
	s.active = active
	s.activeMatching = activeMatching
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
