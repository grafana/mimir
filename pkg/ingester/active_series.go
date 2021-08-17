// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/active_series.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"fmt"
	"hash"
	"math"
	"sync"
	"time"

	"github.com/cespare/xxhash"
	amlabels "github.com/prometheus/alertmanager/pkg/labels"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/util"
)

const (
	numActiveSeriesStripes = 512
)

// ActiveSeries is keeping track of recently active series for a single tenant.
type ActiveSeries struct {
	asm     *ActiveSeriesMatcher
	stripes [numActiveSeriesStripes]activeSeriesStripe
}

// activeSeriesStripe holds a subset of the series timestamps for a single tenant.
type activeSeriesStripe struct {
	asm *ActiveSeriesMatcher

	// Unix nanoseconds. Only used by purge. Zero = unknown.
	// Updated in purge and when old timestamp is used when updating series (in this case, oldestEntryTs is updated
	// without holding the lock -- hence the atomic).
	oldestEntryTs atomic.Int64

	mu             sync.RWMutex
	refs           map[uint64][]activeSeriesEntry
	active         int   // Number of active entries in this stripe. Only decreased during purge or clear.
	activeMatching []int // Number of active entires in this stripe matching each matcher of the configured ActiveSeriesMatcher.
}

// activeSeriesEntry holds a timestamp for single series.
type activeSeriesEntry struct {
	lbs     labels.Labels
	nanos   *atomic.Int64 // Unix timestamp in nanoseconds. Needs to be a pointer because we don't store pointers to entries in the stripe.
	matches []bool        // Which matchers of ActiveSeriesMatcher does this series match
}

func NewActiveSeries(asm *ActiveSeriesMatcher) *ActiveSeries {
	c := &ActiveSeries{asm: asm}

	// Stripes are pre-allocated so that we only read on them and no lock is required.
	for i := 0; i < numActiveSeriesStripes; i++ {
		c.stripes[i] = activeSeriesStripe{
			asm:            asm,
			refs:           map[uint64][]activeSeriesEntry{},
			activeMatching: asm.IntsSlice(),
		}
	}

	return c
}

// Updates series timestamp to 'now'. Function is called to make a copy of labels if entry doesn't exist yet.
func (c *ActiveSeries) UpdateSeries(series labels.Labels, now time.Time, labelsCopy func(labels.Labels) labels.Labels) {
	fp := fingerprint(series)
	stripeID := fp % numActiveSeriesStripes

	c.stripes[stripeID].updateSeriesTimestamp(now, series, fp, labelsCopy)
}

var sep = []byte{model.SeparatorByte}

var hashPool = sync.Pool{New: func() interface{} { return xxhash.New() }}

func fingerprint(series labels.Labels) uint64 {
	sum := hashPool.Get().(hash.Hash64)
	defer hashPool.Put(sum)

	sum.Reset()
	for _, label := range series {
		_, _ = sum.Write(util.YoloBuf(label.Name))
		_, _ = sum.Write(sep)
		_, _ = sum.Write(util.YoloBuf(label.Value))
		_, _ = sum.Write(sep)
	}

	return sum.Sum64()
}

// Purge removes expired entries from the cache. This function should be called
// periodically to avoid memory leaks.
func (c *ActiveSeries) Purge(keepUntil time.Time) {
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

func (c *ActiveSeries) Active() (int, []int) {
	total := 0
	totalMatching := c.asm.IntsSlice()
	for s := 0; s < numActiveSeriesStripes; s++ {
		c.stripes[s].mu.RLock()
		total += c.stripes[s].active
		for i, a := range c.stripes[s].activeMatching {
			totalMatching[i] += a
		}
		c.stripes[s].mu.RUnlock()
	}
	return total, totalMatching
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

func (s *activeSeriesStripe) purge(keepUntil time.Time) {
	keepUntilNanos := keepUntil.UnixNano()
	if oldest := s.oldestEntryTs.Load(); oldest > 0 && keepUntilNanos <= oldest {
		// Nothing to do.
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	active := 0
	activeMatching := s.asm.IntsSlice()

	oldest := int64(math.MaxInt64)
	for fp, entries := range s.refs {
		// Since we do expect very few fingerprint collisions, we
		// have an optimized implementation for the common case.
		if len(entries) == 1 {
			ts := entries[0].nanos.Load()
			if ts < keepUntilNanos {
				s.asm.PutDirtyBools(entries[0].matches)
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
				s.asm.PutDirtyBools(entries[i].matches)
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
	s.asm.PutDirtyInts(s.activeMatching)
	s.activeMatching = activeMatching
}

func NewActiveSeriesMatcher(cfgs ActiveSeriesCustomTrackersConfigs) (*ActiveSeriesMatcher, error) {
	l := len(cfgs)

	asm := &ActiveSeriesMatcher{
		boolsPool: sync.Pool{New: func() interface{} { s := make([]bool, l); return &s }},
		intsPool:  sync.Pool{New: func() interface{} { s := make([]int, l); return &s }},
	}

	seenMatcherNames := map[string]int{}
	for i, cfg := range cfgs {
		if idx, seen := seenMatcherNames[cfg.Name]; seen {
			return nil, fmt.Errorf("active series matcher %d duplicates the name of matcher at position %d: %q", i, idx, cfg.Name)
		}
		seenMatcherNames[cfg.Name] = i

		sm, err := amlabels.ParseMatchers(cfg.Matcher)
		if err != nil {
			return nil, fmt.Errorf("can't build active series matcher %d: %w", i, err)
		}
		asm.matchers = append(asm.matchers, sm)
		asm.names = append(asm.names, cfg.Name)
	}

	return asm, nil
}

func newEmptyActiveSeriesMatcher() *ActiveSeriesMatcher {
	m, err := NewActiveSeriesMatcher(nil)
	if err != nil {
		panic(err) // should not panic as there's nothing to match
	}
	return m
}

type ActiveSeriesMatcher struct {
	names    []string
	matchers []amlabels.Matchers

	boolsPool sync.Pool
	intsPool  sync.Pool
}

func (asm *ActiveSeriesMatcher) MatcherNames() []string {
	return asm.names
}

func (asm *ActiveSeriesMatcher) Matches(series labels.Labels) []bool {
	ls := labelsToLabelSet(series)
	matches := asm.BoolsSlice()
	for i, sm := range asm.matchers {
		matches[i] = sm.Matches(ls)
	}
	return matches
}

// BoolsSlice provides a slice of bools with 'false' values, with same len as MatcherNames.
// After usage, it's nice to put it back using PutDirtyBools()
func (asm *ActiveSeriesMatcher) BoolsSlice() []bool {
	if len(asm.matchers) == 0 {
		return nil
	}

	return *asm.boolsPool.Get().(*[]bool)
}

// PutDirtyBools puts back a slice of bools. They don't need to be false values.
func (asm *ActiveSeriesMatcher) PutDirtyBools(slice []bool) {
	if len(slice) == 0 {
		return
	}

	for i := range slice {
		slice[i] = false
	}
	asm.boolsPool.Put(&slice)
}

// IntsSlice provides a slice of ints with zero values, with same len as MatcherNames.
// After usage, it's nice to put it back using PutDirtyInts()
func (asm *ActiveSeriesMatcher) IntsSlice() []int {
	if len(asm.matchers) == 0 {
		return nil
	}
	return *asm.intsPool.Get().(*[]int)
}

// PutDirtyInts puts back a slice of ints. They don't need to be zero values.
func (asm *ActiveSeriesMatcher) PutDirtyInts(slice []int) {
	if len(slice) == 0 {
		return
	}

	for i := range slice {
		slice[i] = 0
	}
	asm.intsPool.Put(&slice)
}

func labelsToLabelSet(series labels.Labels) model.LabelSet {
	ls := make(model.LabelSet)
	for _, l := range series {
		ls[model.LabelName(l.Name)] = model.LabelValue(l.Value)
	}
	return ls
}
