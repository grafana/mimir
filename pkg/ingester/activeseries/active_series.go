// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/active_series.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package activeseries

import (
	"flag"
	"math"
	"sync"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"go.uber.org/atomic"
)

const (
	numStripes = 512

	EnabledFlag     = "ingester.active-series-metrics-enabled"
	IdleTimeoutFlag = "ingester.active-series-metrics-idle-timeout"
)

type Config struct {
	Enabled      bool          `yaml:"active_series_metrics_enabled" category:"advanced"`
	UpdatePeriod time.Duration `yaml:"active_series_metrics_update_period" category:"advanced"`
	IdleTimeout  time.Duration `yaml:"active_series_metrics_idle_timeout" category:"advanced"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, EnabledFlag, true, "Enable tracking of active series and export them as metrics.")
	f.DurationVar(&cfg.UpdatePeriod, "ingester.active-series-metrics-update-period", 1*time.Minute, "How often to update active series metrics.")
	f.DurationVar(&cfg.IdleTimeout, IdleTimeoutFlag, 10*time.Minute, "After what time a series is considered to be inactive.")
}

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

	mu                                   sync.RWMutex
	refs                                 map[storage.SeriesRef]seriesEntry
	active                               int   // Number of active entries in this stripe. Only decreased during purge or clear.
	activeMatching                       []int // Number of active entries in this stripe matching each matcher of the configured Matchers.
	activeNativeHistograms               int   // Number of active entries (only native histograms) in this stripe. Only decreased during purge or clear.
	activeMatchingNativeHistograms       []int // Number of active entries (only native histograms) in this stripe matching each matcher of the configured Matchers.
	activeNativeHistogramBuckets         int   // Number of buckets in active native histogram entries in this stripe. Only decreased during purge or clear.
	activeMatchingNativeHistogramBuckets []int // Number of buckets in active native histogram entries in this stripe matching each matcher of the configured Matchers.
}

// seriesEntry holds a timestamp for single series.
type seriesEntry struct {
	nanos                     *atomic.Int64        // Unix timestamp in nanoseconds. Needs to be a pointer because we don't store pointers to entries in the stripe.
	matches                   preAllocDynamicSlice //  Index of the matcher matching
	numNativeHistogramBuckets int                  // Number of buckets in native histogram series, -1 if not a native histogram.
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
// Pass -1 in numNativeHistogramBuckets if the series is not a native histogram series.
func (c *ActiveSeries) UpdateSeries(series labels.Labels, ref storage.SeriesRef, now time.Time, numNativeHistogramBuckets int) {
	stripeID := ref % numStripes

	c.stripes[stripeID].updateSeriesTimestamp(now, series, ref, numNativeHistogramBuckets)
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

func (c *ActiveSeries) ContainsRef(ref storage.SeriesRef) bool {
	stripeID := ref % numStripes
	return c.stripes[stripeID].containsRef(ref)
}

// Active returns the total numbers of active series, active native
// histogram series, and buckets of those native histogram series.
// This method does not purge expired entries, so Purge should be
// called periodically.
func (c *ActiveSeries) Active() (total, totalNativeHistograms, totalNativeHistogramBuckets int) {
	for s := 0; s < numStripes; s++ {
		all, histograms, buckets := c.stripes[s].getTotal()
		total += all
		totalNativeHistograms += histograms
		totalNativeHistogramBuckets += buckets
	}
	return
}

// ActiveWithMatchers returns the total number of active series, as well as a
// slice of active series matching each one of the custom trackers provided (in
// the same order as custom trackers are defined), and then the same thing for
// only active series that are native histograms, then the same for the number
// of buckets in those active native histogram series. This method does not purge
// expired entries, so Purge should be called periodically.
func (c *ActiveSeries) ActiveWithMatchers() (total int, totalMatching []int, totalNativeHistograms int, totalMatchingNativeHistograms []int, totalNativeHistogramBuckets int, totalMatchingNativeHistogramBuckets []int) {
	c.matchersMutex.RLock()
	defer c.matchersMutex.RUnlock()

	totalMatching = resizeAndClear(len(c.matchers.MatcherNames()), nil)
	totalMatchingNativeHistograms = resizeAndClear(len(c.matchers.MatcherNames()), nil)
	totalMatchingNativeHistogramBuckets = resizeAndClear(len(c.matchers.MatcherNames()), nil)
	for s := 0; s < numStripes; s++ {
		all, histograms, buckets := c.stripes[s].getTotalAndUpdateMatching(totalMatching, totalMatchingNativeHistograms, totalMatchingNativeHistogramBuckets)
		total += all
		totalNativeHistograms += histograms
		totalNativeHistogramBuckets += buckets
	}

	return
}

func (s *seriesStripe) containsRef(ref storage.SeriesRef) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.refs[ref]
	return ok
}

// getTotal will return the total active series in the stripe
// and the total active series that are native histograms
// and the total number of buckets in active native histogram series
func (s *seriesStripe) getTotal() (int, int, int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.active, s.activeNativeHistograms, s.activeNativeHistogramBuckets
}

// getTotalAndUpdateMatching will return the total active series in the stripe and also update the slice provided
// with each matcher's total, and will also do the same for total active series that are native histograms
// as well as the total number of buckets in active native histogram series
func (s *seriesStripe) getTotalAndUpdateMatching(matching []int, matchingNativeHistograms []int, matchingNativeHistogramBuckets []int) (int, int, int) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// len(matching) == len(s.activeMatching) by design, and it could be nil
	for i, a := range s.activeMatching {
		matching[i] += a
	}
	for i, a := range s.activeMatchingNativeHistograms {
		matchingNativeHistograms[i] += a
	}
	for i, a := range s.activeMatchingNativeHistogramBuckets {
		matchingNativeHistogramBuckets[i] += a
	}

	return s.active, s.activeNativeHistograms, s.activeNativeHistogramBuckets
}

func (s *seriesStripe) updateSeriesTimestamp(now time.Time, series labels.Labels, ref storage.SeriesRef, numNativeHistogramBuckets int) {
	nowNanos := now.UnixNano()

	e, needsUpdating := s.findEntryForSeries(ref, numNativeHistogramBuckets)
	entryTimeSet := false
	if e == nil || needsUpdating {
		e, entryTimeSet = s.findAndUpdateOrCreateEntryForSeries(ref, series, nowNanos, numNativeHistogramBuckets)
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

func (s *seriesStripe) findEntryForSeries(ref storage.SeriesRef, numNativeHistogramBuckets int) (*atomic.Int64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry := s.refs[ref]
	return entry.nanos, entry.numNativeHistogramBuckets != numNativeHistogramBuckets
}

func (s *seriesStripe) findAndUpdateOrCreateEntryForSeries(ref storage.SeriesRef, series labels.Labels, nowNanos int64, numNativeHistogramBuckets int) (*atomic.Int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if already exists within the entries.
	// This repeats findEntryForSeries(), but under write lock.
	entry, ok := s.refs[ref]
	if ok {
		if entry.numNativeHistogramBuckets != numNativeHistogramBuckets {
			matches := s.matchers.matches(series)
			matchesLen := matches.len()
			if numNativeHistogramBuckets >= 0 && entry.numNativeHistogramBuckets >= 0 {
				// change number of buckets but still a histogram
				diff := numNativeHistogramBuckets - entry.numNativeHistogramBuckets
				s.activeNativeHistogramBuckets += diff
				for i := 0; i < matchesLen; i++ {
					s.activeMatchingNativeHistogramBuckets[matches.get(i)] += diff
				}
			} else if numNativeHistogramBuckets >= 0 {
				// change from float to histogram
				s.activeNativeHistograms++
				s.activeNativeHistogramBuckets += numNativeHistogramBuckets
				for i := 0; i < matchesLen; i++ {
					match := matches.get(i)
					s.activeMatchingNativeHistograms[match]++
					s.activeMatchingNativeHistogramBuckets[match] += numNativeHistogramBuckets
				}
			} else {
				// change from histogram to float
				s.activeNativeHistograms--
				s.activeNativeHistogramBuckets -= entry.numNativeHistogramBuckets
				for i := 0; i < matchesLen; i++ {
					match := matches.get(i)
					s.activeMatchingNativeHistograms[match]--
					s.activeMatchingNativeHistogramBuckets[match] -= entry.numNativeHistogramBuckets
				}
			}
			entry.numNativeHistogramBuckets = numNativeHistogramBuckets
			s.refs[ref] = entry
		}
		return entry.nanos, false
	}

	matches := s.matchers.matches(series)
	matchesLen := matches.len()

	s.active++
	if numNativeHistogramBuckets >= 0 {
		s.activeNativeHistograms++
		s.activeNativeHistogramBuckets += numNativeHistogramBuckets
	}
	for i := 0; i < matchesLen; i++ {
		match := matches.get(i)
		s.activeMatching[match]++
		if numNativeHistogramBuckets >= 0 {
			s.activeMatchingNativeHistograms[match]++
			s.activeMatchingNativeHistogramBuckets[match] += numNativeHistogramBuckets
		}
	}

	e := seriesEntry{
		nanos:                     atomic.NewInt64(nowNanos),
		matches:                   matches,
		numNativeHistogramBuckets: numNativeHistogramBuckets,
	}

	s.refs[ref] = e

	return e.nanos, true
}

// nolint // Linter reports that this method is unused, but it is.
func (s *seriesStripe) clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.oldestEntryTs.Store(0)
	s.refs = map[storage.SeriesRef]seriesEntry{}
	s.active = 0
	s.activeNativeHistograms = 0
	s.activeNativeHistogramBuckets = 0
	for i := range s.activeMatching {
		s.activeMatching[i] = 0
		s.activeMatchingNativeHistograms[i] = 0
		s.activeMatchingNativeHistogramBuckets[i] = 0
	}
}

// Reinitialize assigns new matchers and corresponding size activeMatching slices.
func (s *seriesStripe) reinitialize(asm *Matchers) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.oldestEntryTs.Store(0)
	s.refs = map[storage.SeriesRef]seriesEntry{}
	s.active = 0
	s.activeNativeHistograms = 0
	s.activeNativeHistogramBuckets = 0
	s.matchers = asm
	s.activeMatching = resizeAndClear(len(asm.MatcherNames()), s.activeMatching)
	s.activeMatchingNativeHistograms = resizeAndClear(len(asm.MatcherNames()), s.activeMatchingNativeHistograms)
	s.activeMatchingNativeHistogramBuckets = resizeAndClear(len(asm.MatcherNames()), s.activeMatchingNativeHistogramBuckets)
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
	s.activeNativeHistograms = 0
	s.activeNativeHistogramBuckets = 0
	s.activeMatching = resizeAndClear(len(s.activeMatching), s.activeMatching)
	s.activeMatchingNativeHistograms = resizeAndClear(len(s.activeMatchingNativeHistograms), s.activeMatchingNativeHistograms)
	s.activeMatchingNativeHistogramBuckets = resizeAndClear(len(s.activeMatchingNativeHistogramBuckets), s.activeMatchingNativeHistogramBuckets)

	oldest := int64(math.MaxInt64)
	for ref, entry := range s.refs {
		ts := entry.nanos.Load()
		if ts < keepUntilNanos {
			delete(s.refs, ref)
			continue
		}

		s.active++
		if entry.numNativeHistogramBuckets >= 0 {
			s.activeNativeHistograms++
			s.activeNativeHistogramBuckets += entry.numNativeHistogramBuckets
		}
		ml := entry.matches.len()
		for i := 0; i < ml; i++ {
			match := entry.matches.get(i)
			s.activeMatching[match]++
			if entry.numNativeHistogramBuckets >= 0 {
				s.activeMatchingNativeHistograms[match]++
				s.activeMatchingNativeHistogramBuckets[match] += entry.numNativeHistogramBuckets
			}
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
