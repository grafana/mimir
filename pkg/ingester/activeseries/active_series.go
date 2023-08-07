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
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/zeropool"
	"go.uber.org/atomic"
)

const (
	numStripes = 128

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
	deleted deletedSeries

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

	deleted *deletedSeries

	// Unix nanoseconds. Only used by purge. Zero = unknown.
	// Updated in purge and when old timestamp is used when updating series (in this case, oldestEntryTs is updated
	// without holding the lock -- hence the atomic).
	oldestEntryTs atomic.Int64

	mu                                   sync.RWMutex
	refs                                 map[storage.SeriesRef]seriesEntry
	active                               uint32   // Number of active entries in this stripe. Only decreased during purge or clear.
	activeMatching                       []uint32 // Number of active entries in this stripe matching each matcher of the configured Matchers.
	activeNativeHistograms               uint32   // Number of active entries (only native histograms) in this stripe. Only decreased during purge or clear.
	activeMatchingNativeHistograms       []uint32 // Number of active entries (only native histograms) in this stripe matching each matcher of the configured Matchers.
	activeNativeHistogramBuckets         uint32   // Number of buckets in active native histogram entries in this stripe. Only decreased during purge or clear.
	activeMatchingNativeHistogramBuckets []uint32 // Number of buckets in active native histogram entries in this stripe matching each matcher of the configured Matchers.
}

// seriesEntry holds a timestamp for single series.
type seriesEntry struct {
	nanos                     *atomic.Int64        // Unix timestamp in nanoseconds. Needs to be a pointer because we don't store pointers to entries in the stripe.
	matches                   preAllocDynamicSlice //  Index of the matcher matching
	numNativeHistogramBuckets int                  // Number of buckets in native histogram series, -1 if not a native histogram.

	deleted bool // This series was marked as deleted, so before purging we need to remove the refence to it from the deletedSeries.
}

func NewActiveSeries(asm *Matchers, timeout time.Duration) *ActiveSeries {
	c := &ActiveSeries{matchers: asm, timeout: timeout}

	// Stripes are pre-allocated so that we only read on them and no lock is required.
	for i := 0; i < numStripes; i++ {
		c.stripes[i].reinitialize(asm, &c.deleted)
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
		c.stripes[i].reinitialize(asm, &c.deleted)
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

	created := c.stripes[stripeID].updateSeriesTimestamp(now, series, ref, numNativeHistogramBuckets)
	if created {
		if deleted, ok := c.deleted.find(series); ok {
			deletedStripeID := deleted.ref % numStripes
			c.stripes[deletedStripeID].remove(deleted.ref)
		}
	}
}

// PostDeletion should be called when series are deleted from the head.
// Sometimes series can be deleted before they're considered inactive (this happens with OOO series),
// in this case we need to mark those series as deleted and save their labels to avoid accounting from them twice if
// new series for same labels is created shortly after.
func (c *ActiveSeries) PostDeletion(deleted map[chunks.HeadSeriesRef]labels.Labels) {
	for ref, lbls := range deleted {
		ref := storage.SeriesRef(ref)
		stripeID := ref % numStripes
		if c.stripes[stripeID].containsRef(ref) {
			c.stripes[stripeID].markDeleted(ref, lbls)
		}
	}
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
		total += int(all)
		totalNativeHistograms += int(histograms)
		totalNativeHistogramBuckets += int(buckets)
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

	totalMatching = make([]int, len(c.matchers.MatcherNames()))
	totalMatchingNativeHistograms = make([]int, len(c.matchers.MatcherNames()))
	totalMatchingNativeHistogramBuckets = make([]int, len(c.matchers.MatcherNames()))
	for s := 0; s < numStripes; s++ {
		all, histograms, buckets := c.stripes[s].getTotalAndUpdateMatching(totalMatching, totalMatchingNativeHistograms, totalMatchingNativeHistogramBuckets)
		total += int(all)
		totalNativeHistograms += int(histograms)
		totalNativeHistogramBuckets += int(buckets)
	}

	return
}

func (s *seriesStripe) containsRef(ref storage.SeriesRef) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.refs[ref]
	return ok
}

func (s *seriesStripe) markDeleted(ref storage.SeriesRef, lbls labels.Labels) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.refs[ref]; !ok {
		// Doesn't exist anymore.
		return
	}

	series := s.refs[ref]
	series.deleted = true
	s.refs[ref] = series

	s.deleted.add(ref, lbls)
}

// getTotal will return the total active series in the stripe
// and the total active series that are native histograms
// and the total number of buckets in active native histogram series
func (s *seriesStripe) getTotal() (uint32, uint32, uint32) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.active, s.activeNativeHistograms, s.activeNativeHistogramBuckets
}

// getTotalAndUpdateMatching will return the total active series in the stripe and also update the slice provided
// with each matcher's total, and will also do the same for total active series that are native histograms
// as well as the total number of buckets in active native histogram series
func (s *seriesStripe) getTotalAndUpdateMatching(matching []int, matchingNativeHistograms []int, matchingNativeHistogramBuckets []int) (uint32, uint32, uint32) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// len(matching) == len(s.activeMatching) by design, and it could be nil
	for i, a := range s.activeMatching {
		matching[i] += int(a)
	}
	for i, a := range s.activeMatchingNativeHistograms {
		matchingNativeHistograms[i] += int(a)
	}
	for i, a := range s.activeMatchingNativeHistogramBuckets {
		matchingNativeHistogramBuckets[i] += int(a)
	}

	return s.active, s.activeNativeHistograms, s.activeNativeHistogramBuckets
}

func (s *seriesStripe) updateSeriesTimestamp(now time.Time, series labels.Labels, ref storage.SeriesRef, numNativeHistogramBuckets int) bool {
	nowNanos := now.UnixNano()

	e, needsUpdating := s.findEntryForSeries(ref, numNativeHistogramBuckets)
	created := false
	if e == nil || needsUpdating {
		e, created = s.findAndUpdateOrCreateEntryForSeries(ref, series, nowNanos, numNativeHistogramBuckets)
	}

	entryTimeSet := created
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

	return created
}

func (s *seriesStripe) findEntryForSeries(ref storage.SeriesRef, numNativeHistogramBuckets int) (*atomic.Int64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry := s.refs[ref]
	return entry.nanos, entry.numNativeHistogramBuckets != numNativeHistogramBuckets
}

func (s *seriesStripe) findAndUpdateOrCreateEntryForSeries(ref storage.SeriesRef, series labels.Labels, nowNanos int64, numNativeHistogramBuckets int) (entryTime *atomic.Int64, created bool) {
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
				s.activeNativeHistogramBuckets = uint32(int(s.activeNativeHistogramBuckets) + diff)
				for i := 0; i < matchesLen; i++ {
					s.activeMatchingNativeHistogramBuckets[matches.get(i)] = uint32(int(s.activeMatchingNativeHistogramBuckets[matches.get(i)]) + diff)
				}
			} else if numNativeHistogramBuckets >= 0 {
				// change from float to histogram
				s.activeNativeHistograms++
				s.activeNativeHistogramBuckets += uint32(numNativeHistogramBuckets)
				for i := 0; i < matchesLen; i++ {
					match := matches.get(i)
					s.activeMatchingNativeHistograms[match]++
					s.activeMatchingNativeHistogramBuckets[match] += uint32(numNativeHistogramBuckets)
				}
			} else {
				// change from histogram to float
				s.activeNativeHistograms--
				s.activeNativeHistogramBuckets -= uint32(entry.numNativeHistogramBuckets)
				for i := 0; i < matchesLen; i++ {
					match := matches.get(i)
					s.activeMatchingNativeHistograms[match]--
					s.activeMatchingNativeHistogramBuckets[match] -= uint32(entry.numNativeHistogramBuckets)
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
		s.activeNativeHistogramBuckets += uint32(numNativeHistogramBuckets)
	}
	for i := 0; i < matchesLen; i++ {
		match := matches.get(i)
		s.activeMatching[match]++
		if numNativeHistogramBuckets >= 0 {
			s.activeMatchingNativeHistograms[match]++
			s.activeMatchingNativeHistogramBuckets[match] += uint32(numNativeHistogramBuckets)
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
func (s *seriesStripe) reinitialize(asm *Matchers, deleted *deletedSeries) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.deleted = deleted
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
			if entry.deleted {
				s.deleted.purge(ref)
			}
			delete(s.refs, ref)
			continue
		}

		s.active++
		if entry.numNativeHistogramBuckets >= 0 {
			s.activeNativeHistograms++
			s.activeNativeHistogramBuckets += uint32(entry.numNativeHistogramBuckets)
		}
		ml := entry.matches.len()
		for i := 0; i < ml; i++ {
			match := entry.matches.get(i)
			s.activeMatching[match]++
			if entry.numNativeHistogramBuckets >= 0 {
				s.activeMatchingNativeHistograms[match]++
				s.activeMatchingNativeHistogramBuckets[match] += uint32(entry.numNativeHistogramBuckets)
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

// remove a single series from the stripe.
// This is mostly the same logic from purge() but we decrement counters for a single entry instead of incrementing for each entry.
// Note: we might remove the oldest series here, but the worst thing can happen is that we let run a useless purge() cycle later,
// so this method doesn't update the oldestEntryTs.
func (s *seriesStripe) remove(ref storage.SeriesRef) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.refs[ref]
	if !ok {
		// Was purged already.
		// There's no need to call s.deleted.purge() because since we're holding the lock,
		// we know that nobody could have added this series in the meantime.
		return
	}

	s.active--
	if entry.numNativeHistogramBuckets >= 0 {
		s.activeNativeHistograms--
		s.activeNativeHistogramBuckets -= uint32(entry.numNativeHistogramBuckets)
	}
	ml := entry.matches.len()
	for i := 0; i < ml; i++ {
		match := entry.matches.get(i)
		s.activeMatching[match]--
		if entry.numNativeHistogramBuckets >= 0 {
			s.activeMatchingNativeHistograms[match]--
			s.activeMatchingNativeHistogramBuckets[match] -= uint32(entry.numNativeHistogramBuckets)
		}
	}

	s.deleted.purge(ref)
	delete(s.refs, ref)
}

func resizeAndClear(l int, prev []uint32) []uint32 {
	if cap(prev) < l {
		if l == 0 {
			return nil
		}
		return make([]uint32, l)
	}

	p := prev[:l]
	for i := 0; i < l; i++ {
		p[i] = 0
	}
	return p
}

type deletedSeries struct {
	mu   sync.RWMutex
	keys map[storage.SeriesRef]string
	refs map[string]deletedSeriesRef

	// buf can be used when write lock is held.
	// If only read lock is held, then bufs pool should be used instead.
	buf  []byte
	bufs zeropool.Pool[[]byte]
}

type deletedSeriesRef struct {
	ref storage.SeriesRef
	key string
}

func (ds *deletedSeries) find(lbls labels.Labels) (deletedSeriesRef, bool) {
	buf := ds.bufs.Get()
	defer ds.bufs.Put(buf)

	ds.mu.RLock()
	defer ds.mu.RUnlock()

	// This map lookup won't allocate a string.
	ref, ok := ds.refs[string(lbls.Bytes(buf))]
	return ref, ok
}

// add should be called only when write lock on the seriesStripe is held.
// this way we can avoid race conditions by adding something that is purged and leaking series.
func (ds *deletedSeries) add(ref storage.SeriesRef, lbls labels.Labels) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.keys == nil {
		ds.keys = map[storage.SeriesRef]string{}
		ds.refs = map[string]deletedSeriesRef{}
	}

	key := string(lbls.Bytes(ds.buf))

	ds.keys[ref] = key
	ds.refs[key] = deletedSeriesRef{ref, key}
}

func (ds *deletedSeries) purge(ref storage.SeriesRef) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	key, ok := ds.keys[ref]
	if !ok {
		return
	}

	delete(ds.keys, ref)
	delete(ds.refs, key)
}
