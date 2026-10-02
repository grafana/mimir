// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import "sync"

// maxRetainedBands is the number of hottest locality bands kept per tenant
// on one usage-tracker partition. A band is the top 16 bits of the Nautilus
// locality hash.
const maxRetainedBands = 256

// bandTable keeps the hottest locality bands. Counts of retained bands are
// exact. A band that is not retained enters only by replacing a band that
// currently has a single series; hotter bands stay put.
type bandTable struct {
	mu             sync.Mutex
	n              int
	bands          [maxRetainedBands]uint16
	counts         [maxRetainedBands]uint64
	index          map[uint16]int
	localitySeries uint64
}

func (b *bandTable) admit(band uint16) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.localitySeries++
	if b.index != nil {
		if i, ok := b.index[band]; ok {
			b.counts[i]++
			return
		}
	}
	if b.n < maxRetainedBands {
		if b.index == nil {
			b.index = make(map[uint16]int, maxRetainedBands)
		}
		b.bands[b.n] = band
		b.counts[b.n] = 1
		b.index[band] = b.n
		b.n++
		return
	}
	minI := 0
	for i := 1; i < b.n; i++ {
		if b.counts[i] < b.counts[minI] {
			minI = i
		}
	}
	// Counts are exact for retained bands. A new band has one series, so it
	// only replaces a retained band that also has one. Starting it at min+1
	// would leave a count behind after that series expired.
	if b.counts[minI] > 1 {
		return
	}
	delete(b.index, b.bands[minI])
	b.bands[minI] = band
	b.counts[minI] = 1
	b.index[band] = minI
}

func (b *bandTable) expire(band uint16) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.localitySeries > 0 {
		b.localitySeries--
	}
	if b.index == nil {
		return
	}
	i, ok := b.index[band]
	if !ok {
		return
	}
	if b.counts[i] > 1 {
		b.counts[i]--
		return
	}
	last := b.n - 1
	delete(b.index, band)
	if i != last {
		b.bands[i] = b.bands[last]
		b.counts[i] = b.counts[last]
		b.index[b.bands[i]] = i
	}
	b.bands[last] = 0
	b.counts[last] = 0
	b.n = last
}

// bandCount is one retained band and its estimated series count on this partition.
type bandCount struct {
	band  uint16
	count uint64
}

func (b *bandTable) snapshot() (localitySeries uint64, counts []bandCount) {
	b.mu.Lock()
	defer b.mu.Unlock()
	localitySeries = b.localitySeries
	if b.n == 0 {
		return localitySeries, nil
	}
	counts = make([]bandCount, b.n)
	for i := 0; i < b.n; i++ {
		counts[i] = bandCount{band: b.bands[i], count: b.counts[i]}
	}
	return localitySeries, counts
}
