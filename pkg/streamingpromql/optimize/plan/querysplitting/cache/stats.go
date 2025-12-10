package cache

// TODO: this may just be temporary during testing
type CacheStats struct {
	CachedEntries   int // Entries read from cache
	UncachedEntries int // Entries written to cache
	TotalSeries     int // not deduped
	MinSeries       int
	MaxSeries       int
	TotalBytes      int
	MinBytes        int
	MaxBytes        int
}

func (c *CacheStats) AddCachedEntryStat(seriesCount, size int) {
	c.CachedEntries++
	c.TotalSeries += seriesCount
	if c.MinSeries == 0 || seriesCount < c.MinSeries {
		c.MinSeries = seriesCount
	}
	if seriesCount > c.MaxSeries {
		c.MaxSeries = seriesCount
	}
	c.TotalBytes += size
	if c.MinBytes == 0 || size < c.MinBytes {
		c.MinBytes = size
	}
	if size > c.MaxBytes {
		c.MaxBytes = size
	}
}

func (c *CacheStats) AddUncachedEntryStat(seriesCount, size int) {
	c.UncachedEntries++
	c.TotalSeries += seriesCount
	if c.MinSeries == 0 || seriesCount < c.MinSeries {
		c.MinSeries = seriesCount
	}
	if seriesCount > c.MaxSeries {
		c.MaxSeries = seriesCount
	}
	c.TotalBytes += size
	if c.MinBytes == 0 || size < c.MinBytes {
		c.MinBytes = size
	}
	if size > c.MaxBytes {
		c.MaxBytes = size
	}
}
