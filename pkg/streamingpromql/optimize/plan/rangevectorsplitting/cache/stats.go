package cache

type CacheStats struct {
	ReadEntries    int
	WrittenEntries int
	TotalSeries    int // not deduped
	MinSeries      int
	MaxSeries      int
	TotalBytes     int
	MinBytes       int
	MaxBytes       int
}

func (c *CacheStats) AddReadEntryStat(seriesCount, size int) {
	c.ReadEntries++
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

func (c *CacheStats) AddWriteEntryStat(seriesCount, size int) {
	c.WrittenEntries++
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
