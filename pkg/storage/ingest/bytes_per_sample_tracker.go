// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"time"
)

const (
	// bytesPerSampleBucketCount is the number of buckets for the moving average (1 minute / 10 seconds = 6 buckets).
	bytesPerSampleBucketCount = 6
	// bytesPerSampleBucketDuration is the duration of each bucket.
	bytesPerSampleBucketDuration = 10 * time.Second
)

// bucketData holds aggregated data for a single time bucket.
type bucketData struct {
	totalBytes      int64
	timeseriesCount int64
}

// bytesPerSampleTracker tracks actual bytes per sample per tenant as a 1-minute
// moving average using 10-second buckets. When no data is available for a tenant,
// it returns the configured default value. Returned values are clamped to min/max bounds.
//
// This type is NOT thread-safe. The caller is responsible for synchronization.
type bytesPerSampleTracker struct {
	// Per-tenant bucket data (only the bucket values, not timing).
	tenants map[string]*[bytesPerSampleBucketCount]bucketData

	// Shared bucket timing across all tenants.
	currentBucket      int
	currentBucketStart time.Time

	// Configuration.
	defaultBytesPerSample int // Returned when no data for tenant (from IngestionConcurrencyEstimatedBytesPerSample).
	minBytesPerSample     int // Minimum allowed value (e.g., 100).
	maxBytesPerSample     int // Maximum allowed value (e.g., 1000).

	// For testing: allows injecting a custom time source.
	nowFunc func() time.Time
}

// newBytesPerSampleTracker creates a new bytesPerSampleTracker.
func newBytesPerSampleTracker(defaultBytesPerSample, minBytesPerSample, maxBytesPerSample int) *bytesPerSampleTracker {
	return &bytesPerSampleTracker{
		tenants:               make(map[string]*[bytesPerSampleBucketCount]bucketData),
		defaultBytesPerSample: defaultBytesPerSample,
		minBytesPerSample:     minBytesPerSample,
		maxBytesPerSample:     maxBytesPerSample,
		nowFunc:               time.Now,
	}
}

// RecordSamples records an observation of bytes and timeseries count for a tenant.
// Called when parallelStoragePusher.Close() knows both total bytes and actual timeseries.
func (t *bytesPerSampleTracker) RecordSamples(tenantID string, totalBytes, timeseriesCount int) {
	if timeseriesCount <= 0 || totalBytes <= 0 {
		return // Skip invalid data.
	}

	now := t.nowFunc()
	t.advanceBuckets(now)

	buckets := t.getOrCreateTenantBuckets(tenantID)
	buckets[t.currentBucket].totalBytes += int64(totalBytes)
	buckets[t.currentBucket].timeseriesCount += int64(timeseriesCount)
}

// GetBytesPerSample returns the moving average bytes per sample for a tenant.
// Returns the default value if no data is available for the tenant.
// The returned value is clamped to configured min/max bounds.
func (t *bytesPerSampleTracker) GetBytesPerSample(tenantID string) int {
	buckets, exists := t.tenants[tenantID]
	if !exists {
		return t.defaultBytesPerSample
	}

	// Sum all buckets.
	var totalBytes, totalTimeseries int64
	for _, bucket := range buckets {
		totalBytes += bucket.totalBytes
		totalTimeseries += bucket.timeseriesCount
	}

	if totalTimeseries == 0 {
		return t.defaultBytesPerSample
	}

	bytesPerSample := int(totalBytes / totalTimeseries)

	// Clamp to min/max bounds.
	if bytesPerSample < t.minBytesPerSample {
		return t.minBytesPerSample
	}
	if bytesPerSample > t.maxBytesPerSample {
		return t.maxBytesPerSample
	}

	return bytesPerSample
}

// advanceBuckets advances the bucket index based on elapsed time.
// Zeroes old buckets for all tenants as it advances.
func (t *bytesPerSampleTracker) advanceBuckets(now time.Time) {
	if t.currentBucketStart.IsZero() {
		// First observation ever.
		t.currentBucketStart = now
		return
	}

	elapsed := now.Sub(t.currentBucketStart)
	bucketsToAdvance := int(elapsed / bytesPerSampleBucketDuration)

	if bucketsToAdvance <= 0 {
		return
	}

	// Cap at full cycle - advancing more than a full cycle just means all buckets get zeroed.
	if bucketsToAdvance > bytesPerSampleBucketCount {
		bucketsToAdvance = bytesPerSampleBucketCount
	}

	// Zero out buckets for all tenants as we advance.
	for i := 0; i < bucketsToAdvance; i++ {
		t.currentBucket = (t.currentBucket + 1) % bytesPerSampleBucketCount
		for _, buckets := range t.tenants {
			buckets[t.currentBucket] = bucketData{} // Zero the new bucket.
		}

		// When we wrap around to bucket 0, clean up stale tenants.
		// A tenant is stale if all its buckets have zero samples.
		// This cleanup happens once per minute (when we complete a full cycle),
		// which keeps memory bounded for tenants that stop sending data.
		if t.currentBucket == 0 {
			t.removeStaleTenants()
		}
	}

	t.currentBucketStart = t.currentBucketStart.Add(time.Duration(bucketsToAdvance) * bytesPerSampleBucketDuration)
}

// removeStaleTenants removes tenants that have no samples in any bucket.
func (t *bytesPerSampleTracker) removeStaleTenants() {
	for tenantID, buckets := range t.tenants {
		if t.isTenantStale(buckets) {
			delete(t.tenants, tenantID)
		}
	}
}

// isTenantStale returns true if all buckets for a tenant are empty.
func (t *bytesPerSampleTracker) isTenantStale(buckets *[bytesPerSampleBucketCount]bucketData) bool {
	for _, bucket := range buckets {
		if bucket.timeseriesCount > 0 {
			return false
		}
	}
	return true
}

// getOrCreateTenantBuckets returns the bucket array for a tenant, creating it if necessary.
func (t *bytesPerSampleTracker) getOrCreateTenantBuckets(tenantID string) *[bytesPerSampleBucketCount]bucketData {
	buckets, exists := t.tenants[tenantID]
	if !exists {
		buckets = &[bytesPerSampleBucketCount]bucketData{}
		t.tenants[tenantID] = buckets
	}
	return buckets
}
