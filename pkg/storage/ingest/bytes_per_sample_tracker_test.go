// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBytesPerSampleTracker_GetBytesPerSample_DefaultWhenNoData(t *testing.T) {
	tracker := newBytesPerSampleTracker(500, 100, 1000)

	// No data recorded, should return default.
	assert.Equal(t, 500, tracker.GetBytesPerSample("tenant-1"))
	assert.Equal(t, 500, tracker.GetBytesPerSample("unknown-tenant"))
}

func TestBytesPerSampleTracker_RecordAndGet(t *testing.T) {
	tracker := newBytesPerSampleTracker(500, 100, 1000)

	// Record 1000 bytes for 10 timeseries = 100 bytes per sample.
	tracker.RecordSamples("tenant-1", 1000, 10)
	assert.Equal(t, 100, tracker.GetBytesPerSample("tenant-1"))

	// Record another observation: 2000 bytes for 10 timeseries.
	// Total: 3000 bytes, 20 timeseries = 150 bytes per sample.
	tracker.RecordSamples("tenant-1", 2000, 10)
	assert.Equal(t, 150, tracker.GetBytesPerSample("tenant-1"))
}

func TestBytesPerSampleTracker_MultipleTenants(t *testing.T) {
	tracker := newBytesPerSampleTracker(500, 100, 1000)

	tracker.RecordSamples("tenant-1", 1000, 10)  // 100 bytes/sample
	tracker.RecordSamples("tenant-2", 4000, 10)  // 400 bytes/sample
	tracker.RecordSamples("tenant-3", 10000, 10) // 1000 bytes/sample

	assert.Equal(t, 100, tracker.GetBytesPerSample("tenant-1"))
	assert.Equal(t, 400, tracker.GetBytesPerSample("tenant-2"))
	assert.Equal(t, 1000, tracker.GetBytesPerSample("tenant-3"))
}

func TestBytesPerSampleTracker_MinClamping(t *testing.T) {
	tracker := newBytesPerSampleTracker(500, 100, 1000)

	// Record 50 bytes for 10 timeseries = 5 bytes per sample.
	// Should be clamped to min of 100.
	tracker.RecordSamples("tenant-1", 50, 10)
	assert.Equal(t, 100, tracker.GetBytesPerSample("tenant-1"))
}

func TestBytesPerSampleTracker_MaxClamping(t *testing.T) {
	tracker := newBytesPerSampleTracker(500, 100, 1000)

	// Record 20000 bytes for 10 timeseries = 2000 bytes per sample.
	// Should be clamped to max of 1000.
	tracker.RecordSamples("tenant-1", 20000, 10)
	assert.Equal(t, 1000, tracker.GetBytesPerSample("tenant-1"))
}

func TestBytesPerSampleTracker_SkipsInvalidData(t *testing.T) {
	tracker := newBytesPerSampleTracker(500, 100, 1000)

	// Zero or negative values should be skipped.
	tracker.RecordSamples("tenant-1", 0, 10)
	tracker.RecordSamples("tenant-1", 1000, 0)
	tracker.RecordSamples("tenant-1", -100, 10)
	tracker.RecordSamples("tenant-1", 1000, -10)

	// Should still return default since no valid data was recorded.
	assert.Equal(t, 500, tracker.GetBytesPerSample("tenant-1"))
}

func TestBytesPerSampleTracker_BucketAdvancement(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	tracker := newBytesPerSampleTracker(500, 100, 1000)
	tracker.nowFunc = func() time.Time { return now }

	// Record initial data: 1000 bytes, 10 timeseries = 100 bytes/sample.
	tracker.RecordSamples("tenant-1", 1000, 10)
	assert.Equal(t, 100, tracker.GetBytesPerSample("tenant-1"))

	// Advance time by 10 seconds (1 bucket).
	now = now.Add(10 * time.Second)
	tracker.RecordSamples("tenant-1", 3000, 10) // 300 bytes/sample in new bucket.

	// Total: 4000 bytes, 20 timeseries = 200 bytes/sample.
	assert.Equal(t, 200, tracker.GetBytesPerSample("tenant-1"))
}

func TestBytesPerSampleTracker_OldDataExpires(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	tracker := newBytesPerSampleTracker(500, 100, 1000)
	tracker.nowFunc = func() time.Time { return now }

	// Record initial data: 1000 bytes, 10 timeseries = 100 bytes/sample.
	tracker.RecordSamples("tenant-1", 1000, 10)
	assert.Equal(t, 100, tracker.GetBytesPerSample("tenant-1"))

	// Advance time by 60 seconds (6 buckets = full window).
	// Old data should expire.
	now = now.Add(60 * time.Second)
	tracker.RecordSamples("tenant-1", 5000, 10) // 500 bytes/sample in new bucket.

	// Only the new data should remain.
	assert.Equal(t, 500, tracker.GetBytesPerSample("tenant-1"))
}

func TestBytesPerSampleTracker_PartialDataExpiry(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	tracker := newBytesPerSampleTracker(500, 100, 1000)
	tracker.nowFunc = func() time.Time { return now }

	// Record in bucket 0: 2000 bytes, 10 timeseries.
	tracker.RecordSamples("tenant-1", 2000, 10)

	// Advance 30 seconds (3 buckets) and record more.
	now = now.Add(30 * time.Second)
	tracker.RecordSamples("tenant-1", 4000, 10)

	// Total: 6000 bytes, 20 timeseries = 300 bytes/sample.
	assert.Equal(t, 300, tracker.GetBytesPerSample("tenant-1"))

	// Advance another 30 seconds (3 more buckets).
	// First bucket should now be zeroed.
	now = now.Add(30 * time.Second)
	tracker.RecordSamples("tenant-1", 6000, 10)

	// Only buckets from last 60s remain: 4000+6000=10000 bytes, 20 timeseries = 500 bytes/sample.
	assert.Equal(t, 500, tracker.GetBytesPerSample("tenant-1"))
}

func TestBytesPerSampleTracker_LargeTimeGap(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	tracker := newBytesPerSampleTracker(500, 100, 1000)
	tracker.nowFunc = func() time.Time { return now }

	// Record initial data.
	tracker.RecordSamples("tenant-1", 1000, 10)
	assert.Equal(t, 100, tracker.GetBytesPerSample("tenant-1"))

	// Advance time by a very large amount (e.g., 10 minutes).
	now = now.Add(10 * time.Minute)
	tracker.RecordSamples("tenant-1", 3000, 10)

	// All old data should be expired, only new data remains.
	assert.Equal(t, 300, tracker.GetBytesPerSample("tenant-1"))
}

func TestBytesPerSampleTracker_MultipleTenantsBucketAdvancement(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	tracker := newBytesPerSampleTracker(500, 100, 1000)
	tracker.nowFunc = func() time.Time { return now }

	// Record for both tenants at t=0.
	tracker.RecordSamples("tenant-1", 1000, 10) // 100 bytes/sample
	tracker.RecordSamples("tenant-2", 2000, 10) // 200 bytes/sample

	assert.Equal(t, 100, tracker.GetBytesPerSample("tenant-1"))
	assert.Equal(t, 200, tracker.GetBytesPerSample("tenant-2"))

	// Advance 60 seconds and record only for tenant-1.
	now = now.Add(60 * time.Second)
	tracker.RecordSamples("tenant-1", 5000, 10) // 500 bytes/sample

	// tenant-1 should have new data only.
	assert.Equal(t, 500, tracker.GetBytesPerSample("tenant-1"))
	// tenant-2's old data should be expired, return default.
	assert.Equal(t, 500, tracker.GetBytesPerSample("tenant-2"))
}

func TestBytesPerSampleTracker_StaleTenantRemoval(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	tracker := newBytesPerSampleTracker(500, 100, 1000)
	tracker.nowFunc = func() time.Time { return now }

	// Record data for 3 tenants at t=0 (bucket 0).
	tracker.RecordSamples("active-tenant", 1000, 10)
	tracker.RecordSamples("stale-tenant-1", 1000, 10)
	tracker.RecordSamples("stale-tenant-2", 1000, 10)

	assert.Len(t, tracker.tenants, 3)

	// Advance 30 seconds (3 buckets) and record only for active-tenant (bucket 3).
	now = now.Add(30 * time.Second)
	tracker.RecordSamples("active-tenant", 2000, 10)

	// All tenants should still be in the map (haven't wrapped yet).
	assert.Len(t, tracker.tenants, 3)

	// Advance another 30 seconds (3 more buckets) to wrap around to bucket 0.
	// This triggers stale tenant cleanup.
	now = now.Add(30 * time.Second)
	tracker.RecordSamples("active-tenant", 3000, 10)

	// Stale tenants should be removed, only active-tenant remains.
	assert.Len(t, tracker.tenants, 1)
	assert.Contains(t, tracker.tenants, "active-tenant")
	assert.NotContains(t, tracker.tenants, "stale-tenant-1")
	assert.NotContains(t, tracker.tenants, "stale-tenant-2")

	// active-tenant has data in bucket 3 (2000/10) and bucket 0 (3000/10) = 5000/20 = 250.
	assert.Equal(t, 250, tracker.GetBytesPerSample("active-tenant"))
	// Stale tenants return default.
	assert.Equal(t, 500, tracker.GetBytesPerSample("stale-tenant-1"))
	assert.Equal(t, 500, tracker.GetBytesPerSample("stale-tenant-2"))
}

func TestBytesPerSampleTracker_StaleTenantRemovalOnLargeTimeGap(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	tracker := newBytesPerSampleTracker(500, 100, 1000)
	tracker.nowFunc = func() time.Time { return now }

	// Record data for multiple tenants.
	tracker.RecordSamples("tenant-1", 1000, 10)
	tracker.RecordSamples("tenant-2", 2000, 10)
	tracker.RecordSamples("tenant-3", 3000, 10)

	assert.Len(t, tracker.tenants, 3)

	// Advance by 10 minutes (large time gap, wraps multiple times).
	// All tenants become stale and should be removed.
	now = now.Add(10 * time.Minute)
	tracker.RecordSamples("new-tenant", 5000, 10)

	// Only the new tenant should exist.
	assert.Len(t, tracker.tenants, 1)
	assert.Contains(t, tracker.tenants, "new-tenant")
	assert.Equal(t, 500, tracker.GetBytesPerSample("new-tenant"))
}

func TestBytesPerSampleTracker_TenantNotRemovedIfHasDataInAnyBucket(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	tracker := newBytesPerSampleTracker(500, 100, 1000)
	tracker.nowFunc = func() time.Time { return now }

	// Record data at bucket 0.
	tracker.RecordSamples("tenant-1", 1000, 10)
	assert.Len(t, tracker.tenants, 1)

	// Advance 50 seconds (5 buckets) - now at bucket 5.
	// tenant-1's data is still in bucket 0 (not zeroed yet).
	now = now.Add(50 * time.Second)
	tracker.RecordSamples("tenant-1", 2000, 10)

	// Both bucket 0 and bucket 5 have data.
	assert.Len(t, tracker.tenants, 1)
	// Total: 3000 bytes, 20 timeseries = 150 bytes/sample.
	assert.Equal(t, 150, tracker.GetBytesPerSample("tenant-1"))

	// Advance 10 more seconds to wrap to bucket 0.
	// Bucket 0 gets zeroed, but bucket 5 still has data.
	now = now.Add(10 * time.Second)
	tracker.RecordSamples("tenant-1", 3000, 10)

	// tenant-1 should NOT be removed because bucket 5 still has data.
	assert.Len(t, tracker.tenants, 1)
	assert.Contains(t, tracker.tenants, "tenant-1")
	// Data: bucket 5 (2000/10) + bucket 0 (3000/10) = 5000/20 = 250.
	assert.Equal(t, 250, tracker.GetBytesPerSample("tenant-1"))
}
