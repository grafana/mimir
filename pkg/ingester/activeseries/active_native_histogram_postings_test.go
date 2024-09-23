// SPDX-License-Identifier: AGPL-3.0-only

package activeseries

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"

	asmodel "github.com/grafana/mimir/pkg/ingester/activeseries/model"
)

func TestNativeHistogramPostings_Expand(t *testing.T) {
	ttl := 3
	mockedTime := time.Unix(int64(ttl), 0)
	series := []labels.Labels{
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2"),
		labels.FromStrings("a", "3"), // Will make this series a native histogram.
		labels.FromStrings("a", "4"), // Will make this series a native histogram.
		labels.FromStrings("a", "5"),
	}
	allStorageRefs := []storage.SeriesRef{1, 2, 3, 4, 5}
	storagePostings := index.NewListPostings(allStorageRefs)
<<<<<<< HEAD

	activeSeries := NewActiveSeries(&asmodel.Matchers{}, time.Duration(ttl), "foo", "")
=======
	activeSeries := NewActiveSeries(&Matchers{}, time.Duration(ttl), "foo", "", nil)
>>>>>>> 3c422a8f57 (new service for tracking cost attribution)

	// Update each series at a different time according to its index.
	for i := range allStorageRefs {
		buckets := -1 // No native histogram buckets.
		if i+1 == 3 || i+1 == 4 {
			buckets = 10 // Native histogram with 10 buckets.
		}
		activeSeries.UpdateSeries(series[i], allStorageRefs[i], time.Unix(int64(i), 0), buckets)
	}

	valid := activeSeries.Purge(mockedTime)
	allActive, _, _, _, _, _ := activeSeries.ActiveWithMatchers()
	require.True(t, valid)
	require.Equal(t, 2, allActive)

	activeSeriesPostings := NewNativeHistogramPostings(activeSeries, storagePostings)

	activeRefs, err := index.ExpandPostings(activeSeriesPostings)
	require.NoError(t, err)

	require.Equal(t, allStorageRefs[3:4], activeRefs)
}

func TestNativeHistogramPostings_ExpandWithBucketCount(t *testing.T) {
	ttl := 0
	mockedTime := time.Unix(int64(ttl), 0)
	series := []labels.Labels{
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2"),
		labels.FromStrings("a", "3"), // Will make this series a native histogram.
		labels.FromStrings("a", "4"), // Will make this series a native histogram.
		labels.FromStrings("a", "5"),
	}
	allStorageRefs := []storage.SeriesRef{1, 2, 3, 4, 5}
	storagePostings := index.NewListPostings(allStorageRefs)
<<<<<<< HEAD
	activeSeries := NewActiveSeries(&asmodel.Matchers{}, time.Duration(ttl), "foo", "")
=======
	activeSeries := NewActiveSeries(&Matchers{}, time.Duration(ttl), "foo", "", nil)
>>>>>>> 3c422a8f57 (new service for tracking cost attribution)

	// Update each series at a different time according to its index.
	for i := range allStorageRefs {
		buckets := -1 // No native histogram buckets.
		if i == 2 || i == 3 {
			buckets = i * 10 // Native histogram with i*10 buckets.
		}
		activeSeries.UpdateSeries(series[i], allStorageRefs[i], time.Unix(int64(i), 0), buckets)
	}

	valid := activeSeries.Purge(mockedTime)
	allActive, _, _, _, _, _ := activeSeries.ActiveWithMatchers()
	require.True(t, valid)
	require.Equal(t, 5, allActive)

	activeSeriesPostings := NewNativeHistogramPostings(activeSeries, storagePostings)

	seriesRef := []storage.SeriesRef{}
	bucketCounts := []int{}
	for activeSeriesPostings.Next() {
		ref, count := activeSeriesPostings.AtBucketCount()
		seriesRef = append(seriesRef, ref)
		bucketCounts = append(bucketCounts, count)
	}
	//activeRefs, err := index.ExpandPostings(activeSeriesPostings)
	require.NoError(t, activeSeriesPostings.Err())

	require.Equal(t, allStorageRefs[2:4], seriesRef)
	require.Equal(t, []int{20, 30}, bucketCounts)
}

func TestNativeHistogramPostings_SeekSkipsNonNative(t *testing.T) {
	ttl := 3
	mockedTime := time.Unix(int64(ttl), 0)
	series := []labels.Labels{
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2"),
		labels.FromStrings("a", "3"),
		labels.FromStrings("a", "4"),
		labels.FromStrings("a", "5"),
	}
	allStorageRefs := []storage.SeriesRef{1, 2, 3, 4, 5}
	storagePostings := index.NewListPostings(allStorageRefs)
<<<<<<< HEAD
	activeSeries := NewActiveSeries(&asmodel.Matchers{}, time.Duration(ttl), "foo", "")
=======
	activeSeries := NewActiveSeries(&Matchers{}, time.Duration(ttl), "foo", "", nil)
>>>>>>> 3c422a8f57 (new service for tracking cost attribution)

	// Update each series at a different time according to its index.
	for i := range allStorageRefs {
		buckets := i * 10
		if i+1 == 4 {
			buckets = -1 // Make ref==4 not a native histogram to check that Seek skips it.
		}
		activeSeries.UpdateSeries(series[i], allStorageRefs[i], time.Unix(int64(i), 0), buckets)
	}

	valid := activeSeries.Purge(mockedTime)
	allActive, _, _, _, _, _ := activeSeries.ActiveWithMatchers()
	require.True(t, valid)
	require.Equal(t, 2, allActive)

	activeSeriesPostings := NewNativeHistogramPostings(activeSeries, storagePostings)

	// Seek to a series that is not active.
	require.True(t, activeSeriesPostings.Seek(3))
	// The next active series is 4, but it's not a native histogram.
	require.Equal(t, storage.SeriesRef(5), activeSeriesPostings.At())
	// Check the bucket count as well.
	ref, count := activeSeriesPostings.AtBucketCount()
	require.Equal(t, storage.SeriesRef(5), ref)
	require.Equal(t, 40, count)
}

func TestNativeHistogramPostings_Seek(t *testing.T) {
	ttl := 3
	mockedTime := time.Unix(int64(ttl), 0)
	series := []labels.Labels{
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2"),
		labels.FromStrings("a", "3"),
		labels.FromStrings("a", "4"),
		labels.FromStrings("a", "5"),
	}
	allStorageRefs := []storage.SeriesRef{1, 2, 3, 4, 5}
	storagePostings := index.NewListPostings(allStorageRefs)
<<<<<<< HEAD
	activeSeries := NewActiveSeries(&asmodel.Matchers{}, time.Duration(ttl), "foo", "")
=======
	activeSeries := NewActiveSeries(&Matchers{}, time.Duration(ttl), "foo", "", nil)
>>>>>>> 3c422a8f57 (new service for tracking cost attribution)

	// Update each series at a different time according to its index.
	for i := range allStorageRefs {
		buckets := i * 10
		activeSeries.UpdateSeries(series[i], allStorageRefs[i], time.Unix(int64(i), 0), buckets)
	}

	valid := activeSeries.Purge(mockedTime)
	allActive, _, _, _, _, _ := activeSeries.ActiveWithMatchers()
	require.True(t, valid)
	require.Equal(t, 2, allActive)

	activeSeriesPostings := NewNativeHistogramPostings(activeSeries, storagePostings)

	// Seek to a series that is active.
	require.True(t, activeSeriesPostings.Seek(4))
	// The next active series is 4.
	require.Equal(t, storage.SeriesRef(4), activeSeriesPostings.At())
	// Check the bucket count as well.
	ref, count := activeSeriesPostings.AtBucketCount()
	require.Equal(t, storage.SeriesRef(4), ref)
	require.Equal(t, 30, count)
}

func TestNativeHistogramPostings_SeekToEnd(t *testing.T) {
	ttl := 5
	mockedTime := time.Unix(int64(ttl), 0)
	series := []labels.Labels{
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2"),
		labels.FromStrings("a", "3"),
		labels.FromStrings("a", "4"),
		labels.FromStrings("a", "5"),
	}
	allStorageRefs := []storage.SeriesRef{1, 2, 3, 4, 5}
	storagePostings := index.NewListPostings(allStorageRefs)
<<<<<<< HEAD
	activeSeries := NewActiveSeries(&asmodel.Matchers{}, time.Duration(ttl), "foo", "")
=======
	activeSeries := NewActiveSeries(&Matchers{}, time.Duration(ttl), "foo", "", nil)
>>>>>>> 3c422a8f57 (new service for tracking cost attribution)

	// Update each series at a different time according to its index.
	for i := range allStorageRefs {
		activeSeries.UpdateSeries(series[i], allStorageRefs[i], time.Unix(int64(i), 0), 10)
	}

	valid := activeSeries.Purge(mockedTime)
	allActive, _, _, _, _, _ := activeSeries.ActiveWithMatchers()
	require.True(t, valid)
	require.Equal(t, 0, allActive)

	activeSeriesPostings := NewNativeHistogramPostings(activeSeries, storagePostings)

	// Seek to a series that is not active.
	// There are no active series after 3, so Seek should return false.
	require.False(t, activeSeriesPostings.Seek(3))
}
