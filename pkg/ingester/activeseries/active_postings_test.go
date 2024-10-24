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

func TestPostings_Expand(t *testing.T) {
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
	activeSeries := NewActiveSeries(&asmodel.Matchers{}, time.Duration(ttl), "foo", nil)

	// Update each series at a different time according to its index.
	for i := range allStorageRefs {
		activeSeries.UpdateSeries(series[i], allStorageRefs[i], time.Unix(int64(i), 0), -1)
	}

	valid := activeSeries.Purge(mockedTime)
	allActive, _, _, _, _, _ := activeSeries.ActiveWithMatchers()
	require.True(t, valid)
	require.Equal(t, 2, allActive)

	activeSeriesPostings := NewPostings(activeSeries, storagePostings)

	activeRefs, err := index.ExpandPostings(activeSeriesPostings)
	require.NoError(t, err)

	require.Equal(t, allStorageRefs[ttl:], activeRefs)
}

func TestPostings_Seek(t *testing.T) {
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
	activeSeries := NewActiveSeries(&asmodel.Matchers{}, time.Duration(ttl), "foo", nil)

	// Update each series at a different time according to its index.
	for i := range allStorageRefs {
		activeSeries.UpdateSeries(series[i], allStorageRefs[i], time.Unix(int64(i), 0), -1)
	}

	valid := activeSeries.Purge(mockedTime)
	allActive, _, _, _, _, _ := activeSeries.ActiveWithMatchers()
	require.True(t, valid)
	require.Equal(t, 2, allActive)

	activeSeriesPostings := NewPostings(activeSeries, storagePostings)

	// Seek to a series that is not active.
	require.True(t, activeSeriesPostings.Seek(3))
	// The next active series is 4.
	require.Equal(t, storage.SeriesRef(4), activeSeriesPostings.At())
}

func TestPostings_SeekToEnd(t *testing.T) {
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
	activeSeries := NewActiveSeries(&asmodel.Matchers{}, time.Duration(ttl), "foo", nil)

	// Update each series at a different time according to its index.
	for i := range allStorageRefs {
		activeSeries.UpdateSeries(series[i], allStorageRefs[i], time.Unix(int64(i), 0), -1)
	}

	valid := activeSeries.Purge(mockedTime)
	allActive, _, _, _, _, _ := activeSeries.ActiveWithMatchers()
	require.True(t, valid)
	require.Equal(t, 0, allActive)

	activeSeriesPostings := NewPostings(activeSeries, storagePostings)

	// Seek to a series that is not active.
	// There are no active series after 3, so Seek should return false.
	require.False(t, activeSeriesPostings.Seek(3))
}
