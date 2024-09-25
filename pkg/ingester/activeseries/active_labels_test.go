// SPDX-License-Identifier: AGPL-3.0-only

package activeseries

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"

	asmodel "github.com/grafana/mimir/pkg/ingester/activeseries/model"
)

type mockPostingsReader struct {
	postings *index.MemPostings
}

func (m *mockPostingsReader) Postings(ctx context.Context, name string, values ...string) (index.Postings, error) {
	valuePostings := make([]index.Postings, 0, len(values))

	for _, value := range values {
		valuePostings = append(valuePostings, m.postings.Get(name, value))
	}

	return index.Merge(ctx, valuePostings...), nil
}

func TestIsLabelValueActive(t *testing.T) {
	ctx := context.Background()
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
<<<<<<< HEAD
	activeSeries := NewActiveSeries(&asmodel.Matchers{}, time.Duration(ttl), "foo", "")
=======
	activeSeries := NewActiveSeries(&Matchers{}, time.Duration(ttl), "foo", "", nil, 0)
>>>>>>> 7e628c3508 (address comments)

	memPostings := index.NewMemPostings()
	for i, l := range series {
		memPostings.Add(allStorageRefs[i], l)
	}
	reader := &mockPostingsReader{postings: memPostings}

	// Update each series at a different time according to its index.
	for i := range allStorageRefs {
		activeSeries.UpdateSeries(series[i], allStorageRefs[i], time.Unix(int64(i), 0), -1)
	}

	valid := activeSeries.Purge(mockedTime)
	require.True(t, valid)

	result, err := IsLabelValueActive(ctx, reader, activeSeries, "a", "1")
	require.NoError(t, err)
	require.False(t, result)

	result, err = IsLabelValueActive(ctx, reader, activeSeries, "a", "2")
	require.NoError(t, err)
	require.False(t, result)

	result, err = IsLabelValueActive(ctx, reader, activeSeries, "a", "3")
	require.NoError(t, err)
	require.False(t, result)

	result, err = IsLabelValueActive(ctx, reader, activeSeries, "a", "4")
	require.NoError(t, err)
	require.True(t, result)

	result, err = IsLabelValueActive(ctx, reader, activeSeries, "a", "5")
	require.NoError(t, err)
	require.True(t, result)
}
