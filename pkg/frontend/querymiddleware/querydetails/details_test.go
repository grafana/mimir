// SPDX-License-Identifier: AGPL-3.0-only

package querydetails

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestQueryDetails_Merge_WidensTimeRanges(t *testing.T) {
	d, _ := ContextWithEmptyDetails(context.Background())
	d.Start = time.UnixMilli(1000)
	d.End = time.UnixMilli(2000)
	d.MinT = time.UnixMilli(1001)
	d.MaxT = time.UnixMilli(2000)

	other, _ := ContextWithEmptyDetails(context.Background())
	other.Start = time.UnixMilli(500)
	other.End = time.UnixMilli(1500)
	other.MinT = time.UnixMilli(501)
	other.MaxT = time.UnixMilli(1500)

	d.Merge(other)

	// Start/MinT take the earliest, End/MaxT the latest, across both.
	require.Equal(t, time.UnixMilli(500), d.Start)
	require.Equal(t, time.UnixMilli(2000), d.End)
	require.Equal(t, time.UnixMilli(501), d.MinT)
	require.Equal(t, time.UnixMilli(2000), d.MaxT)
}

func TestQueryDetails_Merge_IgnoresZeroTimes(t *testing.T) {
	d, _ := ContextWithEmptyDetails(context.Background())
	d.Start = time.UnixMilli(1000)
	d.End = time.UnixMilli(2000)

	// A query that never populated its details must not clobber what's already there.
	other, _ := ContextWithEmptyDetails(context.Background())

	d.Merge(other)

	require.Equal(t, time.UnixMilli(1000), d.Start)
	require.Equal(t, time.UnixMilli(2000), d.End)
	require.True(t, d.MinT.IsZero())
	require.True(t, d.MaxT.IsZero())
}

func TestQueryDetails_Merge_AccumulatesCountersAndStats(t *testing.T) {
	d, _ := ContextWithEmptyDetails(context.Background())
	d.ResultsCacheMissBytes = 1
	d.ResultsCacheMissCount = 2
	d.ResultsCacheHitBytes = 3
	d.ResultsCacheHitCount = 4
	d.ResultsCacheSetCount = 5
	d.ResponseSeriesCount = 6
	d.ResponseSamplesCount = 7
	d.LookbackDelta = time.Minute
	d.QuerierStats.AddFetchedSeries(10)

	other, _ := ContextWithEmptyDetails(context.Background())
	other.ResultsCacheMissBytes = 10
	other.ResultsCacheMissCount = 20
	other.ResultsCacheHitBytes = 30
	other.ResultsCacheHitCount = 40
	other.ResultsCacheSetCount = 50
	other.ResponseSeriesCount = 60
	other.ResponseSamplesCount = 70
	other.LookbackDelta = 5 * time.Minute
	other.QuerierStats.AddFetchedSeries(5)

	d.Merge(other)

	require.Equal(t, 11, d.ResultsCacheMissBytes)
	require.Equal(t, 22, d.ResultsCacheMissCount)
	require.Equal(t, 33, d.ResultsCacheHitBytes)
	require.Equal(t, 44, d.ResultsCacheHitCount)
	require.Equal(t, 55, d.ResultsCacheSetCount)
	require.Equal(t, 66, d.ResponseSeriesCount)
	require.Equal(t, 77, d.ResponseSamplesCount)
	// LookbackDelta takes the largest of the two, it is not summed.
	require.Equal(t, 5*time.Minute, d.LookbackDelta)
	require.Equal(t, uint64(15), d.QuerierStats.LoadFetchedSeries())
}

func TestQueryDetails_Merge_NilsAreNoOps(t *testing.T) {
	d, _ := ContextWithEmptyDetails(context.Background())
	d.Start = time.UnixMilli(1000)

	d.Merge(nil)
	require.Equal(t, time.UnixMilli(1000), d.Start)

	// A nil receiver happens when the request has no QueryDetails in its context.
	var nilDetails *QueryDetails
	require.NotPanics(t, func() { nilDetails.Merge(d) })
}
