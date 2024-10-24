// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/active_series_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package activeseries

import (
	"fmt"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	asmodel "github.com/grafana/mimir/pkg/ingester/activeseries/model"
)

func MustNewCustomTrackersConfigFromMap(t require.TestingT, source map[string]string) asmodel.CustomTrackersConfig {
	m, err := asmodel.NewCustomTrackersConfig(source)
	require.NoError(t, err)
	return m
}

const DefaultTimeout = 5 * time.Minute

func TestActiveSeries_UpdateSeries_NoMatchers(t *testing.T) {
	ref1, ls1 := storage.SeriesRef(1), labels.FromStrings("a", "1")
	ref2, ls2 := storage.SeriesRef(2), labels.FromStrings("a", "2")
	ref3, ls3 := storage.SeriesRef(3), labels.FromStrings("a", "3")
	ref4, ls4 := storage.SeriesRef(4), labels.FromStrings("a", "4")
	ref5 := storage.SeriesRef(5) // will be used for ls1 again.

	c := NewActiveSeries(&asmodel.Matchers{}, DefaultTimeout, nil)
	valid := c.Purge(time.Now(), nil)
	assert.True(t, valid)
	allActive, activeMatching, allActiveHistograms, activeMatchingHistograms, allActiveBuckets, activeMatchingBuckets := c.ActiveWithMatchers()
	assert.Equal(t, 0, allActive)
	assert.Empty(t, activeMatching)
	assert.Equal(t, 0, allActiveHistograms)
	assert.Empty(t, activeMatchingHistograms)
	assert.Equal(t, 0, allActiveBuckets)
	assert.Empty(t, activeMatchingBuckets)

	c.UpdateSeries(ls1, ref1, time.Now(), -1, nil)
	valid = c.Purge(time.Now(), nil)
	assert.True(t, valid)
	allActive, _, allActiveHistograms, _, allActiveBuckets, _ = c.ActiveWithMatchers()
	assert.Equal(t, 1, allActive)
	assert.Equal(t, 0, allActiveHistograms)
	assert.Equal(t, 0, allActiveBuckets)
	allActive, allActiveHistograms, allActiveBuckets = c.Active()
	assert.Equal(t, 1, allActive)
	assert.Equal(t, 0, allActiveHistograms)
	assert.Equal(t, 0, allActiveBuckets)

	c.UpdateSeries(ls1, ref1, time.Now(), -1, nil)
	valid = c.Purge(time.Now(), nil)
	assert.True(t, valid)
	allActive, _, allActiveHistograms, _, allActiveBuckets, _ = c.ActiveWithMatchers()
	assert.Equal(t, 1, allActive)
	assert.Equal(t, 0, allActiveHistograms)
	assert.Equal(t, 0, allActiveBuckets)
	allActive, allActiveHistograms, allActiveBuckets = c.Active()
	assert.Equal(t, 1, allActive)
	assert.Equal(t, 0, allActiveHistograms)
	assert.Equal(t, 0, allActiveBuckets)

	c.UpdateSeries(ls2, ref2, time.Now(), -1, nil)
	valid = c.Purge(time.Now(), nil)
	assert.True(t, valid)
	allActive, _, allActiveHistograms, _, allActiveBuckets, _ = c.ActiveWithMatchers()
	assert.Equal(t, 2, allActive)
	assert.Equal(t, 0, allActiveHistograms)
	assert.Equal(t, 0, allActiveBuckets)
	allActive, allActiveHistograms, allActiveBuckets = c.Active()
	assert.Equal(t, 2, allActive)
	assert.Equal(t, 0, allActiveHistograms)
	assert.Equal(t, 0, allActiveBuckets)

	c.UpdateSeries(ls3, ref3, time.Now(), 5, nil)
	valid = c.Purge(time.Now(), nil)
	assert.True(t, valid)
	allActive, _, allActiveHistograms, _, allActiveBuckets, _ = c.ActiveWithMatchers()
	assert.Equal(t, 3, allActive)
	assert.Equal(t, 1, allActiveHistograms)
	assert.Equal(t, 5, allActiveBuckets)
	allActive, allActiveHistograms, allActiveBuckets = c.Active()
	assert.Equal(t, 3, allActive)
	assert.Equal(t, 1, allActiveHistograms)
	assert.Equal(t, 5, allActiveBuckets)

	c.UpdateSeries(ls4, ref4, time.Now(), 3, nil)
	valid = c.Purge(time.Now(), nil)
	assert.True(t, valid)
	allActive, _, allActiveHistograms, _, allActiveBuckets, _ = c.ActiveWithMatchers()
	assert.Equal(t, 4, allActive)
	assert.Equal(t, 2, allActiveHistograms)
	assert.Equal(t, 8, allActiveBuckets)
	allActive, allActiveHistograms, allActiveBuckets = c.Active()
	assert.Equal(t, 4, allActive)
	assert.Equal(t, 2, allActiveHistograms)
	assert.Equal(t, 8, allActiveBuckets)

	// more buckets for a histogram
	c.UpdateSeries(ls3, ref3, time.Now(), 7, nil)
	valid = c.Purge(time.Now(), nil)
	assert.True(t, valid)
	allActive, _, allActiveHistograms, _, allActiveBuckets, _ = c.ActiveWithMatchers()
	assert.Equal(t, 4, allActive)
	assert.Equal(t, 2, allActiveHistograms)
	assert.Equal(t, 10, allActiveBuckets)
	allActive, allActiveHistograms, allActiveBuckets = c.Active()
	assert.Equal(t, 4, allActive)
	assert.Equal(t, 2, allActiveHistograms)
	assert.Equal(t, 10, allActiveBuckets)

	// changing a metric from histogram to float
	c.UpdateSeries(ls4, ref4, time.Now(), -1, nil)
	valid = c.Purge(time.Now(), nil)
	assert.True(t, valid)
	allActive, _, allActiveHistograms, _, allActiveBuckets, _ = c.ActiveWithMatchers()
	assert.Equal(t, 4, allActive)
	assert.Equal(t, 1, allActiveHistograms)
	assert.Equal(t, 7, allActiveBuckets)
	allActive, allActiveHistograms, allActiveBuckets = c.Active()
	assert.Equal(t, 4, allActive)
	assert.Equal(t, 1, allActiveHistograms)
	assert.Equal(t, 7, allActiveBuckets)

	// ref1 was deleted from head, but still active.
	c.PostDeletion(map[chunks.HeadSeriesRef]labels.Labels{
		chunks.HeadSeriesRef(ref1): ls1,
	})
	allActive, _, allActiveHistograms, _, allActiveBuckets, _ = c.ActiveWithMatchers()
	assert.Equal(t, 4, allActive)
	assert.Equal(t, 1, allActiveHistograms)
	assert.Equal(t, 7, allActiveBuckets)
	allActive, allActiveHistograms, allActiveBuckets = c.Active()
	assert.Equal(t, 4, allActive)
	assert.Equal(t, 1, allActiveHistograms)
	assert.Equal(t, 7, allActiveBuckets)

	// Doesn't change after purging.
	valid = c.Purge(time.Now(), nil)
	assert.True(t, valid)
	allActive, _, allActiveHistograms, _, allActiveBuckets, _ = c.ActiveWithMatchers()
	assert.Equal(t, 4, allActive)
	assert.Equal(t, 1, allActiveHistograms)
	assert.Equal(t, 7, allActiveBuckets)
	allActive, allActiveHistograms, allActiveBuckets = c.Active()
	assert.Equal(t, 4, allActive)
	assert.Equal(t, 1, allActiveHistograms)
	assert.Equal(t, 7, allActiveBuckets)

	// ref5 is created with the same labelset as ls1, it shouldn't be accounted as different series.
	c.UpdateSeries(ls1, ref5, time.Now(), -1, nil)
	allActive, _, allActiveHistograms, _, allActiveBuckets, _ = c.ActiveWithMatchers()
	assert.Equal(t, 4, allActive)
	assert.Equal(t, 1, allActiveHistograms)
	assert.Equal(t, 7, allActiveBuckets)
	allActive, allActiveHistograms, allActiveBuckets = c.Active()
	assert.Equal(t, 4, allActive)
	assert.Equal(t, 1, allActiveHistograms)
	assert.Equal(t, 7, allActiveBuckets)

	// Doesn't change after purging.
	valid = c.Purge(time.Now(), nil)
	assert.True(t, valid)
	allActive, _, allActiveHistograms, _, allActiveBuckets, _ = c.ActiveWithMatchers()
	assert.Equal(t, 4, allActive)
	assert.Equal(t, 1, allActiveHistograms)
	assert.Equal(t, 7, allActiveBuckets)
	allActive, allActiveHistograms, allActiveBuckets = c.Active()
	assert.Equal(t, 4, allActive)
	assert.Equal(t, 1, allActiveHistograms)
	assert.Equal(t, 7, allActiveBuckets)

	// Make sure deleted is empty, so we're not leaking.
	assert.Empty(t, c.deleted.refs)
	assert.Empty(t, c.deleted.keys)
}

func TestActiveSeries_ContainsRef(t *testing.T) {
	collision1, collision2 := labelsWithHashCollision()
	series := []labels.Labels{
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2"),
		collision1,
		collision2,
	}

	refs := []storage.SeriesRef{1, 2, 3, 4}

	// Run the same test for increasing TTL values
	for ttl := 1; ttl <= len(series); ttl++ {
		t.Run(fmt.Sprintf("ttl: %d", ttl), func(t *testing.T) {
			mockedTime := time.Unix(int64(ttl), 0)
			c := NewActiveSeries(&asmodel.Matchers{}, DefaultTimeout, nil)

			// Update each series with a different timestamp according to each index
			for i := 0; i < len(series); i++ {
				c.UpdateSeries(series[i], refs[i], time.Unix(int64(i), 0), -1, nil)
			}

			c.purge(time.Unix(int64(ttl), 0), nil)

			// The expected number of series is the total number of series minus the ttl
			// because the first ttl series should be purged
			exp := len(series) - (ttl)
			valid := c.Purge(mockedTime, nil)
			assert.True(t, valid)
			allActive, activeMatching, _, _, _, _ := c.ActiveWithMatchers()
			assert.Equal(t, exp, allActive)
			assert.Empty(t, activeMatching)

			for i := 0; i < len(series); i++ {
				assert.Equal(t, i >= ttl, c.ContainsRef(refs[i]))
			}
		})
	}
}

func TestActiveSeries_UpdateSeries_WithMatchers(t *testing.T) {
	asm := asmodel.NewMatchers(MustNewCustomTrackersConfigFromMap(t, map[string]string{"foo": `{a=~"2|3|4"}`}))
	c := NewActiveSeries(asm, DefaultTimeout, nil)
	testUpdateSeries(t, c)
}

func testUpdateSeries(t *testing.T, c *ActiveSeries) {
	ref1, ls1 := storage.SeriesRef(1), labels.FromStrings("a", "1")
	ref2, ls2 := storage.SeriesRef(2), labels.FromStrings("a", "2")
	ref3, ls3 := storage.SeriesRef(3), labels.FromStrings("a", "3")
	ref4, ls4 := storage.SeriesRef(4), labels.FromStrings("a", "4")
	ref5, ls5 := storage.SeriesRef(5), labels.FromStrings("a", "5")
	ref6 := storage.SeriesRef(6) // same as ls2

	valid := c.Purge(time.Now(), nil)
	assert.True(t, valid)
	allActive, activeMatching, allActiveHistograms, activeMatchingHistograms, allActiveBuckets, activeMatchingBuckets := c.ActiveWithMatchers()
	assert.Equal(t, 0, allActive)
	assert.Equal(t, []int{0}, activeMatching)
	assert.Equal(t, 0, allActiveHistograms)
	assert.Equal(t, []int{0}, activeMatchingHistograms)
	assert.Equal(t, 0, allActiveBuckets)
	assert.Equal(t, []int{0}, activeMatchingBuckets)
	allActive, allActiveHistograms, allActiveBuckets = c.Active()
	assert.Equal(t, 0, allActive)
	assert.Equal(t, 0, allActiveHistograms)
	assert.Equal(t, 0, allActiveBuckets)

	c.UpdateSeries(ls1, ref1, time.Now(), -1, nil)
	valid = c.Purge(time.Now(), nil)
	assert.True(t, valid)
	allActive, activeMatching, allActiveHistograms, activeMatchingHistograms, allActiveBuckets, activeMatchingBuckets = c.ActiveWithMatchers()
	assert.Equal(t, 1, allActive)
	assert.Equal(t, []int{0}, activeMatching)
	assert.Equal(t, 0, allActiveHistograms)
	assert.Equal(t, []int{0}, activeMatchingHistograms)
	assert.Equal(t, 0, allActiveBuckets)
	assert.Equal(t, []int{0}, activeMatchingBuckets)
	allActive, allActiveHistograms, allActiveBuckets = c.Active()
	assert.Equal(t, 1, allActive)
	assert.Equal(t, 0, allActiveHistograms)
	assert.Equal(t, 0, allActiveBuckets)

	c.UpdateSeries(ls2, ref2, time.Now(), -1, nil)
	valid = c.Purge(time.Now(), nil)
	assert.True(t, valid)
	allActive, activeMatching, allActiveHistograms, activeMatchingHistograms, allActiveBuckets, activeMatchingBuckets = c.ActiveWithMatchers()
	assert.Equal(t, 2, allActive)
	assert.Equal(t, []int{1}, activeMatching)
	assert.Equal(t, 0, allActiveHistograms)
	assert.Equal(t, []int{0}, activeMatchingHistograms)
	assert.Equal(t, 0, allActiveBuckets)
	assert.Equal(t, []int{0}, activeMatchingBuckets)
	allActive, allActiveHistograms, allActiveBuckets = c.Active()
	assert.Equal(t, 2, allActive)
	assert.Equal(t, 0, allActiveHistograms)
	assert.Equal(t, 0, allActiveBuckets)

	c.UpdateSeries(ls3, ref3, time.Now(), -1, nil)
	valid = c.Purge(time.Now(), nil)
	assert.True(t, valid)
	allActive, activeMatching, allActiveHistograms, activeMatchingHistograms, allActiveBuckets, activeMatchingBuckets = c.ActiveWithMatchers()
	assert.Equal(t, 3, allActive)
	assert.Equal(t, []int{2}, activeMatching)
	assert.Equal(t, 0, allActiveHistograms)
	assert.Equal(t, []int{0}, activeMatchingHistograms)
	assert.Equal(t, 0, allActiveBuckets)
	assert.Equal(t, []int{0}, activeMatchingBuckets)
	allActive, allActiveHistograms, allActiveBuckets = c.Active()
	assert.Equal(t, 3, allActive)
	assert.Equal(t, 0, allActiveHistograms)
	assert.Equal(t, 0, allActiveBuckets)

	c.UpdateSeries(ls3, ref3, time.Now(), -1, nil)
	valid = c.Purge(time.Now(), nil)
	assert.True(t, valid)
	allActive, activeMatching, allActiveHistograms, activeMatchingHistograms, allActiveBuckets, activeMatchingBuckets = c.ActiveWithMatchers()
	assert.Equal(t, 3, allActive)
	assert.Equal(t, []int{2}, activeMatching)
	assert.Equal(t, 0, allActiveHistograms)
	assert.Equal(t, []int{0}, activeMatchingHistograms)
	assert.Equal(t, 0, allActiveBuckets)
	assert.Equal(t, []int{0}, activeMatchingBuckets)
	allActive, allActiveHistograms, allActiveBuckets = c.Active()
	assert.Equal(t, 3, allActive)
	assert.Equal(t, 0, allActiveHistograms)
	assert.Equal(t, 0, allActiveBuckets)

	c.UpdateSeries(ls4, ref4, time.Now(), 3, nil)
	valid = c.Purge(time.Now(), nil)
	assert.True(t, valid)
	allActive, activeMatching, allActiveHistograms, activeMatchingHistograms, allActiveBuckets, activeMatchingBuckets = c.ActiveWithMatchers()
	assert.Equal(t, 4, allActive)
	assert.Equal(t, []int{3}, activeMatching)
	assert.Equal(t, 1, allActiveHistograms)
	assert.Equal(t, []int{1}, activeMatchingHistograms)
	assert.Equal(t, 3, allActiveBuckets)
	assert.Equal(t, []int{3}, activeMatchingBuckets)
	allActive, allActiveHistograms, allActiveBuckets = c.Active()
	assert.Equal(t, 4, allActive)
	assert.Equal(t, 1, allActiveHistograms)
	assert.Equal(t, 3, allActiveBuckets)

	c.UpdateSeries(ls5, ref5, time.Now(), 5, nil)
	valid = c.Purge(time.Now(), nil)
	assert.True(t, valid)
	allActive, activeMatching, allActiveHistograms, activeMatchingHistograms, allActiveBuckets, activeMatchingBuckets = c.ActiveWithMatchers()
	assert.Equal(t, 5, allActive)
	assert.Equal(t, []int{3}, activeMatching)
	assert.Equal(t, 2, allActiveHistograms)
	assert.Equal(t, []int{1}, activeMatchingHistograms)
	assert.Equal(t, 8, allActiveBuckets)
	assert.Equal(t, []int{3}, activeMatchingBuckets)
	allActive, allActiveHistograms, allActiveBuckets = c.Active()
	assert.Equal(t, 5, allActive)
	assert.Equal(t, 2, allActiveHistograms)
	assert.Equal(t, 8, allActiveBuckets)

	// changing a metric from float to histogram
	c.UpdateSeries(ls3, ref3, time.Now(), 6, nil)
	valid = c.Purge(time.Now(), nil)
	assert.True(t, valid)
	allActive, activeMatching, allActiveHistograms, activeMatchingHistograms, allActiveBuckets, activeMatchingBuckets = c.ActiveWithMatchers()
	assert.Equal(t, 5, allActive)
	assert.Equal(t, []int{3}, activeMatching)
	assert.Equal(t, 3, allActiveHistograms)
	assert.Equal(t, []int{2}, activeMatchingHistograms)
	assert.Equal(t, 14, allActiveBuckets)
	assert.Equal(t, []int{9}, activeMatchingBuckets)
	allActive, allActiveHistograms, allActiveBuckets = c.Active()
	assert.Equal(t, 5, allActive)
	assert.Equal(t, 3, allActiveHistograms)
	assert.Equal(t, 14, allActiveBuckets)

	// fewer (zero) buckets for a histogram
	c.UpdateSeries(ls4, ref4, time.Now(), 0, nil)
	valid = c.Purge(time.Now(), nil)
	assert.True(t, valid)
	allActive, activeMatching, allActiveHistograms, activeMatchingHistograms, allActiveBuckets, activeMatchingBuckets = c.ActiveWithMatchers()
	assert.Equal(t, 5, allActive)
	assert.Equal(t, []int{3}, activeMatching)
	assert.Equal(t, 3, allActiveHistograms)
	assert.Equal(t, []int{2}, activeMatchingHistograms)
	assert.Equal(t, 11, allActiveBuckets)
	assert.Equal(t, []int{6}, activeMatchingBuckets)
	allActive, allActiveHistograms, allActiveBuckets = c.Active()
	assert.Equal(t, 5, allActive)
	assert.Equal(t, 3, allActiveHistograms)
	assert.Equal(t, 11, allActiveBuckets)

	// ref2 is deleted from the head, but still active.
	c.PostDeletion(map[chunks.HeadSeriesRef]labels.Labels{
		chunks.HeadSeriesRef(ref2): ls2,
	})
	// Numbers don't change.
	allActive, activeMatching, allActiveHistograms, activeMatchingHistograms, allActiveBuckets, activeMatchingBuckets = c.ActiveWithMatchers()
	assert.Equal(t, 5, allActive)
	assert.Equal(t, []int{3}, activeMatching)
	assert.Equal(t, 3, allActiveHistograms)
	assert.Equal(t, []int{2}, activeMatchingHistograms)
	assert.Equal(t, 11, allActiveBuckets)
	assert.Equal(t, []int{6}, activeMatchingBuckets)
	allActive, allActiveHistograms, allActiveBuckets = c.Active()
	assert.Equal(t, 5, allActive)
	assert.Equal(t, 3, allActiveHistograms)
	assert.Equal(t, 11, allActiveBuckets)

	// Don't change after purging.
	valid = c.Purge(time.Now(), nil)
	assert.True(t, valid)
	allActive, activeMatching, allActiveHistograms, activeMatchingHistograms, allActiveBuckets, activeMatchingBuckets = c.ActiveWithMatchers()
	assert.Equal(t, 5, allActive)
	assert.Equal(t, []int{3}, activeMatching)
	assert.Equal(t, 3, allActiveHistograms)
	assert.Equal(t, []int{2}, activeMatchingHistograms)
	assert.Equal(t, 11, allActiveBuckets)
	assert.Equal(t, []int{6}, activeMatchingBuckets)
	allActive, allActiveHistograms, allActiveBuckets = c.Active()
	assert.Equal(t, 5, allActive)
	assert.Equal(t, 3, allActiveHistograms)
	assert.Equal(t, 11, allActiveBuckets)

	// ls2 is pushed again, this time with ref6
	c.UpdateSeries(ls2, ref6, time.Now(), -1, nil)
	// Numbers don't change.
	allActive, activeMatching, allActiveHistograms, activeMatchingHistograms, allActiveBuckets, activeMatchingBuckets = c.ActiveWithMatchers()
	assert.Equal(t, 5, allActive)
	assert.Equal(t, []int{3}, activeMatching)
	assert.Equal(t, 3, allActiveHistograms)
	assert.Equal(t, []int{2}, activeMatchingHistograms)
	assert.Equal(t, 11, allActiveBuckets)
	assert.Equal(t, []int{6}, activeMatchingBuckets)
	allActive, allActiveHistograms, allActiveBuckets = c.Active()
	assert.Equal(t, 5, allActive)
	assert.Equal(t, 3, allActiveHistograms)
	assert.Equal(t, 11, allActiveBuckets)

	// Don't change after purging.
	valid = c.Purge(time.Now(), nil)
	assert.True(t, valid)
	allActive, activeMatching, allActiveHistograms, activeMatchingHistograms, allActiveBuckets, activeMatchingBuckets = c.ActiveWithMatchers()
	assert.Equal(t, 5, allActive)
	assert.Equal(t, []int{3}, activeMatching)
	assert.Equal(t, 3, allActiveHistograms)
	assert.Equal(t, []int{2}, activeMatchingHistograms)
	assert.Equal(t, 11, allActiveBuckets)
	assert.Equal(t, []int{6}, activeMatchingBuckets)
	allActive, allActiveHistograms, allActiveBuckets = c.Active()
	assert.Equal(t, 5, allActive)
	assert.Equal(t, 3, allActiveHistograms)
	assert.Equal(t, 11, allActiveBuckets)

	// Make sure deleted is empty, so we're not leaking.
	assert.Empty(t, c.deleted.refs)
	assert.Empty(t, c.deleted.keys)
}

func TestActiveSeries_UpdateSeries_Clear(t *testing.T) {
	asm := asmodel.NewMatchers(MustNewCustomTrackersConfigFromMap(t, map[string]string{"foo": `{a=~"2|3|4"}`}))
	c := NewActiveSeries(asm, DefaultTimeout, nil)
	testUpdateSeries(t, c)

	c.Clear()
	allActive, activeMatching, allActiveHistograms, activeMatchingHistograms, allActiveBuckets, activeMatchingBuckets := c.ActiveWithMatchers()
	assert.Equal(t, 0, allActive)
	assert.Equal(t, []int{0}, activeMatching)
	assert.Equal(t, 0, allActiveHistograms)
	assert.Equal(t, []int{0}, activeMatchingHistograms)
	assert.Equal(t, 0, allActiveBuckets)
	assert.Equal(t, []int{0}, activeMatchingBuckets)
	allActive, allActiveHistograms, allActiveBuckets = c.Active()
	assert.Equal(t, 0, allActive)
	assert.Equal(t, 0, allActiveHistograms)
	assert.Equal(t, 0, allActiveBuckets)

	testUpdateSeries(t, c)
}

func labelsWithHashCollision() (labels.Labels, labels.Labels) {
	// These two series have the same XXHash; thanks to https://github.com/pstibrany/labels_hash_collisions
	ls1 := labels.FromStrings("__name__", "metric", "lbl1", "value", "lbl2", "l6CQ5y")
	ls2 := labels.FromStrings("__name__", "metric", "lbl1", "value", "lbl2", "v7uDlF")

	if ls1.Hash() != ls2.Hash() {
		// These ones are the same when using -tags stringlabels
		ls1 = labels.FromStrings("__name__", "metric", "lbl", "HFnEaGl")
		ls2 = labels.FromStrings("__name__", "metric", "lbl", "RqcXatm")
	}

	if ls1.Hash() != ls2.Hash() {
		panic("This code needs to be updated: find new labels with colliding hash values.")
	}

	return ls1, ls2
}

func TestActiveSeries_ShouldCorrectlyHandleHashCollisions(t *testing.T) {
	ls1, ls2 := labelsWithHashCollision()
	ref1, ref2 := storage.SeriesRef(1), storage.SeriesRef(2)

	c := NewActiveSeries(&asmodel.Matchers{}, DefaultTimeout, nil)
	c.UpdateSeries(ls1, ref1, time.Now(), -1, nil)
	c.UpdateSeries(ls2, ref2, time.Now(), -1, nil)

	valid := c.Purge(time.Now(), nil)
	assert.True(t, valid)
	allActive, _, _, _, _, _ := c.ActiveWithMatchers()
	assert.Equal(t, 2, allActive)
}

func TestActiveSeries_Purge_NoMatchers(t *testing.T) {
	collision1, collision2 := labelsWithHashCollision()
	deletedLabels := labels.FromStrings("deleted", "true")
	series := []labels.Labels{
		deletedLabels,
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2"),
		collision1,
		collision2,
	}

	const deletedRef = 1
	refs := []storage.SeriesRef{1, 2, 3, 4, 5}

	// Run the same test for increasing TTL values
	for ttl := 1; ttl <= len(series); ttl++ {
		t.Run(fmt.Sprintf("ttl: %d", ttl), func(t *testing.T) {
			mockedTime := time.Unix(int64(ttl), 0)
			c := NewActiveSeries(&asmodel.Matchers{}, DefaultTimeout, nil)

			for i := 0; i < len(series); i++ {
				c.UpdateSeries(series[i], refs[i], time.Unix(int64(i), 0), -1, nil)
			}
			c.PostDeletion(map[chunks.HeadSeriesRef]labels.Labels{
				deletedRef: deletedLabels,
			})

			c.purge(time.Unix(int64(ttl), 0), nil)
			// call purge twice, just to hit "quick" path. It doesn't really do anything.
			c.purge(time.Unix(int64(ttl), 0), nil)

			exp := len(series) - (ttl)
			// Purge is not intended to purge
			valid := c.Purge(mockedTime, nil)
			assert.True(t, valid)
			allActive, activeMatching, _, _, _, _ := c.ActiveWithMatchers()
			assert.Equal(t, exp, allActive)
			assert.Empty(t, activeMatching)

			// Deleted series is the first one so it should be always deleted and we should see empty deleted refs & keys.
			assert.Empty(t, c.deleted.refs)
			assert.Empty(t, c.deleted.keys)
		})
	}
}

func TestActiveSeries_Purge_WithMatchers(t *testing.T) {
	collision1, collision2 := labelsWithHashCollision()
	series := []labels.Labels{
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2"),
		collision1,
		collision2,
	}

	refs := []storage.SeriesRef{1, 2, 3, 4}

	asm := asmodel.NewMatchers(MustNewCustomTrackersConfigFromMap(t, map[string]string{"foo": `{_=~"y.*"}`}))

	// Run the same test for increasing TTL values
	for ttl := 1; ttl <= len(series); ttl++ {
		t.Run(fmt.Sprintf("ttl=%d", ttl), func(t *testing.T) {
			mockedTime := time.Unix(int64(ttl), 0)

			c := NewActiveSeries(asm, 5*time.Minute, nil)

			exp := len(series) - ttl
			expMatchingSeries := 0

			for i, s := range series {
				c.UpdateSeries(series[i], refs[i], time.Unix(int64(i), 0), -1, nil)

				// if this series is matching, and they're within the ttl
				tmp := asm.Matches(s)
				if tmp.Len() > 0 && i >= ttl {
					expMatchingSeries++
				}
			}

			c.purge(time.Unix(int64(ttl), 0), nil)
			// call purge twice, just to hit "quick" path. It doesn't really do anything.
			c.purge(time.Unix(int64(ttl), 0), nil)

			valid := c.Purge(mockedTime, nil)
			assert.True(t, valid)
			allActive, activeMatching, _, _, _, _ := c.ActiveWithMatchers()
			assert.Equal(t, exp, allActive)
			assert.Equal(t, []int{expMatchingSeries}, activeMatching)
		})
	}
}

func TestActiveSeries_PurgeOpt(t *testing.T) {
	ls1, ls2 := labelsWithHashCollision()
	ref1, ref2 := storage.SeriesRef(1), storage.SeriesRef(2)

	currentTime := time.Now()
	c := NewActiveSeries(&asmodel.Matchers{}, 59*time.Second, nil)

	c.UpdateSeries(ls1, ref1, currentTime.Add(-2*time.Minute), -1, nil)
	c.UpdateSeries(ls2, ref2, currentTime, -1, nil)

	valid := c.Purge(currentTime, nil)
	assert.True(t, valid)
	allActive, _, _, _, _, _ := c.ActiveWithMatchers()
	assert.Equal(t, 1, allActive)

	c.UpdateSeries(ls1, ref1, currentTime.Add(-1*time.Minute), -1, nil)
	c.UpdateSeries(ls2, ref2, currentTime, -1, nil)

	valid = c.Purge(currentTime, nil)
	assert.True(t, valid)
	allActive, _, _, _, _, _ = c.ActiveWithMatchers()
	assert.Equal(t, 1, allActive)

	// This will *not* update the series, since there is already newer timestamp.
	c.UpdateSeries(ls2, ref2, currentTime.Add(-1*time.Minute), -1, nil)

	valid = c.Purge(currentTime, nil)
	assert.True(t, valid)
	allActive, _, _, _, _, _ = c.ActiveWithMatchers()
	assert.Equal(t, 1, allActive)
}

func TestActiveSeries_ReloadSeriesMatchers(t *testing.T) {
	ref1, ls1 := storage.SeriesRef(1), labels.FromStrings("a", "1")
	ref2, ls2 := storage.SeriesRef(2), labels.FromStrings("a", "2")
	ref3, ls3 := storage.SeriesRef(3), labels.FromStrings("a", "3")
	ref4, ls4 := storage.SeriesRef(4), labels.FromStrings("a", "4")

	asm := asmodel.NewMatchers(MustNewCustomTrackersConfigFromMap(t, map[string]string{"foo": `{a=~.*}`}))

	currentTime := time.Now()
	c := NewActiveSeries(asm, DefaultTimeout, nil)

	valid := c.Purge(currentTime, nil)
	assert.True(t, valid)
	allActive, activeMatching, _, _, _, _ := c.ActiveWithMatchers()
	assert.Equal(t, 0, allActive)
	assert.Equal(t, []int{0}, activeMatching)

	c.UpdateSeries(ls1, ref1, currentTime, -1, nil)
	valid = c.Purge(currentTime, nil)
	assert.True(t, valid)
	allActive, activeMatching, _, _, _, _ = c.ActiveWithMatchers()
	assert.Equal(t, 1, allActive)
	assert.Equal(t, []int{1}, activeMatching)

	c.ReloadMatchers(asm, currentTime)
	valid = c.Purge(currentTime, nil)
	assert.False(t, valid)

	// Adding timeout time to make Purge results valid.
	currentTime = currentTime.Add(DefaultTimeout)
	c.UpdateSeries(ls1, ref1, currentTime, -1, nil)
	c.UpdateSeries(ls2, ref2, currentTime, -1, nil)
	valid = c.Purge(currentTime, nil)
	assert.True(t, valid)
	allActive, activeMatching, _, _, _, _ = c.ActiveWithMatchers()
	assert.Equal(t, 2, allActive)
	assert.Equal(t, []int{2}, activeMatching)

	asmWithLessMatchers := asmodel.NewMatchers(MustNewCustomTrackersConfigFromMap(t, map[string]string{}))
	c.ReloadMatchers(asmWithLessMatchers, currentTime)

	// Adding timeout time to make Purge results valid.
	currentTime = currentTime.Add(DefaultTimeout)
	c.UpdateSeries(ls3, ref3, currentTime, -1, nil)
	valid = c.Purge(currentTime, nil)
	assert.True(t, valid)
	allActive, activeMatching, _, _, _, _ = c.ActiveWithMatchers()
	assert.Equal(t, 1, allActive)
	assert.Empty(t, activeMatching)

	asmWithMoreMatchers := asmodel.NewMatchers(MustNewCustomTrackersConfigFromMap(t, map[string]string{
		"a": `{a="3"}`,
		"b": `{a="4"}`,
	}))
	c.ReloadMatchers(asmWithMoreMatchers, currentTime)

	// Adding timeout time to make Purge results valid.
	currentTime = currentTime.Add(DefaultTimeout)
	c.UpdateSeries(ls4, ref4, currentTime, -1, nil)
	valid = c.Purge(currentTime, nil)
	assert.True(t, valid)
	allActive, activeMatching, _, _, _, _ = c.ActiveWithMatchers()
	assert.Equal(t, 1, allActive)
	assert.Equal(t, []int{0, 1}, activeMatching)
}

func TestActiveSeries_ReloadSeriesMatchers_LessMatchers(t *testing.T) {
	ref1, ls1 := storage.SeriesRef(1), labels.FromStrings("a", "1")

	asm := asmodel.NewMatchers(MustNewCustomTrackersConfigFromMap(t, map[string]string{
		"foo": `{a=~.+}`,
		"bar": `{a=~.+}`,
	}))

	currentTime := time.Now()
	c := NewActiveSeries(asm, DefaultTimeout, nil)
	valid := c.Purge(currentTime, nil)
	assert.True(t, valid)
	allActive, activeMatching, _, _, _, _ := c.ActiveWithMatchers()
	assert.Equal(t, 0, allActive)
	assert.Equal(t, []int{0, 0}, activeMatching)

	c.UpdateSeries(ls1, ref1, currentTime, -1, nil)
	valid = c.Purge(currentTime, nil)
	assert.True(t, valid)
	allActive, activeMatching, _, _, _, _ = c.ActiveWithMatchers()
	assert.Equal(t, 1, allActive)
	assert.Equal(t, []int{1, 1}, activeMatching)

	asm = asmodel.NewMatchers(MustNewCustomTrackersConfigFromMap(t, map[string]string{
		"foo": `{a=~.+}`,
	}))

	c.ReloadMatchers(asm, currentTime)
	c.purge(time.Time{}, nil)
	// Adding timeout time to make Purge results valid.
	currentTime = currentTime.Add(DefaultTimeout)
	valid = c.Purge(currentTime, nil)
	assert.True(t, valid)
	allActive, activeMatching, _, _, _, _ = c.ActiveWithMatchers()
	assert.Equal(t, 0, allActive)
	assert.Equal(t, []int{0}, activeMatching)
}

func TestActiveSeries_ReloadSeriesMatchers_SameSizeNewLabels(t *testing.T) {
	ref1, ls1 := storage.SeriesRef(1), labels.FromStrings("a", "1")

	asm := asmodel.NewMatchers(MustNewCustomTrackersConfigFromMap(t, map[string]string{
		"foo": `{a=~.+}`,
		"bar": `{a=~.+}`,
	}))

	currentTime := time.Now()

	c := NewActiveSeries(asm, DefaultTimeout, nil)
	valid := c.Purge(currentTime, nil)
	assert.True(t, valid)
	allActive, activeMatching, _, _, _, _ := c.ActiveWithMatchers()
	assert.Equal(t, 0, allActive)
	assert.Equal(t, []int{0, 0}, activeMatching)

	c.UpdateSeries(ls1, ref1, currentTime, -1, nil)
	valid = c.Purge(currentTime, nil)
	assert.True(t, valid)
	allActive, activeMatching, _, _, _, _ = c.ActiveWithMatchers()
	assert.Equal(t, 1, allActive)
	assert.Equal(t, []int{1, 1}, activeMatching)

	asm = asmodel.NewMatchers(MustNewCustomTrackersConfigFromMap(t, map[string]string{
		"foo": `{b=~.+}`,
		"bar": `{b=~.+}`,
	}))

	c.ReloadMatchers(asm, currentTime)
	c.purge(time.Time{}, nil)
	// Adding timeout time to make Purge results valid.
	currentTime = currentTime.Add(DefaultTimeout)

	valid = c.Purge(currentTime, nil)
	assert.True(t, valid)
	allActive, activeMatching, _, _, _, _ = c.ActiveWithMatchers()
	assert.Equal(t, 0, allActive)
	assert.Equal(t, []int{0, 0}, activeMatching)
}

func BenchmarkActiveSeries_UpdateSeriesConcurrency(b *testing.B) {
	for _, numSeries := range []int{1, 1_000_000} {
		for _, numGoroutines := range []int{50, 100, 500, 1000} {
			for _, withPurge := range []bool{false, true} {
				b.Run(fmt.Sprintf("series = %d, concurrency = %d, purge = %t", numSeries, numGoroutines, withPurge), func(b *testing.B) {
					benchmarkActiveSeriesUpdateSeriesConcurrency(b, numSeries, numGoroutines, withPurge)
				})
			}
		}
	}
}

func benchmarkActiveSeriesUpdateSeriesConcurrency(b *testing.B, numSeries, numGoroutines int, withPurge bool) {
	// Create the series.
	seriesList := make([]labels.Labels, 0, numSeries)
	for i := 0; i < numSeries; i++ {
		seriesList = append(seriesList, labels.FromStrings("series_id", strconv.Itoa(i)))
	}

	var (
		// Run the active series tracker with an active timeout = 0 so that the Purge() will always
		// purge the series.
		c           = NewActiveSeries(&asmodel.Matchers{}, 0, nil)
		updateGroup = &sync.WaitGroup{}
		purgeGroup  = &sync.WaitGroup{}
		start       = make(chan struct{})
		stopPurge   = make(chan struct{})
		max         = int(math.Ceil(float64(b.N) / float64(numGoroutines)))
		nowMillis   = atomic.NewInt64(time.Now().UnixNano())
	)

	// Utility function generate monotonic time increases.
	now := func() time.Time {
		return time.UnixMilli(nowMillis.Inc())
	}

	future := func() time.Time {
		return time.UnixMilli(nowMillis.Add(time.Hour.Milliseconds()))
	}

	for i := 0; i < numGoroutines; i++ {
		updateGroup.Add(1)

		go func(workerID int) {
			defer updateGroup.Done()
			<-start

			// Each worker starts from a different position of the series list,
			// to better simulate a real world scenario.
			nextSeriesID := (numSeries / numGoroutines) * workerID

			for ix := 0; ix < max; ix++ {
				if nextSeriesID >= numSeries {
					nextSeriesID = 0
				}

				c.UpdateSeries(seriesList[nextSeriesID], storage.SeriesRef(nextSeriesID), now(), -1, nil)
			}
		}(i)
	}

	if withPurge {
		purgeGroup.Add(1)

		go func() {
			defer purgeGroup.Done()
			<-start

			for {
				select {
				case <-stopPurge:
					return
				default:
					c.Purge(future(), nil)
				}

				// Throttle, but keep high pressure from Purge().
				time.Sleep(time.Millisecond)
			}
		}()
	}

	b.ResetTimer()
	close(start)
	updateGroup.Wait()

	// The test is over so we can stop the purge routine.
	close(stopPurge)
	purgeGroup.Wait()
}

func BenchmarkActiveSeries_UpdateSeries(b *testing.B) {
	for _, tt := range []struct {
		nRounds   int // Number of times we update the same series
		nSeries   int // Number of series we create
		nMatchers int
	}{
		{
			nRounds: 0, // Just benchmarking NewActiveSeries.
			nSeries: 0,
		},
		{
			nRounds:   0, // Benchmarking NewActiveSeries with matchers.
			nSeries:   0,
			nMatchers: 100,
		},
		{
			nRounds: 1,
			nSeries: 100000,
		},
		{
			nRounds:   1,
			nSeries:   100000,
			nMatchers: 100,
		},
		{
			nRounds: 1,
			nSeries: 1000000,
		},
		{
			nRounds: 10,
			nSeries: 100000,
		},
		{
			nRounds: 10,
			nSeries: 1000000,
		},
		{
			nRounds:   10,
			nSeries:   100000,
			nMatchers: 100,
		},
	} {
		b.Run(fmt.Sprintf("rounds=%d series=%d matchers=%d", tt.nRounds, tt.nSeries, tt.nMatchers), func(b *testing.B) {
			// Prepare series
			const nLabels = 10
			builder := labels.NewScratchBuilder(nLabels)
			series := make([]labels.Labels, tt.nSeries)
			refs := make([]storage.SeriesRef, tt.nSeries)
			for s := 0; s < tt.nSeries; s++ {
				builder.Reset()
				for i := 0; i < nLabels; i++ {
					// Label ~20B name, ~40B value.
					builder.Add(fmt.Sprintf("abcdefghijabcdefghi%d", i), fmt.Sprintf("abcdefghijabcdefghijabcdefghijabcd%d", s))
				}
				series[s] = builder.Labels()
				refs[s] = storage.SeriesRef(s)
			}

			// Prepare matchers.
			m := map[string]string{}
			for i := 0; i < tt.nMatchers; i++ {
				m[fmt.Sprintf("matcher%d", i)] = fmt.Sprintf(`{abcdefghijabcdefghi0=~.*%d}`, i)
			}
			asm := asmodel.NewMatchers(MustNewCustomTrackersConfigFromMap(b, m))

			now := time.Now().UnixNano()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				c := NewActiveSeries(asm, DefaultTimeout, nil)
				for round := 0; round <= tt.nRounds; round++ {
					for ix := 0; ix < tt.nSeries; ix++ {
						c.UpdateSeries(series[ix], refs[ix], time.Unix(0, now), -1, nil)
						now++
					}
				}
			}
		})
	}
}

func BenchmarkActiveSeries_Active_once(b *testing.B) {
	benchmarkPurge(b, false)
}

func BenchmarkActiveSeries_Active_twice(b *testing.B) {
	benchmarkPurge(b, true)
}

func benchmarkPurge(b *testing.B, twice bool) {
	const numSeries = 10000
	const numExpiresSeries = numSeries / 25

	currentTime := time.Now()
	c := NewActiveSeries(&asmodel.Matchers{}, DefaultTimeout, nil)

	series := [numSeries]labels.Labels{}
	refs := [numSeries]storage.SeriesRef{}
	for s := 0; s < numSeries; s++ {
		series[s] = labels.FromStrings("a", strconv.Itoa(s))
		refs[s] = storage.SeriesRef(s)
	}

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		// Prepare series
		for ix, s := range series {
			if ix < numExpiresSeries {
				c.UpdateSeries(s, refs[ix], currentTime.Add(-DefaultTimeout), -1, nil)
			} else {
				c.UpdateSeries(s, refs[ix], currentTime, -1, nil)
			}
		}

		valid := c.Purge(currentTime, nil)
		assert.True(b, valid)
		allActive, _, _, _, _, _ := c.ActiveWithMatchers()
		assert.Equal(b, numSeries, allActive)
		b.StartTimer()

		// Purge is going to purge everything
		currentTime = currentTime.Add(DefaultTimeout)
		valid = c.Purge(currentTime, nil)
		assert.True(b, valid)
		allActive, _, _, _, _, _ = c.ActiveWithMatchers()
		assert.Equal(b, numSeries-numExpiresSeries, allActive)

		if twice {
			valid = c.Purge(currentTime, nil)
			assert.True(b, valid)
			allActive, _, _, _, _, _ = c.ActiveWithMatchers()
			assert.Equal(b, numSeries-numExpiresSeries, allActive)
		}
	}
}
