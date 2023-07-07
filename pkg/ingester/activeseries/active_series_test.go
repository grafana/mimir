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
	"github.com/stretchr/testify/assert"
)

const DefaultTimeout = 5 * time.Minute

func TestActiveSeries_UpdateSeries_NoMatchers(t *testing.T) {
	ref1, ls1 := uint64(1), labels.FromStrings("a", "1")
	ref2, ls2 := uint64(2), labels.FromStrings("a", "2")
	ref3, ls3 := uint64(3), labels.FromStrings("a", "3")
	ref4, ls4 := uint64(4), labels.FromStrings("a", "4")

	c := NewActiveSeries(&Matchers{}, DefaultTimeout)
	valid := c.Purge(time.Now())
	assert.True(t, valid)
	allActive, activeMatching, allActiveHistograms, activeMatchingHistograms, allActiveBuckets, activeMatchingBuckets := c.ActiveWithMatchers()
	assert.Equal(t, 0, allActive)
	assert.Nil(t, activeMatching)
	assert.Equal(t, 0, allActiveHistograms)
	assert.Nil(t, activeMatchingHistograms)
	assert.Equal(t, 0, allActiveBuckets)
	assert.Nil(t, activeMatchingBuckets)

	c.UpdateSeries(ls1, ref1, time.Now(), false, 0)
	valid = c.Purge(time.Now())
	assert.True(t, valid)
	allActive, _, allActiveHistograms, _, allActiveBuckets, _ = c.ActiveWithMatchers()
	assert.Equal(t, 1, allActive)
	assert.Equal(t, 0, allActiveHistograms)
	assert.Equal(t, 0, allActiveBuckets)
	allActive, allActiveHistograms, allActiveBuckets = c.Active()
	assert.Equal(t, 1, allActive)
	assert.Equal(t, 0, allActiveHistograms)
	assert.Equal(t, 0, allActiveBuckets)

	c.UpdateSeries(ls1, ref1, time.Now(), false, 0)
	valid = c.Purge(time.Now())
	assert.True(t, valid)
	allActive, _, allActiveHistograms, _, allActiveBuckets, _ = c.ActiveWithMatchers()
	assert.Equal(t, 1, allActive)
	assert.Equal(t, 0, allActiveHistograms)
	assert.Equal(t, 0, allActiveBuckets)
	allActive, allActiveHistograms, allActiveBuckets = c.Active()
	assert.Equal(t, 1, allActive)
	assert.Equal(t, 0, allActiveHistograms)
	assert.Equal(t, 0, allActiveBuckets)

	c.UpdateSeries(ls2, ref2, time.Now(), false, 0)
	valid = c.Purge(time.Now())
	assert.True(t, valid)
	allActive, _, allActiveHistograms, _, allActiveBuckets, _ = c.ActiveWithMatchers()
	assert.Equal(t, 2, allActive)
	assert.Equal(t, 0, allActiveHistograms)
	assert.Equal(t, 0, allActiveBuckets)
	allActive, allActiveHistograms, allActiveBuckets = c.Active()
	assert.Equal(t, 2, allActive)
	assert.Equal(t, 0, allActiveHistograms)
	assert.Equal(t, 0, allActiveBuckets)

	c.UpdateSeries(ls3, ref3, time.Now(), true, 5)
	valid = c.Purge(time.Now())
	assert.True(t, valid)
	allActive, _, allActiveHistograms, _, allActiveBuckets, _ = c.ActiveWithMatchers()
	assert.Equal(t, 3, allActive)
	assert.Equal(t, 1, allActiveHistograms)
	assert.Equal(t, 5, allActiveBuckets)
	allActive, allActiveHistograms, allActiveBuckets = c.Active()
	assert.Equal(t, 3, allActive)
	assert.Equal(t, 1, allActiveHistograms)
	assert.Equal(t, 5, allActiveBuckets)

	c.UpdateSeries(ls4, ref4, time.Now(), true, 3)
	valid = c.Purge(time.Now())
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
	c.UpdateSeries(ls3, ref3, time.Now(), true, 7)
	valid = c.Purge(time.Now())
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
	c.UpdateSeries(ls4, ref4, time.Now(), false, 0)
	valid = c.Purge(time.Now())
	assert.True(t, valid)
	allActive, _, allActiveHistograms, _, allActiveBuckets, _ = c.ActiveWithMatchers()
	assert.Equal(t, 4, allActive)
	assert.Equal(t, 1, allActiveHistograms)
	assert.Equal(t, 7, allActiveBuckets)
	allActive, allActiveHistograms, allActiveBuckets = c.Active()
	assert.Equal(t, 4, allActive)
	assert.Equal(t, 1, allActiveHistograms)
	assert.Equal(t, 7, allActiveBuckets)
}

func TestActiveSeries_ContainsRef(t *testing.T) {
	collision1, collision2 := labelsWithHashCollision()
	series := []labels.Labels{
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2"),
		collision1,
		collision2,
	}

	refs := []uint64{1, 2, 3, 4}

	// Run the same test for increasing TTL values
	for ttl := 1; ttl <= len(series); ttl++ {
		t.Run(fmt.Sprintf("ttl: %d", ttl), func(t *testing.T) {
			mockedTime := time.Unix(int64(ttl), 0)
			c := NewActiveSeries(&Matchers{}, DefaultTimeout)

			// Update each series with a different timestamp according to each index
			for i := 0; i < len(series); i++ {
				c.UpdateSeries(series[i], refs[i], time.Unix(int64(i), 0), false, 0)
			}

			c.purge(time.Unix(int64(ttl), 0))

			// The expected number of series is the total number of series minus the ttl
			// because the first ttl series should be purged
			exp := len(series) - (ttl)
			valid := c.Purge(mockedTime)
			assert.True(t, valid)
			allActive, activeMatching, _, _, _, _ := c.ActiveWithMatchers()
			assert.Equal(t, exp, allActive)
			assert.Nil(t, activeMatching)

			for i := 0; i < len(series); i++ {
				assert.Equal(t, i >= ttl, c.ContainsRef(refs[i]))
			}
		})
	}
}

func TestActiveSeries_UpdateSeries_WithMatchers(t *testing.T) {
	ref1, ls1 := uint64(1), labels.FromStrings("a", "1")
	ref2, ls2 := uint64(2), labels.FromStrings("a", "2")
	ref3, ls3 := uint64(3), labels.FromStrings("a", "3")
	ref4, ls4 := uint64(4), labels.FromStrings("a", "4")
	ref5, ls5 := uint64(5), labels.FromStrings("a", "5")

	asm := NewMatchers(mustNewCustomTrackersConfigFromMap(t, map[string]string{"foo": `{a=~"2|3|4"}`}))

	c := NewActiveSeries(asm, DefaultTimeout)
	valid := c.Purge(time.Now())
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

	c.UpdateSeries(ls1, ref1, time.Now(), false, 0)
	valid = c.Purge(time.Now())
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

	c.UpdateSeries(ls2, ref2, time.Now(), false, 0)
	valid = c.Purge(time.Now())
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

	c.UpdateSeries(ls3, ref3, time.Now(), false, 0)
	valid = c.Purge(time.Now())
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

	c.UpdateSeries(ls3, ref3, time.Now(), false, 0)
	valid = c.Purge(time.Now())
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

	c.UpdateSeries(ls4, ref4, time.Now(), true, 3)
	valid = c.Purge(time.Now())
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

	c.UpdateSeries(ls5, ref5, time.Now(), true, 5)
	valid = c.Purge(time.Now())
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
	c.UpdateSeries(ls3, ref3, time.Now(), true, 6)
	valid = c.Purge(time.Now())
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
	c.UpdateSeries(ls4, ref4, time.Now(), true, 0)
	valid = c.Purge(time.Now())
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
	ref1, ref2 := uint64(1), uint64(2)

	c := NewActiveSeries(&Matchers{}, DefaultTimeout)
	c.UpdateSeries(ls1, ref1, time.Now(), false, 0)
	c.UpdateSeries(ls2, ref2, time.Now(), false, 0)

	valid := c.Purge(time.Now())
	assert.True(t, valid)
	allActive, _, _, _, _, _ := c.ActiveWithMatchers()
	assert.Equal(t, 2, allActive)
}

func TestActiveSeries_Purge_NoMatchers(t *testing.T) {
	collision1, collision2 := labelsWithHashCollision()
	series := []labels.Labels{
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2"),
		collision1,
		collision2,
	}

	refs := []uint64{1, 2, 3, 4}

	// Run the same test for increasing TTL values
	for ttl := 1; ttl <= len(series); ttl++ {
		t.Run(fmt.Sprintf("ttl: %d", ttl), func(t *testing.T) {
			mockedTime := time.Unix(int64(ttl), 0)
			c := NewActiveSeries(&Matchers{}, DefaultTimeout)

			for i := 0; i < len(series); i++ {
				c.UpdateSeries(series[i], refs[i], time.Unix(int64(i), 0), false, 0)
			}

			c.purge(time.Unix(int64(ttl), 0))
			// call purge twice, just to hit "quick" path. It doesn't really do anything.
			c.purge(time.Unix(int64(ttl), 0))

			exp := len(series) - (ttl)
			// Purge is not intended to purge
			valid := c.Purge(mockedTime)
			assert.True(t, valid)
			allActive, activeMatching, _, _, _, _ := c.ActiveWithMatchers()
			assert.Equal(t, exp, allActive)
			assert.Nil(t, activeMatching)
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

	refs := []uint64{1, 2, 3, 4}

	asm := NewMatchers(mustNewCustomTrackersConfigFromMap(t, map[string]string{"foo": `{_=~"y.*"}`}))

	// Run the same test for increasing TTL values
	for ttl := 1; ttl <= len(series); ttl++ {
		t.Run(fmt.Sprintf("ttl=%d", ttl), func(t *testing.T) {
			mockedTime := time.Unix(int64(ttl), 0)

			c := NewActiveSeries(asm, 5*time.Minute)

			exp := len(series) - ttl
			expMatchingSeries := 0

			for i, s := range series {
				c.UpdateSeries(series[i], refs[i], time.Unix(int64(i), 0), false, 0)

				// if this series is matching, and they're within the ttl
				if asm.matchers[0].Matches(s) && i >= ttl {
					expMatchingSeries++
				}
			}

			c.purge(time.Unix(int64(ttl), 0))
			// call purge twice, just to hit "quick" path. It doesn't really do anything.
			c.purge(time.Unix(int64(ttl), 0))

			valid := c.Purge(mockedTime)
			assert.True(t, valid)
			allActive, activeMatching, _, _, _, _ := c.ActiveWithMatchers()
			assert.Equal(t, exp, allActive)
			assert.Equal(t, []int{expMatchingSeries}, activeMatching)
		})
	}
}

func TestActiveSeries_PurgeOpt(t *testing.T) {
	ls1, ls2 := labelsWithHashCollision()
	ref1, ref2 := uint64(1), uint64(2)

	currentTime := time.Now()
	c := NewActiveSeries(&Matchers{}, 59*time.Second)

	c.UpdateSeries(ls1, ref1, currentTime.Add(-2*time.Minute), false, 0)
	c.UpdateSeries(ls2, ref2, currentTime, false, 0)

	valid := c.Purge(currentTime)
	assert.True(t, valid)
	allActive, _, _, _, _, _ := c.ActiveWithMatchers()
	assert.Equal(t, 1, allActive)

	c.UpdateSeries(ls1, ref1, currentTime.Add(-1*time.Minute), false, 0)
	c.UpdateSeries(ls2, ref2, currentTime, false, 0)

	valid = c.Purge(currentTime)
	assert.True(t, valid)
	allActive, _, _, _, _, _ = c.ActiveWithMatchers()
	assert.Equal(t, 1, allActive)

	// This will *not* update the series, since there is already newer timestamp.
	c.UpdateSeries(ls2, ref2, currentTime.Add(-1*time.Minute), false, 0)

	valid = c.Purge(currentTime)
	assert.True(t, valid)
	allActive, _, _, _, _, _ = c.ActiveWithMatchers()
	assert.Equal(t, 1, allActive)
}

func TestActiveSeries_ReloadSeriesMatchers(t *testing.T) {
	ref1, ls1 := uint64(1), labels.FromStrings("a", "1")
	ref2, ls2 := uint64(2), labels.FromStrings("a", "2")
	ref3, ls3 := uint64(3), labels.FromStrings("a", "3")
	ref4, ls4 := uint64(4), labels.FromStrings("a", "4")

	asm := NewMatchers(mustNewCustomTrackersConfigFromMap(t, map[string]string{"foo": `{a=~.*}`}))

	currentTime := time.Now()
	c := NewActiveSeries(asm, DefaultTimeout)

	valid := c.Purge(currentTime)
	assert.True(t, valid)
	allActive, activeMatching, _, _, _, _ := c.ActiveWithMatchers()
	assert.Equal(t, 0, allActive)
	assert.Equal(t, []int{0}, activeMatching)

	c.UpdateSeries(ls1, ref1, currentTime, false, 0)
	valid = c.Purge(currentTime)
	assert.True(t, valid)
	allActive, activeMatching, _, _, _, _ = c.ActiveWithMatchers()
	assert.Equal(t, 1, allActive)
	assert.Equal(t, []int{1}, activeMatching)

	c.ReloadMatchers(asm, currentTime)
	valid = c.Purge(currentTime)
	assert.False(t, valid)

	// Adding timeout time to make Purge results valid.
	currentTime = currentTime.Add(DefaultTimeout)
	c.UpdateSeries(ls1, ref1, currentTime, false, 0)
	c.UpdateSeries(ls2, ref2, currentTime, false, 0)
	valid = c.Purge(currentTime)
	assert.True(t, valid)
	allActive, activeMatching, _, _, _, _ = c.ActiveWithMatchers()
	assert.Equal(t, 2, allActive)
	assert.Equal(t, []int{2}, activeMatching)

	asmWithLessMatchers := NewMatchers(mustNewCustomTrackersConfigFromMap(t, map[string]string{}))
	c.ReloadMatchers(asmWithLessMatchers, currentTime)

	// Adding timeout time to make Purge results valid.
	currentTime = currentTime.Add(DefaultTimeout)
	c.UpdateSeries(ls3, ref3, currentTime, false, 0)
	valid = c.Purge(currentTime)
	assert.True(t, valid)
	allActive, activeMatching, _, _, _, _ = c.ActiveWithMatchers()
	assert.Equal(t, 1, allActive)
	assert.Equal(t, []int(nil), activeMatching)

	asmWithMoreMatchers := NewMatchers(mustNewCustomTrackersConfigFromMap(t, map[string]string{
		"a": `{a="3"}`,
		"b": `{a="4"}`,
	}))
	c.ReloadMatchers(asmWithMoreMatchers, currentTime)

	// Adding timeout time to make Purge results valid.
	currentTime = currentTime.Add(DefaultTimeout)
	c.UpdateSeries(ls4, ref4, currentTime, false, 0)
	valid = c.Purge(currentTime)
	assert.True(t, valid)
	allActive, activeMatching, _, _, _, _ = c.ActiveWithMatchers()
	assert.Equal(t, 1, allActive)
	assert.Equal(t, []int{0, 1}, activeMatching)
}

func TestActiveSeries_ReloadSeriesMatchers_LessMatchers(t *testing.T) {
	ref1, ls1 := uint64(1), labels.FromStrings("a", "1")

	asm := NewMatchers(mustNewCustomTrackersConfigFromMap(t, map[string]string{
		"foo": `{a=~.+}`,
		"bar": `{a=~.+}`,
	}))

	currentTime := time.Now()
	c := NewActiveSeries(asm, DefaultTimeout)
	valid := c.Purge(currentTime)
	assert.True(t, valid)
	allActive, activeMatching, _, _, _, _ := c.ActiveWithMatchers()
	assert.Equal(t, 0, allActive)
	assert.Equal(t, []int{0, 0}, activeMatching)

	c.UpdateSeries(ls1, ref1, currentTime, false, 0)
	valid = c.Purge(currentTime)
	assert.True(t, valid)
	allActive, activeMatching, _, _, _, _ = c.ActiveWithMatchers()
	assert.Equal(t, 1, allActive)
	assert.Equal(t, []int{1, 1}, activeMatching)

	asm = NewMatchers(mustNewCustomTrackersConfigFromMap(t, map[string]string{
		"foo": `{a=~.+}`,
	}))

	c.ReloadMatchers(asm, currentTime)
	c.purge(time.Time{})
	// Adding timeout time to make Purge results valid.
	currentTime = currentTime.Add(DefaultTimeout)
	valid = c.Purge(currentTime)
	assert.True(t, valid)
	allActive, activeMatching, _, _, _, _ = c.ActiveWithMatchers()
	assert.Equal(t, 0, allActive)
	assert.Equal(t, []int{0}, activeMatching)
}

func TestActiveSeries_ReloadSeriesMatchers_SameSizeNewLabels(t *testing.T) {
	ref1, ls1 := uint64(1), labels.FromStrings("a", "1")

	asm := NewMatchers(mustNewCustomTrackersConfigFromMap(t, map[string]string{
		"foo": `{a=~.+}`,
		"bar": `{a=~.+}`,
	}))

	currentTime := time.Now()

	c := NewActiveSeries(asm, DefaultTimeout)
	valid := c.Purge(currentTime)
	assert.True(t, valid)
	allActive, activeMatching, _, _, _, _ := c.ActiveWithMatchers()
	assert.Equal(t, 0, allActive)
	assert.Equal(t, []int{0, 0}, activeMatching)

	c.UpdateSeries(ls1, ref1, currentTime, false, 0)
	valid = c.Purge(currentTime)
	assert.True(t, valid)
	allActive, activeMatching, _, _, _, _ = c.ActiveWithMatchers()
	assert.Equal(t, 1, allActive)
	assert.Equal(t, []int{1, 1}, activeMatching)

	asm = NewMatchers(mustNewCustomTrackersConfigFromMap(t, map[string]string{
		"foo": `{b=~.+}`,
		"bar": `{b=~.+}`,
	}))

	c.ReloadMatchers(asm, currentTime)
	c.purge(time.Time{})
	// Adding timeout time to make Purge results valid.
	currentTime = currentTime.Add(DefaultTimeout)

	valid = c.Purge(currentTime)
	assert.True(t, valid)
	allActive, activeMatching, _, _, _, _ = c.ActiveWithMatchers()
	assert.Equal(t, 0, allActive)
	assert.Equal(t, []int{0, 0}, activeMatching)
}

var activeSeriesTestGoroutines = []int{50, 100, 500}

func BenchmarkActiveSeriesTest_single_series(b *testing.B) {
	for _, num := range activeSeriesTestGoroutines {
		b.Run(fmt.Sprintf("%d", num), func(b *testing.B) {
			benchmarkActiveSeriesConcurrencySingleSeries(b, num)
		})
	}
}

func benchmarkActiveSeriesConcurrencySingleSeries(b *testing.B, goroutines int) {
	series := labels.FromStrings("a", "a")
	ref := uint64(1)

	c := NewActiveSeries(&Matchers{}, DefaultTimeout)

	wg := &sync.WaitGroup{}
	start := make(chan struct{})
	max := int(math.Ceil(float64(b.N) / float64(goroutines)))

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			now := time.Now()

			for ix := 0; ix < max; ix++ {
				now = now.Add(time.Duration(ix) * time.Millisecond)
				c.UpdateSeries(series, ref, now, false, 0)
			}
		}()
	}

	b.ResetTimer()
	close(start)
	wg.Wait()
}

func BenchmarkActiveSeries_UpdateSeries(b *testing.B) {
	for _, tt := range []struct {
		nRounds int // Number of times we update the same series
		nSeries int // Number of series we create
	}{
		{
			nRounds: 1,
			nSeries: 100000,
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
	} {
		b.Run(fmt.Sprintf("rounds=%d series=%d", tt.nRounds, tt.nSeries), func(b *testing.B) {
			// Prepare series
			const nLabels = 10
			builder := labels.NewScratchBuilder(nLabels)
			series := make([]labels.Labels, tt.nSeries)
			refs := make([]uint64, tt.nSeries)
			for s := 0; s < tt.nSeries; s++ {
				builder.Reset()
				for i := 0; i < nLabels; i++ {
					// Label ~20B name, ~40B value.
					builder.Add(fmt.Sprintf("abcdefghijabcdefghi%d", i), fmt.Sprintf("abcdefghijabcdefghijabcdefghijabcd%d", s))
				}
				series[s] = builder.Labels()
				refs[s] = uint64(s)
			}

			now := time.Now().UnixNano()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				c := NewActiveSeries(&Matchers{}, DefaultTimeout)
				for round := 0; round <= tt.nRounds; round++ {
					for ix := 0; ix < tt.nSeries; ix++ {
						c.UpdateSeries(series[ix], refs[ix], time.Unix(0, now), false, 0)
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
	c := NewActiveSeries(&Matchers{}, DefaultTimeout)

	series := [numSeries]labels.Labels{}
	refs := [numSeries]uint64{}
	for s := 0; s < numSeries; s++ {
		series[s] = labels.FromStrings("a", strconv.Itoa(s))
		refs[s] = uint64(s)
	}

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		// Prepare series
		for ix, s := range series {
			if ix < numExpiresSeries {
				c.UpdateSeries(s, refs[ix], currentTime.Add(-DefaultTimeout), false, 0)
			} else {
				c.UpdateSeries(s, refs[ix], currentTime, false, 0)
			}
		}

		valid := c.Purge(currentTime)
		assert.True(b, valid)
		allActive, _, _, _, _, _ := c.ActiveWithMatchers()
		assert.Equal(b, numSeries, allActive)
		b.StartTimer()

		// Purge is going to purge everything
		currentTime = currentTime.Add(DefaultTimeout)
		valid = c.Purge(currentTime)
		assert.True(b, valid)
		allActive, _, _, _, _, _ = c.ActiveWithMatchers()
		assert.Equal(b, numSeries-numExpiresSeries, allActive)

		if twice {
			valid = c.Purge(currentTime)
			assert.True(b, valid)
			allActive, _, _, _, _, _ = c.ActiveWithMatchers()
			assert.Equal(b, numSeries-numExpiresSeries, allActive)
		}
	}
}
