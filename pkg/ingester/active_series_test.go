// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/active_series_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
)

func copyFn(l labels.Labels) labels.Labels { return l }

func TestActiveSeries_UpdateSeries_NoMatchers(t *testing.T) {
	ls1 := []labels.Label{{Name: "a", Value: "1"}}
	ls2 := []labels.Label{{Name: "a", Value: "2"}}

	c := NewActiveSeries(&ActiveSeriesMatchers{})
	allActive, activeMatching := c.Active()
	assert.Equal(t, 0, allActive)
	assert.Nil(t, activeMatching)

	c.UpdateSeries(ls1, time.Now(), copyFn)
	allActive, _ = c.Active()
	assert.Equal(t, 1, allActive)

	c.UpdateSeries(ls1, time.Now(), copyFn)
	allActive, _ = c.Active()
	assert.Equal(t, 1, allActive)

	c.UpdateSeries(ls2, time.Now(), copyFn)
	allActive, _ = c.Active()
	assert.Equal(t, 2, allActive)
}

func TestActiveSeries_UpdateSeries_WithMatchers(t *testing.T) {
	ls1 := []labels.Label{{Name: "a", Value: "1"}}
	ls2 := []labels.Label{{Name: "a", Value: "2"}}
	ls3 := []labels.Label{{Name: "a", Value: "3"}}

	asm, err := NewActiveSeriesMatchers(ActiveSeriesCustomTrackersConfig{"foo": `{a=~"2|3"}`})
	require.NoError(t, err)

	c := NewActiveSeries(asm)
	allActive, activeMatching := c.Active()
	assert.Equal(t, 0, allActive)
	assert.Equal(t, []int{0}, activeMatching)

	c.UpdateSeries(ls1, time.Now(), copyFn)
	allActive, activeMatching = c.Active()
	assert.Equal(t, 1, allActive)
	assert.Equal(t, []int{0}, activeMatching)

	c.UpdateSeries(ls2, time.Now(), copyFn)
	allActive, activeMatching = c.Active()
	assert.Equal(t, 2, allActive)
	assert.Equal(t, []int{1}, activeMatching)

	c.UpdateSeries(ls3, time.Now(), copyFn)
	allActive, activeMatching = c.Active()
	assert.Equal(t, 3, allActive)
	assert.Equal(t, []int{2}, activeMatching)

	c.UpdateSeries(ls3, time.Now(), copyFn)
	allActive, activeMatching = c.Active()
	assert.Equal(t, 3, allActive)
	assert.Equal(t, []int{2}, activeMatching)
}

func TestActiveSeries_ShouldCorrectlyHandleFingerprintCollisions(t *testing.T) {
	metric := labels.NewBuilder(labels.FromStrings("__name__", "logs"))
	ls1 := metric.Set("_", "ypfajYg2lsv").Labels()
	ls2 := metric.Set("_", "KiqbryhzUpn").Labels()

	require.True(t, client.Fingerprint(ls1) == client.Fingerprint(ls2))

	c := NewActiveSeries(&ActiveSeriesMatchers{})
	c.UpdateSeries(ls1, time.Now(), copyFn)
	c.UpdateSeries(ls2, time.Now(), copyFn)

	allActive, _ := c.Active()
	assert.Equal(t, 2, allActive)
}

func TestActiveSeries_Purge_NoMatchers(t *testing.T) {
	series := [][]labels.Label{
		{{Name: "a", Value: "1"}},
		{{Name: "a", Value: "2"}},
		// The two following series have the same Fingerprint
		{{Name: "_", Value: "ypfajYg2lsv"}, {Name: "__name__", Value: "logs"}},
		{{Name: "_", Value: "KiqbryhzUpn"}, {Name: "__name__", Value: "logs"}},
	}

	// Run the same test for increasing TTL values
	for ttl := 1; ttl <= len(series); ttl++ {
		t.Run(fmt.Sprintf("ttl: %d", ttl), func(t *testing.T) {
			c := NewActiveSeries(&ActiveSeriesMatchers{})

			for i := 0; i < len(series); i++ {
				c.UpdateSeries(series[i], time.Unix(int64(i), 0), copyFn)
			}

			c.Purge(time.Unix(int64(ttl), 0))
			// call purge twice, just to hit "quick" path. It doesn't really do anything.
			c.Purge(time.Unix(int64(ttl), 0))

			exp := len(series) - (ttl)
			allActive, activeMatching := c.Active()
			assert.Equal(t, exp, allActive)
			assert.Nil(t, activeMatching)
		})
	}
}

func TestActiveSeries_Purge_WithMatchers(t *testing.T) {
	series := [][]labels.Label{
		{{Name: "a", Value: "1"}},
		{{Name: "a", Value: "2"}},
		// The two following series have the same Fingerprint
		{{Name: "_", Value: "ypfajYg2lsv"}, {Name: "__name__", Value: "logs"}},
		{{Name: "_", Value: "KiqbryhzUpn"}, {Name: "__name__", Value: "logs"}},
	}

	asm, err := NewActiveSeriesMatchers(ActiveSeriesCustomTrackersConfig{"foo": `{_=~"y.*"}`})
	require.NoError(t, err)

	// Run the same test for increasing TTL values
	for ttl := 1; ttl <= len(series); ttl++ {
		t.Run(fmt.Sprintf("ttl=%d", ttl), func(t *testing.T) {
			c := NewActiveSeries(asm)

			exp := len(series) - ttl
			expMatchingSeries := 0

			for i, s := range series {
				c.UpdateSeries(series[i], time.Unix(int64(i), 0), copyFn)

				// if this series is matching, and they're within the ttl
				if asm.matchers[0].Matches(s) && i >= ttl {
					expMatchingSeries++
				}
			}

			c.Purge(time.Unix(int64(ttl), 0))
			// call purge twice, just to hit "quick" path. It doesn't really do anything.
			c.Purge(time.Unix(int64(ttl), 0))

			allActive, activeMatching := c.Active()
			assert.Equal(t, exp, allActive)
			assert.Equal(t, []int{expMatchingSeries}, activeMatching)
		})
	}
}

func TestActiveSeries_PurgeOpt(t *testing.T) {
	metric := labels.NewBuilder(labels.FromStrings("__name__", "logs"))
	ls1 := metric.Set("_", "ypfajYg2lsv").Labels()
	ls2 := metric.Set("_", "KiqbryhzUpn").Labels()

	c := NewActiveSeries(&ActiveSeriesMatchers{})

	now := time.Now()
	c.UpdateSeries(ls1, now.Add(-2*time.Minute), copyFn)
	c.UpdateSeries(ls2, now, copyFn)
	c.Purge(now)

	allActive, _ := c.Active()
	assert.Equal(t, 1, allActive)

	c.UpdateSeries(ls1, now.Add(-1*time.Minute), copyFn)
	c.UpdateSeries(ls2, now, copyFn)
	c.Purge(now)

	allActive, _ = c.Active()
	assert.Equal(t, 1, allActive)

	// This will *not* update the series, since there is already newer timestamp.
	c.UpdateSeries(ls2, now.Add(-1*time.Minute), copyFn)
	c.Purge(now)

	allActive, _ = c.Active()
	assert.Equal(t, 1, allActive)
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
	series := labels.Labels{
		{Name: "a", Value: "a"},
	}

	c := NewActiveSeries(&ActiveSeriesMatchers{})

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
				c.UpdateSeries(series, now, copyFn)
			}
		}()
	}

	b.ResetTimer()
	close(start)
	wg.Wait()
}

func BenchmarkActiveSeries_UpdateSeries(b *testing.B) {
	c := NewActiveSeries(&ActiveSeriesMatchers{})

	// Prepare series
	nameBuf := bytes.Buffer{}
	for i := 0; i < 50; i++ {
		nameBuf.WriteString("abcdefghijklmnopqrstuvzyx")
	}
	name := nameBuf.String()

	series := make([]labels.Labels, b.N)
	for s := 0; s < b.N; s++ {
		series[s] = labels.Labels{{Name: name, Value: name + strconv.Itoa(s)}}
	}

	now := time.Now().UnixNano()

	b.ResetTimer()
	for ix := 0; ix < b.N; ix++ {
		c.UpdateSeries(series[ix], time.Unix(0, now+int64(ix)), copyFn)
	}
}

func BenchmarkActiveSeries_Purge_once(b *testing.B) {
	benchmarkPurge(b, false)
}

func BenchmarkActiveSeries_Purge_twice(b *testing.B) {
	benchmarkPurge(b, true)
}

func benchmarkPurge(b *testing.B, twice bool) {
	const numSeries = 10000
	const numExpiresSeries = numSeries / 25

	now := time.Now()
	c := NewActiveSeries(&ActiveSeriesMatchers{})

	series := [numSeries]labels.Labels{}
	for s := 0; s < numSeries; s++ {
		series[s] = labels.Labels{{Name: "a", Value: strconv.Itoa(s)}}
	}

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		// Prepare series
		for ix, s := range series {
			if ix < numExpiresSeries {
				c.UpdateSeries(s, now.Add(-time.Minute), copyFn)
			} else {
				c.UpdateSeries(s, now, copyFn)
			}
		}

		allActive, _ := c.Active()
		assert.Equal(b, numSeries, allActive)
		b.StartTimer()

		// Purge everything
		c.Purge(now)
		allActive, _ = c.Active()
		assert.Equal(b, numSeries-numExpiresSeries, allActive)

		if twice {
			c.Purge(now)
			allActive, _ = c.Active()
			assert.Equal(b, numSeries-numExpiresSeries, allActive)
		}
	}
}

func TestActiveSeriesMatcher(t *testing.T) {
	t.Run("malformed matcher", func(t *testing.T) {
		for _, matcher := range []string{
			`{foo}`,
			`{foo=~"}`,
		} {
			t.Run(matcher, func(t *testing.T) {
				config := ActiveSeriesCustomTrackersConfig{
					"malformed": matcher,
				}

				_, err := NewActiveSeriesMatchers(config)
				assert.Error(t, err)
			})
		}
	})

	t.Run("matches series", func(t *testing.T) {
		config := ActiveSeriesCustomTrackersConfig{
			"has_foo_label":                 `{foo!=""}`,
			"does_not_have_foo_label":       `{foo=""}`,
			"has_foo_and_bar_starts_with_1": `{foo!="", bar=~"1.*"}`,
			"bar_starts_with_1":             `{bar=~"1.*"}`,
		}

		asm, err := NewActiveSeriesMatchers(config)
		require.NoError(t, err)

		for _, tc := range []struct {
			series   labels.Labels
			expected []bool
		}{
			{
				series: labels.Labels{{Name: "foo", Value: "true"}, {Name: "baz", Value: "unrelated"}},
				expected: []bool{
					true,  // has_foo_label
					false, // does_not_have_foo_label
					false, // has_foo_and_bar_starts_with_1
					false, // bar_starts_with_1
				},
			},
			{
				series: labels.Labels{{Name: "foo", Value: "true"}, {Name: "bar", Value: "100"}, {Name: "baz", Value: "unrelated"}},
				expected: []bool{
					true,  // has_foo_label
					false, // does_not_have_foo_label
					true,  // has_foo_and_bar_starts_with_1
					true,  // bar_starts_with_1
				},
			},
			{
				series: labels.Labels{{Name: "foo", Value: "true"}, {Name: "bar", Value: "200"}, {Name: "baz", Value: "unrelated"}},
				expected: []bool{
					true,  // has_foo_label
					false, // does_not_have_foo_label
					false, // has_foo_and_bar_starts_with_1
					false, // bar_starts_with_1
				},
			},
			{
				series: labels.Labels{{Name: "bar", Value: "200"}, {Name: "baz", Value: "unrelated"}},
				expected: []bool{
					false, // has_foo_label
					true,  // does_not_have_foo_label
					false, // has_foo_and_bar_starts_with_1
					false, // bar_starts_with_1
				},
			},
			{
				series: labels.Labels{{Name: "bar", Value: "100"}, {Name: "baz", Value: "unrelated"}},
				expected: []bool{
					false, // has_foo_label
					true,  // does_not_have_foo_label
					false, // has_foo_and_bar_starts_with_1
					true,  // bar_starts_with_1
				},
			},
			{
				series: labels.Labels{{Name: "baz", Value: "unrelated"}},
				expected: []bool{
					false, // has_foo_label
					true,  // does_not_have_foo_label
					false, // has_foo_and_bar_starts_with_1
					false, // bar_starts_with_1
				},
			},
		} {
			t.Run(tc.series.String(), func(t *testing.T) {
				got := asm.Matches(tc.series)
				assert.Equal(t, tc.expected, got)
			})
		}
	})
}
