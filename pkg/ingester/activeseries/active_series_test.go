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
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
)

func copyFn(l labels.Labels) labels.Labels { return l }

const DefaultTimeout = 5 * time.Minute

func TestActiveSeries_UpdateSeries_NoMatchers(t *testing.T) {
	ls1 := []labels.Label{{Name: "a", Value: "1"}}
	ls2 := []labels.Label{{Name: "a", Value: "2"}}

	c := NewActiveSeries(&Matchers{}, DefaultTimeout)
	allActive, activeMatching, valid := c.Active(time.Now())
	assert.Equal(t, 0, allActive)
	assert.Nil(t, activeMatching)
	assert.True(t, valid)

	c.UpdateSeries(ls1, time.Now(), copyFn)
	allActive, _, valid = c.Active(time.Now())
	assert.Equal(t, 1, allActive)
	assert.True(t, valid)

	c.UpdateSeries(ls1, time.Now(), copyFn)
	allActive, _, valid = c.Active(time.Now())
	assert.Equal(t, 1, allActive)
	assert.True(t, valid)

	c.UpdateSeries(ls2, time.Now(), copyFn)
	allActive, _, valid = c.Active(time.Now())
	assert.Equal(t, 2, allActive)
	assert.True(t, valid)
}

func TestActiveSeries_UpdateSeries_WithMatchers(t *testing.T) {
	ls1 := []labels.Label{{Name: "a", Value: "1"}}
	ls2 := []labels.Label{{Name: "a", Value: "2"}}
	ls3 := []labels.Label{{Name: "a", Value: "3"}}

	asm := NewMatchers(mustNewCustomTrackersConfigFromMap(t, map[string]string{"foo": `{a=~"2|3"}`}))

	c := NewActiveSeries(asm, DefaultTimeout)
	allActive, activeMatching, valid := c.Active(time.Now())
	assert.Equal(t, 0, allActive)
	assert.Equal(t, []int{0}, activeMatching)
	assert.True(t, valid)

	c.UpdateSeries(ls1, time.Now(), copyFn)
	allActive, activeMatching, valid = c.Active(time.Now())
	assert.Equal(t, 1, allActive)
	assert.Equal(t, []int{0}, activeMatching)
	assert.True(t, valid)

	c.UpdateSeries(ls2, time.Now(), copyFn)
	allActive, activeMatching, valid = c.Active(time.Now())
	assert.Equal(t, 2, allActive)
	assert.Equal(t, []int{1}, activeMatching)
	assert.True(t, valid)

	c.UpdateSeries(ls3, time.Now(), copyFn)
	allActive, activeMatching, valid = c.Active(time.Now())
	assert.Equal(t, 3, allActive)
	assert.Equal(t, []int{2}, activeMatching)
	assert.True(t, valid)

	c.UpdateSeries(ls3, time.Now(), copyFn)
	allActive, activeMatching, valid = c.Active(time.Now())
	assert.Equal(t, 3, allActive)
	assert.Equal(t, []int{2}, activeMatching)
	assert.True(t, valid)
}

func TestActiveSeries_ShouldCorrectlyHandleFingerprintCollisions(t *testing.T) {
	metric := labels.NewBuilder(labels.FromStrings("__name__", "logs"))
	ls1 := metric.Set("_", "ypfajYg2lsv").Labels()
	ls2 := metric.Set("_", "KiqbryhzUpn").Labels()

	require.True(t, client.Fingerprint(ls1) == client.Fingerprint(ls2))
	c := NewActiveSeries(&Matchers{}, DefaultTimeout)
	c.UpdateSeries(ls1, time.Now(), copyFn)
	c.UpdateSeries(ls2, time.Now(), copyFn)

	allActive, _, valid := c.Active(time.Now())
	assert.Equal(t, 2, allActive)
	assert.True(t, valid)
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
			mockedTime := time.Unix(int64(ttl), 0)
			c := NewActiveSeries(&Matchers{}, DefaultTimeout)

			for i := 0; i < len(series); i++ {
				c.UpdateSeries(series[i], time.Unix(int64(i), 0), copyFn)
			}

			c.purge(time.Unix(int64(ttl), 0))
			// call purge twice, just to hit "quick" path. It doesn't really do anything.
			c.purge(time.Unix(int64(ttl), 0))

			exp := len(series) - (ttl)
			// c.Active is not intended to purge
			allActive, activeMatching, valid := c.Active(mockedTime)
			assert.Equal(t, exp, allActive)
			assert.Nil(t, activeMatching)
			assert.True(t, valid)
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

	asm := NewMatchers(mustNewCustomTrackersConfigFromMap(t, map[string]string{"foo": `{_=~"y.*"}`}))

	// Run the same test for increasing TTL values
	for ttl := 1; ttl <= len(series); ttl++ {
		t.Run(fmt.Sprintf("ttl=%d", ttl), func(t *testing.T) {
			mockedTime := time.Unix(int64(ttl), 0)

			c := NewActiveSeries(asm, 5*time.Minute)

			exp := len(series) - ttl
			expMatchingSeries := 0

			for i, s := range series {
				c.UpdateSeries(series[i], time.Unix(int64(i), 0), copyFn)

				// if this series is matching, and they're within the ttl
				if asm.matchers[0].Matches(s) && i >= ttl {
					expMatchingSeries++
				}
			}

			c.purge(time.Unix(int64(ttl), 0))
			// call purge twice, just to hit "quick" path. It doesn't really do anything.
			c.purge(time.Unix(int64(ttl), 0))

			// c.Active is not intended to purge
			allActive, activeMatching, valid := c.Active(mockedTime)
			assert.Equal(t, exp, allActive)
			assert.Equal(t, []int{expMatchingSeries}, activeMatching)
			assert.True(t, valid)
		})
	}
}

func TestActiveSeries_PurgeOpt(t *testing.T) {
	metric := labels.NewBuilder(labels.FromStrings("__name__", "logs"))
	ls1 := metric.Set("_", "ypfajYg2lsv").Labels()
	ls2 := metric.Set("_", "KiqbryhzUpn").Labels()

	currentTime := time.Now()
	c := NewActiveSeries(&Matchers{}, 59*time.Second)

	c.UpdateSeries(ls1, currentTime.Add(-2*time.Minute), copyFn)
	c.UpdateSeries(ls2, currentTime, copyFn)

	allActive, _, valid := c.Active(currentTime)
	assert.Equal(t, 1, allActive)
	assert.True(t, valid)

	c.UpdateSeries(ls1, currentTime.Add(-1*time.Minute), copyFn)
	c.UpdateSeries(ls2, currentTime, copyFn)

	allActive, _, valid = c.Active(currentTime)
	assert.Equal(t, 1, allActive)
	assert.True(t, valid)

	// This will *not* update the series, since there is already newer timestamp.
	c.UpdateSeries(ls2, currentTime.Add(-1*time.Minute), copyFn)

	allActive, _, valid = c.Active(currentTime)
	assert.Equal(t, 1, allActive)
	assert.True(t, valid)
}

func TestActiveSeries_ReloadSeriesMatchers(t *testing.T) {
	ls1 := []labels.Label{{Name: "a", Value: "1"}}
	ls2 := []labels.Label{{Name: "a", Value: "2"}}
	ls3 := []labels.Label{{Name: "a", Value: "3"}}
	ls4 := []labels.Label{{Name: "a", Value: "4"}}

	asm := NewMatchers(mustNewCustomTrackersConfigFromMap(t, map[string]string{"foo": `{a=~.*}`}))

	currentTime := time.Now()
	c := NewActiveSeries(asm, DefaultTimeout)

	allActive, activeMatching, valid := c.Active(currentTime)
	assert.Equal(t, 0, allActive)
	assert.Equal(t, []int{0}, activeMatching)
	assert.True(t, valid)

	c.UpdateSeries(ls1, currentTime, copyFn)
	allActive, activeMatching, valid = c.Active(currentTime)
	assert.Equal(t, 1, allActive)
	assert.Equal(t, []int{1}, activeMatching)
	assert.True(t, valid)

	c.ReloadMatchers(asm, currentTime)
	_, _, valid = c.Active(currentTime)
	assert.False(t, valid)

	// Adding timeout time to make Active results valid.
	currentTime = currentTime.Add(DefaultTimeout)
	c.UpdateSeries(ls1, currentTime, copyFn)
	c.UpdateSeries(ls2, currentTime, copyFn)
	allActive, activeMatching, valid = c.Active(currentTime)
	assert.Equal(t, 2, allActive)
	assert.Equal(t, []int{2}, activeMatching)
	assert.True(t, valid)

	asmWithLessMatchers := NewMatchers(mustNewCustomTrackersConfigFromMap(t, map[string]string{}))
	c.ReloadMatchers(asmWithLessMatchers, currentTime)

	// Adding timeout time to make Active results valid.
	currentTime = currentTime.Add(DefaultTimeout)
	c.UpdateSeries(ls3, currentTime, copyFn)
	allActive, activeMatching, valid = c.Active(currentTime)
	assert.Equal(t, 1, allActive)
	assert.Equal(t, []int(nil), activeMatching)
	assert.True(t, valid)

	asmWithMoreMatchers := NewMatchers(mustNewCustomTrackersConfigFromMap(t, map[string]string{
		"a": `{a="3"}`,
		"b": `{a="4"}`,
	}))
	c.ReloadMatchers(asmWithMoreMatchers, currentTime)

	// Adding timeout time to make Active results valid.
	currentTime = currentTime.Add(DefaultTimeout)
	c.UpdateSeries(ls4, currentTime, copyFn)
	allActive, activeMatching, valid = c.Active(currentTime)
	assert.Equal(t, 1, allActive)
	assert.Equal(t, []int{0, 1}, activeMatching)
	assert.True(t, valid)
}

func TestActiveSeries_ReloadSeriesMatchers_LessMatchers(t *testing.T) {
	ls1 := []labels.Label{{Name: "a", Value: "1"}}

	asm := NewMatchers(mustNewCustomTrackersConfigFromMap(t, map[string]string{
		"foo": `{a=~.+}`,
		"bar": `{a=~.+}`,
	}))

	currentTime := time.Now()
	c := NewActiveSeries(asm, DefaultTimeout)
	allActive, activeMatching, valid := c.Active(currentTime)
	assert.Equal(t, 0, allActive)
	assert.Equal(t, []int{0, 0}, activeMatching)
	assert.True(t, valid)

	c.UpdateSeries(ls1, currentTime, copyFn)
	allActive, activeMatching, valid = c.Active(currentTime)
	assert.Equal(t, 1, allActive)
	assert.Equal(t, []int{1, 1}, activeMatching)
	assert.True(t, valid)

	asm = NewMatchers(mustNewCustomTrackersConfigFromMap(t, map[string]string{
		"foo": `{a=~.+}`,
	}))

	c.ReloadMatchers(asm, currentTime)
	c.purge(time.Time{})
	// Adding timeout time to make Active results valid.
	currentTime = currentTime.Add(DefaultTimeout)
	allActive, activeMatching, valid = c.Active(currentTime)
	assert.Equal(t, 0, allActive)
	assert.Equal(t, []int{0}, activeMatching)
	assert.True(t, valid)
}

func TestActiveSeries_ReloadSeriesMatchers_SameSizeNewLabels(t *testing.T) {
	ls1 := []labels.Label{{Name: "a", Value: "1"}}

	asm := NewMatchers(mustNewCustomTrackersConfigFromMap(t, map[string]string{
		"foo": `{a=~.+}`,
		"bar": `{a=~.+}`,
	}))

	currentTime := time.Now()

	c := NewActiveSeries(asm, DefaultTimeout)
	allActive, activeMatching, valid := c.Active(currentTime)
	assert.Equal(t, 0, allActive)
	assert.Equal(t, []int{0, 0}, activeMatching)
	assert.True(t, valid)

	c.UpdateSeries(ls1, currentTime, copyFn)
	allActive, activeMatching, valid = c.Active(currentTime)
	assert.Equal(t, 1, allActive)
	assert.Equal(t, []int{1, 1}, activeMatching)
	assert.True(t, valid)

	asm = NewMatchers(mustNewCustomTrackersConfigFromMap(t, map[string]string{
		"foo": `{b=~.+}`,
		"bar": `{b=~.+}`,
	}))

	c.ReloadMatchers(asm, currentTime)
	c.purge(time.Time{})
	// Adding timeout time to make Active results valid.
	currentTime = currentTime.Add(DefaultTimeout)

	allActive, activeMatching, valid = c.Active(currentTime)
	assert.Equal(t, 0, allActive)
	assert.Equal(t, []int{0, 0}, activeMatching)
	assert.True(t, valid)
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
				c.UpdateSeries(series, now, copyFn)
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
			series := make([]labels.Labels, tt.nSeries)
			for s := 0; s < tt.nSeries; s++ {
				lbls := make(labels.Labels, 10)
				for i := 0; i < len(lbls); i++ {
					// Label ~20B name, ~40B value.
					lbls[i] = labels.Label{Name: fmt.Sprintf("abcdefghijabcdefghi%d", i), Value: fmt.Sprintf("abcdefghijabcdefghijabcdefghijabcd%d", s)}
				}
				series[s] = lbls
			}

			now := time.Now().UnixNano()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				c := NewActiveSeries(&Matchers{}, DefaultTimeout)
				for round := 0; round <= tt.nRounds; round++ {
					for ix := 0; ix < tt.nSeries; ix++ {
						c.UpdateSeries(series[ix], time.Unix(0, now), copyFn)
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
	for s := 0; s < numSeries; s++ {
		series[s] = labels.Labels{{Name: "a", Value: strconv.Itoa(s)}}
	}

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		// Prepare series
		for ix, s := range series {
			if ix < numExpiresSeries {
				c.UpdateSeries(s, currentTime.Add(-DefaultTimeout), copyFn)
			} else {
				c.UpdateSeries(s, currentTime, copyFn)
			}
		}

		allActive, _, valid := c.Active(currentTime)
		assert.Equal(b, numSeries, allActive)
		assert.True(b, valid)
		b.StartTimer()

		// Active is going to purge everything
		currentTime = currentTime.Add(DefaultTimeout)
		allActive, _, valid = c.Active(currentTime)
		assert.Equal(b, numSeries-numExpiresSeries, allActive)
		assert.True(b, valid)

		if twice {
			allActive, _, valid = c.Active(currentTime)
			assert.Equal(b, numSeries-numExpiresSeries, allActive)
			assert.True(b, valid)
		}
	}
}
