// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/active_series_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package activeseries

import (
	"bytes"
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

func TestActiveSeries_UpdateSeries_NoMatchers(t *testing.T) {
	ls1 := []labels.Label{{Name: "a", Value: "1"}}
	ls2 := []labels.Label{{Name: "a", Value: "2"}}

	currentNow := time.Now()
	c := NewActiveSeries(&Matchers{}, 5*time.Minute)
	allActive, activeMatching := c.Active(currentNow)
	assert.Equal(t, 0, allActive)
	assert.Nil(t, activeMatching)

	c.UpdateSeries(ls1, time.Now(), copyFn)
	allActive, _ = c.Active(currentNow)
	assert.Equal(t, 1, allActive)

	c.UpdateSeries(ls1, time.Now(), copyFn)
	allActive, _ = c.Active(currentNow)
	assert.Equal(t, 1, allActive)

	c.UpdateSeries(ls2, time.Now(), copyFn)
	allActive, _ = c.Active(currentNow)
	assert.Equal(t, 2, allActive)
}

func TestActiveSeries_UpdateSeries_WithMatchers(t *testing.T) {
	ls1 := []labels.Label{{Name: "a", Value: "1"}}
	ls2 := []labels.Label{{Name: "a", Value: "2"}}
	ls3 := []labels.Label{{Name: "a", Value: "3"}}

	asm := NewMatchers(mustNewCustomTrackersConfigFromMap(t, map[string]string{"foo": `{a=~"2|3"}`}))

	currentNow := time.Now()
	c := NewActiveSeries(asm, 5*time.Minute)
	allActive, activeMatching := c.Active(currentNow)
	assert.Equal(t, 0, allActive)
	assert.Equal(t, []int{0}, activeMatching)

	c.UpdateSeries(ls1, time.Now(), copyFn)
	allActive, activeMatching = c.Active(currentNow)
	assert.Equal(t, 1, allActive)
	assert.Equal(t, []int{0}, activeMatching)

	c.UpdateSeries(ls2, time.Now(), copyFn)
	allActive, activeMatching = c.Active(currentNow)
	assert.Equal(t, 2, allActive)
	assert.Equal(t, []int{1}, activeMatching)

	c.UpdateSeries(ls3, time.Now(), copyFn)
	allActive, activeMatching = c.Active(currentNow)
	assert.Equal(t, 3, allActive)
	assert.Equal(t, []int{2}, activeMatching)

	c.UpdateSeries(ls3, time.Now(), copyFn)
	allActive, activeMatching = c.Active(currentNow)
	assert.Equal(t, 3, allActive)
	assert.Equal(t, []int{2}, activeMatching)
}

func TestActiveSeries_ShouldCorrectlyHandleFingerprintCollisions(t *testing.T) {
	metric := labels.NewBuilder(labels.FromStrings("__name__", "logs"))
	ls1 := metric.Set("_", "ypfajYg2lsv").Labels()
	ls2 := metric.Set("_", "KiqbryhzUpn").Labels()

	require.True(t, client.Fingerprint(ls1) == client.Fingerprint(ls2))
	currentNow := time.Now()
	c := NewActiveSeries(&Matchers{}, 5*time.Minute)
	c.UpdateSeries(ls1, time.Now(), copyFn)
	c.UpdateSeries(ls2, time.Now(), copyFn)

	allActive, _ := c.Active(currentNow)
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
			c := NewActiveSeries(&Matchers{}, time.Since(time.Unix(0, 0)))

			for i := 0; i < len(series); i++ {
				c.UpdateSeries(series[i], time.Unix(int64(i), 0), copyFn)
			}

			c.purge(time.Unix(int64(ttl), 0))
			// call purge twice, just to hit "quick" path. It doesn't really do anything.
			c.purge(time.Unix(int64(ttl), 0))

			exp := len(series) - (ttl)
			// c.Active is not intended to purge
			allActive, activeMatching := c.Active(time.Unix(0, 0))
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

	asm := NewMatchers(mustNewCustomTrackersConfigFromMap(t, map[string]string{"foo": `{_=~"y.*"}`}))

	// Run the same test for increasing TTL values
	for ttl := 1; ttl <= len(series); ttl++ {
		t.Run(fmt.Sprintf("ttl=%d", ttl), func(t *testing.T) {
			c := NewActiveSeries(asm, time.Since(time.Unix(0, 0)))

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
			allActive, activeMatching := c.Active(time.Unix(0, 0))
			assert.Equal(t, exp, allActive)
			assert.Equal(t, []int{expMatchingSeries}, activeMatching)
		})
	}
}

func TestActiveSeries_PurgeOpt(t *testing.T) {
	metric := labels.NewBuilder(labels.FromStrings("__name__", "logs"))
	ls1 := metric.Set("_", "ypfajYg2lsv").Labels()
	ls2 := metric.Set("_", "KiqbryhzUpn").Labels()

	currentNow := time.Now()
	c := NewActiveSeries(&Matchers{}, 59*time.Second)

	c.UpdateSeries(ls1, currentNow.Add(-2*time.Minute), copyFn)
	c.UpdateSeries(ls2, currentNow, copyFn)

	allActive, _ := c.Active(currentNow)
	assert.Equal(t, 1, allActive)

	c.UpdateSeries(ls1, currentNow.Add(-1*time.Minute), copyFn)
	c.UpdateSeries(ls2, currentNow, copyFn)

	allActive, _ = c.Active(currentNow)
	assert.Equal(t, 1, allActive)

	// This will *not* update the series, since there is already newer timestamp.
	c.UpdateSeries(ls2, currentNow.Add(-1*time.Minute), copyFn)

	allActive, _ = c.Active(currentNow)
	assert.Equal(t, 1, allActive)
}

func TestActiveSeries_ReloadSeriesMatchers(t *testing.T) {
	ls1 := []labels.Label{{Name: "a", Value: "1"}}
	ls2 := []labels.Label{{Name: "a", Value: "2"}}
	ls3 := []labels.Label{{Name: "a", Value: "3"}}
	ls4 := []labels.Label{{Name: "a", Value: "4"}}

	asm := NewMatchers(mustNewCustomTrackersConfigFromMap(t, map[string]string{"foo": `{a=~.*}`}))

	currentNow := time.Now()
	c := NewActiveSeries(asm, 5*time.Minute)

	allActive, activeMatching := c.Active(time.Now())
	assert.Equal(t, 0, allActive)
	assert.Equal(t, []int{0}, activeMatching)

	c.UpdateSeries(ls1, currentNow, copyFn)
	allActive, activeMatching = c.Active(time.Now())
	assert.Equal(t, 1, allActive)
	assert.Equal(t, []int{1}, activeMatching)

	c.ReloadSeriesMatchers(asm)
	assert.Greater(t, c.lastAsmUpdate.Load(), currentNow.UnixNano())
	assert.Less(t, c.lastAsmUpdate.Load(), time.Now().UnixNano())
	allActive, activeMatching = c.Active(time.Now())
	assert.Equal(t, 0, allActive)
	assert.Equal(t, []int{0}, activeMatching)

	c.UpdateSeries(ls1, currentNow, copyFn)
	c.UpdateSeries(ls2, currentNow, copyFn)
	allActive, activeMatching = c.Active(time.Now())
	assert.Equal(t, 2, allActive)
	assert.Equal(t, []int{2}, activeMatching)

	asmWithLessMatchers := NewMatchers(mustNewCustomTrackersConfigFromMap(t, map[string]string{}))
	c.ReloadSeriesMatchers(asmWithLessMatchers)

	c.UpdateSeries(ls3, currentNow, copyFn)
	allActive, activeMatching = c.Active(time.Now())
	assert.Equal(t, 1, allActive)
	assert.Equal(t, []int(nil), activeMatching)

	asmWithMoreMatchers := NewMatchers(mustNewCustomTrackersConfigFromMap(t, map[string]string{
		"a": `{a="3"}`,
		"b": `{a="4"}`,
	}))
	c.ReloadSeriesMatchers(asmWithMoreMatchers)

	c.UpdateSeries(ls4, currentNow, copyFn)
	allActive, activeMatching = c.Active(time.Now())
	assert.Equal(t, 1, allActive)
	assert.Equal(t, []int{0, 1}, activeMatching)
}

func TestActiveSeries_ReloadSeriesMatchers_LessMatchers(t *testing.T) {
	ls1 := []labels.Label{{Name: "a", Value: "1"}}

	asm := NewMatchers(mustNewCustomTrackersConfigFromMap(t, map[string]string{
		"foo": `{a=~.+}`,
		"bar": `{a=~.+}`,
	}))

	c := NewActiveSeries(asm, 5*time.Minute)
	allActive, activeMatching := c.Active(time.Now())
	assert.Equal(t, 0, allActive)
	assert.Equal(t, []int{0, 0}, activeMatching)

	c.UpdateSeries(ls1, time.Now(), copyFn)
	allActive, activeMatching = c.Active(time.Now())
	assert.Equal(t, 1, allActive)
	assert.Equal(t, []int{1, 1}, activeMatching)

	asm = NewMatchers(mustNewCustomTrackersConfigFromMap(t, map[string]string{
		"foo": `{a=~.+}`,
	}))

	c.ReloadSeriesMatchers(asm)
	c.purge(time.Time{})

	allActive, activeMatching = c.Active(time.Now())
	assert.Equal(t, 0, allActive)
	assert.Equal(t, []int{0}, activeMatching)
}

func TestActiveSeries_ReloadSeriesMatchers_SameSizeNewLabels(t *testing.T) {
	ls1 := []labels.Label{{Name: "a", Value: "1"}}

	asm := NewMatchers(mustNewCustomTrackersConfigFromMap(t, map[string]string{
		"foo": `{a=~.+}`,
		"bar": `{a=~.+}`,
	}))

	c := NewActiveSeries(asm, 5*time.Minute)
	allActive, activeMatching := c.Active(time.Now())
	assert.Equal(t, 0, allActive)
	assert.Equal(t, []int{0, 0}, activeMatching)

	c.UpdateSeries(ls1, time.Now(), copyFn)
	allActive, activeMatching = c.Active(time.Now())
	assert.Equal(t, 1, allActive)
	assert.Equal(t, []int{1, 1}, activeMatching)

	asm = NewMatchers(mustNewCustomTrackersConfigFromMap(t, map[string]string{
		"foo": `{b=~.+}`,
		"bar": `{b=~.+}`,
	}))

	c.ReloadSeriesMatchers(asm)
	c.purge(time.Time{})

	allActive, activeMatching = c.Active(time.Now())
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
	series := labels.Labels{
		{Name: "a", Value: "a"},
	}

	c := NewActiveSeries(&Matchers{}, 5*time.Minute)

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
	c := NewActiveSeries(&Matchers{}, 5*time.Minute)

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

	nowNano := time.Now().UnixNano()

	b.ResetTimer()
	for ix := 0; ix < b.N; ix++ {
		c.UpdateSeries(series[ix], time.Unix(0, nowNano+int64(ix)), copyFn)
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

	currentNow := time.Now()
	c := NewActiveSeries(&Matchers{}, 5*time.Minute)

	series := [numSeries]labels.Labels{}
	for s := 0; s < numSeries; s++ {
		series[s] = labels.Labels{{Name: "a", Value: strconv.Itoa(s)}}
	}

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		// Prepare series
		for ix, s := range series {
			if ix < numExpiresSeries {
				c.UpdateSeries(s, currentNow.Add(-time.Minute), copyFn)
			} else {
				c.UpdateSeries(s, currentNow, copyFn)
			}
		}

		allActive, _ := c.Active(time.Now())
		assert.Equal(b, numSeries, allActive)
		b.StartTimer()

		// Purge everything
		now := time.Now()
		c.purge(now)
		allActive, _ = c.Active(now)
		assert.Equal(b, numSeries-numExpiresSeries, allActive)

		if twice {
			c.purge(now)
			allActive, _ = c.Active(now)
			assert.Equal(b, numSeries-numExpiresSeries, allActive)
		}
	}
}
