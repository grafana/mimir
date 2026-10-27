// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/grafana/regexp"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestPercentilesInterpolate(t *testing.T) {
	p := Percentiles{"0": 1, "0.5": 10, "1": 100}
	require.InDelta(t, 1, p.Quantile(0), 1e-9)
	require.InDelta(t, 10, p.Quantile(0.5), 1e-9)
	require.InDelta(t, 100, p.Quantile(1), 1e-9)
	// Log-linear between points.
	require.InDelta(t, 31.62, p.Quantile(0.75), 0.01)
}

func TestWorldScrapesEverySlotOncePerInterval(t *testing.T) {
	w := newWorld(defaultProfile().Series, 20_000, 1)
	require.InDelta(t, 20_000, w.totalSeries(), 2_000)
	for _, tenant := range w.tenants[:5] {
		for _, m := range tenant.metrics[:min(3, len(tenant.metrics))] {
			scraped := map[int]int{}
			start := int64(1_700_000_000_000)
			for s := start; s < start+m.intervalMs; s += 1000 {
				m.slotsAt(s, func(slot int, at int64) {
					require.GreaterOrEqual(t, at, s)
					require.Less(t, at, s+1000)
					scraped[slot]++
				})
			}
			require.Len(t, scraped, m.slots, "%s %s", tenant.id, m.name)
			for slot, n := range scraped {
				require.Equal(t, 1, n, "slot %d", slot)
			}
		}
	}
}

func TestWorldChurnsAtTheProfilesRate(t *testing.T) {
	p := defaultProfile().Series
	p.ChurnPerHour = 0.5
	w := newWorld(p, 20_000, 1)
	m := w.tenants[0].metrics[0]
	start := int64(1_700_000_000_000)
	changed := 0
	for slot := range m.slots {
		if w.generation(m, slot, start) != w.generation(m, slot, start+3_600_000) {
			changed++
		}
	}
	require.InDelta(t, 0.5, float64(changed)/float64(m.slots), 0.1)
}

func TestSeriesLabelsAreSortedAndUnique(t *testing.T) {
	w := newWorld(defaultProfile().Series, 20_000, 1)
	f := &feed{world: w, leValues: []string{"0.1", "1", "+Inf"}}
	seen := map[string]bool{}
	for _, tenant := range w.tenants[:10] {
		for _, m := range tenant.metrics {
			for slot := range min(m.slots, 50) {
				for _, labels := range f.familyLabels(m, slot, 3) {
					for i := 1; i < len(labels); i++ {
						require.Less(t, labels[i-1].Name, labels[i].Name)
					}
					key := tenant.id + labelsKey(labels)
					require.False(t, seen[key], key)
					seen[key] = true
				}
			}
		}
	}
}

func TestDifference(t *testing.T) {
	require.Equal(t, map[uint64]int{1: 1, 4: -1}, difference([]uint64{1, 2, 3}, []uint64{2, 3, 4}))
	require.Empty(t, difference([]uint64{1}, []uint64{1}))
}

func TestInstantiatedValuesMatchTheirKind(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	values := []string{"alpha-one", "beta-two", "gamma-three"}
	for kind, matches := range map[string]bool{"prefix": true, "suffix": true, "contains": true, "alternation": true, "regex": true} {
		v := instantiateValue(r, Matcher{Type: "=~", Value: Value{Kind: kind, Size: 2}}, values)
		re := regexp.MustCompile("^(?:" + v + ")$")
		found := false
		for _, value := range values {
			found = found || re.MatchString(value)
		}
		require.Equal(t, matches, found, "%s %q", kind, v)
	}
	require.Equal(t, "", instantiateValue(r, Matcher{Type: "=", Value: Value{Kind: "empty"}}, values))
}

func TestLabelsKeyIgnoresOrder(t *testing.T) {
	a := []mimirpb.LabelAdapter{{Name: "b", Value: "2"}, {Name: "a", Value: "1"}}
	b := []mimirpb.LabelAdapter{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}}
	require.Equal(t, labelsKey(a), labelsKey(b))
}

// BenchmarkFeedBackfill writes ten minutes of 100000 series to the in-process Kafka, as a run's backfill does.
func BenchmarkFeedBackfill(b *testing.B) {
	w := newWorld(defaultProfile().Series, 100_000, 1)
	for range b.N {
		f, err := newFeed(w, "", 0)
		require.NoError(b, err)
		start := int64(1_700_000_000_000)
		require.NoError(b, f.produceSeconds(b.Context(), start, start+10*60_000))
		b.ReportMetric(float64(f.samples.Load())/b.Elapsed().Seconds()*float64(b.N), "samples/s")
		f.close()
	}
}

func TestResultsIndexHasEveryRun(t *testing.T) {
	results := t.TempDir()
	for i, cpu := range []float64{2, 1} {
		report := Report{Started: int64(1_700_000_000 + i*3600), Series: 1000, Tenants: 2, Config: map[string]string{"bench_revision": fmt.Sprintf("rev%d", i), "backfill": "1h"},
			Resources: [2]Resources{{CPUCores: 4}, {CPUCores: cpu}}, APIs: map[string]APIReport{}}
		_, err := saveResult(results, report, "")
		require.NoError(t, err)
	}
	index, err := os.ReadFile(filepath.Join(results, "index.html"))
	require.NoError(t, err)
	require.Contains(t, string(index), `"bench_revision":"rev0"`)
	require.Contains(t, string(index), `"bench_revision":"rev1"`)
	require.NotContains(t, string(index), "__RUNS_DATA__")
	entries, err := os.ReadDir(results)
	require.NoError(t, err)
	require.Len(t, entries, 3)
}

func TestDefaultQueryClassesReadEveryRPCInTheirWays(t *testing.T) {
	p := defaultProfile()
	w := newWorld(p.Series, 20_000, 1)
	now := int64(1_700_000_000_000)
	g := newQueryGen(w, p.Queries, 13*3_600_000, func() int64 { return now - 13*3_600_000 }, func() int64 { return now })
	r := rand.New(rand.NewPCG(1, 2))
	for rpc := range p.Queries.RatePerMillionSeries {
		requests := 0
		for range 1000 {
			if g.next(r, rpc) != nil {
				requests++
			}
		}
		require.Greater(t, requests, 900, rpc)
	}
	var ranged, sharded, exclusions, regexes, instant int
	for range 5000 {
		req := g.next(r, "QueryStream")
		if req == nil {
			continue
		}
		q := req.query
		if q.EndTimestampMs-q.StartTimestampMs > 3_600_000 {
			ranged++
		}
		if q.EndTimestampMs-q.StartTimestampMs < 20*60_000 {
			instant++
		}
		notEqual := 0
		for _, m := range q.Matchers {
			switch {
			case m.Name == "__query_shard__":
				sharded++
			case m.Type == client.NOT_EQUAL:
				notEqual++
			case m.Type == client.REGEX_MATCH || m.Type == client.REGEX_NO_MATCH:
				regexes++
			}
		}
		if notEqual >= 5 {
			exclusions++
		}
	}
	for name, count := range map[string]int{"ranged": ranged, "sharded": sharded, "exclusions": exclusions, "regexes": regexes, "instant": instant} {
		require.Greater(t, count, 25, name)
	}
	require.Greater(t, instant, ranged, "rules' instant queries are most")
}

func TestWorldIsTheSeedsInEveryProcess(t *testing.T) {
	w := newWorld(defaultProfile().Series, 1_000, 1)
	m := w.tenants[0].metrics[0]
	// Values of seed 1, as every process has them: a hash seeded by the process would change them from run to run.
	require.Equal(t, uint64(0x1ba92e6129a0293d), m.hash64(0, 0))
	require.Equal(t, "ecbaaa0", valueString(1, "l2", 0, 7))

	other := newWorld(defaultProfile().Series, 1_000, 2)
	require.NotEqual(t, m.hash64(0, 0), other.tenants[0].metrics[0].hash64(0, 0))
}

func TestClassicHistogramSeriesFollowTheProfilesBuckets(t *testing.T) {
	p := defaultProfile().Series
	p.ClassicBuckets, p.ClassicHistogramShare, p.NativeHistogramShare = 4, 1, 0
	w := newWorld(p, 1_000, 1)
	f, err := newFeed(w, "", 0)
	require.NoError(t, err)
	defer f.close()

	for _, m := range w.tenants[0].metrics {
		require.Equal(t, kindClassicHistogram, m.kind)
		// Its buckets, its sum and its count.
		require.Equal(t, m.slots*(len(f.leValues)+2), m.seriesCount())
	}
}

// A backdated tenant writes some of its live scrapes again, between an hour and its window late and half a second off
// the scrapes, reads as far back, and has the window as its out-of-order one; the other tenants are as without it.
func TestBackdatedTenantsWriteAndReadUpToTheirWindowLate(t *testing.T) {
	const windowMs = 30 * 24 * 3_600_000
	p := defaultProfile()
	w := newWorld(p.Series, 20_000, 1)
	w.backdate(windowMs)
	f, err := newFeed(w, "", 0)
	require.NoError(t, err)
	defer f.close()
	now := int64(1_700_000_000_000)
	for _, tenant := range w.tenants {
		for _, m := range tenant.metrics {
			for slot := range m.slots {
				f.scrape(m, slot, now, func(ts *mimirpb.TimeSeries) {
					for _, sample := range ts.Samples {
						require.Equal(t, now, sample.TimestampMs, "nothing backdated in the backfill")
					}
				})
			}
		}
	}
	f.backdating.Store(true)

	backdatedTenants := map[string]bool{}
	scrapes, late, backdatedSeries := 0, 0, 0
	for _, tenant := range w.tenants {
		for _, m := range tenant.metrics {
			for slot := range m.slots {
				if tenant.backdated {
					scrapes++
				}
				f.scrape(m, slot, now, func(ts *mimirpb.TimeSeries) {
					for _, sample := range ts.Samples {
						if sample.TimestampMs != now {
							late++
							require.True(t, tenant.backdated, tenant.id)
							require.GreaterOrEqual(t, sample.TimestampMs, now-windowMs+3_600_000)
							require.LessOrEqual(t, sample.TimestampMs, now-3_600_000)
							require.Equal(t, int64(500), sample.TimestampMs%1000)
						}
					}
				})
			}
		}
		if tenant.backdated {
			backdatedTenants[tenant.id] = true
			backdatedSeries += tenant.series
		}
	}
	require.InDelta(t, p.Series.BackdatedShare, float64(late)/float64(scrapes), 0.01)
	require.InDelta(t, p.Series.BackdatedSeriesShare, float64(backdatedSeries)/float64(w.totalSeries()), 0.1)
	require.Less(t, len(backdatedTenants), len(w.tenants)/2)

	path := filepath.Join(t.TempDir(), "overrides.json")
	require.NoError(t, writeOverrides(w, path))
	overrides, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Len(t, regexp.MustCompile(`"out_of_order_time_window": "30d"`).FindAll(overrides, -1), len(backdatedTenants))

	g := newQueryGen(w, p.Queries, 13*3_600_000, func() int64 { return now - 13*3_600_000 }, func() int64 { return now })
	r := rand.New(rand.NewPCG(1, 2))
	whole := 0
	for range 20_000 {
		req := g.next(r, "QueryStream")
		if req == nil {
			continue
		}
		if !backdatedTenants[req.tenant] {
			require.GreaterOrEqual(t, req.query.StartTimestampMs, now-13*3_600_000, req.tenant)
			continue
		}
		if req.query.StartTimestampMs == now-windowMs {
			whole++
		}
	}
	require.Positive(t, whole, "reads over all of the window")
}
