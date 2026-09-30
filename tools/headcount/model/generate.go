// SPDX-License-Identifier: AGPL-3.0-only

package model

import (
	"fmt"
	"math/rand" // v1: math/rand/v2 has no Zipf sampler, and NewZipf needs a v1 *rand.Rand.
	"sort"
	"time"

	"github.com/prometheus/prometheus/model/labels"
)

// New generates a population from cfg. It is deterministic: the same cfg and
// cfg.Seed always produce the same Model, because generation draws from a
// single seeded RNG in a fixed order (metric names, then that name's series).
func New(cfg Config) (*Model, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	rng := rand.New(rand.NewSource(cfg.Seed))
	zipf := rand.NewZipf(rng, cfg.SeriesZipfS, 1, cfg.SeriesCap-cfg.SeriesFloor)

	trends := make(map[int]Trend, len(cfg.Trends))
	for _, tr := range cfg.Trends {
		trends[tr.Metric] = tr
	}

	var all []Series
	for i := 0; i < cfg.MetricNames; i++ {
		name := metricName(rng, i)
		if i == cfg.SpikeMetric {
			all = append(all, spikeMetricSeries(cfg, name)...)
			continue
		}
		if tr, ok := trends[i]; ok {
			all = append(all, trendSeries(cfg, name, tr)...)
			continue
		}
		var count uint64
		if i < len(cfg.FixedSeries) {
			count = cfg.FixedSeries[i]
		} else {
			count = cfg.SeriesFloor + zipf.Uint64()
		}
		all = append(all, metricSeries(cfg, name, count)...)
	}

	applyGaps(cfg, rng, all)
	applyStaleness(cfg, rng, all)

	return &Model{cfg: cfg, Series: all}, nil
}

// metricName returns a name at least 40 bytes long, so per-name index-header
// cost in the generated blocks matches realistic label sizes rather than the
// few bytes a bare counter would need.
func metricName(rng *rand.Rand, i int) string {
	base := fmt.Sprintf("metric_%06d_", i)
	total := 40 + rng.Intn(21) // 40-60 bytes.
	return base + randHex(rng, max(0, total-len(base)))
}

func randHex(rng *rand.Rand, n int) string {
	const alphabet = "0123456789abcdef"
	b := make([]byte, n)
	for i := range b {
		b[i] = alphabet[rng.Intn(len(alphabet))]
	}
	return string(b)
}

// metricSeries generates count concurrently-live series for one metric name.
// A ChurnFraction of them are churn groups: instead of one series spanning
// the whole range, each is a chain of series with a "pod" label that changes
// every ChurnPeriod, so at any instant a churn group still contributes
// exactly one live series, but the model as a whole contains more distinct
// label sets than count over the full time range.
func metricSeries(cfg Config, name string, count uint64) []Series {
	churnGroups := uint64(cfg.ChurnFraction * float64(count))
	static := count - churnGroups

	out := make([]Series, 0, count)
	var instance uint64
	for ; instance < static; instance++ {
		out = append(out, Series{
			Labels:    labels.FromStrings("__name__", name, "instance", fmt.Sprintf("instance-%d", instance)),
			Intervals: []Interval{{cfg.startMillis(), cfg.endMillis()}},
		})
	}
	for g := uint64(0); g < churnGroups; g++ {
		out = append(out, churnChain(cfg, name, instance)...)
		instance++
	}
	return out
}

// churnChain returns the sequence of series that make up one churned
// workload identity: consecutive, non-overlapping intervals covering
// [cfg.Start, cfg.End), each under a distinct "pod" label value.
func churnChain(cfg Config, name string, instance uint64) []Series {
	start, end := cfg.startMillis(), cfg.endMillis()
	period := cfg.ChurnPeriod.Milliseconds()

	var chain []Series
	for t, gen := start, 0; t < end; t, gen = t+period, gen+1 {
		ivEnd := min(t+period, end)
		chain = append(chain, Series{
			Labels: labels.FromStrings(
				"__name__", name,
				"instance", fmt.Sprintf("instance-%d", instance),
				"pod", fmt.Sprintf("pod-%d-%d", instance, gen),
			),
			Intervals: []Interval{{t, ivEnd}},
		})
	}
	return chain
}

// spikeMetricSeries returns one metric's series with a "pod" label whose
// value count jumps from SpikeBaseValues to SpikePeakValues for SpikeDay
// only: the base values are live for the whole range, the extra peak values
// exist only during that one day.
func spikeMetricSeries(cfg Config, name string) []Series {
	start, end := cfg.startMillis(), cfg.endMillis()
	day := 24 * time.Hour.Milliseconds()
	spikeStart := start + int64(cfg.SpikeDay)*day
	spikeEnd := min(spikeStart+day, end)

	out := make([]Series, 0, cfg.SpikePeakValues)
	for k := 0; k < cfg.SpikeBaseValues; k++ {
		out = append(out, Series{
			Labels:    labels.FromStrings("__name__", name, "pod", fmt.Sprintf("pod-base-%d", k)),
			Intervals: []Interval{{start, end}},
		})
	}
	for k := 0; k < cfg.SpikePeakValues-cfg.SpikeBaseValues; k++ {
		out = append(out, Series{
			Labels:    labels.FromStrings("__name__", name, "pod", fmt.Sprintf("pod-spike-%d", k)),
			Intervals: []Interval{{spikeStart, spikeEnd}},
		})
	}
	return out
}

// trendSeries returns one trend metric's series: series k lives from the
// start of the first day whose count exceeds k to the end of the last
// such day. Counts are linear in the day, so those days are contiguous.
func trendSeries(cfg Config, name string, tr Trend) []Series {
	start, end := cfg.startMillis(), cfg.endMillis()
	day := 24 * time.Hour.Milliseconds()
	days := int((end - start + day - 1) / day)

	peak := 0
	for d := 0; d < days; d++ {
		peak = max(peak, tr.countOn(d))
	}
	out := make([]Series, 0, peak)
	for k := 0; k < peak; k++ {
		first, last := -1, -1
		for d := 0; d < days; d++ {
			if tr.countOn(d) > k {
				if first < 0 {
					first = d
				}
				last = d
			}
		}
		out = append(out, Series{
			Labels:    labels.FromStrings("__name__", name, "instance", fmt.Sprintf("instance-%d", k)),
			Intervals: []Interval{{start + int64(first)*day, min(start+int64(last+1)*day, end)}},
		})
	}
	return out
}

// applyGaps removes one interior GapDuration-long interval from a
// GapFraction of series, splitting their single interval into two. A series
// too short to fit an interior gap without touching either edge is skipped.
func applyGaps(cfg Config, rng *rand.Rand, all []Series) {
	gapMs := cfg.GapDuration.Milliseconds()
	for _, idx := range sample(rng, len(all), cfg.GapFraction) {
		s := &all[idx]
		i := longestInterval(s.Intervals)
		iv := s.Intervals[i]
		span := iv.End - iv.Start
		if span <= 2*gapMs {
			continue
		}

		gapStart := iv.Start + gapMs + rng.Int63n(span-2*gapMs)
		gapEnd := gapStart + gapMs
		s.Intervals[i] = Interval{iv.Start, gapStart}
		s.Intervals = append(s.Intervals, Interval{gapEnd, iv.End})
		sort.Slice(s.Intervals, func(a, b int) bool { return s.Intervals[a].Start < s.Intervals[b].Start })
	}
}

// applyStaleness truncates the last interval of a StaleFraction of series to
// end partway through, simulating a staleness marker rather than a lifetime
// that runs to the end of the observed range.
func applyStaleness(cfg Config, rng *rand.Rand, all []Series) {
	for _, idx := range sample(rng, len(all), cfg.StaleFraction) {
		s := &all[idx]
		last := len(s.Intervals) - 1
		iv := s.Intervals[last]
		span := iv.End - iv.Start
		if span <= 0 {
			continue
		}
		// Cut somewhere in the second half, so the series keeps a
		// meaningful lifetime before the marker.
		s.Intervals[last] = Interval{iv.Start, iv.Start + span/2 + rng.Int63n(span/2+1)}
	}
}

func longestInterval(ivs []Interval) int {
	longest := 0
	for i, iv := range ivs {
		if iv.End-iv.Start > ivs[longest].End-ivs[longest].Start {
			longest = i
		}
	}
	return longest
}

// sample returns a deterministic, order-shuffled subset of round(total*frac)
// indices in [0, total), each returned at most once.
func sample(rng *rand.Rand, total int, frac float64) []int {
	n := int(frac * float64(total))
	if n == 0 || total == 0 {
		return nil
	}
	return rng.Perm(total)[:n]
}
