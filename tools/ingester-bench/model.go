// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"cmp"
	"fmt"
	"math"
	"math/rand/v2" //#nosec G404 -- Synthetic data and reads, reproducible from a seed: no need for a CSPRNG -- nosemgrep: math-random-used
	"slices"
	"sort"
	"strconv"

	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/value"

	"github.com/grafana/mimir/pkg/mimirpb"
)

type metricKind int

const (
	kindGauge metricKind = iota
	kindConstantGauge
	kindCounter
	kindClassicHistogram
	kindNativeHistogram
)

// world is the synthetic series of an ingester: what they are and what each sample is follow from the seed and the
// time, so that nothing per series is kept and the query driver knows what the ingesters should have.
type world struct {
	profile    SeriesProfile
	seed       uint64
	tenants    []*tenantModel
	lifetimeMs int64
	// The out-of-order window of the backdated tenants, 0 without any.
	backdatedWindowMs int64
	// Label names by rank: "l0" is the one most metrics have.
	labelWeights []float64
}

type tenantModel struct {
	id      string
	metrics []*metricModel
	// Whether the tenant writes samples up to the world's backdated window late.
	backdated bool
	// Label values by label name, shared by the tenant's metrics, which use the first ones they need.
	pools  map[string][]string
	series int
}

type metricModel struct {
	tenant *tenantModel
	name   string
	kind   metricKind
	// Labels whose values follow the series' slot, and the one whose value changes with each generation of a slot.
	labels     []labelDef
	churnLabel string
	slots      int
	// Series of each slot: a classic histogram's buckets, sum and count, or the one series of the others.
	seriesPerSlot int
	intervalMs    int64
	// The hash of the run's seed, the tenant and the metric, which the hashes of its series start from.
	base uint64
	// Per-second rate of counters and scale of gauges.
	scale float64
}

type labelDef struct {
	name   string
	values []string
}

func newWorld(p SeriesProfile, totalSeries int, seed uint64) *world {
	r := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	w := &world{profile: p, seed: seed, lifetimeMs: int64(math.Max(1, 1/math.Max(p.ChurnPerHour, 1e-6)) * 3_600_000)}
	if p.ChurnPerHour <= 0 {
		w.lifetimeMs = math.MaxInt64 / 4
	}
	w.labelWeights = make([]float64, max(1, p.LabelNames))
	for i := range w.labelWeights {
		w.labelWeights[i] = 1 / math.Pow(float64(i+1), p.LabelSkew)
	}
	// Tenant sizes, as shares of the ingester's series.
	shares := make([]float64, max(1, p.Tenants))
	total := 0.0
	for i := range shares {
		shares[i] = p.SeriesPerTenant.Sample(r)
		total += shares[i]
	}
	sort.Sort(sort.Reverse(sort.Float64Slice(shares)))
	for i, share := range shares {
		series := max(1, int(math.Round(share/total*float64(totalSeries))))
		w.tenants = append(w.tenants, w.newTenant(r, fmt.Sprintf("tenant-%03d", i), series))
	}
	return w
}

// backdate gives tenants with the profile's share of the series, picked by a hash of their ids so that the world is
// the same otherwise, an out-of-order window of windowMs, which they write samples that late in.
func (w *world) backdate(windowMs int64) {
	w.backdatedWindowMs = windowMs
	if windowMs <= 0 {
		return
	}
	tenants := slices.Clone(w.tenants)
	slices.SortFunc(tenants, func(a, b *tenantModel) int {
		return cmp.Compare(seededHash(w.seed, a.id, "backdated"), seededHash(w.seed, b.id, "backdated"))
	})
	series, total := 0, w.totalSeries()
	for _, t := range tenants {
		if float64(series) >= w.profile.BackdatedSeriesShare*float64(total) {
			break
		}
		t.backdated = true
		series += t.series
	}
}

func (w *world) newTenant(r *rand.Rand, id string, series int) *tenantModel {
	p := w.profile
	t := &tenantModel{id: id, pools: map[string][]string{}, series: series}
	metrics := max(1, int(math.Round(float64(series)/p.SeriesPerMetric)))
	weights := make([]float64, metrics)
	sum := 0.0
	for k := range weights {
		weights[k] = 1 / math.Pow(float64(k+1), p.MetricSkew)
		sum += weights[k]
	}
	intervals := map[string]float64{}
	for k, v := range p.ScrapeIntervals {
		intervals[k] = v
	}
	for k := range metrics {
		size := max(1, int(math.Round(weights[k]/sum*float64(series))))
		m := &metricModel{tenant: t, name: fmt.Sprintf("metric_%d", k), seriesPerSlot: 1}
		m.base = seededHash(w.seed, t.id, m.name)
		switch x := r.Float64(); {
		case x < p.NativeHistogramShare:
			m.kind = kindNativeHistogram
		case x < p.NativeHistogramShare+p.ClassicHistogramShare:
			m.kind = kindClassicHistogram
		case x < p.NativeHistogramShare+p.ClassicHistogramShare+p.CounterShare:
			m.kind = kindCounter
		case r.Float64() < 0.4:
			m.kind = kindConstantGauge
		default:
			m.kind = kindGauge
		}
		m.slots = size
		if m.kind == kindClassicHistogram {
			m.seriesPerSlot = classicHistogramSeries(p)
			m.slots = max(1, size/m.seriesPerSlot)
		}
		seconds, _ := strconv.Atoi(pick(r, intervals))
		m.intervalMs = int64(max(1, seconds)) * 1000
		m.scale = math.Exp(r.Float64()*12 - 4)
		w.chooseLabels(r, t, m)
		t.metrics = append(t.metrics, m)
	}
	return t
}

// chooseLabels gives the metric labels by rank, with value counts from the profile, and makes the most varied one
// tell the series apart.
func (w *world) chooseLabels(r *rand.Rand, t *tenantModel, m *metricModel) {
	p := w.profile
	count := max(1, int(math.Round(p.LabelsPerMetric.Sample(r))))
	chosen := map[int]bool{}
	for len(chosen) < min(count, len(w.labelWeights)) {
		chosen[weightedIndex(r, w.labelWeights)] = true
	}
	ranks := make([]int, 0, len(chosen))
	for rank := range chosen {
		ranks = append(ranks, rank)
	}
	sort.Ints(ranks)
	for i, rank := range ranks {
		name := "l" + strconv.Itoa(rank)
		if i == len(ranks)-1 {
			// The series' own label, like an instance or a pod: one value per slot, a new one each generation.
			m.churnLabel = name
			continue
		}
		cardinality := min(m.slots, max(1, int(math.Round(p.ValuesPerLabel.Sample(r)))))
		m.labels = append(m.labels, labelDef{name: name, values: w.pool(r, t, name, cardinality)})
	}
}

func (w *world) pool(r *rand.Rand, t *tenantModel, name string, n int) []string {
	pool := t.pools[name]
	if len(pool) < n {
		length := max(1, int(math.Round(w.profile.ValueLength.Sample(r))))
		for i := len(pool); i < n; i++ {
			pool = append(pool, valueString(w.seed, name, i, length))
		}
		t.pools[name] = pool
	}
	return pool[:n]
}

// valueString is the i-th value of a label, length characters long.
func valueString(seed uint64, label string, i, length int) string {
	s := strconv.FormatInt(int64(i), 36)
	if len(s) >= length {
		return s
	}
	pad := make([]byte, length-len(s))
	h := seededHash(seed, label)
	for j := range pad {
		pad[j] = 'a' + byte((h>>(uint(j)%60))%26)
	}
	return string(pad) + s
}

func weightedIndex(r *rand.Rand, weights []float64) int {
	total := 0.0
	for _, w := range weights {
		total += w
	}
	x := r.Float64() * total
	for i, w := range weights {
		x -= w
		if x < 0 {
			return i
		}
	}
	return len(weights) - 1
}

func (w *world) totalSeries() int {
	n := 0
	for _, t := range w.tenants {
		n += t.series
	}
	return n
}

// hash64 is a hash of the metric, the slot and a salt, for all that is random about a series: the same seed hashes
// them the same in every run.
func (m *metricModel) hash64(slot int, salt uint64) uint64 {
	var b [24]byte
	putUint64(b[:8], m.base)
	putUint64(b[8:16], uint64(slot))
	putUint64(b[16:], salt)
	return xxhash.Sum64(b[:])
}

// seededHash is a hash of the parts after the run's seed: the same seed hashes them the same in every run.
func seededHash(seed uint64, parts ...string) uint64 {
	var b [8]byte
	putUint64(b[:], seed)
	d := xxhash.New()
	_, _ = d.Write(b[:])
	for _, part := range parts {
		_, _ = d.WriteString(part)
		_, _ = d.Write([]byte{0})
	}
	return d.Sum64()
}

// classicHistogramSeries is the series of a classic histogram: its buckets, as many as the feed has "le" values, its
// sum and its count.
func classicHistogramSeries(p SeriesProfile) int {
	return max(1, p.ClassicBuckets-1) + 1 + 2
}

func putUint64(b []byte, v uint64) {
	for i := range 8 {
		b[i] = byte(v >> (8 * i))
	}
}

// generation is the slot's generation at t: each lives lifetimeMs, from an offset of the slot's.
func (w *world) generation(m *metricModel, slot int, t int64) int64 {
	phase := int64(m.hash64(slot, 1) % uint64(w.lifetimeMs))
	return (t + phase) / w.lifetimeMs
}

func (w *world) generationStart(m *metricModel, slot int, generation int64) int64 {
	phase := int64(m.hash64(slot, 1) % uint64(w.lifetimeMs))
	return generation*w.lifetimeMs - phase
}

// scrapeOffsetMs is when in each interval the slot is scraped: slots spread over the seconds of the interval.
func (m *metricModel) scrapeOffsetMs(slot int) int64 {
	seconds := m.intervalMs / 1000
	return int64(slot)%seconds*1000 + int64(m.hash64(slot, 2)%1000)
}

// slotsAt calls f for the slots of the metric scraped in the second [s, s+1000), at their scrape time.
func (m *metricModel) slotsAt(s int64, f func(slot int, t int64)) {
	seconds := m.intervalMs / 1000
	second := (s / 1000) % seconds
	base := s - s%m.intervalMs
	for slot := int(second); slot < m.slots; slot += int(seconds) {
		t := base + m.scrapeOffsetMs(slot)
		if t < s {
			t += m.intervalMs
		}
		if t >= s && t < s+1000 {
			f(slot, t)
		}
	}
}

// seriesLabels returns the labels of the slot's generation, sorted, with the name of the series in the family.
func (m *metricModel) seriesLabels(slot int, generation int64, suffix string, extra ...mimirpb.LabelAdapter) []mimirpb.LabelAdapter {
	labels := make([]mimirpb.LabelAdapter, 0, len(m.labels)+2+len(extra))
	labels = append(labels, mimirpb.LabelAdapter{Name: "__name__", Value: m.name + suffix})
	stride := 1
	for _, l := range m.labels {
		labels = append(labels, mimirpb.LabelAdapter{Name: l.name, Value: l.values[(slot/stride)%len(l.values)]})
		stride = min(stride*len(l.values), 1<<40)
	}
	labels = append(labels, mimirpb.LabelAdapter{Name: m.churnLabel, Value: churnValue(slot, generation)})
	labels = append(labels, extra...)
	sort.Slice(labels, func(a, b int) bool { return labels[a].Name < labels[b].Name })
	return labels
}

func churnValue(slot int, generation int64) string {
	return strconv.FormatInt(int64(slot), 36) + "-" + strconv.FormatInt(generation, 36)
}

// sample is the float value of the slot at t, of the generation started at start.
func (m *metricModel) sample(slot int, t, start int64) float64 {
	h := m.hash64(slot, 3)
	switch m.kind {
	case kindCounter, kindClassicHistogram:
		rate := m.scale * (0.5 + float64(h%1000)/1000)
		return math.Floor(rate * float64(t-start) / 1000)
	case kindConstantGauge:
		return math.Round(m.scale*float64(h%100)) / 10
	default:
		period := float64(3_600_000 + h%7_200_000)
		return m.scale * (1 + 0.2*math.Sin(2*math.Pi*float64(t)/period+float64(h%628)/100)) * (1 + float64(m.hash64(slot, uint64(t/60_000))%100)/10_000)
	}
}

// nativeHistogram is the slot's native histogram at t: a counter histogram whose buckets grow with time.
func (m *metricModel) nativeHistogram(slot int, t, start int64, buckets int) mimirpb.Histogram {
	elapsed := float64(t-start) / 1000
	rate := m.scale * (0.5 + float64(m.hash64(slot, 3)%1000)/1000)
	h := &histogram.Histogram{Schema: 0, ZeroThreshold: 1e-128, PositiveSpans: []histogram.Span{{Offset: 0, Length: uint32(buckets)}}}
	h.PositiveBuckets = make([]int64, buckets)
	var previous, total int64
	for i := range buckets {
		// Counts per bucket peak in the middle, as latencies do.
		count := int64(rate * elapsed * math.Exp(-math.Pow(float64(i-buckets/2), 2)/4))
		h.PositiveBuckets[i] = count - previous
		previous = count
		total += count
	}
	h.Count = uint64(total)
	h.Sum = float64(total) * 0.25
	return mimirpb.FromHistogramToHistogramProto(t, h)
}

var staleNaN = math.Float64frombits(value.StaleNaN)
