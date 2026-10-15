// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"sort"
)

// Profile describes the series an ingester holds and the reads it serves, as distributions only. The built-in one has
// generic, rounded numbers; -profile loads another, such as one measured from a running cluster and anonymized, with
// the same fields.
type Profile struct {
	Series  SeriesProfile  `json:"series"`
	Queries QueriesProfile `json:"queries"`
}

type SeriesProfile struct {
	// Tenants on the ingester, and how many series each holds, as a percentile table of the share of the ingester's series.
	Tenants         int         `json:"tenants"`
	SeriesPerTenant Percentiles `json:"series_per_tenant"`
	// How the series of a tenant spread over its metrics: the n-th largest metric has a share proportional to
	// 1/n^MetricSkew.
	MetricSkew      float64     `json:"metric_skew"`
	SeriesPerMetric float64     `json:"series_per_metric"`
	LabelsPerMetric Percentiles `json:"labels_per_metric"`
	ValuesPerLabel  Percentiles `json:"values_per_label"`
	ValueLength     Percentiles `json:"value_length"`
	// Label names, ranked by how many metrics have them: the n-th has a weight 1/n^LabelSkew.
	LabelNames int     `json:"label_names"`
	LabelSkew  float64 `json:"label_skew"`
	// Shares of the metrics that are classic histograms (a _bucket, _sum and _count family with ClassicBuckets "le"
	// values) and native histograms, with NativeBuckets buckets.
	ClassicHistogramShare float64 `json:"classic_histogram_share"`
	ClassicBuckets        int     `json:"classic_buckets"`
	NativeHistogramShare  float64 `json:"native_histogram_share"`
	NativeBuckets         int     `json:"native_buckets"`
	CounterShare          float64 `json:"counter_share"`
	// How often each series is scraped, and the share of each interval, in seconds.
	ScrapeIntervals map[string]float64 `json:"scrape_intervals"`
	// Series replaced every hour, as a share of all, and the share of samples written late, up to LateMaxSeconds.
	ChurnPerHour   float64 `json:"churn_per_hour"`
	LateShare      float64 `json:"late_share"`
	LateMaxSeconds int     `json:"late_max_seconds"`
	ExemplarShare  float64 `json:"exemplar_share"`
}

type QueriesProfile struct {
	// Requests per second to the ingester, per million of its series, by ingester RPC.
	RatePerMillionSeries map[string]float64 `json:"rate_per_million_series"`
	// Query shapes: either a list of them, sampled uniformly, or the distributions they are drawn from.
	Shapes        []QueryShape  `json:"shapes,omitempty"`
	Distributions *ShapeProfile `json:"distributions,omitempty"`
}

// QueryShape is one anonymized read: how far back it goes and how much it selects, and its selectors' matchers, whose
// labels are ranks ("l0" the most used) or "__name__", and values only their kind.
type QueryShape struct {
	API        string     `json:"api"`
	RangeMs    int64      `json:"range_ms"`
	StepMs     int64      `json:"step_ms,omitempty"`
	SinceMaxMs int64      `json:"since_max_ms"`
	Sharded    int        `json:"sharded,omitempty"`
	Fetched    int        `json:"fetched_series"`
	Selectors  []Selector `json:"selectors"`
}

type Selector struct {
	RangeMs  int64     `json:"range_ms,omitempty"`
	Matchers []Matcher `json:"matchers"`
}

type Matcher struct {
	Label string `json:"label"`
	Type  string `json:"type"`
	Value Value  `json:"value"`
}

type Value struct {
	// literal, empty, any, nonempty, alternation, prefix, suffix, contains or regex.
	Kind string `json:"kind"`
	Size int    `json:"size,omitempty"`
}

// ShapeProfile is what query shapes are drawn from when a profile has none.
type ShapeProfile struct {
	RangeShare        float64            `json:"range_share"`
	Range             Percentiles        `json:"range_ms"`
	Step              Percentiles        `json:"step_ms"`
	SinceMax          Percentiles        `json:"since_max_ms"`
	SelectorRange     Percentiles        `json:"selector_range_ms"`
	ShardedShare      float64            `json:"sharded_share"`
	Shards            int                `json:"shards"`
	Fetched           Percentiles        `json:"fetched_series"`
	SelectorsPerQuery Percentiles        `json:"selectors_per_query"`
	MatchersPerSel    Percentiles        `json:"matchers_per_selector"`
	NameMatcher       map[string]float64 `json:"name_matcher"`
	OtherMatchers     map[string]float64 `json:"other_matchers"`
	AlternationSize   Percentiles        `json:"alternation_size"`
	LabelRank         Percentiles        `json:"label_rank"`
}

// Percentiles is a quantile table, {"0.5": 11, "0.9": 24, ...}, sampled by interpolating log-linearly between them.
type Percentiles map[string]float64

func (p Percentiles) points() ([]float64, []float64) {
	type point struct{ q, v float64 }
	var points []point
	for q, v := range p {
		var f float64
		if _, err := fmt.Sscan(q, &f); err == nil {
			points = append(points, point{f, v})
		}
	}
	sort.Slice(points, func(a, b int) bool { return points[a].q < points[b].q })
	qs, vs := make([]float64, len(points)), make([]float64, len(points))
	for i, p := range points {
		qs[i], vs[i] = p.q, p.v
	}
	return qs, vs
}

// Quantile returns the value at q, between 0 and 1.
func (p Percentiles) Quantile(q float64) float64 {
	qs, vs := p.points()
	if len(qs) == 0 {
		return 0
	}
	if q <= qs[0] {
		return vs[0]
	}
	for i := 1; i < len(qs); i++ {
		if q <= qs[i] {
			f := (q - qs[i-1]) / (qs[i] - qs[i-1])
			lo, hi := math.Max(vs[i-1], 1e-9), math.Max(vs[i], 1e-9)
			return math.Exp(math.Log(lo) + f*(math.Log(hi)-math.Log(lo)))
		}
	}
	return vs[len(vs)-1]
}

func (p Percentiles) Sample(r *rand.Rand) float64 { return p.Quantile(r.Float64()) }

// pick returns a key of weights, drawn by weight.
func pick(r *rand.Rand, weights map[string]float64) string {
	keys := make([]string, 0, len(weights))
	total := 0.0
	for k, w := range weights {
		keys = append(keys, k)
		total += w
	}
	sort.Strings(keys)
	x := r.Float64() * total
	for _, k := range keys {
		x -= weights[k]
		if x < 0 {
			return k
		}
	}
	return keys[len(keys)-1]
}

func loadProfile(path string) (Profile, error) {
	p := defaultProfile()
	if path == "" {
		return p, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return p, err
	}
	// Fields the file has replace the defaults'.
	if err := json.Unmarshal(data, &p); err != nil {
		return p, fmt.Errorf("parse profile %s: %w", path, err)
	}
	return p, nil
}

// defaultProfile has rounded numbers in the range of a large multi-tenant cluster's ingesters.
func defaultProfile() Profile {
	return Profile{
		Series: SeriesProfile{
			Tenants: 100,
			// Shares of the ingester's series: most tenants are small, a few hold most series.
			SeriesPerTenant:       Percentiles{"0": 0.000002, "0.1": 0.000005, "0.5": 0.002, "0.9": 0.012, "0.99": 0.05, "1": 0.09},
			MetricSkew:            1,
			SeriesPerMetric:       150,
			LabelsPerMetric:       Percentiles{"0": 2, "0.1": 6, "0.5": 12, "0.9": 24, "0.99": 40, "1": 60},
			ValuesPerLabel:        Percentiles{"0": 1, "0.1": 1, "0.5": 6, "0.9": 700, "0.99": 5000, "1": 20000},
			ValueLength:           Percentiles{"0": 1, "0.5": 12, "0.9": 40, "0.99": 90, "1": 200},
			LabelNames:            600,
			LabelSkew:             1.1,
			ClassicHistogramShare: 0.15,
			ClassicBuckets:        12,
			NativeHistogramShare:  0.002,
			NativeBuckets:         11,
			CounterShare:          0.6,
			ScrapeIntervals:       map[string]float64{"60": 1},
			ChurnPerHour:          0.1,
			LateShare:             0.013,
			LateMaxSeconds:        600,
			ExemplarShare:         0.003,
		},
		Queries: QueriesProfile{
			RatePerMillionSeries: map[string]float64{
				"QueryStream":             600,
				"ActiveSeries":            30,
				"LabelNamesAndValues":     0.5,
				"LabelValues":             0.25,
				"MetricsForLabelMatchers": 0.15,
				"LabelNames":              0.03,
				"QueryExemplars":          0.07,
				"LabelValuesCardinality":  0.005,
				"UserStats":               0.005,
			},
			Distributions: &ShapeProfile{
				RangeShare:        0.3,
				Range:             Percentiles{"0": 60_000, "0.5": 600_000, "0.9": 21_600_000, "0.99": 172_800_000},
				Step:              Percentiles{"0": 15_000, "0.5": 60_000, "0.9": 120_000, "0.99": 3_600_000},
				SinceMax:          Percentiles{"0": 0, "0.5": 1, "0.9": 250_000, "0.99": 40_000_000},
				SelectorRange:     Percentiles{"0": 60_000, "0.5": 240_000, "0.9": 3_600_000, "0.99": 86_400_000},
				ShardedShare:      0.01,
				Shards:            16,
				Fetched:           Percentiles{"0": 1, "0.5": 20, "0.9": 500, "0.99": 20_000, "0.999": 100_000},
				SelectorsPerQuery: Percentiles{"0": 1, "0.5": 1, "0.9": 2, "0.99": 5},
				MatchersPerSel:    Percentiles{"0": 1, "0.5": 3, "0.9": 15, "0.99": 22},
				NameMatcher:       map[string]float64{"=": 0.97, "=~": 0.02, "none": 0.01},
				OtherMatchers: map[string]float64{
					"!= literal": 0.5, "= literal": 0.27, "!= empty": 0.08, "=~ alternation": 0.04, "= empty": 0.02,
					"=~ literal": 0.02, "!~ regex": 0.016, "=~ regex": 0.014, "=~ prefix": 0.014, "=~ any": 0.006,
					"!~ alternation": 0.004, "=~ contains": 0.003, "=~ suffix": 0.002,
				},
				AlternationSize: Percentiles{"0": 2, "0.5": 2, "0.9": 3, "0.99": 13, "1": 50},
				LabelRank:       Percentiles{"0": 0, "0.5": 6, "0.9": 50, "0.99": 230},
			},
		},
	}
}
