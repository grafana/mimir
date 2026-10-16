// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"os"
	"sort"
	"strings"
)

// Profile describes the series an ingester holds and the reads it serves, as distributions only. The built-in one is
// made up: round numbers for the series, and the reads as classes of the queries that dashboards, rules and the label and
// cardinality pages of a Grafana send. -profile loads another with the same fields.
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
	// Query shapes: a list of them, sampled uniformly; or distributions they are drawn from; or classes of queries, each
	// with distributions of its own, drawn by weight among the classes of the RPC. The first a profile has is used.
	Shapes        []QueryShape  `json:"shapes,omitempty"`
	Distributions *ShapeProfile `json:"distributions,omitempty"`
	Classes       []QueryClass  `json:"classes,omitempty"`
}

// QueryClass is a kind of query, such as an alerting rule's or a dashboard panel's: what it reads correlates, as a
// panel's query of many series has a range, regexes of a dashboard's variables and exclusions, where a rule's is
// instant, of few series, by equalities.
type QueryClass struct {
	Name   string   `json:"name"`
	Weight float64  `json:"weight"`
	RPCs   []string `json:"rpcs"`
	ShapeProfile
	// The matchers besides the metric name's, each in a share of the selectors: OtherMatchers and MatchersPerSel are
	// used instead when there are none.
	Matchers []ClassMatcher `json:"matchers,omitempty"`
}

type ClassMatcher struct {
	Type  string  `json:"type"`
	Kind  string  `json:"kind"`
	Share float64 `json:"share"`
	// How many of them a selector that has one has, 1 when not set.
	Count     Percentiles `json:"count,omitempty"`
	LabelRank Percentiles `json:"label_rank,omitempty"`
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
	if strings.HasSuffix(path, ".gz") {
		// Profiles with many query shapes, compressed to fit a ConfigMap.
		r, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return p, err
		}
		if data, err = io.ReadAll(r); err != nil {
			return p, err
		}
	}
	// Fields the file has replace the defaults'.
	if err := json.Unmarshal(data, &p); err != nil {
		return p, fmt.Errorf("parse profile %s: %w", path, err)
	}
	return p, nil
}

// defaultProfile is a made-up multi-tenant ingester.
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
			Classes: defaultClasses(),
		},
	}
}

// Ranges and times, in milliseconds.
const (
	minute = 60_000
	hour   = 60 * minute
	day    = 24 * hour
)

// defaultClasses are the reads of the made-up ingester: mostly rules and dashboards over the last minutes and hours.
func defaultClasses() []QueryClass {
	instant := Percentiles{"0": 0, "1": 0}
	now := Percentiles{"0": 0, "0.9": 0, "0.97": minute, "1": 10 * minute}
	rateRange := Percentiles{"0": minute, "0.5": 5 * minute, "0.9": 10 * minute, "1": hour}
	common := Percentiles{"0": 0, "0.5": 2, "0.9": 8, "1": 20}
	one := map[string]float64{"=": 1}
	return []QueryClass{
		{
			// sum(rate(requests_total{job="api", status!="2xx"}[5m])) > 10: instant, of a few series, mostly equalities.
			Name: "alerting rule", Weight: 0.38, RPCs: []string{"QueryStream"},
			ShapeProfile: ShapeProfile{
				Range: instant, SinceMax: instant, SelectorRange: rateRange, NameMatcher: one,
				Fetched:           Percentiles{"0": 1, "0.5": 8, "0.9": 120, "0.99": 2_000, "1": 10_000},
				SelectorsPerQuery: Percentiles{"0": 1, "0.6": 1, "0.95": 2, "1": 4},
			},
			Matchers: []ClassMatcher{
				{Type: "=", Kind: "literal", Share: 0.9, Count: Percentiles{"0": 1, "0.7": 1, "1": 3}, LabelRank: common},
				{Type: "!=", Kind: "literal", Share: 0.3, LabelRank: common},
				{Type: "=~", Kind: "alternation", Share: 0.15, LabelRank: common},
				{Type: "!=", Kind: "empty", Share: 0.1, LabelRank: common},
			},
		},
		{
			// sum by (cluster, namespace) (rate(cpu_seconds_total[5m])): instant, of a whole metric.
			Name: "recording rule", Weight: 0.14, RPCs: []string{"QueryStream"},
			ShapeProfile: ShapeProfile{
				Range: instant, SinceMax: instant, SelectorRange: rateRange, NameMatcher: one,
				Fetched:           Percentiles{"0": 20, "0.5": 400, "0.9": 8_000, "0.99": 60_000, "1": 150_000},
				SelectorsPerQuery: Percentiles{"0": 1, "0.8": 1, "1": 3},
			},
			Matchers: []ClassMatcher{
				{Type: "=", Kind: "literal", Share: 0.4, LabelRank: Percentiles{"0": 0, "1": 3}},
				{Type: "!=", Kind: "empty", Share: 0.1, LabelRank: common},
			},
		},
		{
			// rate(requests_total{cluster="$cluster", namespace="$namespace", job=~"$job"}[$__rate_interval]) over the
			// dashboard's hours, with variables set to a few values or to all of them.
			Name: "dashboard panel", Weight: 0.25, RPCs: []string{"QueryStream", "QueryExemplars"},
			ShapeProfile: ShapeProfile{
				RangeShare: 1, SinceMax: now, SelectorRange: rateRange, NameMatcher: one,
				Range:             Percentiles{"0": 5 * minute, "0.5": hour, "0.8": 6 * hour, "0.95": day, "1": 7 * day},
				Step:              Percentiles{"0": 15_000, "0.5": 30_000, "0.9": 2 * minute, "1": 10 * minute},
				Fetched:           Percentiles{"0": 1, "0.5": 40, "0.9": 1_000, "0.99": 15_000, "1": 60_000},
				SelectorsPerQuery: Percentiles{"0": 1, "0.6": 1, "0.9": 3, "1": 6},
				AlternationSize:   Percentiles{"0": 2, "0.5": 3, "0.9": 12, "1": 60},
			},
			Matchers: []ClassMatcher{
				{Type: "=", Kind: "literal", Share: 0.95, Count: Percentiles{"0": 1, "0.5": 2, "1": 3}, LabelRank: Percentiles{"0": 0, "0.5": 1, "1": 10}},
				{Type: "=~", Kind: "alternation", Share: 0.35, LabelRank: common},
				{Type: "=~", Kind: "any", Share: 0.15, LabelRank: common},
				{Type: "!=", Kind: "literal", Share: 0.25, LabelRank: common},
				{Type: "!=", Kind: "empty", Share: 0.15, LabelRank: common},
			},
		},
		{
			// Container metrics without the pause containers and the cgroups of no container:
			// container_memory_working_set_bytes{namespace="x", container!="", container!="POD", image!=""}.
			Name: "dashboard panel with exclusions", Weight: 0.07, RPCs: []string{"QueryStream"},
			ShapeProfile: ShapeProfile{
				RangeShare: 0.7, SinceMax: now, SelectorRange: rateRange, NameMatcher: one,
				Range:             Percentiles{"0": 5 * minute, "0.5": hour, "0.9": 12 * hour, "1": 7 * day},
				Step:              Percentiles{"0": 15_000, "0.5": 30_000, "0.9": 2 * minute, "1": 10 * minute},
				Fetched:           Percentiles{"0": 5, "0.5": 300, "0.9": 5_000, "0.99": 30_000, "1": 80_000},
				SelectorsPerQuery: Percentiles{"0": 1, "0.6": 1, "0.9": 2, "1": 4},
			},
			Matchers: []ClassMatcher{
				{Type: "=", Kind: "literal", Share: 0.9, LabelRank: Percentiles{"0": 0, "1": 4}},
				{Type: "!=", Kind: "literal", Share: 1, Count: Percentiles{"0": 2, "0.5": 6, "0.9": 15, "1": 22}, LabelRank: Percentiles{"0": 0, "0.5": 4, "1": 40}},
				{Type: "!=", Kind: "empty", Share: 0.6, Count: Percentiles{"0": 1, "0.7": 1, "1": 3}, LabelRank: common},
			},
		},
		{
			// Someone looking around: {__name__=~"http_.*", pod=~"api-.*"} over a day or two.
			Name: "explore", Weight: 0.06, RPCs: []string{"QueryStream"},
			ShapeProfile: ShapeProfile{
				RangeShare: 0.6, SinceMax: now, SelectorRange: rateRange,
				NameMatcher:       map[string]float64{"=": 0.8, "=~": 0.15, "none": 0.05},
				Range:             Percentiles{"0": 5 * minute, "0.5": hour, "0.9": day, "1": 14 * day},
				Step:              Percentiles{"0": 15_000, "0.5": minute, "0.9": 5 * minute, "1": hour},
				Fetched:           Percentiles{"0": 1, "0.5": 100, "0.9": 5_000, "0.99": 50_000, "1": 100_000},
				SelectorsPerQuery: Percentiles{"0": 1, "0.8": 1, "1": 2},
			},
			Matchers: []ClassMatcher{
				{Type: "=", Kind: "literal", Share: 0.6, LabelRank: common},
				{Type: "=~", Kind: "prefix", Share: 0.3, LabelRank: Percentiles{"0": 0, "0.5": 5, "1": 60}},
				{Type: "=~", Kind: "contains", Share: 0.15, LabelRank: Percentiles{"0": 0, "0.5": 5, "1": 60}},
				{Type: "=~", Kind: "regex", Share: 0.15, LabelRank: common},
				{Type: "!~", Kind: "regex", Share: 0.1, LabelRank: common},
			},
		},
		{
			// Last week or last month, of which the ingesters have the most recent hours.
			Name: "long range", Weight: 0.04, RPCs: []string{"QueryStream"},
			ShapeProfile: ShapeProfile{
				RangeShare: 1, SelectorRange: rateRange, NameMatcher: one,
				SinceMax:          Percentiles{"0": 0, "0.5": 0, "0.7": hour, "1": 7 * day},
				Range:             Percentiles{"0": day, "0.5": 7 * day, "1": 30 * day},
				Step:              Percentiles{"0": 5 * minute, "0.5": hour, "1": 6 * hour},
				Fetched:           Percentiles{"0": 1, "0.5": 50, "0.9": 2_000, "1": 20_000},
				SelectorsPerQuery: Percentiles{"0": 1, "0.8": 1, "1": 3},
			},
			Matchers: []ClassMatcher{
				{Type: "=", Kind: "literal", Share: 0.9, Count: Percentiles{"0": 1, "1": 3}, LabelRank: common},
				{Type: "=~", Kind: "alternation", Share: 0.2, LabelRank: common},
			},
		},
		{
			// An aggregation over a large metric, which the query-frontend shards.
			Name: "sharded aggregation", Weight: 0.02, RPCs: []string{"QueryStream"},
			ShapeProfile: ShapeProfile{
				RangeShare: 0.7, SinceMax: now, SelectorRange: rateRange, NameMatcher: one, ShardedShare: 1, Shards: 16,
				Range:             Percentiles{"0": hour, "0.5": 3 * hour, "1": day},
				Step:              Percentiles{"0": 15_000, "0.5": minute, "1": 5 * minute},
				Fetched:           Percentiles{"0": 5_000, "0.5": 40_000, "1": 300_000},
				SelectorsPerQuery: Percentiles{"0": 1, "1": 1},
			},
			Matchers: []ClassMatcher{
				{Type: "=", Kind: "literal", Share: 0.6, LabelRank: Percentiles{"0": 0, "1": 3}},
				{Type: "=~", Kind: "alternation", Share: 0.3, LabelRank: common},
			},
		},
		{
			// A query of one or a few pods or instances, by a label of many values.
			Name: "by instance", Weight: 0.04, RPCs: []string{"QueryStream"},
			ShapeProfile: ShapeProfile{
				RangeShare: 0.6, SinceMax: now, SelectorRange: rateRange, NameMatcher: one,
				Range:             Percentiles{"0": 5 * minute, "0.5": hour, "1": day},
				Step:              Percentiles{"0": 15_000, "0.5": 30_000, "1": 5 * minute},
				Fetched:           Percentiles{"0": 1, "0.5": 3, "0.9": 30, "1": 300},
				SelectorsPerQuery: Percentiles{"0": 1, "0.7": 1, "1": 3},
				AlternationSize:   Percentiles{"0": 2, "0.5": 4, "1": 30},
			},
			Matchers: []ClassMatcher{
				{Type: "=", Kind: "literal", Share: 0.7, Count: Percentiles{"0": 1, "1": 2}, LabelRank: Percentiles{"0": 0, "1": 3}},
				{Type: "=", Kind: "literal", Share: 0.6, LabelRank: Percentiles{"0": 3, "0.5": 15, "1": 100}},
				{Type: "=~", Kind: "alternation", Share: 0.4, LabelRank: Percentiles{"0": 3, "0.5": 15, "1": 100}},
				{Type: "=~", Kind: "prefix", Share: 0.1, LabelRank: Percentiles{"0": 3, "0.5": 15, "1": 100}},
			},
		},
		{
			// The active series of a metric, or of a namespace, in the cardinality pages.
			Name: "active series", Weight: 1, RPCs: []string{"ActiveSeries"},
			ShapeProfile: ShapeProfile{
				SinceMax: instant, NameMatcher: map[string]float64{"=": 0.6, "none": 0.4},
				Fetched:           Percentiles{"0": 1, "0.5": 200, "0.9": 20_000, "1": 300_000},
				SelectorsPerQuery: Percentiles{"0": 1, "1": 1},
			},
			Matchers: []ClassMatcher{{Type: "=", Kind: "literal", Share: 0.6, LabelRank: Percentiles{"0": 0, "1": 5}}},
		},
		{
			// A dashboard's variable, label_values(up{cluster="$cluster"}, namespace), over its range.
			Name: "dashboard variable", Weight: 1, RPCs: []string{"LabelValues", "LabelNames"},
			ShapeProfile: ShapeProfile{
				RangeShare: 1, SinceMax: now, NameMatcher: map[string]float64{"=": 0.7, "none": 0.3},
				Range:             Percentiles{"0": 5 * minute, "0.5": hour, "0.9": 6 * hour, "1": 7 * day},
				Fetched:           Percentiles{"0": 1, "0.5": 100, "0.9": 5_000, "1": 50_000},
				SelectorsPerQuery: Percentiles{"0": 1, "1": 1},
			},
			Matchers: []ClassMatcher{
				{Type: "=", Kind: "literal", Share: 0.7, Count: Percentiles{"0": 1, "1": 2}, LabelRank: Percentiles{"0": 0, "1": 5}},
				{Type: "=~", Kind: "alternation", Share: 0.2, LabelRank: common},
			},
		},
		{
			// The series of a metric, or of metrics by a prefix, as a metrics browser lists them.
			Name: "series browser", Weight: 1, RPCs: []string{"MetricsForLabelMatchers"},
			ShapeProfile: ShapeProfile{
				RangeShare: 1, SinceMax: now, NameMatcher: map[string]float64{"=": 0.8, "=~": 0.2},
				Range:             Percentiles{"0": 5 * minute, "0.5": hour, "1": day},
				Fetched:           Percentiles{"0": 1, "0.5": 50, "0.9": 2_000, "1": 50_000},
				SelectorsPerQuery: Percentiles{"0": 1, "1": 1},
			},
			Matchers: []ClassMatcher{{Type: "=", Kind: "literal", Share: 0.5, LabelRank: common}},
		},
		{
			// The cardinality pages' label names and values of a metric or a tenant.
			Name: "cardinality", Weight: 1, RPCs: []string{"LabelNamesAndValues", "LabelValuesCardinality"},
			ShapeProfile: ShapeProfile{
				SinceMax: instant, NameMatcher: map[string]float64{"=": 0.8, "none": 0.2},
				Fetched:           Percentiles{"0": 10, "0.5": 2_000, "0.9": 50_000, "1": 500_000},
				SelectorsPerQuery: Percentiles{"0": 1, "1": 1},
			},
			Matchers: []ClassMatcher{{Type: "=", Kind: "literal", Share: 0.3, LabelRank: Percentiles{"0": 0, "1": 3}}},
		},
	}
}
