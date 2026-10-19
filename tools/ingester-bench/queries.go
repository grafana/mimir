// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"fmt"
	"math"
	"math/rand/v2" //#nosec G404 -- Synthetic data and reads, reproducible from a seed: no need for a CSPRNG -- nosemgrep: math-random-used
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/grafana/mimir/pkg/ingester/client"
)

// request is one read of the bench, for both ingesters alike.
type request struct {
	api    string
	tenant string
	// One of these, by api.
	query          *client.QueryRequest
	activeSeries   *client.ActiveSeriesRequest
	labelValues    *client.LabelValuesRequest
	labelNames     *client.LabelNamesRequest
	namesAndValues *client.LabelNamesAndValuesRequest
	metricsFor     *client.MetricsForLabelMatchersRequest
	exemplars      *client.ExemplarQueryRequest
	cardinality    *client.LabelValuesCardinalityRequest
	userStats      *client.UserStatsRequest
}

func (r *request) String() string {
	var body fmt.Stringer
	switch {
	case r.query != nil:
		body = r.query
	case r.activeSeries != nil:
		body = r.activeSeries
	case r.labelValues != nil:
		body = r.labelValues
	case r.labelNames != nil:
		body = r.labelNames
	case r.namesAndValues != nil:
		body = r.namesAndValues
	case r.metricsFor != nil:
		body = r.metricsFor
	case r.exemplars != nil:
		body = r.exemplars
	case r.cardinality != nil:
		body = r.cardinality
	default:
		return r.api + " " + r.tenant
	}
	return r.api + " " + r.tenant + " " + body.String()
}

// queryGen draws the bench's reads from the profile's query shapes, over the world's series.
type queryGen struct {
	world   *world
	profile QueriesProfile
	// Shapes by the ingester RPC they turn into.
	shapes map[string][]QueryShape
	// The profile's query classes of each RPC.
	classes map[string][]QueryClass
	// Each tenant's metrics by series, smallest first, and tenants weighted by how much they query.
	bySize        map[*tenantModel][]*metricModel
	tenantWeights []float64
	// The ingesters hold samples from dataStart, up to retention back.
	dataStart func() int64
	retention int64
	// Queries' end is relative to now, which the bench sets.
	now func() int64
}

// The query-frontend APIs whose shapes each ingester RPC takes.
var shapeAPIs = map[string][]string{
	"QueryStream":             {"query", "query_range"},
	"ActiveSeries":            {"cardinality_active_series"},
	"LabelNamesAndValues":     {"cardinality_label_names"},
	"LabelValuesCardinality":  {"cardinality_label_values", "cardinality_label_names"},
	"LabelValues":             {"label_name_values"},
	"LabelNames":              {"labels"},
	"MetricsForLabelMatchers": {"series"},
	"QueryExemplars":          {"query_exemplars"},
}

func newQueryGen(w *world, p QueriesProfile, retention int64, dataStart, now func() int64) *queryGen {
	g := &queryGen{world: w, profile: p, shapes: map[string][]QueryShape{}, bySize: map[*tenantModel][]*metricModel{}, retention: retention, dataStart: dataStart, now: now}
	for rpc, apis := range shapeAPIs {
		for _, s := range p.Shapes {
			for _, api := range apis {
				if s.API == api {
					g.shapes[rpc] = append(g.shapes[rpc], s)
				}
			}
		}
	}
	g.classes = map[string][]QueryClass{}
	for _, c := range p.Classes {
		for _, rpc := range c.RPCs {
			g.classes[rpc] = append(g.classes[rpc], c)
		}
	}
	for _, t := range w.tenants {
		metrics := append([]*metricModel(nil), t.metrics...)
		sort.Slice(metrics, func(a, b int) bool { return metrics[a].seriesCount() < metrics[b].seriesCount() })
		g.bySize[t] = metrics
		// Bigger tenants query more, but not in proportion to their size.
		g.tenantWeights = append(g.tenantWeights, math.Sqrt(float64(t.series)))
	}
	return g
}

func (m *metricModel) seriesCount() int {
	if m.kind == kindClassicHistogram {
		return m.slots * 14
	}
	return m.slots
}

// rates returns the requests per second of each RPC, for the world's series.
func (g *queryGen) rates(scale float64) map[string]float64 {
	out := map[string]float64{}
	millions := float64(g.world.totalSeries()) / 1e6
	for rpc, rate := range g.profile.RatePerMillionSeries {
		out[rpc] = rate * millions * scale
	}
	return out
}

// shape returns a shape for the RPC: one of the profile's, or one drawn from its distributions or its classes.
func (g *queryGen) shape(r *rand.Rand, rpc string) QueryShape {
	if shapes := g.shapes[rpc]; len(shapes) > 0 {
		return shapes[r.IntN(len(shapes))]
	}
	if d := g.profile.Distributions; d != nil {
		return drawShape(r, rpc, d, nil)
	}
	classes := g.classes[rpc]
	if len(classes) == 0 {
		return drawShape(r, rpc, &ShapeProfile{NameMatcher: map[string]float64{"=": 1}}, nil)
	}
	weights := make([]float64, len(classes))
	for i, c := range classes {
		weights[i] = c.Weight
	}
	c := classes[weightedIndex(r, weights)]
	return drawShape(r, rpc, &c.ShapeProfile, c.Matchers)
}

// drawShape draws a shape from d, with the matchers besides the name's drawn from matchers when there are some.
func drawShape(r *rand.Rand, rpc string, d *ShapeProfile, matchers []ClassMatcher) QueryShape {
	s := QueryShape{API: "query", Fetched: max(1, int(d.Fetched.Sample(r))), SinceMaxMs: int64(d.SinceMax.Sample(r))}
	if r.Float64() < d.RangeShare {
		s.RangeMs = int64(d.Range.Sample(r))
		if rpc == "QueryStream" {
			s.API, s.StepMs = "query_range", int64(d.Step.Sample(r))
		}
	}
	if r.Float64() < d.ShardedShare {
		s.Sharded = d.Shards
	}
	alternation := func() Value {
		return Value{Kind: "alternation", Size: max(2, int(math.Round(d.AlternationSize.Sample(r))))}
	}
	selectors := max(1, int(math.Round(d.SelectorsPerQuery.Sample(r))))
	for range selectors {
		sel := Selector{}
		// Half of the selectors are of a range function, the others of the latest samples.
		if len(d.SelectorRange) > 0 && r.Float64() < 0.5 {
			sel.RangeMs = int64(d.SelectorRange.Sample(r))
		}
		switch pick(r, d.NameMatcher) {
		case "=":
			sel.Matchers = append(sel.Matchers, Matcher{Label: "__name__", Type: "=", Value: Value{Kind: "literal"}})
		case "=~":
			sel.Matchers = append(sel.Matchers, Matcher{Label: "__name__", Type: "=~", Value: Value{Kind: "alternation", Size: 2}})
		}
		if matchers != nil {
			for _, m := range matchers {
				if r.Float64() >= m.Share {
					continue
				}
				count := 1
				if len(m.Count) > 0 {
					count = max(1, int(math.Round(m.Count.Sample(r))))
				}
				for range count {
					v := Value{Kind: m.Kind}
					if v.Kind == "alternation" {
						v = alternation()
					}
					sel.Matchers = append(sel.Matchers, Matcher{Label: "l" + strconv.Itoa(int(m.LabelRank.Sample(r))), Type: m.Type, Value: v})
				}
			}
			s.Selectors = append(s.Selectors, sel)
			continue
		}
		for range max(0, int(math.Round(d.MatchersPerSel.Sample(r)))-len(sel.Matchers)) {
			kind := strings.SplitN(pick(r, d.OtherMatchers), " ", 2)
			v := Value{Kind: kind[1]}
			if v.Kind == "alternation" {
				v = alternation()
			}
			sel.Matchers = append(sel.Matchers, Matcher{Label: "l" + strconv.Itoa(int(d.LabelRank.Sample(r))), Type: kind[0], Value: v})
		}
		s.Selectors = append(s.Selectors, sel)
	}
	return s
}

// next returns a request of the RPC, or nil when the shape drawn reads nothing the ingesters have.
func (g *queryGen) next(r *rand.Rand, rpc string) *request {
	t := g.world.tenants[weightedIndex(r, g.tenantWeights)]
	req := &request{api: rpc, tenant: t.id}
	if rpc == "UserStats" {
		req.userStats = &client.UserStatsRequest{}
		return req
	}
	s := g.shape(r, rpc)
	now := g.now()
	end := now - s.SinceMaxMs
	// Reads of what's older than the ingesters' data go to the store-gateways only.
	if end < max(now-g.retention, g.dataStart()) {
		return nil
	}
	var sel Selector
	if len(s.Selectors) > 0 {
		sel = s.Selectors[r.IntN(len(s.Selectors))]
	}
	target := max(1, s.Fetched/max(1, len(s.Selectors)))
	m := g.metricFor(r, t, target)
	matchers := g.matchers(r, t, m, sel)
	start := max(g.dataStart(), end-s.RangeMs-sel.RangeMs-5*60_000)
	switch rpc {
	case "QueryStream":
		if s.Sharded > 0 {
			matchers = append(matchers, &client.LabelMatcher{Type: client.EQUAL, Name: "__query_shard__", Value: fmt.Sprintf("%d_of_%d", r.IntN(s.Sharded)+1, s.Sharded)})
		}
		req.query = &client.QueryRequest{StartTimestampMs: start, EndTimestampMs: end, Matchers: matchers, StreamingChunksBatchSize: 256}
	case "ActiveSeries":
		req.activeSeries = &client.ActiveSeriesRequest{Matchers: matchers, Type: client.SERIES}
	case "LabelNamesAndValues":
		req.namesAndValues = &client.LabelNamesAndValuesRequest{Matchers: matchers, CountMethod: countMethod(r)}
	case "LabelValuesCardinality":
		req.cardinality = &client.LabelValuesCardinalityRequest{LabelNames: []string{g.labelOf(r, m)}, Matchers: matchers, CountMethod: countMethod(r)}
	case "LabelValues":
		req.labelValues = &client.LabelValuesRequest{LabelName: g.labelOf(r, m), StartTimestampMs: start, EndTimestampMs: end, Matchers: &client.LabelMatchers{Matchers: matchers}}
	case "LabelNames":
		req.labelNames = &client.LabelNamesRequest{StartTimestampMs: start, EndTimestampMs: end, Matchers: &client.LabelMatchers{Matchers: matchers}}
	case "MetricsForLabelMatchers":
		req.metricsFor = &client.MetricsForLabelMatchersRequest{StartTimestampMs: start, EndTimestampMs: end, MatchersSet: []*client.LabelMatchers{{Matchers: matchers}}}
	case "QueryExemplars":
		req.exemplars = &client.ExemplarQueryRequest{StartTimestampMs: start, EndTimestampMs: end, Matchers: []*client.LabelMatchers{{Matchers: matchers}}}
	default:
		return nil
	}
	return req
}

func countMethod(r *rand.Rand) client.CountMethod {
	if r.IntN(2) == 0 {
		return client.IN_MEMORY
	}
	return client.ACTIVE
}

// metricFor returns one of the tenant's metrics of about target series.
func (g *queryGen) metricFor(r *rand.Rand, t *tenantModel, target int) *metricModel {
	metrics := g.bySize[t]
	i := sort.Search(len(metrics), func(i int) bool { return metrics[i].seriesCount() >= target })
	i = min(len(metrics)-1, max(0, i-2+r.IntN(5)))
	return metrics[i]
}

// labelOf returns one of the metric's label names.
func (g *queryGen) labelOf(r *rand.Rand, m *metricModel) string {
	if len(m.labels) == 0 || r.IntN(len(m.labels)+1) == 0 {
		return m.churnLabel
	}
	return m.labels[r.IntN(len(m.labels))].name
}

// matchers instantiates the selector's matchers on the metric: labels by the closest rank it has, values its own.
func (g *queryGen) matchers(r *rand.Rand, t *tenantModel, m *metricModel, sel Selector) []*client.LabelMatcher {
	var out []*client.LabelMatcher
	name := m.name
	if m.kind == kindClassicHistogram {
		name += []string{"_bucket", "_bucket", "_count", "_sum"}[r.IntN(4)]
	}
	used := map[string]bool{}
	for _, matcher := range sel.Matchers {
		lm := &client.LabelMatcher{Type: matchType(matcher.Type)}
		if matcher.Label == "__name__" {
			lm.Name = "__name__"
			lm.Value = name
			if matcher.Type == "=~" {
				other := g.bySize[t][r.IntN(len(g.bySize[t]))].name
				lm.Value = regexp.QuoteMeta(name) + "|" + regexp.QuoteMeta(other)
			}
			out = append(out, lm)
			continue
		}
		label, values := g.closestLabel(m, matcher.Label, used)
		if label == "" {
			continue
		}
		used[label] = true
		lm.Name = label
		lm.Value = instantiateValue(r, matcher, values(r))
		out = append(out, lm)
	}
	if len(out) == 0 || (len(out) == 1 && out[0].Type != client.EQUAL && out[0].Name != "__name__") {
		// A selector has to match something specific; the profile's few that don't name a metric still have a label.
		out = append(out, &client.LabelMatcher{Type: client.EQUAL, Name: "__name__", Value: name})
	}
	return out
}

// closestLabel returns the metric's label of the rank closest to the shape's, and how to draw values of it.
func (g *queryGen) closestLabel(m *metricModel, rank string, used map[string]bool) (string, func(*rand.Rand) []string) {
	want, _ := strconv.Atoi(strings.TrimPrefix(rank, "l"))
	best, bestDistance := "", math.MaxInt
	var bestValues func(*rand.Rand) []string
	consider := func(name string, values func(*rand.Rand) []string) {
		if used[name] {
			return
		}
		got, _ := strconv.Atoi(strings.TrimPrefix(name, "l"))
		if d := abs(got - want); d < bestDistance {
			best, bestDistance, bestValues = name, d, values
		}
	}
	for _, l := range m.labels {
		consider(l.name, func(r *rand.Rand) []string { return l.values })
	}
	consider(m.churnLabel, func(r *rand.Rand) []string {
		// Values of live series: a few slots' current generations.
		now := g.now()
		out := make([]string, 0, 16)
		for range 16 {
			slot := r.IntN(m.slots)
			out = append(out, churnValue(slot, g.world.generation(m, slot, now)))
		}
		return out
	})
	return best, bestValues
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func matchType(t string) client.MatchType {
	switch t {
	case "!=":
		return client.NOT_EQUAL
	case "=~":
		return client.REGEX_MATCH
	case "!~":
		return client.REGEX_NO_MATCH
	}
	return client.EQUAL
}

// instantiateValue returns a value of the kind, from the label's values.
func instantiateValue(r *rand.Rand, m Matcher, values []string) string {
	v := values[r.IntN(len(values))]
	switch m.Value.Kind {
	case "empty":
		return ""
	case "any":
		return ".*"
	case "nonempty":
		return ".+"
	case "alternation":
		alts := make([]string, 0, m.Value.Size)
		for range max(2, m.Value.Size) {
			alts = append(alts, regexp.QuoteMeta(values[r.IntN(len(values))]))
		}
		return strings.Join(alts, "|")
	case "prefix":
		return regexp.QuoteMeta(v[:max(1, len(v)/2)]) + ".*"
	case "suffix":
		return ".*" + regexp.QuoteMeta(v[len(v)/2:])
	case "contains":
		return ".*" + regexp.QuoteMeta(v[len(v)/4:max(len(v)/4+1, 3*len(v)/4)]) + ".*"
	case "regex":
		return "(" + regexp.QuoteMeta(v[:max(1, len(v)/2)]) + ".+|" + regexp.QuoteMeta(values[r.IntN(len(values))]) + ")"
	case "literal":
		if m.Type == "!=" && r.IntN(2) == 0 {
			// Excluded values often aren't there at all.
			return v + "-absent"
		}
		if m.Type == "=~" || m.Type == "!~" {
			return regexp.QuoteMeta(v)
		}
		return v
	}
	return v
}
