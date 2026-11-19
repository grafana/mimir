// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/regexp"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
)

// Query names: the request shapes the estimate takes, one match[] and one group_by each.
const (
	queryTenantByName    = "tenant_by_name"     // series per metric name across the tenant
	queryMetricByLabel   = "metric_by_label"    // series per value of one of a metric's high-cardinality labels, and its distinct values
	queryMatcherByName   = "matcher_by_name"    // series per metric name under an arbitrary matcher
	queryMetricTotal     = "metric_total"       // series of one metric
	queryTenantByLabel   = "tenant_by_label"    // series per value of one label across every metric
	queryTenantLabelVals = "tenant_label_vals"  // distinct values of the tenant's highest-cardinality label
	queryNameRegex       = "name_regex"         // series per metric name under a regex on a name prefix
	queryMetricNotEqual  = "metric_not_equal"   // series of one metric without one value of a label, by that label; series without the label count
	queryMetricRegex     = "metric_label_regex" // series of one metric under a regex on a label's values, by that label
	queryTwoMatchers     = "two_matchers"       // series of one metric with one value of a label, by another label
)

var allQueries = []string{queryTenantByName, queryMetricByLabel, queryMatcherByName, queryMetricTotal, queryTenantByLabel, queryTenantLabelVals, queryNameRegex, queryMetricNotEqual, queryMetricRegex, queryTwoMatchers}

// Window kinds: which components answer.
const (
	windowBlocks    = "blocks"    // ends before now minus query-ingesters-within: store-gateways only
	windowIngesters = "ingesters" // inside query-ingesters-within
	windowBoundary  = "boundary"  // across the boundary between the two
	windowRecent    = "recent"    // ends now and starts before the boundary: the "last N hours" a dashboard asks for, answered by ingesters and store-gateways
)

var allWindowKinds = []string{windowBlocks, windowIngesters, windowBoundary, windowRecent}

type window struct {
	kind       string
	start, end int64 // milliseconds, [start, end)
}

func (w window) length() time.Duration { return time.Duration(w.end-w.start) * time.Millisecond }

// windowConfig places the windows relative to now.
type windowConfig struct {
	lengths              []time.Duration
	queryIngestersWithin time.Duration
	// lag keeps ingester and boundary windows this far behind now, so both requests see the same head.
	lag time.Duration
	// blocksMargin keeps block windows this far before the ingesters' range.
	blocksMargin time.Duration
}

// blocksEnd is the end of the block windows: before the ingesters' range by blocksMargin, on a whole minute.
func (c windowConfig) blocksEnd(now time.Time) int64 {
	return now.Add(-c.queryIngestersWithin - c.blocksMargin).Truncate(time.Minute).UnixMilli()
}

// windows returns every window kind for every length that fits. A length too long for the ingesters gets a recent window in place of the ingester one. Edges are whole minutes.
func (c windowConfig) windows(now time.Time) []window {
	ms := func(t time.Time) int64 { return t.Truncate(time.Minute).UnixMilli() }
	boundary := now.Add(-c.queryIngestersWithin)
	latest := now.Add(-c.lag)
	var out []window
	for _, l := range c.lengths {
		end := c.blocksEnd(now)
		out = append(out, window{windowBlocks, end - l.Milliseconds(), end})

		end = ms(latest)
		if start := end - l.Milliseconds(); start > boundary.Add(c.lag).UnixMilli() {
			out = append(out, window{windowIngesters, start, end})
		} else {
			out = append(out, window{windowRecent, start, end})
		}

		start := ms(boundary.Add(-l / 2))
		if end := start + l.Milliseconds(); end <= latest.UnixMilli() {
			out = append(out, window{windowBoundary, start, end})
		}
	}
	return out
}

// targets are the metric names and labels a round fills its queries with, picked from the tenant's data.
type targets struct {
	names []string
	// labels holds each name's label names, most distinct values first, without __name__.
	labels map[string][]string
	// tenantLabels are the tenant's label names, most distinct values first, without __name__.
	tenantLabels []string
	// matcherValues are the values of the matcher label with the most series.
	matcherValues []string
	// facets holds, for each name, the label its matcher queries filter on.
	facets map[string]facet
	// facetRefusals holds the refusal class of each name whose facet probe failed, so its skipped matcher queries show in the refusals counter.
	facetRefusals []string
	// tenantSeries is the tenant's series over the target window, the total of its name table.
	tenantSeries int64
}

// facet is a label of one metric name and its values with the most series.
type facet struct {
	label  string
	values []string
	// partial is set when some of the name's series lack the label, which the name's negative matcher must count.
	partial bool
}

// Target sources: where the metric names and label values to draw from are read.
const (
	targetSourceBounds      = "bounds"       // bounds mode over a window ending at the block cutoff, or the heads' row set when no block range answers
	targetSourceHead        = "head"         // the heads' row set of bounds mode over the last hour
	targetSourceLabelValues = "label_values" // /api/v1/cardinality/label_values, which the tenant's cardinality analysis limit caps
)

var allTargetSources = []string{targetSourceBounds, targetSourceHead, targetSourceLabelValues}

type targetConfig struct {
	// ranks are the 1-based ranks, by series count, of the metric names drawn. The largest names' PromQL truths exceed the query limits, so the draw reaches into the table: ranks 5, 50, 500 and 5,000.
	ranks        []int
	labels       int
	matcherLabel string
	// facets enables picking a facet per name, which only the matcher queries need.
	facets bool
	source string
	// labelValuesLimit is the most rows read from the label values endpoint: the tenant's cardinality analysis limit, 500 by default. Ranks above it are drawn from bounds mode only.
	labelValuesLimit int
}

// targetWindows are the windows target picking reads.
type targetWindows struct {
	// blocks is as long as -target-window and ends at the block cutoff, where only store-gateways answer.
	blocks window
	// recent is the last hour the ingesters hold.
	recent window
}

// rows are one label's values by series count, largest first.
type rows struct {
	values []string
	// total is an upper value of the series the values count over window: a bounds range's upper total, or the heads' in-memory total.
	total  int64
	window window
}

// readRows reads up to limit values of groupBy with the most series from the configured source, calling next before each request.
func readRows(ctx context.Context, c *client, cfg targetConfig, tw targetWindows, groupBy string, limit int, logger log.Logger, next func()) (rows, error) {
	if cfg.source == targetSourceLabelValues {
		next()
		values, total, err := c.labelValuesCardinality(ctx, groupBy, min(limit, cfg.labelValuesLimit))
		return rows{values: values, total: total, window: tw.recent}, err
	}
	if cfg.source != targetSourceHead {
		next()
		res, latency, err := c.bounds(ctx, groupBy, tw.blocks.start, tw.blocks.end, limit)
		if err != nil {
			return rows{}, err
		}
		logBoundsAnswer(logger, queryTargets, tw.blocks, res, latency)
		// The last range with groups is the newest one the store-gateways answered.
		for i := len(res.Ranges) - 1; i >= 0; i-- {
			rng := res.Ranges[i]
			if len(rng.Groups) == 0 {
				continue
			}
			out := rows{total: rng.UpperTotal, window: window{kind: windowBlocks, start: rng.Start, end: rng.End}}
			for _, g := range rng.Groups {
				out.values = append(out.values, g.Value)
			}
			return out, nil
		}
	}
	next()
	res, latency, err := c.bounds(ctx, groupBy, tw.recent.start, tw.recent.end, limit)
	if err != nil {
		return rows{}, err
	}
	logBoundsAnswer(logger, queryTargets, tw.recent, res, latency)
	if res.Head == nil {
		return rows{}, fmt.Errorf("the window [%d, %d) doesn't reach the ingesters", tw.recent.start, tw.recent.end)
	}
	out := rows{total: res.Head.InMemoryTotal, window: tw.recent}
	for _, g := range res.Head.Groups {
		out.values = append(out.values, g.Value)
	}
	return out, nil
}

// namesByRank returns the values at the given 1-based ranks, skipping a rank past the end, or the smallest value when none fits.
func namesByRank(values []string, ranks []int) []string {
	var out []string
	for _, r := range ranks {
		if r >= 1 && r <= len(values) && !slices.Contains(out, values[r-1]) {
			out = append(out, values[r-1])
		}
	}
	if len(out) == 0 && len(values) > 0 {
		out = append(out, values[len(values)-1])
	}
	return out
}

// facetQueries are the queries that filter on a name's facet.
var facetQueries = []string{queryMetricNotEqual, queryMetricRegex, queryTwoMatchers}

// allSeriesMatchers select every series of the tenant.
func allSeriesMatchers() []*labels.Matcher {
	return []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, ".+")}
}

var allSeries = selector(allSeriesMatchers()...)

// pickTargets reads the tenant's metric names at cfg.ranks and highest-cardinality labels, calling pace before each request after the first. The names come from one source row set, not from the tenant-wide exact table, which a large tenant is refused.
// A name whose facet can't be picked gets none, and its matcher queries are skipped.
func pickTargets(ctx context.Context, c *client, cfg targetConfig, tw targetWindows, logger log.Logger, pace func()) (*targets, error) {
	t := &targets{labels: map[string][]string{}, facets: map[string]facet{}}
	sent := false
	next := func() {
		if sent {
			pace()
		}
		sent = true
	}
	byName, err := readRows(ctx, c, cfg, tw, model.MetricNameLabel, slices.Max(cfg.ranks), logger, next)
	if err != nil {
		return nil, fmt.Errorf("largest metric names: %w", err)
	}
	t.tenantSeries = byName.total
	t.names = namesByRank(byName.values, cfg.ranks)
	if len(t.names) == 0 {
		return nil, fmt.Errorf("the tenant has no series in [%d, %d)", byName.window.start, byName.window.end)
	}
	if skipped := len(cfg.ranks) - len(t.names); skipped > 0 {
		level.Info(logger).Log("msg", "fewer metric names than the ranks asked for", "names", len(byName.values), "ranks", fmt.Sprint(cfg.ranks))
	}
	for _, name := range t.names {
		next()
		ls, err := c.labelNames(ctx, metricSelector(name), cfg.labels+1)
		if err != nil {
			return nil, fmt.Errorf("label names of %q: %w", name, err)
		}
		t.labels[name] = withoutName(ls)
		if !cfg.facets {
			continue
		}
		if f, ok, err := pickFacet(ctx, c, name, t.labels[name], byName.window, next); err != nil {
			class := classifyRefusal(sideEstimate, err)
			t.facetRefusals = append(t.facetRefusals, class)
			level.Warn(logger).Log("msg", "picking a facet failed; skipping the matcher queries of this name", "name", name, "class", class, "err", err)
		} else if ok {
			t.facets[name] = f
		}
	}
	next()
	ls, err := c.labelNames(ctx, "", cfg.labels+1)
	if err != nil {
		return nil, fmt.Errorf("tenant label names: %w", err)
	}
	t.tenantLabels = withoutName(ls)
	byValue, err := readRows(ctx, c, cfg, tw, cfg.matcherLabel, 3, logger, next)
	if err != nil {
		return nil, fmt.Errorf("largest %s values: %w", cfg.matcherLabel, err)
	}
	for _, v := range byValue.values {
		if v != "" {
			t.matcherValues = append(t.matcherValues, v)
		}
	}
	return t, nil
}

// facetValues is how many of a facet label's values the regex and equality matchers draw from.
const facetValues = 3

// pickFacet picks the first of the name's labels that some of its series lack, or its first label if they all have each, with the label's largest values. It calls next before each request.
func pickFacet(ctx context.Context, c *client, name string, ls []string, w window, next func()) (facet, bool, error) {
	if len(ls) == 0 {
		return facet{}, false, nil
	}
	nameMatcher := labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, name)
	f := facet{label: ls[0]}
	for _, l := range ls {
		next()
		lacking, _, err := c.estimate(ctx, selector(nameMatcher, labels.MustNewMatcher(labels.MatchEqual, l, "")), model.MetricNameLabel, w.start, w.end, 1)
		if err != nil {
			return facet{}, false, fmt.Errorf("series of %q without %s: %w", name, l, err)
		}
		if lacking.Total > 0 {
			f.label, f.partial = l, true
			break
		}
	}
	next()
	byValue, _, err := c.estimate(ctx, selector(nameMatcher), f.label, w.start, w.end, facetValues+1)
	if err != nil {
		return facet{}, false, fmt.Errorf("largest %s values of %q: %w", f.label, name, err)
	}
	for _, g := range byValue.Groups {
		if g.Value != "" && len(f.values) < facetValues {
			f.values = append(f.values, g.Value)
		}
	}
	return f, len(f.values) > 0, nil
}

// namePrefix is the name up to and including its first underscore, or the whole name without one.
func namePrefix(name string) string {
	if i := strings.IndexByte(name, '_'); i >= 0 {
		return name[:i+1]
	}
	return name
}

// valuesRegex matches exactly the given values.
func valuesRegex(vs []string) string {
	quoted := make([]string, len(vs))
	for i, v := range vs {
		quoted[i] = regexp.QuoteMeta(v)
	}
	return strings.Join(quoted, "|")
}

func withoutName(ls []string) []string {
	out := ls[:0]
	for _, l := range ls {
		if l != model.MetricNameLabel {
			out = append(out, l)
		}
	}
	return out
}

// pair is one estimate and its PromQL truth over the same selector and window.
type pair struct {
	query    string
	matchers []*labels.Matcher
	groupBy  string
	window   window
	// limit caps the estimate's table; groups it leaves out are compared through group_count and total.
	limit int
	// perValue skips the grouped truth and counts the estimate's largest groups one at a time.
	perValue bool
}

func (p pair) selector() string { return selector(p.matchers...) }

func (p pair) String() string {
	return fmt.Sprintf("%s %s by %s over %s %s", p.query, p.selector(), p.groupBy, p.window.kind, model.Duration(p.window.length()))
}

type pairConfig struct {
	matcherLabel string
	// tableLimit caps the tenant-wide tables.
	tableLimit int
	// maxReferenceSeries, when > 0 and below the tenant's series, makes the tenant-wide tables skip their grouped truth.
	maxReferenceSeries int64
}

// candidates fills every enabled query with targets, on every window.
func candidates(queries []string, t *targets, windows []window, cfg pairConfig, rng *rand.Rand) []pair {
	pick := func(s []string) string {
		if len(s) == 0 {
			return ""
		}
		return s[rng.IntN(len(s))]
	}
	tenantPerValue := cfg.maxReferenceSeries > 0 && t.tenantSeries > cfg.maxReferenceSeries
	var out []pair
	for _, w := range windows {
		name := pick(t.names)
		nameMatcher := labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, name)
		for _, q := range queries {
			p := pair{query: q, window: w}
			switch q {
			case queryTenantByName:
				p.matchers, p.groupBy, p.limit, p.perValue = allSeriesMatchers(), model.MetricNameLabel, cfg.tableLimit, tenantPerValue
			case queryMetricByLabel:
				l := pick(t.labels[name])
				if l == "" {
					continue
				}
				p.matchers, p.groupBy = []*labels.Matcher{nameMatcher}, l
			case queryMatcherByName:
				v := pick(t.matcherValues)
				if v == "" {
					continue
				}
				p.matchers, p.groupBy = []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, cfg.matcherLabel, v)}, model.MetricNameLabel
			case queryMetricTotal:
				p.matchers, p.groupBy = []*labels.Matcher{nameMatcher}, model.MetricNameLabel
			case queryTenantByLabel:
				p.matchers, p.groupBy, p.limit, p.perValue = allSeriesMatchers(), cfg.matcherLabel, cfg.tableLimit, tenantPerValue
			case queryTenantLabelVals:
				if len(t.tenantLabels) == 0 {
					continue
				}
				p.matchers, p.groupBy, p.limit, p.perValue = allSeriesMatchers(), t.tenantLabels[0], cfg.tableLimit, tenantPerValue
			case queryNameRegex:
				p.matchers, p.groupBy, p.limit = []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, regexp.QuoteMeta(namePrefix(name))+".*")}, model.MetricNameLabel, cfg.tableLimit
			case queryMetricNotEqual, queryMetricRegex, queryTwoMatchers:
				f, ok := t.facets[name]
				if !ok {
					continue
				}
				switch q {
				case queryMetricNotEqual:
					p.matchers, p.groupBy = []*labels.Matcher{nameMatcher, labels.MustNewMatcher(labels.MatchNotEqual, f.label, f.values[0])}, f.label
				case queryMetricRegex:
					p.matchers, p.groupBy = []*labels.Matcher{nameMatcher, labels.MustNewMatcher(labels.MatchRegexp, f.label, valuesRegex(f.values[:min(2, len(f.values))]))}, f.label
				case queryTwoMatchers:
					// Grouped by another of the name's labels, or by name when it has no other.
					p.groupBy = model.MetricNameLabel
					for _, l := range t.labels[name] {
						if l != f.label {
							p.groupBy = l
							break
						}
					}
					p.matchers = []*labels.Matcher{nameMatcher, labels.MustNewMatcher(labels.MatchEqual, f.label, pick(f.values))}
				}
			default:
				continue
			}
			out = append(out, p)
		}
	}
	return out
}

func selector(ms ...*labels.Matcher) string {
	parts := make([]string, len(ms))
	for i, m := range ms {
		parts[i] = m.String()
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

func metricSelector(name string) string {
	return selector(labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, name))
}

// truthQuery counts by label the series matching matchers that have a sample in the window of length w ending at the query time.
func truthQuery(matchers []*labels.Matcher, label string, w time.Duration) string {
	return fmt.Sprintf("count by (%s) (last_over_time(%s[%s]))", byLabel(label), selector(matchers...), model.Duration(w))
}

// edgeQuery counts by label the series matching matchers that have a sample in the slack before the window of length w ending at the query time, and none in the window. last_over_time keeps every label, so unless matches each series with itself.
func edgeQuery(matchers []*labels.Matcher, label string, w, slack time.Duration) string {
	sel := selector(matchers...)
	return fmt.Sprintf("count by (%s) (last_over_time(%s[%s] offset %s) unless last_over_time(%s[%s]))", byLabel(label), sel, model.Duration(slack), model.Duration(w), sel, model.Duration(w))
}

func byLabel(label string) string {
	if !model.LegacyValidation.IsValidLabelName(label) {
		return strconv.Quote(label)
	}
	return label
}
