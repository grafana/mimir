// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
)

// Query names: the request shapes the estimate takes, one match[] and one group_by each.
const (
	queryTenantByName    = "tenant_by_name"    // series per metric name across the tenant
	queryMetricByLabel   = "metric_by_label"   // series per value of one of a metric's high-cardinality labels, and its distinct values
	queryMatcherByName   = "matcher_by_name"   // series per metric name under an arbitrary matcher
	queryMetricTotal     = "metric_total"      // series of one metric
	queryMetricSteps     = "metric_steps"      // series of one metric per step
	queryTenantByLabel   = "tenant_by_label"   // series per value of one label across every metric
	queryTenantLabelVals = "tenant_label_vals" // distinct values of the tenant's highest-cardinality label
)

var allQueries = []string{queryTenantByName, queryMetricByLabel, queryMatcherByName, queryMetricTotal, queryMetricSteps, queryTenantByLabel, queryTenantLabelVals}

// Window kinds: which components answer.
const (
	windowBlocks    = "blocks"    // ends before now minus query-ingesters-within: store-gateways only
	windowIngesters = "ingesters" // inside query-ingesters-within
	windowBoundary  = "boundary"  // across the boundary between the two
)

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

// windows returns every window kind for every length that fits. Edges are whole minutes.
func (c windowConfig) windows(now time.Time) []window {
	ms := func(t time.Time) int64 { return t.Truncate(time.Minute).UnixMilli() }
	boundary := now.Add(-c.queryIngestersWithin)
	latest := now.Add(-c.lag)
	var out []window
	for _, l := range c.lengths {
		end := ms(boundary.Add(-c.blocksMargin))
		out = append(out, window{windowBlocks, end - l.Milliseconds(), end})

		end = ms(latest)
		if start := end - l.Milliseconds(); start > boundary.Add(c.lag).UnixMilli() {
			out = append(out, window{windowIngesters, start, end})
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
}

type targetConfig struct {
	names, labels int
	matcherLabel  string
}

// allSeriesMatchers select every series of the tenant.
func allSeriesMatchers() []*labels.Matcher {
	return []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, ".+")}
}

var allSeries = selector(allSeriesMatchers()...)

// pickTargets reads the tenant's largest metric names and highest-cardinality labels over w.
func pickTargets(ctx context.Context, c *client, cfg targetConfig, w window) (*targets, error) {
	t := &targets{labels: map[string][]string{}}
	byName, _, err := c.estimate(ctx, allSeries, model.MetricNameLabel, w.start, w.end, 0, cfg.names)
	if err != nil {
		return nil, fmt.Errorf("largest metric names: %w", err)
	}
	for _, g := range byName.Groups {
		t.names = append(t.names, g.Value)
	}
	if len(t.names) == 0 {
		return nil, fmt.Errorf("the tenant has no series in [%d, %d)", w.start, w.end)
	}
	for _, name := range t.names {
		ls, err := c.labelNames(ctx, metricSelector(name), cfg.labels+1)
		if err != nil {
			return nil, fmt.Errorf("label names of %q: %w", name, err)
		}
		t.labels[name] = withoutName(ls)
	}
	ls, err := c.labelNames(ctx, "", cfg.labels+1)
	if err != nil {
		return nil, fmt.Errorf("tenant label names: %w", err)
	}
	t.tenantLabels = withoutName(ls)
	byValue, _, err := c.estimate(ctx, allSeries, cfg.matcherLabel, w.start, w.end, 0, 3)
	if err != nil {
		return nil, fmt.Errorf("largest %s values: %w", cfg.matcherLabel, err)
	}
	for _, g := range byValue.Groups {
		if g.Value != "" {
			t.matcherValues = append(t.matcherValues, g.Value)
		}
	}
	return t, nil
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
	// steps, when > 0, cuts the window into that many buckets, each compared on its own.
	steps int
	// limit caps the estimate's table; groups it leaves out are compared through group_count and total.
	limit int
}

func (p pair) selector() string { return selector(p.matchers...) }

func (p pair) step() time.Duration {
	if p.steps == 0 {
		return 0
	}
	return p.window.length() / time.Duration(p.steps)
}

func (p pair) String() string {
	s := fmt.Sprintf("%s %s by %s over %s %s", p.query, p.selector(), p.groupBy, p.window.kind, model.Duration(p.window.length()))
	if p.steps > 0 {
		s += fmt.Sprintf(" step %s", model.Duration(p.step()))
	}
	return s
}

type pairConfig struct {
	matcherLabel string
	steps        int
	// tableLimit caps the tenant-wide tables.
	tableLimit int
}

// candidates fills every enabled query with targets, on every window.
func candidates(queries []string, t *targets, windows []window, cfg pairConfig, rng *rand.Rand) []pair {
	pick := func(s []string) string {
		if len(s) == 0 {
			return ""
		}
		return s[rng.IntN(len(s))]
	}
	var out []pair
	for _, w := range windows {
		name := pick(t.names)
		nameMatcher := labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, name)
		for _, q := range queries {
			p := pair{query: q, window: w}
			switch q {
			case queryTenantByName:
				p.matchers, p.groupBy, p.limit = allSeriesMatchers(), model.MetricNameLabel, cfg.tableLimit
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
			case queryMetricSteps:
				if cfg.steps <= 0 || w.length()%time.Duration(cfg.steps) != 0 || (w.length()/time.Duration(cfg.steps))%time.Second != 0 {
					continue
				}
				p.matchers, p.groupBy, p.steps = []*labels.Matcher{nameMatcher}, model.MetricNameLabel, cfg.steps
			case queryTenantByLabel:
				p.matchers, p.groupBy, p.limit = allSeriesMatchers(), cfg.matcherLabel, cfg.tableLimit
			case queryTenantLabelVals:
				if len(t.tenantLabels) == 0 {
					continue
				}
				p.matchers, p.groupBy, p.limit = allSeriesMatchers(), t.tenantLabels[0], cfg.tableLimit
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
	by := label
	if !model.LegacyValidation.IsValidLabelName(label) {
		by = strconv.Quote(label)
	}
	return fmt.Sprintf("count by (%s) (last_over_time(%s[%s]))", by, selector(matchers...), model.Duration(w))
}
