// SPDX-License-Identifier: AGPL-3.0-only

// Package nativemetadataprober probes the native metadata prototype on a cell's experiment tenants: it runs info()
// queries with native target_info and with the target_info join, and counts where they differ and where native wasn't
// native, and it checks that the native metadata lookups answer.
package nativemetadataprober

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
)

// Mode is how a query finds target_info for info().
type Mode string

const (
	// ModeNative synthesizes target_info from the ingesters' native resources.
	ModeNative Mode = "native"
	// ModeJoin reads the stored target_info series.
	ModeJoin Mode = "join"
)

// Sample is a series of an instant query's answer. Two answers of one Client compare by it: Value is a form of the
// value in which equal values are equal.
type Sample struct {
	Labels labels.Labels
	Value  string
}

// QueryResult is an instant query's answer, with the native metadata prototype's stats of it.
type QueryResult struct {
	Samples []Sample
	// The bytes of the ingesters' answers to native lookups: none means info() didn't look target_info up natively.
	LookupResponseBytes uint64
	// The selects of target_info that fell back to the join.
	TargetInfoFallbacks uint64
	// The inner series info()'s own mode enriched from their own resources, and by the join.
	OwnSeries, JoinSeries uint64
}

// Lookup is one of the native metadata lookup endpoints.
type Lookup string

const (
	LookupResources      Lookup = "resources"
	LookupMetricMetadata Lookup = "native_metric_metadata"
)

// LookupResult is a lookup's answer.
type LookupResult struct {
	Series int
	// Whether more series matched than the lookup's limit let through.
	Limited bool
}

// Client queries a tenant.
type Client interface {
	// Query evaluates query at ts, with target_info found as mode says.
	Query(ctx context.Context, tenant, query string, ts time.Time, mode Mode) (QueryResult, error)
	// Lookup looks up the native metadata of the series matching any of matchers over [start, end].
	Lookup(ctx context.Context, tenant string, lookup Lookup, matchers []string, start, end time.Time) (LookupResult, error)
}

// Query is a probe's info() query. Its name labels the probe's metrics.
type Query struct {
	Name, Expr string
}

// DefaultQueries are info() over a plain selector, over rate(), with a data label matcher, and with target_info named
// explicitly, on the series every Prometheus scrape writes.
var DefaultQueries = []Query{
	{Name: "plain", Expr: `info(up)`},
	{Name: "rate", Expr: `info(rate(scrape_samples_scraped[5m]))`},
	{Name: "data_label", Expr: `info(up, {k8s_namespace_name=~".+"})`},
	{Name: "target_info", Expr: `info(up, {__name__="target_info"})`},
}

// Config is what a Prober probes, and how often.
type Config struct {
	Tenants flagext.StringSliceCSV
	Queries []Query
	// The lookups' match[], and how far back they look.
	LookupMatchers flagext.StringSlice
	LookupRange    time.Duration
	// How far before a round its queries are evaluated, so that both modes see the same samples, though the modes
	// answer one after the other. Samples can arrive more than a scrape interval late: a stopped scrape writes its
	// staleness markers an interval and 10% after their timestamp, and a marker arriving between the two answers
	// drops its series from the second only.
	EvaluationDelay time.Duration
	Interval        time.Duration
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.Var(&cfg.Tenants, "tenants", "Tenants to probe, comma separated.")
	cfg.Queries = DefaultQueries
	f.Var(&queriesFlag{queries: &cfg.Queries}, "query", "An info() query to compare natively and through the join, as name=expression, which can be repeated. The name labels its metrics. Without it, the default queries: info() over up, over a rate(), with a data label matcher, and with target_info named.")
	cfg.LookupMatchers = flagext.StringSlice{`{__name__="up"}`}
	f.Var(&cfg.LookupMatchers, "lookup-match", "A series selector of the native metadata lookups, their match[], which can be repeated.")
	f.DurationVar(&cfg.LookupRange, "lookup-range", time.Hour, "How far back the resources lookup looks.")
	f.DurationVar(&cfg.EvaluationDelay, "evaluation-delay", 3*time.Minute, "How far before a round its queries are evaluated, so that both modes see the same samples. It must be longer than samples take to arrive, which for staleness markers is over a scrape interval.")
	f.DurationVar(&cfg.Interval, "interval", time.Minute, "How often to probe.")
}

func (cfg *Config) Validate() error {
	if len(cfg.Tenants) == 0 {
		return errors.New("no tenants to probe")
	}
	if cfg.Interval <= 0 {
		return errors.New("the interval must be positive")
	}
	names := map[string]bool{}
	for _, q := range cfg.Queries {
		if names[q.Name] {
			return fmt.Errorf("two queries are named %q", q.Name)
		}
		names[q.Name] = true
	}
	return nil
}

// queriesFlag is a repeatable -query name=expression flag, whose first value replaces the default queries.
type queriesFlag struct {
	queries *[]Query
	set     bool
}

func (f *queriesFlag) String() string {
	if f.queries == nil {
		return ""
	}
	parts := make([]string, 0, len(*f.queries))
	for _, q := range *f.queries {
		parts = append(parts, q.Name+"="+q.Expr)
	}
	return strings.Join(parts, " ")
}

func (f *queriesFlag) Set(value string) error {
	name, expr, ok := strings.Cut(value, "=")
	if !ok || name == "" || expr == "" {
		return fmt.Errorf("%q isn't name=expression", value)
	}
	if !f.set {
		*f.queries = nil
		f.set = true
	}
	*f.queries = append(*f.queries, Query{Name: name, Expr: expr})
	return nil
}

// Prober runs rounds of probes.
type Prober struct {
	cfg     Config
	client  Client
	logger  log.Logger
	metrics *metrics
}

func NewProber(cfg Config, client Client, logger log.Logger, reg prometheus.Registerer) *Prober {
	return &Prober{cfg: cfg, client: client, logger: logger, metrics: newMetrics(reg)}
}

// Run runs a round every interval until ctx is done.
func (p *Prober) Run(ctx context.Context) {
	ticker := time.NewTicker(p.cfg.Interval)
	defer ticker.Stop()
	for {
		p.RunOnce(ctx, time.Now())
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// RunOnce probes every tenant: each query in both modes, and each lookup.
func (p *Prober) RunOnce(ctx context.Context, now time.Time) {
	for _, tenant := range p.cfg.Tenants {
		for _, q := range p.cfg.Queries {
			p.probeQuery(ctx, tenant, q, now.Add(-p.cfg.EvaluationDelay))
		}
		for _, lookup := range []Lookup{LookupResources, LookupMetricMetadata} {
			p.probeLookup(ctx, tenant, lookup, now.Add(-p.cfg.LookupRange), now)
		}
	}
}

// Reasons a native answer wasn't native.
const (
	notNativeFallback = "fallback"
	notNativeNoLookup = "no_lookup"
	// info()'s own mode enriched no inner series from its own resource.
	notNativeOwnModeJoin = "own_mode_join"
)

// Kinds of answers of info()'s own mode: every inner series enriched from its own resource, or some by the join.
const (
	ownAnswer   = "own"
	mixedAnswer = "mixed"
)

// How many of a mismatch's differences are logged.
const loggedDifferences = 5

func (p *Prober) probeQuery(ctx context.Context, tenant string, q Query, ts time.Time) {
	native, nativeErr := p.query(ctx, tenant, q, ts, ModeNative)
	join, joinErr := p.query(ctx, tenant, q, ts, ModeJoin)
	if nativeErr != nil || joinErr != nil {
		return
	}
	p.metrics.queryProbes.WithLabelValues(tenant, q.Name).Inc()
	p.metrics.lookupResponseBytes.WithLabelValues(tenant, q.Name).Add(float64(native.LookupResponseBytes))

	if len(native.Samples) == 0 && len(join.Samples) == 0 {
		// Nothing to enrich, which info() needn't look up for: the query doesn't fit the tenant's series.
		p.metrics.queryEmpty.WithLabelValues(tenant, q.Name).Inc()
		return
	}
	switch {
	case native.TargetInfoFallbacks > 0:
		p.metrics.queryNotNative.WithLabelValues(tenant, q.Name, notNativeFallback).Inc()
	case native.OwnSeries > 0 && native.JoinSeries > 0:
		p.metrics.queryOwn.WithLabelValues(tenant, q.Name, mixedAnswer).Inc()
	case native.OwnSeries > 0:
		p.metrics.queryOwn.WithLabelValues(tenant, q.Name, ownAnswer).Inc()
	case native.JoinSeries > 0:
		// Own mode needn't look up anything, for an inner expression whose series aren't stored ones.
		p.metrics.queryNotNative.WithLabelValues(tenant, q.Name, notNativeOwnModeJoin).Inc()
	case native.LookupResponseBytes == 0:
		// With MQE's fallback on, a query MQE doesn't support runs on Prometheus' engine, whose info() is the join.
		p.metrics.queryNotNative.WithLabelValues(tenant, q.Name, notNativeNoLookup).Inc()
	}

	differences := compare(native.Samples, join.Samples)
	if len(differences) == 0 {
		return
	}
	p.metrics.queryMismatches.WithLabelValues(tenant, q.Name).Inc()
	p.metrics.queryMismatchedSeries.WithLabelValues(tenant, q.Name).Add(float64(len(differences)))
	if native.OwnSeries > 0 {
		// Own resources differ from the join where a series' own resource isn't its target's, a class of its own.
		p.metrics.queryOwnMismatches.WithLabelValues(tenant, q.Name).Inc()
	}
	logged := differences[:min(len(differences), loggedDifferences)]
	level.Warn(p.logger).Log("msg", "native info() and the join differ", "tenant", tenant, "query", q.Name, "time", ts.UTC().Format(time.RFC3339), "series", len(differences), "differences", strings.Join(logged, "; "))
}

func (p *Prober) query(ctx context.Context, tenant string, q Query, ts time.Time, mode Mode) (QueryResult, error) {
	start := time.Now()
	res, err := p.client.Query(ctx, tenant, q.Expr, ts, mode)
	p.metrics.queryDuration.WithLabelValues(tenant, string(mode)).Observe(time.Since(start).Seconds())
	if err != nil {
		p.metrics.queryFailures.WithLabelValues(tenant, q.Name, string(mode)).Inc()
		level.Warn(p.logger).Log("msg", "query failed", "tenant", tenant, "query", q.Name, "mode", mode, "err", err)
	}
	return res, err
}

func (p *Prober) probeLookup(ctx context.Context, tenant string, lookup Lookup, start, end time.Time) {
	p.metrics.lookups.WithLabelValues(tenant, string(lookup)).Inc()
	begin := time.Now()
	res, err := p.client.Lookup(ctx, tenant, lookup, p.cfg.LookupMatchers, start, end)
	p.metrics.lookupDuration.WithLabelValues(tenant, string(lookup)).Observe(time.Since(begin).Seconds())
	if err != nil {
		p.metrics.lookupFailures.WithLabelValues(tenant, string(lookup)).Inc()
		level.Warn(p.logger).Log("msg", "lookup failed", "tenant", tenant, "lookup", lookup, "err", err)
		return
	}
	if res.Limited {
		p.metrics.lookupLimited.WithLabelValues(tenant, string(lookup)).Inc()
	}
	p.metrics.lookupSeries.WithLabelValues(tenant, string(lookup)).Set(float64(res.Series))
}

// compare describes each series whose value differs between the native answer and the join's, or that only one has.
func compare(native, join []Sample) []string {
	joinValues := make(map[string]string, len(join))
	for _, s := range join {
		joinValues[s.Labels.String()] = s.Value
	}
	var differences []string
	for _, s := range native {
		key := s.Labels.String()
		joinValue, ok := joinValues[key]
		switch {
		case !ok:
			differences = append(differences, fmt.Sprintf("only native: %s %s", key, s.Value))
		case joinValue != s.Value:
			differences = append(differences, fmt.Sprintf("native %s, join %s: %s", s.Value, joinValue, key))
		}
		delete(joinValues, key)
	}
	for _, s := range join {
		if _, ok := joinValues[s.Labels.String()]; ok {
			differences = append(differences, fmt.Sprintf("only join: %s %s", s.Labels.String(), s.Value))
		}
	}
	return differences
}

type metrics struct {
	queryProbes           *prometheus.CounterVec
	queryFailures         *prometheus.CounterVec
	queryEmpty            *prometheus.CounterVec
	queryNotNative        *prometheus.CounterVec
	queryOwn              *prometheus.CounterVec
	queryMismatches       *prometheus.CounterVec
	queryOwnMismatches    *prometheus.CounterVec
	queryMismatchedSeries *prometheus.CounterVec
	queryDuration         *prometheus.HistogramVec
	lookupResponseBytes   *prometheus.CounterVec

	lookups        *prometheus.CounterVec
	lookupFailures *prometheus.CounterVec
	lookupLimited  *prometheus.CounterVec
	lookupSeries   *prometheus.GaugeVec
	lookupDuration *prometheus.HistogramVec
}

func newMetrics(reg prometheus.Registerer) *metrics {
	duration := func(name, help string, labelNames ...string) *prometheus.HistogramVec {
		return promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:                            name,
			Help:                            help,
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: time.Hour,
		}, labelNames)
	}
	return &metrics{
		queryProbes: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "native_metadata_prober_query_probes_total",
			Help: "Queries answered in both modes and compared.",
		}, []string{"tenant", "query"}),
		queryFailures: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "native_metadata_prober_query_failures_total",
			Help: "Queries that failed, by mode.",
		}, []string{"tenant", "query", "mode"}),
		queryEmpty: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "native_metadata_prober_query_empty_total",
			Help: "Compared queries that both modes answered with no series: the query doesn't fit the tenant's series.",
		}, []string{"tenant", "query"}),
		queryNotNative: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "native_metadata_prober_query_not_native_total",
			Help: "Compared queries whose native answer was the join's, because native target_info fell back or no lookup was made.",
		}, []string{"tenant", "query", "reason"}),
		queryMismatches: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "native_metadata_prober_query_mismatches_total",
			Help: "Compared queries whose native answer differed from the join's.",
		}, []string{"tenant", "query"}),
		queryOwn: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "native_metadata_prober_query_own_total",
			Help: "Compared queries whose native answer info()'s own mode gave, by kind: own, every inner series enriched from its own native resource, or mixed, some by the join.",
		}, []string{"tenant", "query", "kind"}),
		queryOwnMismatches: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "native_metadata_prober_query_own_mismatches_total",
			Help: "Compared queries whose native answer from info()'s own mode differed from the join's.",
		}, []string{"tenant", "query"}),
		queryMismatchedSeries: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "native_metadata_prober_query_mismatched_series_total",
			Help: "Series that differed between the native answer and the join's, or that only one had.",
		}, []string{"tenant", "query"}),
		queryDuration: duration("native_metadata_prober_query_duration_seconds", "Duration of the queries, by mode.", "tenant", "mode"),
		lookupResponseBytes: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "native_metadata_prober_query_lookup_response_bytes_total",
			Help: "Bytes of the ingesters' answers to the native queries' lookups.",
		}, []string{"tenant", "query"}),

		lookups: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "native_metadata_prober_lookups_total",
			Help: "Native metadata lookups made.",
		}, []string{"tenant", "lookup"}),
		lookupFailures: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "native_metadata_prober_lookup_failures_total",
			Help: "Native metadata lookups that failed.",
		}, []string{"tenant", "lookup"}),
		lookupLimited: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "native_metadata_prober_lookup_limited_total",
			Help: "Native metadata lookups whose answer the limit cut short.",
		}, []string{"tenant", "lookup"}),
		lookupSeries: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "native_metadata_prober_lookup_series",
			Help: "Series in the last answer to the native metadata lookup.",
		}, []string{"tenant", "lookup"}),
		lookupDuration: duration("native_metadata_prober_lookup_duration_seconds", "Duration of the native metadata lookups.", "tenant", "lookup"),
	}
}
