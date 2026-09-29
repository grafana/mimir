// SPDX-License-Identifier: AGPL-3.0-only

// query-load sends a steady, ingester-heavy mix of queries to a query-frontend: range and instant
// queries, with label regexes and over classic and native histograms, plus label lookups, all
// inside the window queriers serve from ingesters. It queries the metrics with the most series in
// ingesters, within bounds, and drops metrics that Adaptive Metrics aggregates, which only answer
// aggregated queries from a few series.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type stringList []string

func (s *stringList) String() string     { return strings.Join(*s, ",") }
func (s *stringList) Set(v string) error { *s = append(*s, v); return nil }

type config struct {
	address            string
	tenants            stringList
	clusterLabel       string
	concurrency        int
	pause              time.Duration
	maxRange           time.Duration
	metricsSample      int
	minSeries          int
	maxSeries          int
	maxHistogramSeries int
	points             int
	refreshInterval    time.Duration
	reportInterval     time.Duration
	timeout            time.Duration
}

type result struct {
	kind    string
	latency time.Duration
	err     error
}

func main() {
	var cfg config
	flag.StringVar(&cfg.address, "address", "http://query-frontend.mimir-dev-10.svc.cluster.local.:80/prometheus", "Query-frontend Prometheus API prefix")
	flag.Var(&cfg.tenants, "tenant", "Tenant to query (repeatable)")
	flag.StringVar(&cfg.clusterLabel, "cluster-label", "", "Cluster validation label sent as X-Cluster")
	flag.IntVar(&cfg.concurrency, "concurrency", 4, "Concurrent query loops")
	flag.DurationVar(&cfg.pause, "pause", time.Second, "Pause between queries in each loop")
	flag.DurationVar(&cfg.maxRange, "max-range", 6*time.Hour, "Longest range query; keep it within -querier.query-ingesters-within so ingesters serve it")
	flag.IntVar(&cfg.metricsSample, "metrics-per-tenant", 200, "Metric names sampled per tenant")
	flag.IntVar(&cfg.minSeries, "min-series", 1000, "Fewest series in ingesters of a queried metric")
	flag.IntVar(&cfg.maxSeries, "max-series", 200_000, "Most series in ingesters of a queried metric, so queriers don't fetch too much")
	flag.IntVar(&cfg.maxHistogramSeries, "max-histogram-series", 50_000, "Most series of a queried histogram, whose samples are much larger")
	flag.IntVar(&cfg.points, "points", 60, "Points of a range query: ingesters read every chunk of the range whatever the step, queriers evaluate each point")
	flag.DurationVar(&cfg.refreshInterval, "refresh-interval", 10*time.Minute, "How often to resample metric names")
	flag.DurationVar(&cfg.reportInterval, "report-interval", time.Minute, "How often to print latency and error summaries")
	flag.DurationVar(&cfg.timeout, "timeout", 2*time.Minute, "Per-request timeout")
	flag.Parse()
	if len(cfg.tenants) == 0 {
		fmt.Fprintln(os.Stderr, "at least one -tenant is required")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg config) error {
	client := &http.Client{Timeout: cfg.timeout}
	metrics := newMetrics()
	refresh := func() {
		for _, tenant := range cfg.tenants {
			sample, err := sampleMetricNames(ctx, client, cfg, tenant)
			if err != nil {
				fmt.Fprintf(os.Stderr, "tenant=%s metric name refresh failed: %v\n", tenant, err)
				continue
			}
			histograms, err := histogramFamilies(ctx, client, cfg, tenant)
			if err != nil {
				fmt.Fprintf(os.Stderr, "tenant=%s metadata refresh failed: %v\n", tenant, err)
			}
			metrics.set(tenant, classify(sample, histograms, cfg.maxHistogramSeries))
		}
	}
	refresh()
	results := make(chan result, 1024)
	var wg sync.WaitGroup
	for range cfg.concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			loop(ctx, client, cfg, metrics, results)
		}()
	}
	go func() {
		wg.Wait()
		close(results)
	}()
	refreshTicker := time.NewTicker(cfg.refreshInterval)
	defer refreshTicker.Stop()
	reportTicker := time.NewTicker(cfg.reportInterval)
	defer reportTicker.Stop()
	window := map[string][]result{}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-refreshTicker.C:
			go refresh()
		case <-reportTicker.C:
			report(window)
			window = map[string][]result{}
		case r, ok := <-results:
			if !ok {
				return ctx.Err()
			}
			window[r.kind] = append(window[r.kind], r)
		}
	}
}

func loop(ctx context.Context, client *http.Client, cfg config, metrics *metrics, results chan<- result) {
	for ctx.Err() == nil {
		tenant := cfg.tenants[rand.IntN(len(cfg.tenants))]
		m, ok := metrics.pick(tenant)
		if !ok {
			sleep(ctx, cfg.pause)
			continue
		}
		kind, path, params := nextQuery(cfg, m, metrics.jobs(tenant, m.name), metrics.peers(tenant, 2), time.Now())
		started := time.Now()
		var err error
		if kind == "label_values" {
			// Its answer gives later queries job values for their label regexes.
			var jobs []string
			jobs, err = getLabelValues(ctx, client, cfg, tenant, path, params)
			if err == nil {
				metrics.setJobs(tenant, m.name, jobs)
			}
		} else {
			err = get(ctx, client, cfg, tenant, path, params)
		}
		if ctx.Err() != nil {
			return
		}
		if err != nil && strings.Contains(err.Error(), "Can't query aggregated metric") {
			metrics.remove(tenant, m.name)
		}
		results <- result{kind: kind, latency: time.Since(started), err: err}
		sleep(ctx, cfg.pause)
	}
}

type metricKind int

const (
	plain metricKind = iota
	// Its series are the family's _bucket, _count and _sum.
	classicHistogram
	nativeHistogram
)

type metric struct {
	name string
	kind metricKind
}

// A metric name and its series in ingesters, from the cardinality API.
type sampled struct {
	name   string
	series int
}

// nextQuery stays inside max-range of now so queriers read ingesters. Queries aggregate, so
// queriers answer with a few series while ingesters still read every chunk. jobs are the metric's
// job values, once a label values query found them, for label regexes, and peers other sampled
// metrics, which a name regex selects along with it.
func nextQuery(cfg config, m metric, jobs, peers []string, now time.Time) (kind, path string, params url.Values) {
	params = url.Values{}
	name := m.name
	if m.kind == classicHistogram {
		name += "_bucket"
	}
	selector := fmt.Sprintf("{__name__=%q}", name)
	var queries []string
	switch m.kind {
	case classicHistogram:
		queries = []string{
			fmt.Sprintf("histogram_quantile(0.99, sum by (le) (rate(%s[5m])))", selector),
			fmt.Sprintf("histogram_quantile(0.5, sum by (job, le) (rate(%s[5m])))", selector),
			fmt.Sprintf("sum by (job) (rate({__name__=%q}[5m]))", m.name+"_count"),
		}
	case nativeHistogram:
		queries = []string{
			fmt.Sprintf("histogram_quantile(0.99, sum(rate(%s[5m])))", selector),
			fmt.Sprintf("histogram_quantile(0.9, sum by (job) (rate(%s[5m])))", selector),
			fmt.Sprintf("histogram_count(sum(rate(%s[5m])))", selector),
			fmt.Sprintf("histogram_avg(sum by (job) (rate(%s[5m])))", selector),
		}
	default:
		queries = []string{
			fmt.Sprintf("sum by (job) (rate(%s[5m]))", selector),
			fmt.Sprintf("count(%s)", selector),
			fmt.Sprintf("max(max_over_time(%s[10m]))", selector),
			fmt.Sprintf("max(quantile_over_time(0.9, %s[10m]))", selector),
			fmt.Sprintf("topk(5, sum by (job) (increase(%s[1h])))", selector),
		}
	}
	queries = append(queries, regexQueries(name, jobs, peers)...)
	// Weighted toward what costs ingesters more than queriers, which run out first: label lookups,
	// and range queries of few points, whose ingesters still read every chunk of the range.
	switch rand.IntN(10) {
	case 0, 1, 2, 3:
		span := time.Duration(30+rand.Int64N(int64(cfg.maxRange/time.Minute)-29)) * time.Minute
		step := max(15*time.Second, (span / time.Duration(cfg.points)).Truncate(15*time.Second))
		params.Set("query", queries[rand.IntN(len(queries))])
		params.Set("start", formatTime(now.Add(-span)))
		params.Set("end", formatTime(now))
		params.Set("step", strconv.FormatFloat(step.Seconds(), 'f', -1, 64))
		return "range", "/api/v1/query_range", params
	case 4, 5:
		queries = append(queries, fmt.Sprintf("count by (job) (%s)", selector))
		params.Set("query", queries[rand.IntN(len(queries))])
		params.Set("time", formatTime(now))
		return "instant", "/api/v1/query", params
	case 6, 7:
		params.Set("match[]", selector)
		params.Set("start", formatTime(now.Add(-time.Hour)))
		params.Set("end", formatTime(now))
		return "labels", "/api/v1/labels", params
	case 8:
		params.Set("match[]", selector)
		params.Set("start", formatTime(now.Add(-time.Hour)))
		params.Set("end", formatTime(now))
		// Its series, which queriers pass on: bounded, since every query stays under max-series.
		params.Set("limit", "1000")
		return "series", "/api/v1/series", params
	default:
		params.Set("match[]", selector)
		params.Set("start", formatTime(now.Add(-time.Hour)))
		params.Set("end", formatTime(now))
		return "label_values", "/api/v1/label/job/values", params
	}
}

// regexQueries select a metric and its peers by a name regex and, once its jobs are known, some of
// its jobs by an alternation, a prefix and a negation. Names are alternated rather than matched by
// prefix: a prefix like go_.+ selects a whole family, millions of series, which OOM queriers.
func regexQueries(name string, jobs, peers []string) []string {
	var queries []string
	if len(peers) > 0 {
		alternatives := []string{regexp.QuoteMeta(name)}
		for _, peer := range peers {
			alternatives = append(alternatives, regexp.QuoteMeta(peer))
		}
		queries = append(queries, fmt.Sprintf("count by (__name__) ({__name__=~%q})", strings.Join(alternatives, "|")))
	}
	if len(jobs) == 0 {
		return queries
	}
	picked := make([]string, 0, 3)
	for _, index := range rand.Perm(len(jobs))[:min(3, len(jobs))] {
		picked = append(picked, regexp.QuoteMeta(jobs[index]))
	}
	first := jobs[rand.IntN(len(jobs))]
	prefix := regexp.QuoteMeta(first[:min(len(first), 4)]) + ".*"
	return append(queries,
		fmt.Sprintf("sum by (job) (rate({__name__=%q, job=~%q}[5m]))", name, strings.Join(picked, "|")),
		fmt.Sprintf("count({__name__=%q, job=~%q})", name, prefix),
		fmt.Sprintf("sum(rate({__name__=%q, job!~%q}[5m]))", name, picked[0]),
	)
}

func get(ctx context.Context, client *http.Client, cfg config, tenant, path string, params url.Values) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.address+path+"?"+params.Encode(), nil)
	if err != nil {
		return err
	}
	request.Header.Set("X-Scope-OrgID", tenant)
	// Bypass the results cache so every request reaches queriers and ingesters.
	request.Header.Set("Cache-Control", "no-store")
	if cfg.clusterLabel != "" {
		request.Header.Set("X-Cluster", cfg.clusterLabel)
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 1024))
		return fmt.Errorf("status %d: %s", response.StatusCode, truncate(string(body), 200))
	}
	// Only the load matters, and a large result would not fit in memory.
	_, err = io.Copy(io.Discard, response.Body)
	return err
}

func getLabelValues(ctx context.Context, client *http.Client, cfg config, tenant, path string, params url.Values) ([]string, error) {
	var decoded struct {
		Data []string `json:"data"`
	}
	if err := getJSON(ctx, client, cfg, tenant, path+"?"+params.Encode(), &decoded); err != nil {
		return nil, err
	}
	return decoded.Data[:min(len(decoded.Data), 50)], nil
}

// sampleMetricNames picks metric names by their series in ingesters, from the cardinality API.
func sampleMetricNames(ctx context.Context, client *http.Client, cfg config, tenant string) ([]sampled, error) {
	params := url.Values{}
	params.Set("label_names[]", "__name__")
	// The API's largest limit.
	params.Set("limit", "500")
	var decoded struct {
		Labels []struct {
			Cardinality []struct {
				LabelValue  string `json:"label_value"`
				SeriesCount int    `json:"series_count"`
			} `json:"cardinality"`
		} `json:"labels"`
	}
	if err := getJSON(ctx, client, cfg, tenant, "/api/v1/cardinality/label_values?"+params.Encode(), &decoded); err != nil {
		return nil, err
	}
	var names []sampled
	for _, label := range decoded.Labels {
		for _, value := range label.Cardinality {
			if value.SeriesCount >= cfg.minSeries && value.SeriesCount <= cfg.maxSeries {
				names = append(names, sampled{name: value.LabelValue, series: value.SeriesCount})
			}
		}
	}
	rand.Shuffle(len(names), func(i, j int) { names[i], names[j] = names[j], names[i] })
	return names[:min(cfg.metricsSample, len(names))], nil
}

// histogramFamilies are the tenant's metric families of type histogram, from their metadata.
func histogramFamilies(ctx context.Context, client *http.Client, cfg config, tenant string) (map[string]bool, error) {
	type entry struct {
		Type string `json:"type"`
	}
	var decoded struct {
		Data map[string][]entry `json:"data"`
	}
	if err := getJSON(ctx, client, cfg, tenant, "/api/v1/metadata?limit=100000", &decoded); err != nil {
		return nil, err
	}
	families := map[string]bool{}
	for name, entries := range decoded.Data {
		if slices.ContainsFunc(entries, func(e entry) bool { return e.Type == "histogram" }) {
			families[name] = true
		}
	}
	return families, nil
}

// classify tells histograms from other metrics: a classic histogram's family shows as its _bucket
// series, and a native histogram's as the family name itself. Histograms with more than
// maxHistogramSeries series are left out.
func classify(names []sampled, histograms map[string]bool, maxHistogramSeries int) []metric {
	metrics := make([]metric, 0, len(names))
	for _, sample := range names {
		family, bucket := strings.CutSuffix(sample.name, "_bucket")
		histogram := histograms[family] && bucket || histograms[sample.name]
		switch {
		case histogram && sample.series > maxHistogramSeries:
		case bucket && histograms[family]:
			metrics = append(metrics, metric{name: family, kind: classicHistogram})
		case histograms[sample.name]:
			metrics = append(metrics, metric{name: sample.name, kind: nativeHistogram})
		default:
			metrics = append(metrics, metric{name: sample.name})
		}
	}
	return metrics
}

func getJSON(ctx context.Context, client *http.Client, cfg config, tenant, path string, into any) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.address+path, nil)
	if err != nil {
		return err
	}
	request.Header.Set("X-Scope-OrgID", tenant)
	if cfg.clusterLabel != "" {
		request.Header.Set("X-Cluster", cfg.clusterLabel)
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d: %s", response.StatusCode, truncate(string(body), 200))
	}
	return json.Unmarshal(body, into)
}

// metrics are the sampled metrics of each tenant, and the job values found for them.
type metrics struct {
	mu       sync.RWMutex
	byTenant map[string][]metric
	jobsOf   map[[2]string][]string
}

func newMetrics() *metrics {
	return &metrics{byTenant: map[string][]metric{}, jobsOf: map[[2]string][]string{}}
}

func (m *metrics) set(tenant string, sample []metric) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.byTenant[tenant] = sample
}

func (m *metrics) remove(tenant, name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.byTenant[tenant] = slices.DeleteFunc(m.byTenant[tenant], func(candidate metric) bool { return candidate.name == name })
}

func (m *metrics) pick(tenant string) (metric, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	sample := m.byTenant[tenant]
	if len(sample) == 0 {
		return metric{}, false
	}
	return sample[rand.IntN(len(sample))], true
}

// peers are up to count other sampled plain metrics of the tenant.
func (m *metrics) peers(tenant string, count int) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var peers []string
	sample := m.byTenant[tenant]
	for _, index := range rand.Perm(len(sample)) {
		if len(peers) == count {
			break
		}
		if sample[index].kind == plain {
			peers = append(peers, sample[index].name)
		}
	}
	return peers
}

func (m *metrics) setJobs(tenant, name string, jobs []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobsOf[[2]string{tenant, name}] = jobs
}

func (m *metrics) jobs(tenant, name string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.jobsOf[[2]string{tenant, name}]
}

func report(window map[string][]result) {
	kinds := make([]string, 0, len(window))
	for kind := range window {
		kinds = append(kinds, kind)
	}
	slices.Sort(kinds)
	for _, kind := range kinds {
		results := window[kind]
		latencies := make([]time.Duration, 0, len(results))
		var errorCount int
		var lastError error
		for _, r := range results {
			latencies = append(latencies, r.latency)
			if r.err != nil {
				errorCount++
				lastError = r.err
			}
		}
		slices.Sort(latencies)
		line := fmt.Sprintf("kind=%s requests=%d errors=%d p50=%s p95=%s max=%s", kind, len(results), errorCount,
			percentile(latencies, 0.5), percentile(latencies, 0.95), latencies[len(latencies)-1].Round(time.Millisecond))
		if lastError != nil {
			line += fmt.Sprintf(" last_error=%q", truncate(lastError.Error(), 200))
		}
		fmt.Println(line)
	}
}

func percentile(sorted []time.Duration, quantile float64) time.Duration {
	return sorted[min(len(sorted)-1, int(float64(len(sorted))*quantile))].Round(time.Millisecond)
}

func formatTime(t time.Time) string {
	return strconv.FormatFloat(float64(t.UnixMilli())/1000, 'f', 3, 64)
}

func truncate(value string, length int) string {
	if len(value) <= length {
		return value
	}
	return value[:length]
}

func sleep(ctx context.Context, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
