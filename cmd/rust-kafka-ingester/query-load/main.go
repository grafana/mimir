// SPDX-License-Identifier: AGPL-3.0-only

// query-load sends a steady, ingester-heavy mix of queries to a query-frontend: recent range and
// instant queries plus label lookups, all inside the window queriers serve from ingesters.
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
	address         string
	tenants         stringList
	clusterLabel    string
	concurrency     int
	pause           time.Duration
	maxRange        time.Duration
	metricsSample   int
	refreshInterval time.Duration
	reportInterval  time.Duration
	timeout         time.Duration
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
	flag.DurationVar(&cfg.maxRange, "max-range", 5*time.Hour, "Longest range query; keep it below -querier.query-store-after so ingesters serve it")
	flag.IntVar(&cfg.metricsSample, "metrics-per-tenant", 200, "Metric names sampled per tenant")
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
	names := newMetricNames()
	refresh := func() {
		for _, tenant := range cfg.tenants {
			sample, err := sampleMetricNames(ctx, client, cfg, tenant)
			if err != nil {
				fmt.Fprintf(os.Stderr, "tenant=%s metric name refresh failed: %v\n", tenant, err)
				continue
			}
			names.set(tenant, sample)
		}
	}
	refresh()
	results := make(chan result, 1024)
	var wg sync.WaitGroup
	for range cfg.concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			loop(ctx, client, cfg, names, results)
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

func loop(ctx context.Context, client *http.Client, cfg config, names *metricNames, results chan<- result) {
	for ctx.Err() == nil {
		tenant := cfg.tenants[rand.IntN(len(cfg.tenants))]
		metric, ok := names.pick(tenant)
		if !ok {
			sleep(ctx, cfg.pause)
			continue
		}
		kind, path, params := nextQuery(cfg, metric, time.Now())
		started := time.Now()
		err := get(ctx, client, cfg, tenant, path, params)
		if ctx.Err() != nil {
			return
		}
		results <- result{kind: kind, latency: time.Since(started), err: err}
		sleep(ctx, cfg.pause)
	}
}

// nextQuery stays inside max-range of now so queriers read ingesters rather than store-gateways.
func nextQuery(cfg config, metric string, now time.Time) (kind, path string, params url.Values) {
	selector := fmt.Sprintf("{__name__=%q}", metric)
	params = url.Values{}
	switch rand.IntN(5) {
	case 0, 1:
		span := time.Duration(30+rand.Int64N(int64(cfg.maxRange/time.Minute)-29)) * time.Minute
		step := max(15*time.Second, (span / 240).Truncate(15*time.Second))
		queries := []string{
			fmt.Sprintf("sum by (job) (rate(%s[5m]))", selector),
			fmt.Sprintf("count(%s)", selector),
			fmt.Sprintf("max_over_time(%s[10m])", selector),
		}
		params.Set("query", queries[rand.IntN(len(queries))])
		params.Set("start", formatTime(now.Add(-span)))
		params.Set("end", formatTime(now))
		params.Set("step", strconv.FormatFloat(step.Seconds(), 'f', -1, 64))
		return "range", "/api/v1/query_range", params
	case 2, 3:
		params.Set("query", fmt.Sprintf("count by (job) (%s)", selector))
		params.Set("time", formatTime(now))
		return "instant", "/api/v1/query", params
	default:
		params.Set("match[]", selector)
		params.Set("start", formatTime(now.Add(-time.Hour)))
		params.Set("end", formatTime(now))
		return "labels", "/api/v1/labels", params
	}
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
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d: %s", response.StatusCode, truncate(string(body), 200))
	}
	return nil
}

func sampleMetricNames(ctx context.Context, client *http.Client, cfg config, tenant string) ([]string, error) {
	now := time.Now()
	params := url.Values{}
	params.Set("start", formatTime(now.Add(-time.Hour)))
	params.Set("end", formatTime(now))
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.address+"/api/v1/label/__name__/values?"+params.Encode(), nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("X-Scope-OrgID", tenant)
	if cfg.clusterLabel != "" {
		request.Header.Set("X-Cluster", cfg.clusterLabel)
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	var decoded struct {
		Status string   `json:"status"`
		Data   []string `json:"data"`
	}
	if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
		return nil, fmt.Errorf("status %d: %w", response.StatusCode, err)
	}
	if decoded.Status != "success" {
		return nil, fmt.Errorf("status %d: %s", response.StatusCode, decoded.Status)
	}
	rand.Shuffle(len(decoded.Data), func(i, j int) { decoded.Data[i], decoded.Data[j] = decoded.Data[j], decoded.Data[i] })
	return decoded.Data[:min(cfg.metricsSample, len(decoded.Data))], nil
}

type metricNames struct {
	mu     sync.RWMutex
	byName map[string][]string
}

func newMetricNames() *metricNames {
	return &metricNames{byName: map[string][]string{}}
}

func (m *metricNames) set(tenant string, names []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.byName[tenant] = names
}

func (m *metricNames) pick(tenant string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	names := m.byName[tenant]
	if len(names) == 0 {
		return "", false
	}
	return names[rand.IntN(len(names))], true
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
