// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/commands/analyse_prometheus.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package commands

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/concurrency"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/config"
	"github.com/prometheus/common/model"

	"github.com/grafana/mimir/pkg/mimirtool/analyze"
	"github.com/grafana/mimir/pkg/mimirtool/client"
)

var defaultBackoffConfig = backoff.Config{
	MinBackoff: 100 * time.Millisecond,
	MaxBackoff: 10 * time.Second,
	MaxRetries: 10,
}

type PrometheusAnalyzeCommand struct {
	address              string
	prometheusHTTPPrefix string
	username             string
	tenantID             string
	password             string
	authToken            string
	readTimeout          time.Duration
	backoffConfig        backoff.Config

	grafanaMetricsFile string
	rulerMetricsFile   string
	outputFile         string
	concurrency        int

	logger log.Logger
}

func (cmd *PrometheusAnalyzeCommand) run(_ *kingpin.ParseContext) error {
	metricsUsed, err := cmd.parseUsedMetrics()
	if err != nil {
		return err
	}

	v1api, err := cmd.newAPI()
	if err != nil {
		return err
	}

	metricsInPrometheus, err := IdentifyMetricsInPrometheusWithBackoff(metricsUsed, v1api, cmd.concurrency, cmd.readTimeout, cmd.backoffConfig, cmd.logger)
	if err != nil {
		return err
	}

	return cmd.write(metricsInPrometheus)
}

func (cmd *PrometheusAnalyzeCommand) parseUsedMetrics() (model.LabelValues, error) {
	var (
		metricsUsed    model.LabelValues
		grafanaMetrics = &analyze.MetricsInGrafana{}
		rulerMetrics   = &analyze.MetricsInRuler{}
	)

	if err := parseMetricFileIfExist(cmd.grafanaMetricsFile, grafanaMetrics); err != nil {
		return nil, err
	}
	metricsUsed = append(metricsUsed, grafanaMetrics.MetricsUsed...)

	if err := parseMetricFileIfExist(cmd.rulerMetricsFile, rulerMetrics); err != nil {
		return nil, err
	}
	metricsUsed = append(metricsUsed, rulerMetrics.MetricsUsed...)

	if len(metricsUsed) == 0 {
		return nil, errors.New("no Grafana or Ruler metrics files")
	}

	return metricsUsed, nil
}

func (cmd *PrometheusAnalyzeCommand) newAPI() (v1.API, error) {
	rt := api.DefaultRoundTripper
	rt = config.NewUserAgentRoundTripper(client.UserAgent(), rt)

	switch {
	case cmd.username != "":
		rt = config.NewBasicAuthRoundTripper(config.NewInlineSecret(cmd.username), config.NewInlineSecret(cmd.password), rt)

	case cmd.password != "":
		rt = config.NewBasicAuthRoundTripper(config.NewInlineSecret(cmd.tenantID), config.NewInlineSecret(cmd.password), rt)

	case cmd.authToken != "":
		rt = config.NewAuthorizationCredentialsRoundTripper("Bearer", config.NewInlineSecret(cmd.authToken), rt)
	}

	rt = &setTenantIDTransport{
		RoundTripper: rt,
		tenantID:     cmd.tenantID,
	}

	address, err := url.JoinPath(cmd.address, cmd.prometheusHTTPPrefix)
	if err != nil {
		return nil, err
	}
	apiClient, err := api.NewClient(api.Config{
		Address:      address,
		RoundTripper: rt,
	})
	if err != nil {
		return nil, err
	}

	return v1.NewAPI(&statusAwareClient{Client: apiClient}), nil
}

func queryMetricNames(api v1.API, readTimeout time.Duration, backoffCfg backoff.Config, logger log.Logger) (model.LabelValues, error) {
	ctx, cancel := context.WithTimeout(context.Background(), readTimeout)
	defer cancel()

	var metricNames model.LabelValues
	err := withBackoffConfig(ctx, backoffCfg, func() error {
		var err error
		metricNames, _, _, err = api.LabelValues(ctx, model.MetricNameLabel, nil, time.Now().Add(-10*time.Minute), time.Now())
		return err
	})
	if err != nil {
		return nil, errors.Wrap(err, "error querying for metric names")
	}

	level.Info(logger).Log("msg", "found metric names", "count", len(metricNames))
	return metricNames, nil
}

// AnalyzePrometheus analyze prometheus through the given provided API and return the list metrics used in it.
func AnalyzePrometheus(api v1.API, metrics model.LabelValues, skip func(model.LabelValue) bool, jobConcurrency int, readTimeout time.Duration, logger log.Logger) (AnalyzeResult, error) {
	return AnalyzePrometheusWithBackoff(api, metrics, skip, jobConcurrency, readTimeout, defaultBackoffConfig, logger)
}

// AnalyzePrometheusWithBackoff analyzes prometheus metrics using the provided backoff configuration.
func AnalyzePrometheusWithBackoff(api v1.API, metrics model.LabelValues, skip func(model.LabelValue) bool, jobConcurrency int, readTimeout time.Duration, backoffCfg backoff.Config, logger log.Logger) (AnalyzeResult, error) {
	var (
		stats        = make(stats)
		metricErrors []string
		cardinality  uint64
		mutex        sync.Mutex
	)

	err := concurrency.ForEachJob(context.Background(), len(metrics), jobConcurrency, func(ctx context.Context, idx int) error {
		metric := metrics[idx]
		if skip(metric) {
			return nil
		}

		ctx, cancel := context.WithTimeout(ctx, readTimeout)
		defer cancel()

		var result model.Value
		// The __ignore_usage__ label selector tells Adaptive Metrics' recommendations
		// service to not factor this query into its analysis.
		// See https://grafana.com/docs/grafana-cloud/adaptive-telemetry/adaptive-metrics/manage-recommendations/understand-recommended-rules/#make-the-recommendations-service-ignore-a-query
		query := fmt.Sprintf(`count by (job) (%s{__ignore_usage__=""})`, metric)
		err := withBackoffConfig(ctx, backoffCfg, func() error {
			var err error
			result, _, _, err = api.Query(ctx, query, time.Now())
			return err
		})
		if err != nil {
			errStr := fmt.Sprintf("skipped %s analysis because failed to run query %v: %s", metric, query, err.Error())
			level.Warn(logger).Log("msg", errStr)
			mutex.Lock()
			metricErrors = append(metricErrors, errStr)
			mutex.Unlock()
			return nil
		}

		vec, ok := result.(model.Vector)
		if !ok || len(vec) == 0 {
			level.Debug(logger).Log("msg", "no active metrics found for", "metric", metric)
			return nil
		}

		for _, sample := range vec {
			mutex.Lock()
			counts := stats[metric]
			if counts.jobCount == nil {
				counts.jobCount = make(map[string]int)
			}

			counts.totalCount += int(sample.Value)
			counts.jobCount[string(sample.Metric["job"])] += int(sample.Value)
			stats[metric] = counts
			cardinality += uint64(sample.Value)
			mutex.Unlock()
		}

		level.Debug(logger).Log("msg", "metric", "metric", metric, "value", vec[0].Value)
		return nil
	})
	if err != nil {
		return AnalyzeResult{}, err
	}

	return AnalyzeResult{
		stats:        stats,
		cardinality:  cardinality,
		metricErrors: metricErrors,
	}, nil
}

// IdentifyMetricsInPrometheus analyze prometheus metrics against metrics used metricsUsed and metricNames. It returns stats about used/unused timeseries.
func IdentifyMetricsInPrometheus(metricsUsed model.LabelValues, v1api v1.API, jobConcurrency int, readTimeout time.Duration, logger log.Logger) (analyze.MetricsInPrometheus, error) {
	return IdentifyMetricsInPrometheusWithBackoff(metricsUsed, v1api, jobConcurrency, readTimeout, defaultBackoffConfig, logger)
}

// IdentifyMetricsInPrometheusWithBackoff analyzes prometheus metrics against metricsUsed and metricNames using the provided backoff configuration.
func IdentifyMetricsInPrometheusWithBackoff(metricsUsed model.LabelValues, v1api v1.API, jobConcurrency int, readTimeout time.Duration, backoffCfg backoff.Config, logger log.Logger) (analyze.MetricsInPrometheus, error) {
	level.Debug(logger).Log("msg", "starting to analyze metrics in use")
	var metricsInPrometheus analyze.MetricsInPrometheus

	inUseMetricsAnalysisResult, err := AnalyzePrometheusWithBackoff(v1api, metricsUsed, func(model.LabelValue) bool { return false }, jobConcurrency, readTimeout, backoffCfg, logger)
	if err != nil {
		return metricsInPrometheus, err
	}
	level.Info(logger).Log("msg", "active series are being used in dashboards", "count", inUseMetricsAnalysisResult.cardinality)

	level.Debug(logger).Log("msg", "starting to analyze metrics not in use")
	metricNames, err := queryMetricNames(v1api, readTimeout, backoffCfg, logger)
	if err != nil {
		return metricsInPrometheus, err
	}
	metricsNotInUseResult, err := AnalyzePrometheusWithBackoff(v1api, metricNames, func(metric model.LabelValue) bool {
		return inUseMetricsAnalysisResult.stats[metric].totalCount > 0
	}, jobConcurrency, readTimeout, backoffCfg, logger)
	if err != nil {
		return metricsInPrometheus, err
	}
	level.Info(logger).Log("msg", "active series are NOT being used in dashboards", "count", metricsNotInUseResult.cardinality)

	metricsInPrometheus = result(inUseMetricsAnalysisResult, metricsNotInUseResult)
	level.Info(logger).Log("msg", "in use active series metric count", "count", len(metricsInPrometheus.InUseMetricCounts))
	level.Info(logger).Log("msg", "not in use active series metric count", "count", len(metricsInPrometheus.AdditionalMetricCounts))

	return metricsInPrometheus, nil
}

func result(inUse, notInUse AnalyzeResult) analyze.MetricsInPrometheus {
	return analyze.MetricsInPrometheus{
		InUseActiveSeries:      inUse.cardinality,
		AdditionalActiveSeries: notInUse.cardinality,
		InUseMetricCounts:      inUse.metricsCounts(),
		AdditionalMetricCounts: notInUse.metricsCounts(),
		TotalActiveSeries:      inUse.cardinality + notInUse.cardinality,
		Errors:                 append(inUse.metricErrors, notInUse.metricErrors...),
	}
}

func (cmd *PrometheusAnalyzeCommand) write(output analyze.MetricsInPrometheus) error {
	buf, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(cmd.outputFile, buf, os.FileMode(0o666))
}

type stat struct {
	totalCount int
	jobCount   map[string]int
}

type stats map[model.LabelValue]stat

type AnalyzeResult struct {
	stats        stats
	cardinality  uint64
	metricErrors []string
}

func (a AnalyzeResult) metricsCounts() []analyze.MetricCount {
	var metricCount []analyze.MetricCount

	for metric, counts := range a.stats {
		jobCounts := make([]analyze.JobCount, 0, len(counts.jobCount))
		for job, count := range counts.jobCount {
			jobCounts = append(jobCounts, analyze.JobCount{
				Job:   job,
				Count: count,
			})
		}
		slices.SortFunc(jobCounts, func(a, b analyze.JobCount) int {
			return b.Count - a.Count
		})

		metricCount = append(metricCount, analyze.MetricCount{Metric: string(metric), Count: counts.totalCount, JobCounts: jobCounts})
	}

	slices.SortFunc(metricCount, func(a, b analyze.MetricCount) int {
		return b.Count - a.Count
	})

	return metricCount
}

func parseMetricFileIfExist(path string, out any) error {
	if _, err := os.Stat(path); err != nil {
		// if given file does not exist, it's OK to skip.
		return nil
	}

	buf, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	return json.Unmarshal(buf, out)
}

func withBackoff(ctx context.Context, fn func() error) error {
	return withBackoffConfig(ctx, defaultBackoffConfig, fn)
}

func withBackoffConfig(ctx context.Context, cfg backoff.Config, fn func() error) error {
	boff := backoff.New(ctx, normalizeBackoffConfig(cfg))
	var err error
	for boff.Ongoing() {
		if err = fn(); err == nil {
			return nil
		}
		if !isRetryableError(err) {
			return err
		}

		delay := boff.NextDelay()
		if retryAfter := retryAfterDuration(err); retryAfter > delay {
			delay = retryAfter
		}
		if !boff.Ongoing() {
			break
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}

	if err != nil {
		return err
	}

	return boff.Err()
}

func normalizeBackoffConfig(cfg backoff.Config) backoff.Config {
	if cfg.MinBackoff <= 0 {
		cfg.MinBackoff = defaultBackoffConfig.MinBackoff
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = defaultBackoffConfig.MaxBackoff
	}
	if cfg.MaxBackoff < cfg.MinBackoff {
		cfg.MaxBackoff = cfg.MinBackoff
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = defaultBackoffConfig.MaxRetries
	}
	return cfg
}

type statusAwareClient struct {
	api.Client
}

type httpStatusError struct {
	statusCode int
	retryAfter time.Duration
	v1Err      *v1.Error
}

func (e *httpStatusError) Error() string {
	return e.v1Err.Error()
}

func (e *httpStatusError) Unwrap() error {
	return e.v1Err
}

func (c *statusAwareClient) Do(ctx context.Context, req *http.Request) (*http.Response, []byte, error) {
	resp, body, err := c.Client.Do(ctx, req)
	if err != nil || resp == nil {
		return resp, body, err
	}
	if resp.StatusCode/100 == 2 || resp.StatusCode == http.StatusNoContent {
		return resp, body, nil
	}
	if req.Method == http.MethodPost && (resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusMethodNotAllowed || resp.StatusCode == http.StatusNotImplemented) {
		return resp, body, nil
	}

	retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"), time.Now())
	if resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusUnprocessableEntity {
		var envelope struct {
			Status    string       `json:"status"`
			ErrorType v1.ErrorType `json:"errorType"`
			Error     string       `json:"error"`
		}
		if json.Unmarshal(body, &envelope) == nil && envelope.Status == "error" && envelope.ErrorType != "" {
			return resp, nil, &httpStatusError{
				statusCode: resp.StatusCode,
				retryAfter: retryAfter,
				v1Err: &v1.Error{
					Type: envelope.ErrorType,
					Msg:  envelope.Error,
				},
			}
		}
	}

	errType := v1.ErrBadResponse
	errMsg := fmt.Sprintf("bad response code %d", resp.StatusCode)
	switch resp.StatusCode / 100 {
	case 4:
		errType = v1.ErrClient
		errMsg = fmt.Sprintf("client error: %d", resp.StatusCode)
	case 5:
		errType = v1.ErrServer
		errMsg = fmt.Sprintf("server error: %d", resp.StatusCode)
	}

	return resp, nil, &httpStatusError{
		statusCode: resp.StatusCode,
		retryAfter: retryAfter,
		v1Err: &v1.Error{
			Type:   errType,
			Msg:    errMsg,
			Detail: string(body),
		},
	}
}

func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	if stderrors.Is(err, context.Canceled) || stderrors.Is(err, context.DeadlineExceeded) {
		return false
	}

	var statusErr *httpStatusError
	if stderrors.As(err, &statusErr) {
		if statusErr.v1Err != nil && (statusErr.v1Err.Type == v1.ErrTimeout || statusErr.v1Err.Type == v1.ErrServer) {
			return true
		}
		return statusErr.statusCode == http.StatusTooManyRequests ||
			statusErr.statusCode == http.StatusRequestTimeout ||
			statusErr.statusCode >= http.StatusInternalServerError
	}

	var v1Err *v1.Error
	if stderrors.As(err, &v1Err) {
		switch v1Err.Type {
		case v1.ErrBadData, v1.ErrCanceled:
			return false
		case v1.ErrClient:
			return strings.HasSuffix(v1Err.Msg, " 429") || strings.HasSuffix(v1Err.Msg, " 408")
		default:
			return true
		}
	}

	return true
}

func retryAfterDuration(err error) time.Duration {
	var statusErr *httpStatusError
	if stderrors.As(err, &statusErr) {
		return statusErr.retryAfter
	}
	return 0
}

func parseRetryAfter(val string, now time.Time) time.Duration {
	val = strings.TrimSpace(val)
	if val == "" {
		return 0
	}
	if secs, err := strconv.Atoi(val); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	if retryAt, err := http.ParseTime(val); err == nil {
		if d := retryAt.Sub(now); d > 0 {
			return d
		}
	}
	return 0
}

