// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/commands/analyse_prometheus.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/concurrency"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	log "github.com/sirupsen/logrus"

	"github.com/grafana/mimir/pkg/mimirtool/analyze"
	"github.com/grafana/mimir/pkg/mimirtool/client"
)

type PrometheusAnalyzeCommand struct {
	address              string
	prometheusHTTPPrefix string
	username             string
	tenantID             string
	password             string
	authToken            string
	readTimeout          time.Duration

	grafanaMetricsFile string
	rulerMetricsFile   string
	outputFile         string
	concurrency        int
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

	metricsInPrometheus, err := IdentifyMetricsInPrometheus(metricsUsed, v1api, cmd.concurrency, cmd.readTimeout)
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
	client, err := api.NewClient(api.Config{
		Address:      address,
		RoundTripper: rt,
	})
	if err != nil {
		return nil, err
	}

	return v1.NewAPI(client), nil
}

func queryMetricNames(api v1.API, readTimeout time.Duration) (model.LabelValues, error) {
	ctx, cancel := context.WithTimeout(context.Background(), readTimeout)
	defer cancel()

	var metricNames model.LabelValues
	err := withBackoff(ctx, func() error {
		var err error
		metricNames, _, err = api.LabelValues(ctx, labels.MetricName, nil, time.Now().Add(-10*time.Minute), time.Now())
		return err
	})
	if err != nil {
		return nil, errors.Wrap(err, "error querying for metric names")
	}

	log.Infof("Found %d metric names", len(metricNames))
	return metricNames, nil
}

// AnalyzePrometheus analyze prometheus through the given provided API and return the list metrics used in it.
func AnalyzePrometheus(api v1.API, metrics model.LabelValues, skip func(model.LabelValue) bool, jobConcurrency int, readTimeout time.Duration) (AnalyzeResult, error) {
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
		query := string("count by (job) (" + metric + ")")
		err := withBackoff(ctx, func() error {
			var err error
			result, _, err = api.Query(ctx, query, time.Now())
			return err
		})
		if err != nil {
			errStr := fmt.Sprintf("skipped %s analysis because failed to run query %v: %s", metric, query, err.Error())
			log.Warnln(errStr)
			mutex.Lock()
			metricErrors = append(metricErrors, errStr)
			mutex.Unlock()
			return nil
		}

		vec, ok := result.(model.Vector)
		if !ok || len(vec) == 0 {
			log.Debugln("no active metrics found for", metric, 0)
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

		log.Debugln("metric", metric, vec[0].Value)
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
func IdentifyMetricsInPrometheus(metricsUsed model.LabelValues, v1api v1.API, jobConcurrency int, readTimeout time.Duration) (analyze.MetricsInPrometheus, error) {
	log.Debugln("Starting to analyze metrics in use")
	var metricsInPrometheus analyze.MetricsInPrometheus

	inUseMetricsAnalysisResult, err := AnalyzePrometheus(v1api, metricsUsed, func(model.LabelValue) bool { return false }, jobConcurrency, readTimeout)
	if err != nil {
		return metricsInPrometheus, err
	}
	log.Infof("%d active series are being used in dashboards", inUseMetricsAnalysisResult.cardinality)

	log.Debugln("Starting to analyze metrics not in use")
	metricNames, err := queryMetricNames(v1api, readTimeout)
	if err != nil {
		return metricsInPrometheus, err
	}
	metricsNotInUseResult, err := AnalyzePrometheus(v1api, metricNames, func(metric model.LabelValue) bool {
		return inUseMetricsAnalysisResult.stats[metric].totalCount > 0
	}, jobConcurrency, readTimeout)
	if err != nil {
		return metricsInPrometheus, err
	}
	log.Infof("%d active series are NOT being used in dashboards", metricsNotInUseResult.cardinality)

	metricsInPrometheus = result(inUseMetricsAnalysisResult, metricsNotInUseResult)
	log.Infof("%d in use active series metric count", len(metricsInPrometheus.InUseMetricCounts))
	log.Infof("%d not in use active series metric count", len(metricsInPrometheus.AdditionalMetricCounts))

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
		sort.Slice(jobCounts, func(i, j int) bool {
			return jobCounts[i].Count > jobCounts[j].Count
		})

		metricCount = append(metricCount, analyze.MetricCount{Metric: string(metric), Count: counts.totalCount, JobCounts: jobCounts})
	}

	sort.Slice(metricCount, func(i, j int) bool {
		return metricCount[i].Count > metricCount[j].Count
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
	backoff := backoff.New(ctx, backoff.Config{
		MaxRetries: 10,
	})

	for backoff.Ongoing() {
		if err := fn(); err == nil {
			return nil
		}

		backoff.Wait()
	}

	return backoff.Err()
}
