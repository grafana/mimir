// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/commands/analyse_prometheus.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package commands

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"sort"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	log "github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/grafana/mimir/cmd/cortextool/pkg/analyse"
)

type PrometheusAnalyseCommand struct {
	address     string
	username    string
	password    string
	readTimeout time.Duration

	grafanaMetricsFile string
	rulerMetricsFile   string
	outputFile         string
}

func (cmd *PrometheusAnalyseCommand) run(k *kingpin.ParseContext) error {
	var (
		hasGrafanaMetrics, hasRulerMetrics = false, false
		grafanaMetrics                     = analyse.MetricsInGrafana{}
		rulerMetrics                       = analyse.MetricsInRuler{}
		metricsUsed                        []string
	)

	if _, err := os.Stat(cmd.grafanaMetricsFile); err == nil {
		hasGrafanaMetrics = true
		byt, err := ioutil.ReadFile(cmd.grafanaMetricsFile)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(byt, &grafanaMetrics); err != nil {
			return err
		}
		metricsUsed = append(metricsUsed, grafanaMetrics.MetricsUsed...)
	}

	if _, err := os.Stat(cmd.rulerMetricsFile); err == nil {
		hasRulerMetrics = true
		byt, err := ioutil.ReadFile(cmd.rulerMetricsFile)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(byt, &rulerMetrics); err != nil {
			return err
		}
		metricsUsed = append(metricsUsed, rulerMetrics.MetricsUsed...)
	}

	if !hasGrafanaMetrics && !hasRulerMetrics {
		return errors.New("No Grafana or Ruler metrics files")
	}

	rt := api.DefaultRoundTripper
	if cmd.username != "" {
		rt = config.NewBasicAuthRoundTripper(cmd.username, config.Secret(cmd.password), "", api.DefaultRoundTripper)
	}
	promClient, err := api.NewClient(api.Config{
		Address:      cmd.address,
		RoundTripper: rt,
	})
	if err != nil {
		return err
	}

	v1api := v1.NewAPI(promClient)

	ctx, cancel := context.WithTimeout(context.Background(), cmd.readTimeout)
	defer cancel()
	metricNames, _, err := v1api.LabelValues(ctx, labels.MetricName, nil, time.Now().Add(-10*time.Minute), time.Now())
	if err != nil {
		return errors.Wrap(err, "error querying for metric names")
	}
	log.Infof("Found %d metric names\n", len(metricNames))

	inUseMetrics := map[string]struct {
		totalCount int
		jobCount   map[string]int
	}{}
	inUseCardinality := 0

	for _, metric := range metricsUsed {
		ctx, cancel := context.WithTimeout(context.Background(), cmd.readTimeout)
		defer cancel()

		query := "count by (job) (" + metric + ")"
		result, _, err := v1api.Query(ctx, query, time.Now())
		if err != nil {
			return errors.Wrap(err, "error querying "+query)
		}

		vec := result.(model.Vector)
		if len(vec) == 0 {
			counts := inUseMetrics[metric]
			counts.totalCount += 0
			inUseMetrics[metric] = counts
			log.Debugln("in use", metric, 0)

			continue
		}

		for _, sample := range vec {
			counts := inUseMetrics[metric]
			if counts.jobCount == nil {
				counts.jobCount = make(map[string]int)
			}

			counts.totalCount += int(sample.Value)
			counts.jobCount[string(sample.Metric["job"])] += int(sample.Value)
			inUseMetrics[metric] = counts

			inUseCardinality += int(sample.Value)
		}

		log.Debugln("in use", metric, vec[0].Value)
	}

	log.Infof("%d active series are being used in dashboards", inUseCardinality)

	// Count the not-in-use active series.
	additionalMetrics := map[string]struct {
		totalCount int
		jobCount   map[string]int
	}{}
	additionalMetricsCardinality := 0
	for _, metricName := range metricNames {
		metric := string(metricName)
		if _, ok := inUseMetrics[metric]; ok {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), cmd.readTimeout)
		defer cancel()

		query := "count by (job) (" + metric + ")"
		result, _, err := v1api.Query(ctx, query, time.Now())
		if err != nil {
			return errors.Wrap(err, "error querying "+query)
		}

		vec := result.(model.Vector)
		if len(vec) == 0 {
			counts := additionalMetrics[metric]
			counts.totalCount += 0
			additionalMetrics[metric] = counts

			log.Debugln("additional", metric, 0)

			continue
		}

		for _, sample := range vec {
			counts := additionalMetrics[metric]
			if counts.jobCount == nil {
				counts.jobCount = make(map[string]int)
			}

			counts.totalCount += int(sample.Value)
			counts.jobCount[string(sample.Metric["job"])] += int(sample.Value)
			additionalMetrics[metric] = counts

			additionalMetricsCardinality += int(sample.Value)
		}

		log.Debugln("additional", metric, vec[0].Value)
	}

	log.Infof("%d active series are NOT being used in dashboards", additionalMetricsCardinality)

	output := analyse.MetricsInPrometheus{}
	output.TotalActiveSeries = inUseCardinality + additionalMetricsCardinality
	output.InUseActiveSeries = inUseCardinality
	output.AdditionalActiveSeries = additionalMetricsCardinality

	for metric, counts := range inUseMetrics {
		jobCounts := make([]analyse.JobCount, 0, len(counts.jobCount))
		for job, count := range counts.jobCount {
			jobCounts = append(jobCounts, analyse.JobCount{
				Job:   job,
				Count: count,
			})
		}
		sort.Slice(jobCounts, func(i, j int) bool {
			return jobCounts[i].Count > jobCounts[j].Count
		})

		output.InUseMetricCounts = append(output.InUseMetricCounts, analyse.MetricCount{Metric: metric, Count: counts.totalCount, JobCounts: jobCounts})
	}
	sort.Slice(output.InUseMetricCounts, func(i, j int) bool {
		return output.InUseMetricCounts[i].Count > output.InUseMetricCounts[j].Count
	})

	for metric, counts := range additionalMetrics {
		jobCounts := make([]analyse.JobCount, 0, len(counts.jobCount))
		for job, count := range counts.jobCount {
			jobCounts = append(jobCounts, analyse.JobCount{
				Job:   job,
				Count: count,
			})
		}
		sort.Slice(jobCounts, func(i, j int) bool {
			return jobCounts[i].Count > jobCounts[j].Count
		})
		output.AdditionalMetricCounts = append(output.AdditionalMetricCounts, analyse.MetricCount{Metric: metric, Count: counts.totalCount, JobCounts: jobCounts})
	}
	sort.Slice(output.AdditionalMetricCounts, func(i, j int) bool {
		return output.AdditionalMetricCounts[i].Count > output.AdditionalMetricCounts[j].Count
	})

	out, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return err
	}

	if err := ioutil.WriteFile(cmd.outputFile, out, os.FileMode(int(0666))); err != nil {
		return err
	}

	return nil
}
