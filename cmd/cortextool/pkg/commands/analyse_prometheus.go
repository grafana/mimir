package commands

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"sort"
	"time"

	"github.com/grafana/cortex-tools/pkg/analyse"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	log "github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
)

type PrometheusAnalyseCommand struct {
	address     string
	username    string
	password    string
	readTimeout time.Duration

	grafanaMetricsFile string
	outputFile         string
}

func (cmd *PrometheusAnalyseCommand) run(k *kingpin.ParseContext) error {
	grafanaMetrics := analyse.MetricsInGrafana{}

	byt, err := ioutil.ReadFile(cmd.grafanaMetricsFile)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(byt, &grafanaMetrics); err != nil {
		return err
	}

	rt := api.DefaultRoundTripper
	if cmd.username != "" {
		rt = config.NewBasicAuthRoundTripper(cmd.username, config.Secret(cmd.password), "", api.DefaultRoundTripper)
	}
	promClient, err := api.NewClient(api.Config{
		Address: cmd.address,

		RoundTripper: rt,
	})
	if err != nil {
		return err
	}

	v1api := v1.NewAPI(promClient)

	ctx, cancel := context.WithTimeout(context.Background(), cmd.readTimeout)
	defer cancel()
	metricNames, _, err := v1api.LabelValues(ctx, labels.MetricName, time.Now().Add(-10*time.Minute), time.Now())
	if err != nil {
		return errors.Wrap(err, "error querying for metric names")
	}
	log.Infof("Found %d metric names\n", len(metricNames))

	inUseMetrics := map[string]int{}
	inUseCardinality := 0

	for _, metric := range grafanaMetrics.MetricsUsed {
		ctx, cancel := context.WithTimeout(context.Background(), cmd.readTimeout)
		defer cancel()

		query := "count(" + metric + ")"
		result, _, err := v1api.Query(ctx, query, time.Now())
		if err != nil {
			return errors.Wrap(err, "error querying "+query)
		}

		vec := result.(model.Vector)
		if len(vec) == 0 {
			inUseMetrics[metric] += 0
			log.Debugln(metric, 0)

			continue
		}

		inUseMetrics[metric] += int(vec[0].Value)
		inUseCardinality += int(vec[0].Value)

		log.Debugln("in use", metric, vec[0].Value)
	}

	log.Infof("%d active series are being used in dashboards", inUseCardinality)

	// Count the not-in-use active series.
	additionalMetrics := map[string]int{}
	additionalMetricsCardinality := 0
	for _, metricName := range metricNames {
		metric := string(metricName)
		if _, ok := inUseMetrics[metric]; ok {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), cmd.readTimeout)
		defer cancel()

		query := "count(" + metric + ")"
		result, _, err := v1api.Query(ctx, query, time.Now())
		if err != nil {
			return errors.Wrap(err, "error querying "+query)
		}

		vec := result.(model.Vector)
		if len(vec) == 0 {
			additionalMetrics[metric] += 0
			log.Debugln(metric, 0)

			continue
		}

		additionalMetrics[metric] += int(vec[0].Value)
		additionalMetricsCardinality += int(vec[0].Value)

		log.Debugln("additional", metric, vec[0].Value)
	}

	log.Infof("%d active series are NOT being used in dashboards", additionalMetricsCardinality)

	output := analyse.MetricsInPrometheus{}
	output.TotalActiveSeries = inUseCardinality + additionalMetricsCardinality
	output.InUseActiveSeries = inUseCardinality
	output.AdditionalActiveSeries = additionalMetricsCardinality

	for metric, count := range inUseMetrics {
		output.InUseMetricCounts = append(output.InUseMetricCounts, analyse.MetricCount{Metric: metric, Count: count})
	}
	sort.Slice(output.InUseMetricCounts, func(i, j int) bool {
		return output.InUseMetricCounts[i].Count > output.InUseMetricCounts[j].Count
	})

	for metric, count := range additionalMetrics {
		output.AdditionalMetricCounts = append(output.AdditionalMetricCounts, analyse.MetricCount{Metric: metric, Count: count})
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
